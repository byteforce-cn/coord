;; M5 —— agent 本地面（coord.agent.*）wire 自检。
;;
;; 用法（控制机，coord-test 项目根目录）：
;;   LEIN_ROOT=true lein -o run -m clojure.main scripts/check-agent-wire.clj
;;
;; 三件事，任何一件不成立就退出码 1（可进 CI/门禁）：
;;   1. **服务名/方法名**与 `coord-core/src/grpc_auth.rs` 的 `rpc_capability`
;;      表逐字一致。名字漂移（例如 `Lock` 写成 `lock`）会让请求打到不存在的
;;      方法上 —— 那是 UNIMPLEMENTED（还算好），但如果漂移成了**另一个真实
;;      方法**，鉴权表也会跟着错位，`--via-agent` 的路由证明就失去意义。
;;   2. **字段名 / 字段号 / wire type / 重复性**与
;;      `coord-proto/src/proto/<domain>.proto`（contracts/v1.2.0 起每个 domain 一个
;;      内部副本；内部面仍在 `agent_api.proto`）**全量**一致 —— 解析原文件比对，
;;      而不是比对另一份手写副本。**全量**指 `AGENT_FILES` 里的每个 message、
;;      每个字段，不是手工挑出来的子集。
;;      ⇒ 这条在本轮之前只覆盖手工枚举的 22 个 message，MQ 不在其中，于是
;;      `MqAckRequest` 把 `partition`/`consumer_group` 的号写反（2/3）也没人发现：
;;      客户端把 int32 发在服务端的 string 字段上，**wire type 不匹配** ⇒
;;      protobuf 解码失败 ⇒ `Ack` 100% 失败（jepsen F-67 `:poll-ack-failures 59/59`）。
;;      现场看起来像「被测系统不提交偏移」，实为**测试侧编码缺陷**。
;;      因此判据必须覆盖 wire type，且必须**全量**（挑了子集就等于没挑到的地方全裸奔）。
;;   3. 每个构造器真能构建出 DynamicMessage 并序列化（descriptor 里的
;;      type/oneof/重复字段写错时，往往第一次 `.build` 才炸）。
;;
;; 为什么需要它：CoordRpc.java 的 descriptor 是**手写**的（不用 protoc 生成），
;; 手写就会漂移。这与 Rust 侧 `coord-agent/src/plugin/abi.rs` 的账本是同一个
;; 用意（那边的三条 ABI 路径也是手写对齐的）。

(require '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[jepsen.coord.proto :as p])
(import '[jepsen.coord CoordRpc])

(def failures (atom []))

(defn- check! [ok? msg]
  (when-not ok?
    (swap! failures conj msg)
    (println "FAIL" msg)))

;; ── 1. 方法名（与鉴权表一致；表在 grpc_auth.rs，这里硬编码期望值）──────────
(def expected-methods
  {"/coord.lock.v1.Lock/Acquire"             CoordRpc/LOCK_ACQUIRE
   "/coord.lock.v1.Lock/Release"             CoordRpc/LOCK_RELEASE
   "/coord.lock.v1.Lock/Renew"               CoordRpc/LOCK_RENEW
   "/coord.lock.v1.Lock/GetLockInfo"         CoordRpc/LOCK_GET_INFO
   "/coord.idgen.v1.IdGen/NextId"             CoordRpc/IDGEN_NEXT_ID
   "/coord.idgen.v1.IdGen/NextBatch"          CoordRpc/IDGEN_NEXT_BATCH
   "/coord.election.v1.LeaderElection/Campaign"  CoordRpc/ELECTION_CAMPAIGN
   "/coord.election.v1.LeaderElection/Resign"    CoordRpc/ELECTION_RESIGN
   "/coord.election.v1.LeaderElection/GetLeader" CoordRpc/ELECTION_GET_LEADER
   "/coord.registry.v1.Registry/Register"        CoordRpc/REGISTRY_REGISTER
   "/coord.registry.v1.Registry/Deregister"      CoordRpc/REGISTRY_DEREGISTER
   "/coord.registry.v1.Registry/Heartbeat"       CoordRpc/REGISTRY_HEARTBEAT
   "/coord.registry.v1.Registry/Discover"        CoordRpc/REGISTRY_DISCOVER})

(doseq [[want ^io.grpc.MethodDescriptor m] expected-methods]
  ;; gRPC 的 `getFullMethodName` 是 "package.Service/Method"（**没有**前导斜杠；
  ;; HTTP/2 的 :path 才是 "/" + 它）。鉴权表用的是带斜杠的 :path，两边都规范化。
  (let [want-n (str/replace want #"^/" "")
        got    (.getFullMethodName m)]
    (check! (= want-n got)
            (str "method name: want " want-n ", got " got))))

;; ── 2. 字段号 / 字段名：解析 agent_api.proto 原文比对 ──────────────────────
(defn- wire-class
  "proto 标量/复合类型 → protobuf 的**线格式类**（varint / length / fixed32 / fixed64）。

  这是本卡口的核心判据：字段号写反、类型写错（int32 写成 string 之类），
  单独看「字段名→号」可能仍然自洽，但**线上字节**解不出来。
  `map<k,v>` 与 message / enum 这两类的判别需要类型解析，本 parser 不追求：
  map 与 message 是 length，enum 是 varint（枚举名从 `enum X {` 收集）。"
  [type-name enums]
  (cond
    (#{"string" "bytes"} type-name)                                  :length
    (str/starts-with? type-name "map<")                                :length
    (contains? enums (last (str/split type-name #"\.")))              :varint
    (#{"int32" "int64" "uint32" "uint64" "sint32" "sint64" "bool"} type-name) :varint
    (#{"fixed32" "sfixed32" "float"} type-name)                       :fixed32
    (#{"fixed64" "sfixed64" "double"} type-name)                      :fixed64
    :else                                                             :length))

(defn- find-blocks
  "按**花括号配平**切出 `keyword name { … }` 块，返回 [[name body] …]。

  不用正则整块匹配（`\\{([^}]*)\\}`）：message 体里一旦出现嵌套 message / enum /
  oneof，正则只截到第一个 `}`，会**悄悄给出半个字段集**（假绿）。
  配平扫描让这种情况变成可见的「body 里还有 `{`」。"
  [text kw]
  (let [re (re-pattern (str kw "\\s+([A-Za-z0-9_]+)\\s*\\{"))]
    (loop [m (re-matcher re text) acc []]
      (if (.find m)
        (let [name (.group m 1)
              body-start (.end m)
              body-end (loop [i body-start depth 1]
                         (cond
                           (>= i (count text)) (count text)
                           (= \{ (.charAt text i)) (recur (inc i) (inc depth))
                           (= \} (.charAt text i)) (if (= depth 1)
                                                     i
                                                     (recur (inc i) (dec depth)))
                           :else (recur (inc i) depth)))]
          (recur m (conj acc [name (subs text body-start body-end)])))
        acc))))

(defn- parse-proto
  "解析一个 proto 原文：返回
    {:messages {name {field-name {:number n :wire :varint|:length|:fixed32|:fixed64
                                  :repeated bool}}}
     :services #{name}
     :enums #{name}}

  刻意不写通用 parser：本套件用到的 message 都是**平的**（无嵌套 message/enum）。
  若某个目标 message 里出现了嵌套块，本函数登记为 **:nested**，由调用方报 FAIL ——
  不许「悄悄给出半个字段集」那种假绿。"
  [path]
  (let [text (-> (slurp path)
                 (str/replace #"(?m)//.*$" ""))   ;; 去行注释
        service-blocks (find-blocks text "service")
        services (->> service-blocks (mapv first) set)
        service-methods (into {}
                              (map (fn [[sname body]]
                                     [sname (->> (re-seq #"rpc\s+([A-Za-z0-9_]+)\s*\(" body)
                                                 (mapv second)
                                                 set)]))
                              service-blocks)
        enums (->> (find-blocks text "enum") (mapv first) set)
        field-of (fn [body]
                   (into {}
                         (map (fn [[_ rep type-name fname num]]
                                [fname {:number (Long/parseLong num)
                                        :wire (wire-class type-name enums)
                                        :repeated (some? rep)}]))
                         (re-seq #"(?m)^\s*(repeated\s+)?(map<[^>]*>|[A-Za-z0-9_.]+)\s+([A-Za-z0-9_]+)\s*=\s*([0-9]+)\s*;"
                                 body)))
        messages (into {}
                       (map (fn [[mname body]]
                              [mname (if (str/includes? body "{")
                                       :nested
                                       (field-of body))]))
                       (find-blocks text "message"))]
    {:messages messages
     :services services
     :service-methods service-methods
     :enums enums}))

(def proto-dir
  "coord-proto/src/proto —— lab 里 /root/coord-test 的上层目录
  （jepsen/ 是仓库的一部分，proto 在 ../coord-proto/）。"
  (let [candidates ["../coord-proto/src/proto"
                    "/opt/coord/coord-proto/src/proto"
                    "coord-proto/src/proto"]]
    (or (first (filter #(.isDirectory (io/file %)) candidates))
        (throw (ex-info "coord-proto/src/proto not found (run from the jepsen/ dir of a coord checkout)"
                        {:tried candidates})))))

(def proto-files
  "全部内部副本：contracts/v1.2.0 迁移后每个 domain 一个文件
  （package coord.<domain>.v1），内部面仍在 agent_api.proto。
  自检需**合并解析**它们，否则迁移后目标 message 会全部解析不到（假红）。"
  (->> (file-seq (io/file proto-dir))
       (filter #(.isFile %))
       (filter #(str/ends-with? (.getName %) ".proto"))
       (map #(.getPath %))
       sort))

(def parsed-protos (mapv (fn [p] [p (parse-proto p)]) proto-files))

(let [messages (into {} (mapcat (comp :messages second) parsed-protos))
      services (into #{} (mapcat (comp :services second) parsed-protos))
      service-methods (apply merge-with into {} (map (comp :service-methods second) parsed-protos))]
  (println "parsed" (count proto-files) "proto files under" proto-dir "->"
           (count messages) "messages," (count services) "services")
  (doseq [s ["Lock" "IdGen" "LeaderElection" "Registry" "Cache" "MQ"]]
    (check! (contains? services s) (str "coord-proto has service " s)))

  ;; ── 2a. message 字段：**全量**（AGENT_FILES 里每个 message 的每个字段）────
  ;;
  ;; 这里刻意**不用**手工枚举的期望表：枚举就等于「没枚举到的地方全裸奔」，
  ;; 而 F-67 正是发生在没枚举到的 MQ 上（`MqAckRequest` 的 2/3 号写反）。
  ;; descriptor 是手写的 ⇒ 判据必须从**手写的那份**出发，逐字段回查原 proto。
  ;;
  ;; 字段**号**与 **wire type** 都要比：号写反若恰好两个字段同型（int32↔int64）
  ;; 线上仍能解出来、只是语义错位（更危险）；类型写错则是硬解码失败（F-67）。
  (let [wire-of (fn [^com.google.protobuf.Descriptors$FieldDescriptor f]
                  ;; 注意：`Descriptors$FieldDescriptor$Type` 的枚举名是**裸的**
                  ;; （`STRING` / `INT32` / …），不是 `FieldDescriptorProto.Type` 的
                  ;; `TYPE_STRING` / `TYPE_INT32`（后者是另一套枚举）。
                  ;; 用错那一套会让每个字段都落到 unknown —— 全表假红。
                  (let [t (.name (.getType f))]
                    (case t
                      ("STRING" "BYTES" "MESSAGE" "GROUP")                       :length
                      ("INT32" "INT64" "UINT32" "UINT64"
                       "SINT32" "SINT64" "BOOL" "ENUM")                         :varint
                      ("FIXED32" "SFIXED32" "FLOAT")                            :fixed32
                      ("FIXED64" "SFIXED64" "DOUBLE")                           :fixed64
                      (do (check! false (str "unknown descriptor field type " t))
                          :unknown))))
        desc-of (fn [^com.google.protobuf.Descriptors$Descriptor d]
                  (into {} (map (fn [^com.google.protobuf.Descriptors$FieldDescriptor f]
                                  [(.getName f)
                                   {:number (.getNumber f)
                                    :wire (wire-of f)
                                    :repeated (.isRepeated f)}]))
                        (.getFields d)))
        descs (mapcat #(vec (.getMessageTypes ^com.google.protobuf.Descriptors$FileDescriptor %))
                      CoordRpc/AGENT_FILES)]
    (check! (pos? (count descs)) "AGENT_FILES 应至少声明一个 message")
    (doseq [^com.google.protobuf.Descriptors$Descriptor d descs]
      (let [mname (.getName d)
            want (get messages mname)]
        (check! (some? want) (str "coord-proto 里没有 message " mname
                                  "（手写 descriptor 声明了幽灵类型）"))
        (check! (not= :nested want)
                (str "proto parser 在 " mname " 上遇到嵌套块 —— 该 message 未被真正校验"))
        (when (map? want)
          (let [got (desc-of d)]
            (doseq [[fname g] (sort-by key got)]
              (let [w (get want fname)]
                (check! (some? w)
                        (str mname "." fname " 在 coord-proto 里不存在（幽灵字段）"))
                (when w
                  (check! (= (:number w) (:number g))
                          (str mname "." fname " 字段号不符：proto #" (:number w)
                               " / descriptor #" (:number g)))
                  (check! (= (:wire w) (:wire g))
                          (str mname "." fname " **wire type** 不符：proto " (:wire w)
                               " / descriptor " (:wire g)
                               "（类型写错 = 线上解码直接失败，F-67 就是这个形态）"))
                  (check! (= (boolean (:repeated w)) (boolean (:repeated g)))
                          (str mname "." fname " repeated 不符：proto " (:repeated w)
                               " / descriptor " (:repeated g))))))
            ;; 反向：原 proto 有的字段，手写 descriptor 也不该缺（缺了就是覆盖不全，
            ;; 调用点一旦用到它会拿到「字段不存在」而不是编译错）
            (doseq [fname (sort (keys want))]
              (check! (contains? got fname)
                      (str mname "." fname " 在 descriptor 里缺失（手写 descriptor 覆盖不全）"))))))))

  ;; ── 2b. 服务名 / 方法名：全量比对 ────────────────────────────────────────
  (doseq [^com.google.protobuf.Descriptors$FileDescriptor f CoordRpc/AGENT_FILES
          ^com.google.protobuf.Descriptors$ServiceDescriptor s (.getServices f)]
    (let [sname (.getName s)]
      (check! (contains? services sname)
              (str "coord-proto 里没有 service " sname))
      (doseq [^com.google.protobuf.Descriptors$MethodDescriptor m (.getMethods s)]
        (check! (contains? (get service-methods sname #{}) (.getName m))
                (str sname " 的 rpc " (.getName m)
                     " 不在 coord-proto（proto 侧有："
                     (pr-str (sort (get service-methods sname #{}))) "）"))))))

;; ── 3. 构造器可构建（type/oneof 写错时在这里炸）──────────────────────────
(let [samples [(p/lock-acquire-req {:name "l" :holder-id "h" :ttl-seconds 5})
               (p/lock-release-req {:name "l" :holder-id "h" :lease-id 1})
               (p/lock-renew-req {:name "l" :holder-id "h" :lease-id 1})
               (p/lock-get-info-req "l")
               (p/idgen-next-id-req {:name "n" :step 1000})
               (p/idgen-next-batch-req {:name "n" :count 7 :step 1000})
               (p/election-campaign-req {:group-name "g" :candidate-id "c" :ttl-seconds 5})
               (p/election-resign-req {:group-name "g" :candidate-id "c" :lease-id 1})
               (p/election-get-leader-req "g")
               (p/registry-register-req {:service-name "s" :instance-id "i"
                                         :metadata "m" :ttl-seconds 5})
               (p/registry-deregister-req {:service-name "s" :instance-id "i" :lease-id 1})
               (p/registry-heartbeat-req {:service-name "s" :instance-id "i" :lease-id 1})
               (p/registry-discover-req {:service-name "s" :filter-mode :exact})]]
  (doseq [m samples]
    (check! (pos? (alength (.toByteArray ^com.google.protobuf.DynamicMessage m)))
            (str "build+serialize "
                 (.getFullName (.getDescriptorForType ^com.google.protobuf.DynamicMessage m)))))
  (println "built" (count samples) "sample requests"))

;; ── 结论 ──────────────────────────────────────────────────────────────────
(if (seq @failures)
  (do (println "\n" (count @failures) "FAILURE(S)")
      (System/exit 1))
  (do (println "\nagent wire self-check: OK"
               (str "(" (count expected-methods) " methods, "
                    (count @failures) " failures)"))
      (System/exit 0)))
