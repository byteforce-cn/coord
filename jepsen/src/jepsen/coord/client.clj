(ns jepsen.coord.client
  "gRPC client for coord.

  Implements the register / cas-register operations with leader discovery:
  on UNAVAILABLE (not leader / election in progress) it rotates to the next
  node; on DEADLINE_EXCEEDED it records :info (the write may or may not have
  been applied); on UNAUTHENTICATED it refreshes the session (coord CCTs
  expire after ~1h) and retries, recording :fail only if the fresh token is
  rejected too."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]]
            [jepsen.coord.proto :as p])
  (:import [io.grpc ManagedChannel Status Status$Code StatusRuntimeException]
           [jepsen.coord CoordRpc]))

(def ^:private ^:const register-key "/jepsen/register")
(def ^:private ^:const rpc-timeout-ms 5000)
(def ^:private ^:const auth-timeout-ms 60000)

;; Values the clients have recently observed; the cas-register generator draws
;; `old` from this pool so CASes frequently target the current register value.
(def seen
  "Atom of recently-observed register values (bounded window)."
  (atom #{nil}))

(def last-seen
  "Atom of the **most recent** observed register value (T1.5 workload quality).

  为什么单独记一个「最近一次观察值」：`seen` 是一个 200 元集合，`rand-nth`
  在它上面均匀抽样，命中的概率约为 1/200 —— 实测 cas-register 45s 只拿到
  13 个 `:ok` / 108 个 cas（全 `cas-miss`），有效线性化样本少了一个量级。
  `last-seen` 是「刚读到的值」：CAS 它 = 教科书里的 read-then-CAS，命中率由
  并发冲突决定而不是由窗口大小决定。

  nil 是合法取值（未写过 / 值为 nil），所以初值是 nil 而不是特殊哨兵。"
  (atom nil))

(defn- reset-seen!
  []
  (reset! seen #{nil})
  (reset! last-seen nil))

(defn- observe!
  "Records a value as recently observed (bounded to the last 200)."
  [v]
  (when (some? v)
    (reset! last-seen v)
    (swap! seen (fn [s]
                  (-> (conj s v)
                      (->> (take-last 200))
                      (set))))))

(defn- status-code
  "gRPC status code of a StatusRuntimeException. (StatusRuntimeException has
  no getCode() in grpc 1.68 -- go through getStatus.)"
  ^Status$Code [^StatusRuntimeException e]
  (.. e getStatus getCode))

(defn- timeout?
  [^StatusRuntimeException e]
  (= Status$Code/DEADLINE_EXCEEDED (status-code e)))

(defn- unauthenticated?
  [^StatusRuntimeException e]
  (= Status$Code/UNAUTHENTICATED (status-code e)))

(defn- unavailable?
  [^StatusRuntimeException e]
  (= Status$Code/UNAVAILABLE (status-code e)))

(defn- resource-exhausted?
  "Login rate limiter (per-IP token bucket) rejection -- retryable with
  backoff, NOT a final failure."
  [^StatusRuntimeException e]
  (= Status$Code/RESOURCE_EXHAUSTED (status-code e)))

(defn- request-id
  []
  (str (System/currentTimeMillis) "-" (rand-int Integer/MAX_VALUE)))

(defn- parse-value
  "Parses a value string read from the register into an integer (nil if empty).

  F-14（测试自身，2026-09-16）：这里原来直接 `Long/parseLong`。而 T1.1 的 map
  workload 写的是**任意字节串**（`--value-size` 填充的 `vs42-12-xxx`）——
  一旦读到一个非数值就抛 NumberFormatException，被 jepsen 记成 `:info`：
  整类 `:read` op 变成「从未成功」（G6 门槛 0 秒抓到）。现在解析失败只返回
  nil，调用方按 workload 决定用不用解析值（见 `CoordClient` 的
  `parse-values?` / `:raw-value`）。"
  [^String s]
  (when (and s (not (.isEmpty s)))
    (try
      (Long/parseLong s)
      (catch NumberFormatException _ nil))))

(defn- authenticate!
  "Authenticates as root against any node, rotating through all channels until
  one succeeds. Throws after auth-timeout-ms. RESOURCE_EXHAUSTED (login rate
  limiter) is retried with a backoff instead of being treated as final:
  the per-IP token bucket refills at ~0.5 tokens/s, so a 2s sleep between
  attempts lets legitimate login bursts through."
  [channels password]
  (let [req      (p/auth-req "root" password)
        n        (count channels)
        deadline (+ (System/currentTimeMillis) auth-timeout-ms)]
    (loop [attempt 0]
      (if (< (System/currentTimeMillis) deadline)
        (let [ch  (nth channels (mod attempt n))
              res (try
                    {:ok (p/cct (p/call ch p/authenticate req))}
                    (catch StatusRuntimeException e
                      {:err e}))]
          (if-let [cct (:ok res)]
            cct
            (if (or (unavailable? (:err res))
                    (timeout? (:err res))
                    (resource-exhausted? (:err res)))
              (do
                (when (resource-exhausted? (:err res))
                  ;; 登录限流：退避 2s 等令牌回补，避免把服务端防爆破
                  ;; 限流当最终失败抛给调用方。
                  (Thread/sleep 2000))
                (recur (inc attempt)))
              (throw (:err res)))))
        (throw (ex-info "Failed to authenticate to coord" {:channels channels}))))))

(defn- reauthenticate!
  "Fetches a fresh CCT (coord tokens expire after ~1h) and swaps in new auth
  channels for every node. Returns the new CCT.

  用 `:auth-raw-channels`（**server** 端点）而不是 ops 端点：agent 不代理 Auth
  （它只代理 KV/Txn/Lease/Watch/Maintenance/Storage 六个数据面服务），所以
  CCT 只能从集群直接取 —— 这也解释了为什么生产上应用的登录路径不经过 agent。"
  [this]
  (let [raw-channels (or @(:auth-raw-channels this) @(:raw-channels this))
        cct          (authenticate! raw-channels (:root-password this))
        channels     (mapv #(p/auth-channel % cct) @(:raw-channels this))]
    (reset! (:cct this) cct)
    (reset! (:channels this) channels)
    ;; 地面真值探针的端点（**server**）也要跟着换新 CCT：探针用的是同一份鉴权，
    ;; 忘了这一句的表现是「刷新之后探针全部 UNAUTHENTICATED」——而那会被记成
    ;; :info（探针读失败），看起来像「没有矛盾」，是假绿。
    (when-let [praw @(:probe-raw-channels this)]
      (reset! (:probe-channels this) (mapv #(p/auth-channel % cct) praw)))
    cct))

(defn- not-leader?
  "True if a StatusRuntimeException is a definitive 'not leader' rejection
  (the server rejected the write before applying it)."
  [^StatusRuntimeException e]
  (boolean (re-find #"(?i)not[ _-]?leader" (str (.getMessage e)))))

(defn- forward-request?
  "True if a StatusRuntimeException is the linearizable-read path's 'not
  leader, forward to current leader' rejection. The server surfaces this as
  INTERNAL ('linearizable read failed: has to forward request to ...') rather
  than UNAVAILABLE, so it must be detected by message."
  [^StatusRuntimeException e]
  (boolean (re-find #"(?i)has to forward request|forward request to"
                    (str (.getMessage e)))))

(defn- op-key
  "The register key an op targets: an explicit :key (multi-register ops) or
  the single legacy register key."
  [op]
  (or (:key op) register-key))

(defn- leader-start
  "Cached leader node index for `key` (0 when unknown)."
  [this key]
  (get @(:leader-idx this) key 0))

(defn- leader-seen!
  "Records that node index `idx` served `key` successfully (per-region/per-key
  leader cache: in multi-region mode each register key may be led by a
  different node)."
  [this key idx]
  (swap! (:leader-idx this) assoc key idx))

(defn- try-nodes
  "Attempts (f channel) against nodes starting from the cached leader index
  for `key`. Rotates through UNAVAILABLE responses (and the read path's
  INTERNAL 'forward request' rejection). Returns {:ok response :node node} on
  success (and advances the per-key leader index), or {:err StatusRuntimeException
  :all-not-leader? bool} when every node failed.

  `offset` 把起点从缓存 leader 往后挪 N 个节点（T1.4 的**确定性换节点重放**
  分支：F-03 说明幂等缓存是单节点进程内的，所以把重放打到另一个节点上必然
  命中空缓存）。offset=0 时行为与修复前完全一致。"
  ([this key f] (try-nodes this key f 0))
  ([this key f offset]
   (let [n     (count @(:channels this))
         start (mod (+ (leader-start this key) (long offset)) n)]
     (loop [i 0, last-err nil, all-not-leader? true]
       (if (< i n)
         (let [idx (mod (+ start i) n)
               ch  (nth @(:channels this) idx)
               res (try
                     (let [resp (f ch)]
                       (leader-seen! this key idx)
                       {:ok resp, :node (nth (:nodes this) idx)})
                     (catch StatusRuntimeException e
                       {:err e}))]
           (if-let [resp (:ok res)]
             res
             ;; 轮换的触发条件：**不该把这个节点判成最终结论**的那些错误。
             ;;
             ;; UNAVAILABLE / "forward to leader" 自不必说；**UNAUTHENTICATED 也要
             ;; 轮换** —— 多 agent 拓扑下一个 agent 可能正处于「刚重启、RoleCache
             ;; 还是空的」状态：它对所有 CCT 都 fail-closed 拒绝，而另一个 agent
             ;; 完好。不轮换的话客户端会**钉在**这个坏 agent 上，把整个 run 打红。
             ;; 实测（M5a 第二轮，idgen/kill-agent）：166 个 op 里 143 个
             ;; `:unauthenticated`，而另一个 agent 一直是好的。
             ;;
             ;; 不轮换的是**业务性**拒绝（PERMISSION_DENIED / NOT_FOUND 之类）：
             ;; 那些换节点也不会变，而且"换了就好了"反而会掩盖真实的鉴权结论。
             (if (or (unavailable? (:err res)) (forward-request? (:err res))
                     (unauthenticated? (:err res)))
               (recur (inc i) (:err res)
                      (and all-not-leader?
                           (or (not-leader? (:err res))
                               (forward-request? (:err res)))))
               res)))
         {:err last-err, :all-not-leader? all-not-leader?})))))

(defn- try-probe
  "在 **server** 端点上做一次 RPC，返回 `{:ok resp :node n}` 或 `{:err e}`。

  与 `try-nodes` 的区别（探针专用，刻意不共用代码路径）：

    * 走 `:probe-channels`（server），**不**走 `:channels`（可能是 agent 隧道）
      —— 探针的全部意义就是绕开 agent；
    * 不写 per-key 的 leader 缓存（探针的失败不应该影响 ops 路径的选路）；
    * 不做 `forward-request?` 之外的语义判断，读失败如实交给调用方记 :info。"
  [this f]
  (let [chs (vec @(:probe-channels this))
        nds (vec (or (seq (:probe-nodes this))
                     (seq (:auth-nodes this))
                     (:nodes this)))]
    (loop [i 0 last-err nil]
      (if (< i (count chs))
        ;; 注意：`recur` 不能跨 `try` 边界（"Can only recur from tail position"），
        ;; 所以先在一个 let 里执行调用并分类，再在 try **之外**决定是否轮换 ——
        ;; 与 try-nodes 同一写法。
        (let [res (try
                    {:ok (f (nth chs i))
                     :node (nth nds (min i (dec (count nds))))}
                    (catch StatusRuntimeException e
                      {:err e}))]
          (if-let [e (:err res)]
            (if (or (unavailable? e) (forward-request? e))
              (recur (inc i) e)
              res)
            res))
        {:err last-err}))))

(defn- result-op
  "Maps an invocation op + either {:ok resp :node n} or {:err e} to the
  completion op, using the doc's :ok/:fail/:info mapping rules.

  For writes/CAS the critical rule is: a failure that *may* have taken effect
  (timeout, connection-level UNAVAILABLE) is recorded as :info, never :fail --
  otherwise a committed-but-misreported write makes later reads look like
  anomalies."
  [op res ok-fn]
  (if-let [resp (:ok res)]
    (ok-fn resp)
    (let [e (:err res)]
      (cond
        (timeout? e)         (assoc op :type :info :error :timeout)
        (unauthenticated? e) (assoc op :type :fail :error :unauthenticated)
        (:all-not-leader? res)
                             (assoc op :type :fail :error :not-leader)
        :else                (assoc op :type :info
                                         :error :unavailable-maybe-applied)))))

(defn- invoke-read [this op]
  (let [key (op-key op)
        res (try-nodes this key #(p/call % p/kv-range (p/range-req key)))]
    (result-op op res
               (fn [resp]
                 (let [kvs   (p/range-kvs resp)
                       raw   (when (seq kvs) (p/kv-value (first kvs)))
                       ;; F-14：register/cas/soak 用数值（knossos 模型是整数），
                       ;; map workload 要求原样字符串（值就是「哪个写」的身份）。
                       value (if (:parse-values? this) (parse-value raw) raw)]
                   (observe! value)
                   (assoc op :type :ok :value value :raw-value raw
                              :node (:node res)))))))

(defn- invoke-write [this op]
  (let [key (op-key op)
        v   (:value op)
        rid (request-id)
        req (p/put-req key (str v) rid)]
    (result-op op
               (try-nodes this key #(p/call % p/put req))
               (fn [resp]
                 (observe! v)
                 ;; T1.3：记下本 key 本次写拿到的 revision（read-at 的依据）
                 (when (pos? (long (p/revision resp)))
                   (swap! (:last-rev this) assoc key (p/revision resp)))
                 (assoc op :type :ok :value v :revision (p/revision resp))))))

(defn- invoke-cas [this op]
  (let [[old new] (:value op)
        old-str   (if (nil? old) "" (str old))
        rid       (request-id)
        req       (p/txn-req
                   [(p/compare-value-equal register-key old-str)]
                   [(p/request-put-op (p/put-req register-key (str new) rid))]
                   []
                   rid)]
    (result-op op
               (try-nodes this register-key #(p/call % p/txn req))
               (fn [resp]
                 (if (p/txn-succeeded resp)
                   (do (observe! new)
                       (assoc op :type :ok :value [old new]))
                   ;; CAS miss: the register value was not `old`. Legitimate
                   ;; failure, not a bug -- knossos drops :fail ops.
                   (assoc op :type :fail :value [old new] :error :cas-miss))))))

;; --------------------------------------------------------------------------
;; T1.4 request_id 幂等专项（F-01/F-02/F-03 的证伪路径）
;;
;; 设计约束：一次 `invoke!` 只产出**一个** completion op，每次重放的响应都
;; 汇总在 op 的 `:attempts` 里。理由是这四条契约断言（见
;; `jepsen.coord.idem`）只依赖「响应内容 + 收尾读」：
;;   * 不重复生效   → k 次响应的 revision / deleted 是否一致 + version 是否 >1
;;   * 返回首次结果 → k 次响应的 prev_kv / prev_kvs 是否逐字段一致
;;   * 不放大破坏面 → 范围删重放后，区间内新写入的 key 是否存活
;;   * 删除不复活   → 全部 delete :ok 后该 key 是否仍然不存在
;; 把单次 RPC 摊进历史只会让这三类断言变复杂（同一 rid 的多个 op 需要跨 op
;; 关联），不会让它们更强。
;; --------------------------------------------------------------------------

(defn- kv-edn
  "KeyValue → 纯 EDN。历史里绝不能出现 byte[]：既不可读，也会让
  `history.edn` 体积爆炸（每次重放都带一个 key/value）。
  字段名用 KeyValue 的契约字段名（create_revision / mod_revision）。"
  [kv]
  (when kv
    {:key             (p/kv-key kv)
     :value           (p/kv-value kv)
     :version         (p/kv-version kv)
     :create-revision (p/kv-create-revision kv)
     :mod-revision    (p/kv-mod-revision kv)}))

(defn- attempt!
  "一次经 try-nodes 的 RPC 尝试，返回分类结果：

    {:ok? true :node \"n2\" ...(ok-fn resp) ...}
    {:ok? false :kind :info|:fail :error :timeout|:unauthenticated|:not-leader|
                                          :unavailable-maybe-applied}

  `:info` 与 `:fail` 的区别是本 workload 的判定基础：首次尝试是 `:info`
  （响应丢失，可能已生效）时，「重放再次生效」是合法历史；只有首次尝试
  `:ok` 才能断言重放不得生效。"
  ([this key f ok-fn] (attempt! this key f ok-fn 0))
  ([this key f ok-fn offset]
   (let [res (try-nodes this key f offset)]
     (if-let [resp (:ok res)]
       (assoc (ok-fn resp) :ok? true :node (:node res))
       (let [^StatusRuntimeException e (:err res)]
         {:ok? false
          :kind  (cond
                   (timeout? e)           :info
                   (unauthenticated? e)   :fail
                   (:all-not-leader? res) :fail
                   :else                  :info)
          :error (cond
                   (timeout? e)           :timeout
                   (unauthenticated? e)   :unauthenticated
                   (:all-not-leader? res) :not-leader
                   :else                  :unavailable-maybe-applied)})))))

(defn- read-kv
  "点读一个 key，返回 `{:ok? bool :node n :kv <KeyValue EDN>}`。

  `:kv` 为 nil 表示 key 不存在（Range 返回空 kvs）。把 KeyValue 放在 `:kv`
  而不是 `:value`：否则 wrapper 的 `:value` 与 KeyValue 自己的 `:value` 会
  撞名（checker 会把字符串当成 map 读）。"
  [this key]
  (attempt! this key
            #(p/call % p/kv-range (p/range-req key))
            (fn [resp] {:kv (kv-edn (first (p/range-kvs resp)))})))

(defn- replay-attempts
  "执行 `n` 次 `(f i)`（i = 第几次，从 0 开始），第 2..n 次之间先 sleep
  `delay-ms`（0 = 不等待）。

  delay 的用途：把「重放」推到一次 kill/重启/换主之后，让重放落到**没有
  该 request_id 缓存**的节点上（F-03：去重缓存不随 raft 复制、不持久化）。
  delay=0 时全部重放在同一 leader 上完成，是对照组（契约成立）。
  把 i 交给 f 是为了 `--idem-replay-node-offset`：重放那几次从**另一个节点**
  开始尝试（确定性复现 F-03，不依赖 nemesis 恰好落在重放窗口里）。"
  [n delay-ms f]
  (loop [i 0, acc []]
    (if (< (long i) (long n))
      (do (when (and (pos? i) delay-ms (pos? (long delay-ms)))
            (Thread/sleep (long delay-ms)))
          (recur (inc i) (conj acc (f i))))
      acc)))

(defn- idem-completion
  "把一次幂等分组的结果做成 completion op。

  类型映射：任一次尝试确定性失败（not-leader / auth）→ `:fail`；全部尝试
  与收尾读都 `:ok` → `:ok`；其余（超时 / 连接级不可用 / 收尾读失败）→
  `:info`（请求可能已生效，记成 `:fail` 会让后续读看起来像异常）。

  `:error` 只在**一个尝试都没成功**且原因是鉴权时才冒泡，让 `invoke-coord!`
  刷新 CCT 后重跑整个分组。已经生效过的分组绝不重跑——重跑会让
  `version`/`revision` 判定出现假红。"
  [op attempts final extra]
  (let [req   (:value op)
        ;; 派生的期望字段（:expected-version / :setup）必须同时进 :request 与
        ;; :value：checker 从 :request 读它们，而 fixture/人工排查看 :value。
        ;; 只写 :value 会让 checker 读到 nil 并回退到默认值 —— 那正是本文件
        ;; 第一次真跑时 20 个 setup 分组被误报 :version-over-advance 的原因。
        req'  (merge req extra)
        kinds (keep :kind attempts)
        errs  (keep :error attempts)
        ty    (cond
                (some #(= :fail %) kinds)                 :fail
                (and (seq attempts) (every? :ok? attempts)
                     (:ok? final))                        :ok
                :else                                     :info)]
    (assoc op
           :type     ty
           :request  req'
           :value    req'
           :attempts (vec attempts)
           :final    final
           :error    (when (and (empty? (filter :ok? attempts))
                                (some #{:unauthenticated} errs))
                       :unauthenticated))))

(defn- invoke-idem-put
  "T1.4：同一 request_id 连续 `Put{prev_kv:true}` `replays` 次，随后点读。

  `:setup-val` 非空时先无 rid 写一次该 key（把 key 变成「已存在」），再做重放——
  这样首次响应会带 `prev_kv`，纯 F-02 形态（幂等命中恒返回 `prev_kv=None`）
  才有复现路径。此时 `:expected-version` = 2（setup 写 1 次 + 本组写 1 次）。

  断言（checker `jepsen.coord.idem`）：k 次响应的 `revision` 与 `prev_kv`
  必须一致；全部 `:ok` 时该 key 的 `version` 必须不超过 `:expected-version`。"
  [this op]
  (let [{:keys [rid key val replays replay-delay-ms setup-val
                replay-node-offset]} (:value op)
        off (long (or replay-node-offset 0))
        setup (when setup-val
                (attempt! this key
                          #(p/call % p/put (p/put-req key (str setup-val) ""))
                          (fn [resp] {:revision (p/revision resp)})))
        put! (fn [i] (attempt! this key
                               #(p/call % p/put (p/put-req key (str val) rid true))
                               (fn [resp] {:revision (p/revision resp)
                                           :prev-kv  (kv-edn (p/put-prev-kv resp))})
                               ;; 首次尝试用缓存 leader；重放从另一个节点开始
                               (if (zero? (long i)) 0 off)))
        attempts (replay-attempts (or replays 2) replay-delay-ms put!)
        final    (read-kv this key)]
    (idem-completion op attempts final
                     {:setup setup
                      :expected-version (if setup-val 2 1)})))

(defn- invoke-idem-delete
  "T1.4：先写一个独占 key（无 rid），再用同一 request_id 连续删 `replays`
  次（`prev_kv:true`），最后点读确认 key 已不存在。

  F-01 的直接证伪路径：`delete` 处理器没有任何 `*_idempotent*` 调用，所以
  重放的 `deleted` 会是 0、`prev_kvs` 会是空——与首次执行的响应不同。"
  [this op]
  (let [{:keys [rid key val replays replay-delay-ms replay-node-offset]} (:value op)
        off (long (or replay-node-offset 0))
        setup (attempt! this key
                        #(p/call % p/put (p/put-req key (str val) ""))
                        (fn [resp] {:revision (p/revision resp)}))
        del!  (fn [i] (attempt! this key
                                #(p/call % p/delete
                                         (p/delete-req key rid {:prev-kv true}))
                                (fn [resp] {:deleted  (p/delete-deleted resp)
                                            :prev-kvs (mapv kv-edn (p/delete-prev-kvs resp))
                                            :revision (p/revision resp)})
                                (if (zero? (long i)) 0 off)))
        attempts (replay-attempts (or replays 2) replay-delay-ms del!)
        final    (read-kv this key)]
    (idem-completion op attempts final {:setup setup
                                        :expected-version 0})))

(defn- invoke-idem-range-delete
  "T1.4/F-01 的范围删形态（「重试放大破坏面」的精确复现）：

    1) 在 `[range-start, range-end)` 内写 3 个 key（无 rid）；
    2) 用 rid 范围删一次（首次执行）；
    3) 在区间内写一个**新** key（post-write，无 rid）；
    4) 用**同一个 rid** 重放范围删；
    5) 点读 post-write key。

  首次执行 `:ok` 时第 4 步必须是 no-op：post-write key 必须存活到第 5 步。
  没有幂等的实现会让第 4 步把它删掉——这正是 F-01 第 3 条预测的形态。"
  [this op]
  (let [{:keys [rid range-start range-end post-write-key post-write-val]} (:value op)
        put!  (fn [k v] (attempt! this k
                                  #(p/call % p/put (p/put-req k (str v) ""))
                                  (fn [resp] {:revision (p/revision resp)})))
        setup (mapv (fn [i] (put! (str range-start "-" i) i)) (range 3))
        del!  (fn [] (attempt! this range-start
                               #(p/call % p/delete
                                        (p/delete-req range-start rid
                                                      {:range-end range-end
                                                       :prev-kv   true}))
                               (fn [resp] {:deleted  (p/delete-deleted resp)
                                           :prev-kvs (mapv kv-edn (p/delete-prev-kvs resp))
                                           :revision (p/revision resp)})))
        a1    (del!)
        post  (put! post-write-key post-write-val)
        a2    (del!)
        final (read-kv this post-write-key)]
    (assoc op
           :type     (cond
                       (some #(= :fail (:kind %)) (remove :ok? [a1 a2])) :fail
                       (and (:ok? a1) (:ok? a2) (:ok? final))             :ok
                       :else                                              :info)
           :request  (:value op)
           :value    (:value op)
           :attempts [a1 a2]
           :setup    setup
           :post-write post
           :post-write-final final)))

;; --------------------------------------------------------------------------

(defn- completion-of
  "把一次 `attempt!` 的结果做成 completion op（`:ok` / `:info` / `:fail` 三档，
  与 `result-op` 同口径）。

  `attempt!` 在成功时把 ok-fn 的字段平铺在返回值上（`:ok?` / `:node` + 载荷），
  这里原样搬进 completion op —— T1.1/T1.2/T1.3 的 checker 就是靠这些载荷
  （`:value` / `:deleted` / `:prev-kvs` / `:kvs` / `:succeeded` / `:wrote` …）
  做判定的。"
  [op res]
  (if (:ok? res)
    (assoc (merge op (dissoc res :ok?)) :type :ok)
    (assoc op :type (or (:kind res) :info) :error (:error res))))

;; --------------------------------------------------------------------------
;; T1.1 map workload：delete / exists
;;
;; 模型：每个 key 是一个寄存器，值域 = {写值} ∪ {nil}；delete = 写 nil
;; （tombstone）。checker（jepsen.coord.mapck）据此判定「tombstone 之后读到旧值」
;; 这类缺陷，并用 Txn 的 Compare{VERSION, EQUAL, 0} 做精确的存在性读。
;; --------------------------------------------------------------------------

(defn- invoke-delete
  "`DeleteRequest`（单键）：带 `prev_kv=true`，所以响应里有被删旧值。

  completion op 的载荷：`:deleted`（实际删除数）、`:prev-kvs`（旧值 EDN）、
  `:prev-kv-requested?`。`:value` 保持 nil —— delete 在寄存器模型里就是
  「写 nil」，knossos 路径会把 op 改写成 `{:f :write :value nil}`（见 mapck）。"
  [this op]
  (let [key (op-key op)
        rid (request-id)
        res (attempt! this key
                      #(p/call % p/delete (p/delete-req key rid {:prev-kv true}))
                      (fn [resp]
                        {:value             nil
                         :deleted           (p/delete-deleted resp)
                         :prev-kvs          (mapv kv-edn (p/delete-prev-kvs resp))
                         :prev-kv-requested? true
                         :revision          (p/revision resp)}))]
    (completion-of op res)))

(defn- invoke-exists
  "精确存在性读：`Txn{compare=[Compare{VERSION, EQUAL, 0}]}`，两个分支都为空。

  `coord-server/src/storage/mvcc.rs:1232` 的 compare 对「不存在（含软删除）」
  取 version = 0，所以 `succeeded=true` ⇔ **该 key 不存在**。这是 §9-⑨
  （version 起始值与「不存在」的表示）的落地判据：它比「读不到就当不存在」
  精确，因为后者无法区分「不存在」与「读了但响应丢失」。

  completion op 载荷：`:exists?`（true = 存在）。"
  [this op]
  (let [key (op-key op)
        rid (request-id)
        req (p/txn-req [(p/compare {:op :equal, :target :version, :key key, :version 0})]
                       []
                       []
                       rid)
        res (attempt! this key
                      #(p/call % p/txn req)
                      (fn [resp] {:exists? (not (p/txn-succeeded resp))
                                  :revision (p/revision resp)}))]
    (completion-of op res)))

;; --------------------------------------------------------------------------
;; T1.2 txn 全形态
;;
;; 每个 completion op 都汇报 checker 需要的全部信息：
;;   :succeeded  —— TxnResponse.succeeded
;;   :wrote      —— **实际生效**的写集 {key -> 值}（值 nil 表示 delete），
;;                  仅当本次 txn 确定成功（:ok 且 succeeded）时才有意义
;;   :wrote-possible —— 任一分支**可能**生效的写集（响应丢失时用；只用于
;;                      「读到的值是否可能是它写的」这类合法性判定）
;;   :post-read  —— 写集回读（`:txn-write-set` 用区间读，其余用点读），
;;                  让 checker 能断言「成功必可见 / 失败必不可见」
;; --------------------------------------------------------------------------

(defn- read-kvs
  "区间读 `[start, end)`，返回 `{:ok? .. :kvs [kv-edn ...]}`。"
  [this start end]
  (attempt! this start
            #(p/call % p/kv-range (p/range-req* {:key start :range-end end}))
            (fn [resp] {:kvs (mapv kv-edn (p/range-kvs resp))})))

(defn- point-read
  "点读，返回 `{:ok? .. :kv kv-edn|nil}`。"
  [this key]
  (attempt! this key
            #(p/call % p/kv-range (p/range-req key))
            (fn [resp] {:kv (kv-edn (first (p/range-kvs resp))) })))

(defn- write-set-of
  "把 (key . value) 写集变成 `{key value}`（delete 用 nil 表示）。"
  [kvs]
  (into {} (map (fn [[k v]] [k v]) kvs)))

(defn- invoke-txn
  "通用 txn 执行：跑一次 `TxnRequest`，并按 `plan` 汇报写集与回读。

  `plan` = {:success-keys [[k v] ...]      ; 成功分支写入（v = nil 表示 delete）
            :failure-keys [[k v] ...]      ; 失败分支写入
            :post-read :point|:range       ; 用哪种回读来验证「成功必可见」
            :post-keys [k ...] :post-range [start end]}

  注意：响应 `DynamicMessage` **绝不进历史**（不可 EDN 序列化），所有载荷都在
  这里转成纯 EDN。"
  [this op txn-req* plan]
  (let [key (or (:key op) (ffirst (:success-keys plan)))
        ;; 可选的 setup 写（无 rid）：让 compare 有一个**已知**的旧值。
        setup (when-let [[sk sv] (:setup plan)]
                (attempt! this sk #(p/call % p/put (p/put-req sk (str sv) ""))
                          (fn [resp] {:revision (p/revision resp)})))
        res (attempt! this key #(p/call % p/txn (txn-req*)) (fn [resp] {:resp resp}))]
    (if (:ok? res)
      (let [resp   (:resp res)
            succ?  (p/txn-succeeded resp)
            branch (if succ? (:success-keys plan) (:failure-keys plan))
            post   (case (:post-read plan)
                     :range (read-kvs this (first (:post-range plan))
                                      (second (:post-range plan)))
                     (point-read this (first (:post-keys plan))))]
        (assoc op :type :ok
               :succeeded succ?
               :wrote (write-set-of branch)
               ;; 两分支**分别**汇报：失败 txn 的成功分支写值必须永远不被观察到
               ;; （`:failure-branch-leak`）。两边写同一个 key 时（cas-delete）
               ;; 并集成单个 map 会丢掉一边，所以分开带。
               :success-wrote (write-set-of (:success-keys plan))
               :failure-wrote (write-set-of (:failure-keys plan))
               :wrote-possible (write-set-of (concat (:success-keys plan)
                                                     (:failure-keys plan)))
               ;; setup 写也是一次真实写，必须进 checker 的写索引（不然读回它
               ;; 会被判 :fabricated 假红）。
               :setup (when (and setup (:ok? setup))
                        {:key (first (:setup plan)) :value (second (:setup plan))})
               :setup-ok? (if setup (:ok? setup) true)
               :response-count (count (p/txn-responses resp))
               :post-read (when (:ok? post)
                            (if (contains? post :kvs)
                              {:kvs (:kvs post)}
                              {:kv (:kv post)}))
               :post-read-ok? (:ok? post)
               :node (:node res)))
      (assoc op :type (or (:kind res) :info)
             :error (:error res)
             :setup-ok? (if setup (:ok? setup) true)
             :success-wrote (write-set-of (:success-keys plan))
             :failure-wrote (write-set-of (:failure-keys plan))
             :wrote-possible (write-set-of (concat (:success-keys plan)
                                                   (:failure-keys plan)))))))

(defn- put-op [k v] (p/request-put-op (p/put-req k (str v) "")))
(defn- del-op [k]   (p/request-delete-op (p/delete-req k "" {})))
(defn- rng-op [k e] (p/request-range-op (p/range-req* {:key k :range-end e})))

(defn- invoke-txn-create
  "`Compare{VERSION, EQUAL, 0}` + `Put`：**创建**语义（create-if-absent）。

  该 key 在本组里是全新的，所以 `succeeded=false` 本身就是异常：
  要么 version=0 的语义错了，要么别的东西写了这个 key（失败分支泄漏 /
  上一个 txn 的写集泄漏）。checker 判 `:create-absent-failed`。"
  [this op]
  (let [{:keys [key val]} (:value op)
        rid (request-id)]
    (invoke-txn this op
                (fn [] (p/txn-req [(p/compare {:op :equal, :target :version,
                                               :key key, :version 0})]
                                  [(put-op key val)]
                                  []
                                  rid))
                {:success-keys [[key val]]
                 :failure-keys []
                 :post-read :point
                 :post-keys [key]
                 :fresh? true})))

(defn- invoke-txn-write-set
  "多 key 写集 + **区间回读**：原子可见性的判据。

  `succeeded=false` 时三个值一个都不许出现；`succeeded=true` 时区间回读必须
  一次看到全部三个 —— 只看到一部分就是「半应用可见」（P0，`:txn-partial-visibility`）。"
  [this op]
  (let [{:keys [prefix vals range-start range-end]} (:value op)
        rid (request-id)
        keys (mapv #(str prefix %) (range (count vals)))
        pairs (mapv vector keys vals)]
    (invoke-txn this op
                (fn [] (p/txn-req []
                                  (mapv (fn [[k v]] (put-op k v)) pairs)
                                  []
                                  rid))
                {:success-keys pairs
                 :failure-keys []
                 :post-read :range
                 :post-range [range-start range-end]})))

(defn- invoke-txn-cas
  "`Compare{VALUE, EQUAL, old}` + `Put{new}`，失败分支为空。

  `succeeded=false` 时 new 必不得出现（失败 txn 的写集不得泄漏，
  `:failure-branch-leak`）；`succeeded=true` 时回读必须是 new。

  `:stale? true`（生成器 1/3 的分组）时 setup 写 `old` 但 compare 用
  `old-stale` —— 比较必然失败，于是**失败分支被真实执行**。没有这类分组，
  失败分支的断言永远跑不到（实测：一遍全绿而 `:branch-taken :failure 0`）。"
  [this op]
  (let [{:keys [key old val stale?]} (:value op)
        ; 注意：`:in` 是 Clojure 1.9+ 的 member 测试，这里用普通名称避免歧义
        cmp-val (if stale? (str old "-stale") (str old))
        rid (request-id)]
    (invoke-txn this op
                (fn [] (p/txn-req [(p/compare {:op :equal, :target :value,
                                               :key key, :value cmp-val})]
                                  [(put-op key val)]
                                  []
                                  rid))
                {:success-keys [[key val]]
                 :failure-keys []
                 :post-read :point
                 :post-keys [key]
                 :setup [key old]})))

(defn- invoke-txn-cas-delete
  "`Compare{VALUE, EQUAL, old}` + 成功分支 `Put{new}` / 失败分支 `Delete`。

  这条同时探测两件事：
    * **失败分支必须执行**：`succeeded=false` 时回读必须看不到该 key
      （delete 生效）—— 否则是「失败分支被静默跳过」；
    * **失败分支不得越界**：`succeeded=false` 时 new 不得出现。
  与 `:txn-create` 的「空失败分支」形成对照：两者一起才能把
  「失败分支没执行」与「成功分支被执行了」区分开。

  `:stale? true` 时 compare 用 `old-stale`，必然走到失败分支（同上）。"
  [this op]
  (let [{:keys [key old val stale?]} (:value op)
        cmp-val (if stale? (str old "-stale") (str old))
        rid (request-id)]
    (invoke-txn this op
                (fn [] (p/txn-req [(p/compare {:op :equal, :target :value,
                                               :key key, :value cmp-val})]
                                  [(put-op key val)]
                                  [(del-op key)]
                                  rid))
                {:success-keys [[key val]]
                 :failure-keys [[key nil]]
                 :post-read :point
                 :post-keys [key]
                 :setup [key old]})))

(defn- invoke-txn-read
  "txn 内的读（`success=[Range]`，compare 为空）：覆盖 txn 内 ReadOp 路径，
  并且它本身是一次寄存器读（completion 的 `:value` = 读到的值）。

  注意：`:value` 会被读到的值覆盖（`completion-of` 的合并顺序），所以 key 必须
  **另存**在 `:key` 上 —— checker 是按 `:key` 分组建写索引的。"
  [this op]
  (let [{:keys [key]} (:value op)
        rid (request-id)
        res (attempt! this key
                      #(p/call % p/txn (p/txn-req [] [(rng-op key "")] [] rid))
                      (fn [resp]
                        (let [rr  (some p/response-range (p/txn-responses resp))
                              kv  (first (p/range-kvs rr))]
                          {:value (when kv (p/kv-value kv))
                           :kv    (kv-edn kv)
                           :revision (p/revision resp)})))]
    (assoc (completion-of op res) :key key)))

;; --------------------------------------------------------------------------
;; T1.3 scan / revision 读
;; --------------------------------------------------------------------------

(defn- invoke-scan
  "`RangeRequest` 全形态扫描（range_end / limit / keys_only / count_only）。

  completion op 载荷：`:kvs`（EDN 列表）、`:count`（RangeResponse.count）、
  `:revision`。checker（jepsen.coord.scanck）据此判：字典序、无重复、区间内、
  limit 生效、count 与 kvs 一致。"
  [this op]
  (let [{:keys [key range-end limit keys-only count-only]} (:value op)
        res (attempt! this key
                      #(p/call % p/kv-range
                               (p/range-req* {:key key :range-end range-end
                                              :limit limit :keys-only keys-only
                                              :count-only count-only}))
                      (fn [resp] {:kvs (mapv kv-edn (p/range-kvs resp))
                                  :count (p/range-count resp)
                                  :revision (p/revision resp)}))]
    (completion-of op res)))

(defn- invoke-read-at
  "历史 revision 读：`RangeRequest{revision=r}`。

  写 op 的响应里带 `:revision`，客户端按 key 记下最近一次写的 revision
  （`(:last-rev this)`），op 里 `:revision :last` 就解析成它。checker 用
  「写响应 revision == r ⇒ 读回该 revision 必须看到写进去的那个值」做**精确**
  断言。被压缩清理的历史读返回 `OUT_OF_RANGE`（RANGE-COMPACTED）—— 这是
  白名单 `:fail`。

  F-15（测试自身，2026-09-16）：本客户端没写过这个 key 时缓存为空，原来的
  实现直接记 `:info :no-cached-revision` —— 实测 48 个 read-at 里 46 个落在这
  条分支上（G6 因为「几乎没有 :ok」把整个 run 判红）。现在改为**先点读**取
  该 key 当前的 `mod_revision` 作为历史 revision，并把这次点读的结果记在
  `:latest-kv` 里：于是顺带得到一条更强的断言 —— 「按自己刚读到的
  mod_revision 读历史，必须拿回同一个值」。"
  [this op]
  (let [{:keys [key revision]} (:value op)
        cached (if (= :last revision) (get @(:last-rev this) key) revision)
        latest (when (nil? cached) (point-read this key))
        rev    (or cached (some-> latest :kv :mod-revision))]
    (if (nil? rev)
      ;; key 不存在（或读失败）：无法构造历史读，记 :info（无副作用）
      (assoc op :type :info
             :error (if (:ok? latest) :no-value :read-failed))
      (let [res (attempt! this key
                          #(p/call % p/kv-range
                                   (p/range-req* {:key key :revision rev}))
                          (fn [resp] {:kvs (mapv kv-edn (p/range-kvs resp))
                                      :read-revision rev
                                      :revision (p/revision resp)}))]
        (assoc (completion-of op res) :latest-kv (:kv latest))))))

(def ^:private watch-poll-ms
  "单次 `.poll` 的最长等待（毫秒）。短轮询让关流/超时都及时生效。"
  200)

(defn- watch-open!
  "开一条 watch 流：从 `key` 的**缓存 leader** 开始轮询所有节点，取第一个成功
  建流的（与 `try-nodes` 同一套 leader 发现哲学）。

  为什么必须轮询：kill/重启 之后缓存 leader 可能已经不在，直接打到它只会得到
  UNAVAILABLE —— 但这不是「watch 坏了」，而是「该换节点了」。成功时把这个节点
  记回缓存（`leader-seen!`），下一次会话就直接命中。

  全部节点都失败时抛最后一个 StatusRuntimeException（由 `invoke-coord!` 的
  `:unauthenticated` 重试逻辑与 jepsen 的 op 异常处理接管）。"
  [this key create]
  (let [n     (count @(:channels this))
        start (mod (leader-start this key) n)]
    (loop [i 0, last-err nil]
      (if (< i n)
        (let [idx (mod (+ start i) n)
              ch  (nth @(:channels this) idx)
              ;; 注意：`recur` **不能**写在 `catch` 里（Clojure 只允许在尾位置
              ;; recur），所以把异常变成值再在外面 recur —— 与 try-nodes 同构。
              res (try
                    (let [w (p/open-watch ch create)]
                      (leader-seen! this key idx)
                      {:watcher w :node (nth (:nodes this) idx)})
                    (catch StatusRuntimeException e
                      {:err e}))]
          (if (:watcher res)
            res
            (recur (inc i) (:err res))))
        (throw last-err)))))

(defn- watch-poll!
  "`.poll` 包装：jepsen 会用 interrupt 打断 worker，把
  InterruptedException 变成 `::interrupted`（调用方据此收尾）。"
  [^jepsen.coord.CoordRpc$Watcher w ms]
  (try
    (.poll w (long ms))
    (catch InterruptedException _ ::interrupted)))

(defn- watch-until!
  "在**一条**流上收事件：直到 `deadline-ns`、事件数达 `max-events`、或流终止。

  返回 `{:events [..] :error nil|str :node k}`。`events` 是纯 EDN（由
  `p/response-events` 转换）。"
  [this create deadline-ns max-events]
  (let [{:keys [watcher node]} (watch-open! this (:key create) create)]
    (try
      (loop [events []]
        (let [now (System/nanoTime)]
          (if (or (>= now (long deadline-ns))
                  (>= (count events) (long max-events)))
            {:events events :error nil :node node}
            (let [left-ms (max 1 (min watch-poll-ms
                                      (quot (- (long deadline-ns) now) 1000000)))
                  m       (watch-poll! watcher left-ms)]
              (cond
                (= ::interrupted m)
                {:events events :error :interrupted :node node}

                (nil? m)                       ; 轮询超时，继续等
                (recur events)

                (.isError ^jepsen.coord.CoordRpc$WatchMsg m)
                {:events events :error (.error ^jepsen.coord.CoordRpc$WatchMsg m) :node node}

                :else
                (recur (into events
                             (p/response-events (.response ^jepsen.coord.CoordRpc$WatchMsg m)))))))))
      (finally (.close watcher)))))

(defn- invoke-watch-session
  "T2.1：一次 watch 会话（见 ns 注释）。

  `:start-revision` 为 `:last` 时取本客户端**上一次会话观察到的最大
  revision**（= 契约里「断线重连后以已确认最大 revision + 1 重建」的对照路径；
  首次会话没有历史时为 0 = 从最新开始）。op 的 `:start-revision` 记录**实际
  使用**的具体值，checker 按它判「不得收到起点之前的事件」。"
  [this op]
  (let [{:keys [key range-end prev-kv? start-revision window-ms max-events resumes]}
        (:value op)
        remembered (long (or (get @(:last-watch-rev this) key 0) 0))
        ;; **op 级**的起点：整个 op（含会话内的多次重开）只用它来汇报/判定，
        ;; 循环里的 `start` 是**当前尝试**的起点。两者混淆会造出一条假红：
        ;; 汇报成最后一次尝试的起点（比如 102），而事件来自第一次尝试（99..100），
        ;; 于是 checker 判「收到了起点之前的事件」（F-20，实测 6 条例）。
        start0     (if (= :last start-revision) remembered (long (or start-revision 0)))
        window-ms   (long (or window-ms 2000))
        max-events  (long (or max-events 10000))
        max-resumes (long (or resumes 0))
        deadline    (+ (System/nanoTime) (* 1000000 window-ms))
        create      {:key key :range-end range-end
                     :prev-kv? prev-kv? :start-revision start0}]
    (loop [start        start0
           resumes-left max-resumes
           acc-events   []          ; 之前尝试收到的事件（跨 attempt 累积）
           acc-sessions []          ; 每次 attempt 的记录
           acc-errors   []]
      (let [{:keys [events error node]} (watch-until! this (assoc create :start-revision start)
                                                      deadline max-events)
            last-rev  (long (or (:revision (peek events)) start))
            attempt   {:node node :start-revision start
                       :events (vec events)
                       :end-revision last-rev
                       :error error}
            events'   (into acc-events events)
            sessions' (conj acc-sessions attempt)
            errors'   (cond-> acc-errors error (conj [node error]))]
        (if (and error
                 (not= :interrupted error)
                 (pos? resumes-left)
                 (< (System/nanoTime) (long deadline)))
          ;; 契约：断线后以「已确认最大 revision + 1」重建即可续传
          (recur (inc last-rev) (dec resumes-left) events' sessions' errors')
          (let [end-rev (long (or (:revision (peek events')) start))]
            (when (pos? end-rev)
              (swap! (:last-watch-rev this) update key (fnil max 0) end-rev))
            (assoc op :type :ok
                   :key key
                   :events (vec events')
                   ;; `:end-revision` 必须是**最后一个事件**的 revision（没有事件
                   ;; 就没有覆盖区间 ⇒ nil）。用循环里的 `start` 兜底会把
                   ;; 「最后一次尝试的起点」当成覆盖末点，而它可能 > 实际观察到的
                   ;; 任何 revision ⇒ 一条**假红**（F-21，与 F-20 同形：派生字段
                   ;; 取了重绑定的循环变量）。判定用的事件区间一律由 events 自推。
                   :end-revision (:revision (peek events'))
                   :start-revision start0
                   :requested-start (if (= :last start-revision) :last start-revision)
                   :resumes (count acc-errors)
                   :stream-errors (vec errors')
                   :sessions (vec sessions')
                   :nodes (vec (distinct (map :node sessions'))))))))))

;; --------------------------------------------------------------------------
;; T2.2 lease workload（TTL 到期 / KeepAlive 续期 / Revoke 三种场景）
;;
;; 判定基座（coord/lease/lease.proto + F-08）：
;;   * 过期判定用**服务端单调时钟**（`tokio::time::Instant`），所以墙钟跳变
;;     对租约无效 —— 能证伪的故障是 pause（长冻结）与 kill（重启丢 deadline）；
;;   * 实际授予的 TTL 以**响应里的 ttl** 为准（服务端可按配置上下限调整），
;;     因此所有断言用响应值，不用请求值；
;;   * Lease 过期或被 Revoke ⇒ 绑定 Key 被**级联删除**；
;;   * KeepAlive 回应里 ttl=0 表示 Lease 已不存在（客户端须重新 Grant）。
;;
;; 时间基准全是**客户端单调量**（System/nanoTime 相对 op 起点的毫秒），
;; 不跨机比较时钟 —— 这样 checker 的 TTL 判定不受节点时钟差影响。
;; --------------------------------------------------------------------------

(def ^:private lease-poll-ms 150)

(defn- lease-sleep [ms]
  (try (Thread/sleep (long ms)) (catch InterruptedException _ nil)))

(defn- lease-try-read
  "点读一次并做成观测 map。

  `:read-ok? false`（读失败）与 `:present? false`（读成功但 key 不存在）
  **必须分开**：混在一起会把一次 transient 读失败当成「key 提前消失」，
  产出一条看起来像被测系统违约的假红。checker 只在 `:read-ok? true` 的观测上
  判安全/活性。"
  [this el phase key]
  (let [r (try (point-read this key) (catch Exception _ {:ok? false}))]
    {:phase    phase
     :at-ms    (el)
     :read-ok? (boolean (:ok? r))
     :present? (boolean (and (:ok? r) (some? (:kv r))))}))

(defn- lease-wait-gone
  "轮询到 key 不存在或超过 `deadline-ns`。

  返回 `{:absent? bool :at-ms n :reads n :failed n}` —— `:failed` 是其中读失败的
  次数（读失败不算「消失」）。"
  [this el key deadline-ns]
  (loop [reads 0 failed 0]
    (let [r   (try (point-read this key) (catch Exception _ {:ok? false}))
          now (System/nanoTime)]
      (cond
        (and (:ok? r) (nil? (:kv r)))
        {:absent? true :at-ms (el) :reads (inc reads) :failed failed}

        (>= now (long deadline-ns))
        {:absent? false :at-ms (el) :reads (inc reads) :failed failed}

        :else
        (do (lease-sleep lease-poll-ms)
            (recur (inc reads) (if (:ok? r) failed (inc failed))))))))

(defn- lease-open-keepalive!
  "从缓存 leader 开始轮询所有节点，取第一个能建流的（与 `watch-open!` 同构）。

  注意：双向流在**建流时不报错**，连接级故障要到 `.poll` 时才以 error marker
  出现 —— 那正是「续期停了」的观测形态（kill/partition）。
  全部节点都不行时抛最后一个异常。"
  [this key]
  (let [n     (count @(:channels this))
        start (mod (leader-start this key) n)]
    (loop [i 0, last-err nil]
      (if (< i n)
        (let [idx (mod (+ start i) n)
              ch  (nth @(:channels this) idx)
              res (try
                    (let [ka (p/open-keepalive ch)]
                      (leader-seen! this key idx)
                      {:ka ka :node (nth (:nodes this) idx)})
                    (catch Exception e {:err e}))]
          (if (:ka res)
            res
            (recur (inc i) (or (:err res) last-err))))
        (throw (ex-info "cannot open LeaseKeepAlive stream"
                        {:key key :err (str last-err)}))))))

(defn- keepalive-round
  "发一次续期并等最多 `wait-ms` 取一条响应。

  返回 `{:sent? bool :resp <edn>|nil :error str|nil}`。`:error` 非空 = 流已终止
  （kill / partition / 重启）⇒ 续期停了，正是 T2.2 要观察的形态之一。"
  [^jepsen.coord.CoordRpc$KeepAliver ka lid wait-ms]
  (try
    (.send ka (long lid))
    (let [deadline (+ (System/nanoTime) (* 1000000 (long wait-ms)))]
      (loop []
        (let [left (quot (- (long deadline) (System/nanoTime)) 1000000)]
          (if (neg? left)
            {:sent? true :resp nil}
            (let [m (try (.poll ka (max 1 (min 200 left)))
                         (catch InterruptedException _ ::interrupted))]
              (cond
                (= ::interrupted m) {:sent? true :resp nil}
                (nil? m)            (recur)
                (.isError ^jepsen.coord.CoordRpc$KeepAliveMsg m)
                {:sent? true :error (.error ^jepsen.coord.CoordRpc$KeepAliveMsg m)}
                :else
                {:sent? true :resp (p/keepalive->edn m)}))))))
    (catch Exception e {:sent? false :error (str e)})))

(defn- lease-keepalive-probe
  "对（期望已失效的）Lease 发一次 KeepAlive。契约：**ttl=0 表示 Lease 已不存在**。
  返回 `{:ttl n}` / `{:error s}`。"
  [this key lid]
  (try
    (let [{:keys [ka]} (lease-open-keepalive! this key)]
      (try
        (let [r (keepalive-round ka lid 2000)]
          (or (:resp r) {:error (:error r)}))
        (finally (try (.close ka) (catch Exception _ nil)))))
    (catch Exception e {:error (str e)})))

(defn- invoke-lease-ttl
  "T2.2 到期场景（安全 + 活性）：
    grant(ttl) → Put{lease_id} → 到期前 key 必须在 → ttl+grace 内必须消失。

  契约预期：
    * **安全** —— 没有 Renew/Revoke 时，Lease 存活期内绑定 Key 不得消失
                  （§5.1「安全（TTL 内消失）= 0」）；
    * **活性** —— 停止续租的 Key 必须在 ttl+grace 内消失（§5.2 的 100%）。"
  [this op]
  (let [{:keys [key ttl grace-ms]} (:value op)
        t0 (System/nanoTime)
        el (fn [] (quot (- (System/nanoTime) (long t0)) 1000000))
        grant (attempt! this key
                        #(p/call % p/lease-grant (p/lease-grant-req {:ttl ttl}))
                        (fn [resp] {:lease-id (p/lease-id resp)
                                    :granted-ttl (p/lease-ttl resp)}))]
    (if-not (:ok? grant)
      (assoc op :type (or (:kind grant) :info) :error (:error grant))
      (let [lid  (:lease-id grant)
            gttl (:granted-ttl grant)
            put  (attempt! this key
                           #(p/call % p/put (p/put-req* {:key key
                                                         :value (str "lease-" lid)
                                                         :lease-id lid}))
                           (fn [resp] {:revision (p/revision resp)}))]
        (if-not (:ok? put)
          (assoc op :type (or (:kind put) :info) :error (:error put)
                 :lease {:id lid :ttl gttl} :key key)
          (let [o1   (lease-try-read this el :after-write key)
                dl   (+ t0 (* 1000000 (+ (* gttl 1000) (long grace-ms))))
                gone (lease-wait-gone this el key dl)]
            (assoc op :type :ok
                   :lease {:id lid :ttl gttl}
                   :key key
                   :scenario :ttl
                   :grace-ms (long grace-ms)
                   :observations [o1 (assoc gone :phase :first-absent)]
                   :absent-ms (when (:absent? gone) (:at-ms gone)))))))))

(defn- invoke-lease-keepalive
  "T2.2 续期场景：
    grant(ttl) → Put{lease_id} → 持续 KeepAlive **超过一个 ttl** 的时间
    → 续期期内 key 必须在（证明 TTL 真的被延长）→ 停续期 → ttl+grace 内消失。

  `:keepalives` 是每次续期的响应（`:ttl` = 剩余 TTL）；`:keepalive-error` 非空
  表示流在续期期间被中断（kill/partition），此时 `:ok` 的判定由后续
  「停续期后 ttl+grace 内消失」承担。"
  [this op]
  (let [{:keys [key ttl grace-ms keepalive-ms]} (:value op)
        t0 (System/nanoTime)
        el (fn [] (quot (- (System/nanoTime) (long t0)) 1000000))
        grant (attempt! this key
                        #(p/call % p/lease-grant (p/lease-grant-req {:ttl ttl}))
                        (fn [resp] {:lease-id (p/lease-id resp)
                                    :granted-ttl (p/lease-ttl resp)}))]
    (if-not (:ok? grant)
      (assoc op :type (or (:kind grant) :info) :error (:error grant))
      (let [lid  (:lease-id grant)
            gttl (:granted-ttl grant)
            put  (attempt! this key
                           #(p/call % p/put (p/put-req* {:key key
                                                         :value (str "ka-" lid)
                                                         :lease-id lid}))
                           (fn [resp] {:revision (p/revision resp)}))]
        (if-not (:ok? put)
          (assoc op :type (or (:kind put) :info) :error (:error put)
                 :lease {:id lid :ttl gttl} :key key)
          (let [o1  (lease-try-read this el :after-write key)
                {:keys [ka node]} (lease-open-keepalive! this key)
                interval (max 200 (quot (* gttl 1000) 3))
                rounds   (max 1 (quot (long keepalive-ms) interval))]
            (try
              (let [{:keys [resps err]}
                    (loop [i 0, resps [], err nil]
                      (if (or (>= i rounds) (some? err))
                        {:resps resps :err err}
                        (let [r (keepalive-round ka lid interval)]
                          (recur (inc i)
                                 (cond-> resps (:resp r) (conj (:resp r)))
                                 (:error r)))))]
                ;; 先关流（= 停止续期），再等症状出现：这样「停续期后多久消失」
                ;; 的计时才干净。
                (.close ka)
                (let [o2   (lease-try-read this el :during-keepalive key)
                      dl   (+ (System/nanoTime)
                              (* 1000000 (+ (* gttl 1000) (long grace-ms))))
                      gone (lease-wait-gone this el key dl)]
                  (assoc op :type :ok
                         :lease {:id lid :ttl gttl}
                         :key key
                         :node node
                         :scenario :keepalive
                         :grace-ms (long grace-ms)
                         :keepalives (vec resps)
                         :keepalive-error err
                         :observations [o1 o2 (assoc gone :phase :first-absent)]
                         :absent-ms (when (:absent? gone) (:at-ms gone)))))
              (finally (try (.close ka) (catch Exception _ nil))))))))))

(defn- invoke-lease-revoke
  "T2.2 Revoke 场景：
    grant(长 ttl) → Put{lease_id} → key 在 → Revoke
    → key 必须在 grace 内消失（级联删除）
    → 对已失效的 Lease 续期必须回 ttl=0（契约：ttl=0 = 已不存在）。

  ttl 取**长值**（默认 30s）：这样「消失」不可能被自然到期解释。"
  [this op]
  (let [{:keys [key ttl grace-ms]} (:value op)
        t0 (System/nanoTime)
        el (fn [] (quot (- (System/nanoTime) (long t0)) 1000000))
        grant (attempt! this key
                        #(p/call % p/lease-grant (p/lease-grant-req {:ttl ttl}))
                        (fn [resp] {:lease-id (p/lease-id resp)
                                    :granted-ttl (p/lease-ttl resp)}))]
    (if-not (:ok? grant)
      (assoc op :type (or (:kind grant) :info) :error (:error grant))
      (let [lid  (:lease-id grant)
            gttl (:granted-ttl grant)
            put  (attempt! this key
                           #(p/call % p/put (p/put-req* {:key key
                                                         :value (str "rev-" lid)
                                                         :lease-id lid}))
                           (fn [resp] {:revision (p/revision resp)}))]
        (if-not (:ok? put)
          (assoc op :type (or (:kind put) :info) :error (:error put)
                 :lease {:id lid :ttl gttl} :key key)
          (let [o1  (lease-try-read this el :after-write key)
                rev (attempt! this key
                              #(p/call % p/lease-revoke (p/lease-revoke-req lid))
                              (fn [_] {}))
                rev-at (el)
                ka  (when (:ok? rev) (lease-keepalive-probe this key lid))
                dl  (+ (System/nanoTime) (* 1000000 (long grace-ms)))
                gone (lease-wait-gone this el key dl)]
            (assoc op :type (if (:ok? rev) :ok (or (:kind rev) :info))
                   :lease {:id lid :ttl gttl}
                   :key key
                   :scenario :revoke
                   :grace-ms (long grace-ms)
                   :revoked? (boolean (:ok? rev))
                   :revoke-at-ms rev-at
                   :revoke-error (when-not (:ok? rev) (:error rev))
                   :post-revoke-keepalive ka
                   :observations [o1 (assoc gone :phase :after-revoke)]
                   :absent-ms (when (:absent? gone) (:at-ms gone)))))))))

;; --------------------------------------------------------------------------
;; M5a —— agent 本地面（coord.agent.Lock / LeaderElection / IdGen / Registry）
;;
;; 这些服务只存在于 **agent** 上（server 的 router 里没有 coord.agent.*），
;; 所以一次成功的调用本身就是「请求真的落到了 agent」的证明。
;;
;; 时间基准：所有 jepsen 客户端都跑在控制机的**同一个 JVM** 里，因此
;; `:at-ms`（相对 op 起点的单调毫秒）是**同一个时钟**——互斥类的区间重叠判定
;; 因此是精确的，不需要 §5.4-① 的跨机时钟容差。（跨 agent 的时钟差只影响
;; agent 自己那侧的判定，不影响本判定。）
;;
;; **锚点必须是 op 自己读的 `System/nanoTime`（`:t0-ns`），不能是 jepsen 记录的
;; invoke 时刻**：后者是 worker 线程在派发 op 时打的点，中间隔着队列与线程调度。
;; 实测（2026-09-18 的 lock run）同一 JVM 里 `completion.time - (t0 + 最后一个
;; :at-ms)` 在不同 op 之间从 40ms 抖到 290ms —— 也就是**锚点自身有几百毫秒的
;; 噪声**，而它会被算成「两个持有者的区间重叠」（实测 3/126）。`nanoTime` 是同一
;; 进程内的单调时钟，两边都在同一个 JVM ⇒ 用它做锚点是精确的。
;; --------------------------------------------------------------------------

(defn- op-clock
  "Returns `(fn [] ms)` measuring monotonic milliseconds since `t0`.

  与 T2.2 的 lease 判定同一取向：一律用客户端单调量，不跨机比时钟。"
  [t0]
  (fn [] (quot (- (System/nanoTime) (long t0)) 1000000)))

(defn- invoke-lock-contend
  "M5a —— 跨 agent 互斥专项（AG-02）。

  一次 op = 一次完整的「抢 → 持有 → 释放」：
    1. 在 `deadline-ms` 内轮询 Acquire（已持有者不重试）；
    2. 拿到后在 `hold-ms` 期间用 Renew 保活（每个 `renew-ms` 一次）；
    3. Release。

  completion 记录 `:acquired-at-ms`（拿到锁的时刻）与 `:gone-at-ms`（**可举证的
  「锁已不在我名下」的最早时刻**）。**抢不到**在契约里是合法业务结果（别人持有），
  记为 `:fail :error :lock-held`（§5.1 白名单），这样 G6 的 op 级活性门槛不会把
  它当成「这条路没通」。

  ## 区间语义（F-34 的根因，两处都在这里）

  第一版把区间结束记成「**观测循环结束之后**的 el()」，并把「锁空了」判定为
  `exists=false`。两条都会把持有区间**放大**：

    1. `:released-at-ms` 记在观测循环之后 ⇒ 区间尾部被循环的 sleep 拉长
       （实测中位数 ~196ms、最长 ~1.37s）；
    2. `exists=false` 当作「空出来」⇒ 观测窗口里**另一个持有者合法接管**时
       `exists=true`，于是区间被 fail-safe 延长整整 `ttl + grace`（=9s）。

  实测后果：F-34 的 99 次「互斥重叠」在这两条修正后全部归零（真实持有区间
  两两不相交）。现在的口径：

    * `:released-at-ms` = Release RPC 返回的时刻（**观测循环之前**）；
    * `:gone?` = 「有证据表明锁已不在我名下」（release 成功 / 探针删除成功 /
      观测到 holder 不是我）；
    * `:gone-at-ms` = 上述证据里**最早**的时刻（区间最紧，不膨胀）；
    * 都没有 ⇒ `gone?` = false，checker 才走 fail-safe 延长。"
  [this op]
  (let [{:keys [name holder-id ttl-seconds deadline-ms hold-ms grace-ms]} (:value op)
        t0 (System/nanoTime)
        el (op-clock t0)
        skey (str "lock/" name)
        acquire (fn []
                  (try-nodes this skey
                             #(p/call % p/lock-acquire
                                      (p/lock-acquire-req {:name name
                                                           :holder-id holder-id
                                                           :ttl-seconds ttl-seconds}))))
        got (loop []
              (if (> (el) (long deadline-ms))
                nil
                (let [r (acquire)]
                  (cond
                    (and (:ok r) (p/lock-acquired? (:ok r)))
                    {:lease-id (p/lock-lease-id (:ok r))
                     :at-ms (el)
                     :node (:node r)}

                    (:ok r)   ;; 别人持有：等一会儿再抢
                    (do (lease-sleep 100) (recur))

                    :else     ;; agent 不可用/超时：也重试（扰动期的正常形态）
                    (do (lease-sleep 200) (recur))))))]
    (if-not got
      (assoc op :type :fail :error :lock-held
             :name name :holder-id holder-id
             :deadline-ms (long deadline-ms) :at-ms (el))
      (let [lid        (:lease-id got)
            acquired-at (:at-ms got)
            renew      (fn []
                         (try-nodes this skey
                                    #(p/call % p/lock-renew
                                             (p/lock-renew-req {:name name
                                                                :holder-id holder-id
                                                                :lease-id lid}))))
            renews     (loop [acc []]
                         (if (>= (el) (+ (long acquired-at) (long hold-ms)))
                           acc
                           (let [r (renew)
                                 entry (cond
                                         (:ok r) {:at-ms (el)
                                                  :new-ttl (p/lock-new-ttl (:ok r))}
                                         :else   {:at-ms (el)
                                                  :error (str (:err r))})]
                             (lease-sleep 100)
                             (recur (conj acc entry)))))
            ;; Fencing 探针（**在仍持有锁时**打）：用一个错误的 lease_id 尝试
            ;; Release。契约要求只认 (holder_id, lease_id) 匹配者，所以这里必须
            ;; 被拒；若返回 released=true，说明实现是按 name（或只按 holder_id）
            ;; 删的 —— 那么任何知道锁名的调用方都能把别人的锁删掉，
            ;; 互斥承诺直接被第三方破坏（lockck 判 :lock-fencing-missing）。
            ;;
            ;; 顺序很重要：必须在**真释放之前**打，否则锁已经空了，
            ;; 「released=false」就是平凡的（测不到 fencing）。
            foreign (try-nodes this skey
                               #(p/call % p/lock-release
                                        (p/lock-release-req
                                         {:name name :holder-id holder-id
                                          ;; 不存在的 lease：真 lease 是 lid
                                          :lease-id (inc (long lid))})))
            foreign-at (el)
            rel        (try-nodes this skey
                                  #(p/call % p/lock-release
                                           (p/lock-release-req {:name name
                                                                :holder-id holder-id
                                                                :lease-id lid})))
            ;; F-34 根因 ①：这个时刻**必须在观测循环之前**取 —— 记在循环之后
            ;; 等于把区间尾部按循环的 sleep 拉长（实测中位数 +196ms、最长 +1.37s），
            ;; 而下一个持有者在窗口内合法接管后就会与这条膨胀区间「重叠」。
            released-at (el)
            released?  (boolean (and (:ok rel) (p/lock-released? (:ok rel))))
            foreign-released? (boolean (and (:ok foreign)
                                            (p/lock-released? (:ok foreign))))
            ;; **可观测真相**：锁还在不在我名下，只能问 GetLockInfo。
            ;;
            ;; 为什么不直接拿 Release 的返回值当结论：实测（M5a 首轮）release
            ;; 的布尔会撒谎 —— 当前实现不校验 lease_id，我的 fencing 探针
            ;;（错误 lease_id）就能把锁真删掉，于是紧随其后的「正确」Release
            ;; 必然回 false。若拿它当「仍然持有」，区间会被 fail-safe 延长，
            ;; 进而和后面所有持有者重叠 —— 一次真实的 fencing 缺陷会被放大成
            ;; 几千条假重叠（实测 3354 条）。
            ;;
            ;; 轮询几次：lease revoke → 级联删除可能不是瞬时的，只读一次会把
            ;; 正常延迟误判成「没释放」。
            ;;
            ;; F-34 根因 ②：「空了」的判据**不是** `exists=false`，而是
            ;; 「不再挂在我名下」（`exists=false` **或** holder 已换人）。观测
            ;; 窗口里另一个持有者合法接管时 `exists=true`，用第一条判据就会
            ;; 把区间延长整整 ttl+grace（9s），制造出大片假重叠。
            obs        (loop [i 0 last-info nil]
                         (let [r (try-nodes this skey
                                            #(p/call % p/lock-get-info
                                                     (p/lock-get-info-req name)))
                               info (when (:ok r) (p/lock-info->edn (:ok r)))
                               mine? (boolean (and (map? info) (true? (:exists info))
                                                   (= holder-id (:holder-id info))))]
                           (if (or (not mine?) (>= i 4))
                             {:at-ms (el) :info (or info last-info) :mine? mine?}
                             (do (lease-sleep 200)
                                 (recur (inc i) (or info last-info))))))
            fresh      (:info obs)
            still-mine? (:mine? obs)
            ;; 「已不在我名下」的**最早**可举证时刻：三个证据都是上界，取最早
            ;; ⇒ 区间最紧。都没有 ⇒ gone? = false（checker 走 fail-safe）。
            gone-at    (cond
                         foreign-released? foreign-at
                         released?         released-at
                         (not still-mine?) (:at-ms obs)
                         :else             nil)
            gone?      (boolean gone-at)]
        (assoc op :type :ok
               :name name :holder-id holder-id :lease-id lid
               :t0-ns t0
               :acquired-at-ms acquired-at
               :released-at-ms released-at
               :released? released?
               ;; 锁**真的**不在我名下了（判据用这个，不是上面那个布尔）
               :gone? gone?
               :gone-at-ms gone-at
               :still-mine-after-release still-mine?
               :foreign-release {:ok? (boolean (:ok foreign))
                                 :released? foreign-released?
                                 :at-ms foreign-at
                                 :error (some-> (:err foreign) str)}
               :lock-info-after-release fresh
               :acquire-node (:node got)
               :holder-ttl-seconds (long ttl-seconds)
               :grace-ms (long (or grace-ms 0))
               :renews renews
               :at-ms (el))))))

(defn- lock-value->edn
  "从服务端 `/_lock/{name}` 的 JSON 值里取出口径字段。

  **故意**用正则而不是 JSON 解析器：探针只需要「谁的名字挂在 key 上」，不需要
  完整 JSON 语义；正则碰到字段缺失/格式变化的表现是 nil（如实记为未知），不会
  抛异常把整条 op 变成 :info。字段名的权威定义在
  `coord-agent/src/services/lock.rs` 的 `LockInfo`（serde 默认就是这几个名字）。"
  [s]
  (when (string? s)
    {:holder-id (second (re-find #"\"holder_id\"\s*:\s*\"([^\"]*)\"" s))
     :lease-id  (some-> (re-find #"\"lease_id\"\s*:\s*(-?\d+)" s) second Long/parseLong)
     :ttl-secs  (some-> (re-find #"\"ttl_secs\"\s*:\s*(\d+)" s) second Long/parseLong)}))

(defn- invoke-lock-abandon
  "M5a 第二轮（AG-06）—— **弃锁**：拿到锁之后**故意不释放**。

  这是「持有者进程被杀之后，服务端必须回收它持有的锁」这条契约唯一能被判定的
  形态（多进程 + 真 kill 才可判：进程内测试杀不掉自己的宿主）。两条判据都用
  **绕开 agent** 的服务端探针（`:f :lock-probe` 读 `/_lock/{name}`）取样：

    * H2（不得假丢锁）—— 持锁的 agent 活着时，它的后台自动续期任务会一直给
      lease 续命（`lock.rs` 的 `tokio::spawn` 续期循环，每 `ttl/3` 一次）⇒
      服务端 key **必须一直挂在同一个 holder 名下**。历史上这里出过 P0：用本地
      墙钟判 `is_expired()` 清理 held ⇒ 假丢锁 ⇒ **临界区重入**（该路径已删）。
    * H1（不得留下死锁）—— 持锁的 agent 被 `kill -9`（或与集群分区）之后续期
      停止 ⇒ lease 到期 ⇒ 服务端 key 必须在 `ttl + grace` 内不再挂在原 holder
      名下。

  op 本身只负责「拿到就不放」，并把识别信息（holder / lease / 拿锁的 endpoint）
  留在 completion 里；判定在 `lockck`。**故意不**做 renew：续期是 agent 自己的
  后台任务，客户端一声不吭正好模拟「插件进程内的句柄没被 drop」。"
  [this op]
  (let [{:keys [name holder-id ttl-seconds deadline-ms grace-ms]} (:value op)
        t0 (System/nanoTime)
        el (op-clock t0)
        skey (str "lock/" name)
        acquire (fn []
                  (try-nodes this skey
                             #(p/call % p/lock-acquire
                                      (p/lock-acquire-req {:name name
                                                           :holder-id holder-id
                                                           :ttl-seconds ttl-seconds}))))
        got (loop []
              (if (> (el) (long deadline-ms))
                nil
                (let [r (acquire)]
                  (cond
                    (and (:ok r) (p/lock-acquired? (:ok r)))
                    {:lease-id (p/lock-lease-id (:ok r)) :at-ms (el) :node (:node r)}

                    (:ok r) (do (lease-sleep 100) (recur))
                    :else   (do (lease-sleep 200) (recur))))))]
    (if-not got
      (assoc op :type :fail :error :lock-held
             :name name :holder-id holder-id
             :deadline-ms (long deadline-ms) :at-ms (el))
      (assoc op :type :ok
             :name name :holder-id holder-id
             :lease-id (:lease-id got)
             :t0-ns t0
             :abandoned? true
             :acquired-at-ms (:at-ms got)
             ;; 拿锁的 endpoint（agent 隧道地址）—— checker 靠它把「我持有了」
             ;; 归因到具体 agent，再用 nemesis 的 kill/partition 时间窗判
             ;; 「这个 holder 还有没有可能在续期」。
             :acquire-node (:node got)
             :holder-ttl-seconds (long ttl-seconds)
             :grace-ms (long (or grace-ms 0))
             :at-ms (el)))))

(defn- invoke-lock-probe
  "M5a（F-34 的决定性实验）—— **服务端地面真值探针**。

  绕开 agent，直接从 server 用 KV Range 读 `/_lock/{name}`：`lock-contend` 汇报
  的持有区间是**客户端自述**，这条 op 记录的是**服务端 key 到底挂在谁名下**。

  为什么必须绕开 agent：经 agent 读到的仍是 agent 的本地视图（加上代理层），
  那样就无法把「agent 自述」与「服务端真相」分开 —— 而 F-34 要回答的正是这个
  问题（见 `coord-findings.md` §14）。

  记录：`:exists?`、`:server`（`{:holder-id :lease-id :ttl-secs}`）、
  `:server-at-ms`（相对本 op 起点的单调毫秒，与其它 op 同一时钟）。读失败记
  `:info`（**不是** `:fail`）—— 探针不可用不等于「没有矛盾」，checker 会把
  读失败数单独报出来。"
  [this op]
  (let [{:keys [name]} (:value op)
        t0 (System/nanoTime)
        el (op-clock t0)
        key (str "/_lock/" name)
        res (try-probe this #(p/call % p/kv-range (p/range-req key)))]
    (if (:ok res)
      (let [kv  (first (p/range-kvs (:ok res)))
            raw (:value (kv-edn kv))]
        (assoc op :type :ok
               :name name
               :t0-ns t0
               :server-key key
               :server-at-ms (el)
               :exists? (some? kv)
               :raw-value raw
               :server (lock-value->edn raw)
               :node (:node res)))
      (assoc op :type :info
             :name name
             :t0-ns t0
             :server-key key
             :server-at-ms (el)
             :error (if (timeout? (:err res)) :timeout :probe-unavailable)
             :err (some-> (:err res) str)))))

(defn- invoke-elect-campaign
  "M5a —— 选举唯一 leader 专项（AG-02/AG-10）。

  一次 op = campaign → 持有（可选续约）→ resign → 再读一次 GetLeader。

  `:elected?` 为 false 是合法业务结果（别人在位）。`:leader-after-resign`
  记录 resign 之后 GetLeader 看到的东西：契约要求**不再是自己**（旧 leader
  不得因本地缓存而永久在位）。

  区间度量与 lock 面同一套口径（见 `invoke-lock-contend` 的长注释与
  findings 的 F-34/F-35）：

    * `:t0-ns` —— op 自己读的 `System/nanoTime`，作为跨 op 比较的**精确锚点**
      （jepsen 记录的 invoke 时刻会带上派发/调度噪声）；
    * `:gone?` / `:gone-at-ms` —— 「已不在 leader 位」的**最早可举证时刻**
      （Resign 成功 ⇒ resign 返回时刻；或 GetLeader 显示不再是自己 ⇒ 探针时刻）；
    * `:still-leader-after-resign` —— 轮询后仍是自己（判据 `:election-leader-after-resign`）。

  第一版只记了 `campaign-at-ms` / `resigned-at-ms` 两个**相对**毫秒且没带锚点，
  而 checker 直接跨 op 比它们 —— 实测 45s run 里报出 124 次「双 leader」，而同
  一份历史加上锚点后是 **0**（见 findings F-35）。"
  [this op]
  (let [{:keys [group-name candidate-id ttl-seconds deadline-ms hold-ms]} (:value op)
        t0 (System/nanoTime)
        el (op-clock t0)
        skey (str "election/" group-name)
        campaign (fn []
                   (try-nodes this skey
                              #(p/call % p/election-campaign
                                       (p/election-campaign-req
                                        {:group-name group-name
                                         :candidate-id candidate-id
                                         :ttl-seconds ttl-seconds}))))
        won (loop []
              (if (> (el) (long deadline-ms))
                nil
                (let [r (campaign)]
                  (cond
                    (and (:ok r) (p/election-elected? (:ok r)))
                    {:lease-id (p/election-lease-id (:ok r)) :at-ms (el) :node (:node r)}

                    (:ok r) (do (lease-sleep 100) (recur))
                    :else   (do (lease-sleep 200) (recur))))))]
    (if-not won
      (assoc op :type :ok :elected? false
             :group-name group-name :candidate-id candidate-id
             :t0-ns t0 :at-ms (el))
      (let [lid (:lease-id won)
            campaign-at (:at-ms won)]
        (lease-sleep (max 0 (- (long hold-ms) 0)))
        (let [resign (try-nodes this skey
                                #(p/call % p/election-resign
                                         (p/election-resign-req
                                          {:group-name group-name
                                           :candidate-id candidate-id
                                           :lease-id lid})))
              resigned-at (el)
              resigned? (boolean (and (:ok resign) (p/election-resigned? (:ok resign))))
              after (try-nodes this skey
                               #(p/call % p/election-get-leader
                                        (p/election-get-leader-req group-name)))
              after-at (el)
              leader-after (when (:ok after) (p/leader->edn (:ok after)))
              still-leader? (boolean (and (map? leader-after) (:exists leader-after)
                                          (= candidate-id (:leader-id leader-after))))
              ;; 「已不在 leader 位」的最早可举证时刻（同 lock 的 :gone-at-ms 口径）：
              ;;   * Resign 成功 → resign RPC 返回的时刻（上界）；
              ;;   * 不然：GetLeader 明确显示「不再是我」（换人 / 不存在）→ 探针时刻。
              ;; 注意**不能**用「leader 不存在」单条作依据（TTL 被动过期也会那样），
              ;; 但两者都是「不再是我」的证据，对区间闭合足够。
              gone-at (cond
                        resigned? resigned-at
                        (and (map? leader-after) (not still-leader?)) after-at
                        :else nil)]
          (assoc op :type :ok
                 :elected? true
                 :t0-ns t0
                 :group-name group-name :candidate-id candidate-id :lease-id lid
                 :campaign-at-ms campaign-at
                 :resigned-at-ms resigned-at
                 :resigned? resigned?
                 :gone? (boolean gone-at)
                 :gone-at-ms gone-at
                 :still-leader-after-resign still-leader?
                 :holder-ttl-seconds (long ttl-seconds)
                 :leader-after-resign leader-after
                 :at-ms (el)))))))

(defn- election-value->edn
  "从服务端 `/_election/{group}` 的 JSON 值里取出口径字段。

  与 `lock-value->edn` 同一取向（正则而非 JSON 解析器：字段缺失/格式变化的表现是
  nil，如实记为未知，不会抛异常把整条 op 变成 :info）。字段名的权威定义在
  `coord-agent/src/services/leader_election.rs` 的 `ElectionGroup`（serde 默认就是
  这几个名字）；`leader_id` 就是调用方传的 `candidate_id`。"
  [s]
  (when (string? s)
    {:leader-id   (second (re-find #"\"leader_id\"\s*:\s*\"([^\"]*)\"" s))
     :instance-id (second (re-find #"\"instance_id\"\s*:\s*\"([^\"]*)\"" s))
     :lease-id    (some-> (re-find #"\"lease_id\"\s*:\s*(-?\d+)" s) second Long/parseLong)
     :ttl-secs    (some-> (re-find #"\"ttl_secs\"\s*:\s*(\d+)" s) second Long/parseLong)}))

(defn- invoke-election-probe
  "M5a 第二轮（F-35 的残留）—— election 的 **服务端地面真值探针**。

  与 `:lock-probe` 完全同构：绕开 agent，从 **server** 端点用 KV Range 读
  `/_election/{group}`，回答「服务端认为这个 group 的 leader 是谁」。

  为什么必须有它：`electck` 的第一版判据全部基于**客户端自述**（campaign 成功 /
  resign 成功 / GetLeader 的返回）。F-34 的教训是这种自述分诊不了三种解释 ——
  真违约 / agent 的汇报层与 server 不一致 / 度量本身有偏差。lock 面靠
  `:lock-probe` 解决了，election 面此前是**明示的待补**（findings F-35 的
  『残留』段）；这条 op 就是那个补丁。

  读失败记 `:info`（不是 `:fail`）：探针不可用不等于「没有矛盾」，checker 会把
  读失败数单独报出来，并要求至少有一条样本（缺了判未执行）。"
  [this op]
  (let [{:keys [group-name]} (:value op)
        t0 (System/nanoTime)
        el (op-clock t0)
        key (str "/_election/" group-name)
        res (try-probe this #(p/call % p/kv-range (p/range-req key)))]
    (if (:ok res)
      (let [kv  (first (p/range-kvs (:ok res)))
            raw (:value (kv-edn kv))]
        (assoc op :type :ok
               :group-name group-name
               :t0-ns t0
               :server-key key
               :server-at-ms (el)
               :exists? (some? kv)
               :raw-value raw
               :server (election-value->edn raw)
               :node (:node res)))
      (assoc op :type :info
             :group-name group-name
             :t0-ns t0
             :server-key key
             :server-at-ms (el)
             :error (if (timeout? (:err res)) :timeout :probe-unavailable)
             :err (some-> (:err res) str)))))

(defn- invoke-idgen
  "M5a —— IdGen（AG-08）。

  `:batch` = 一次 NextBatch 拿 `:count` 个 ID：契约要求**返回 count 个互异 ID**
  （这条在单个响应内部就可判，不依赖任何跨节点比较）。`:single` = NextId。

  ID 存成字符串（EDN 的 long 在 JSON/EDN 之间来回没有问题，但 snowflake 是
  64-bit 有符号量，保持十进制字符串更不容易在多语言链路里出错）。
  `-` 前缀会出现在极端值上，保留原样。"
  [this op]
  (let [{:keys [name batch? count]} (:value op)
        skey (str "idgen/" name)]
    (if batch?
      (result-op op
                 (try-nodes this skey
                            #(p/call % p/idgen-next-batch
                                     (p/idgen-next-batch-req {:name name
                                                              :count (or count 8)})))
                 (fn [resp]
                   (let [ids (p/idgen-ids resp)]
                     (assoc op :type :ok :name name
                            :batch? true
                            ;; `:requested-count` 供 checker 判「返回个数 = 请求个数」
                            :requested-count (long (or count 8))
                            :ids (mapv str ids)
                            ;; **不要**写成 `(count ids)`：`count` 在这里被上面的
                            ;; destructuring 绑定成了请求个数（一个 Long），
                            ;; 于是 `(count ids)` = 「把 Long 当函数调」⇒
                            ;; ClassCastException: Long cannot be cast to IFn。
                            ;; 实测（M5a 第二轮）：整个 **NextBatch 分支从未执行成功过**
                            ;; （`:batches 0`），而它正是「batch 内部 ID 互异」那条
                            ;; 判据的唯一载体 —— 一类很难被区间/阈值判据抓到的静默
                            ;; 覆盖缺失（jepsen 把它记成 indeterminate，混在
                            ;; `:unauthenticated` 风暴里）。
                            :n (clojure.core/count ids)))))
      (result-op op
                 (try-nodes this skey
                            #(p/call % p/idgen-next-id
                                     (p/idgen-next-id-req {:name name})))
                 (fn [resp]
                   (assoc op :type :ok :name name :batch? false
                          :ids [(str (p/idgen-id resp))]))))))

(defn- registry-discover-once
  "点读一次 Discover（用于观测，不改变状态）。"
  [this name]
  (try-nodes this (str "registry/" name)
             #(p/call % p/registry-discover
                      (p/registry-discover-req {:service-name name
                                                :filter-mode :exact}))))

(defn- invoke-registry-cycle
  "M5a —— registry 生命周期（AG-07 / AG-09 的实例面）。

  一次 op = register(ttl) → discover（必须能看到自己）→ **停止心跳** →
  等到自己消失（ttl+grace 内）→ 尽可能 deregister。

  观测序列带 `:at-ms`，checker 同时得到两个方向的判据：
    * **活性**：注册后立即可见（否则服务发现不可用）；
    * **安全**：停止续约后必须在 ttl+grace 内消失（否则是**幽灵实例**）。

  契约里「重复注册幂等」由一个独立分支覆盖（`:dup?` = 连登两次）。"
  [this op]
  (let [{:keys [name instance-id ttl-seconds grace-ms dup?]} (:value op)
        t0 (System/nanoTime)
        el (op-clock t0)
        skey (str "registry/" name)
        reg (fn []
              (try-nodes this skey
                         #(p/call % p/registry-register
                                  (p/registry-register-req
                                   {:service-name name
                                    :instance-id instance-id
                                    :ttl-seconds ttl-seconds}))))
        r1 (reg)]
    (if-not (:ok r1)
      ;; 失败路径走统一的 result-op 映射（timeout → :info、全部 not-leader →
      ;; :fail not-leader…）；ok-fn 在这条路径上不会被调用。
      (let [mapped (result-op op r1 (fn [resp] (assoc op :type :ok
                                                      :lease-id (p/registry-lease-id resp))))]
        (assoc mapped :name name :instance-id instance-id))
      (let [lid (p/registry-lease-id (:ok r1))
            ;; 幂等分支：同 (service, instance) 再登一次
            r2  (when dup? (reg))
            seen-after-register
            (let [d (registry-discover-once this name)]
              {:phase :after-register :at-ms (el)
               :read-ok? (boolean (:ok d))
               :present? (boolean (some #(= instance-id (:instance-id %))
                                        (when (:ok d) (p/registry-instances (:ok d)))))
               :instances (when (:ok d) (mapv :instance-id (p/registry-instances (:ok d))))})
            ;; 停心跳 → 等自己消失
            dl (+ t0 (* 1000000 (+ (* (long ttl-seconds) 1000) (long (or grace-ms 0)))))
            gone (loop [reads 0 failed 0]
                   (let [d (registry-discover-once this name)
                         ok? (:ok d)
                         present? (boolean (and ok?
                                                (some #(= instance-id (:instance-id %))
                                                      (p/registry-instances (:ok d)))))]
                     (cond
                       (and ok? (not present?))
                       {:absent? true :at-ms (el) :reads (inc reads) :failed failed
                        :read-ok? true :last-present? false}

                       (>= (System/nanoTime) (long dl))
                       ;; 到期仍未消失：带上「最后一个成功读看到了什么」——
                       ;; checker 只在这条证据存在时才判幽灵实例（否则记 unjudged，
                       ;; 不能把「读不到」当成「不存在」）。
                       {:absent? false :at-ms (el) :reads (inc reads) :failed failed
                        :read-ok? (boolean ok?) :last-present? present?}

                       :else (do (lease-sleep lease-poll-ms)
                                 (recur (inc reads) (if ok? failed (inc failed)))))))
            dereg (try-nodes this skey
                             #(p/call % p/registry-deregister
                                      (p/registry-deregister-req
                                       {:service-name name
                                        :instance-id instance-id
                                        :lease-id lid})))]
        (assoc op :type :ok
               :name name :instance-id instance-id :lease-id lid
               :holder-ttl-seconds (long ttl-seconds)
               :grace-ms (long (or grace-ms 0))
               :dup? (boolean dup?)
               :dup-lease-id (when (:ok r2) (p/registry-lease-id (:ok r2)))
               :observations [seen-after-register
                              (assoc gone :phase :after-ttl)]
               :absent-ms (when (:absent? gone) (:at-ms gone))
               :deregistered? (boolean (:ok dereg))
               :at-ms (el))))))

(defn- invoke-registry-discover
  "M5a —— 纯观测：一次 Discover。用于与 `:registry-cycle` 并发跑，
  让「幽灵实例」在**别的客户端**眼里也能被抓到（自我保护快照最容易在这里露）。"
  [this op]
  (let [{:keys [name]} (:value op)]
    (result-op op (registry-discover-once this name)
               (fn [resp]
                 (assoc op :type :ok :name name
                        :instances (mapv :instance-id (p/registry-instances resp))
                        :revision (p/registry-revision resp))))))

;; --------------------------------------------------------------------------
;; M5b —— agent 本地数据面：Cache（AG-09）与 MQ（AG-11）
;;
;; 两者的数据都在 **agent 本地**（cache = redb；MQ = agent 本地日志）⇒ 「读到
;; 自己刚写的值」只在单 agent / 无跨 agent 路由时成立。checker 文档里写明这条
;; 前提，`coord.clj` 会在 `--workload cache|mq` 且 `--agents > 1` 时**拒绝起跑**
;; （见 local-consistency-workloads）。
;;
;; 每个 op 都记 `:t0-ns`（op 入口的 `System/nanoTime`）：跨 op 的区间判据一律
;; 用这个锚点（F-34 根因①：jepsen 记录的 invoke 时刻有 40–290ms 派发噪声）。
;; --------------------------------------------------------------------------

(defn- cache-tag [key] (str "cache/" key))

(defn- invoke-cache-set
  "Cache Set（字符串面）。`ttl-seconds = 0` = **不过期**（持久条目）——
  AG-09 的「TTL 不得提前消失」与「重启后仍在」两条判据都靠它区分。"
  [this op]
  (let [{:keys [key value ttl-seconds]} (:value op)
        t0 (System/nanoTime)
        res (try-nodes this (cache-tag key)
                       #(p/call % p/cache-set
                                (p/cache-set-req {:key key :value value
                                                  :ttl-seconds ttl-seconds})))]
    (result-op op res
               (fn [_]
                 (assoc op :type :ok :key key :value value
                        :ttl-seconds (long (or ttl-seconds 0))
                        :t0-ns t0 :done-ns (System/nanoTime))))))

(defn- invoke-cache-get
  "Cache Get。completion 同时带 `:found` 与 `:value`（**空串也是值**）。"
  [this op]
  (let [{:keys [key]} (:value op)
        t0 (System/nanoTime)
        res (try-nodes this (cache-tag key)
                       #(p/call % p/cache-get (p/cache-get-req key)))]
    (result-op op res
               (fn [resp]
                 (assoc op :type :ok :key key
                        :found (p/cache-value-found? resp)
                        :value (p/cache-value resp) :t0-ns t0 :done-ns (System/nanoTime))))))

(defn- invoke-cache-delete [this op]
  (let [{:keys [key]} (:value op)
        t0 (System/nanoTime)
        res (try-nodes this (cache-tag key)
                       #(p/call % p/cache-delete (p/cache-delete-req key)))]
    (result-op op res
               (fn [resp]
                 (assoc op :type :ok :key key
                        :deleted (boolean (p/cache-deleted? resp)) :t0-ns t0 :done-ns (System/nanoTime))))))

(defn- invoke-cache-lpush
  "List 左推。返回值是**推入后的长度**（契约）——checker 用它做单调性判据。"
  [this op]
  (let [{:keys [key value]} (:value op)
        t0 (System/nanoTime)
        res (try-nodes this (cache-tag key)
                       #(p/call % p/cache-lpush
                                (p/cache-lpush-req {:key key :value value})))]
    (result-op op res
               (fn [resp]
                 (assoc op :type :ok :key key :value value
                        :length (p/cache-length resp) :t0-ns t0 :done-ns (System/nanoTime))))))

(defn- invoke-cache-lrange [this op]
  (let [{:keys [key start stop]} (:value op)
        t0 (System/nanoTime)
        res (try-nodes this (cache-tag key)
                       #(p/call % p/cache-lrange
                                (p/cache-lrange-req {:key key :start start :stop stop})))]
    (result-op op res
               (fn [resp]
                 (assoc op :type :ok :key key
                        :values (p/cache-values resp) :t0-ns t0 :done-ns (System/nanoTime))))))

(defn- invoke-cache-llen [this op]
  (let [{:keys [key]} (:value op)
        t0 (System/nanoTime)
        res (try-nodes this (cache-tag key)
                       #(p/call % p/cache-llen (p/cache-llen-req key)))]
    (result-op op res
               (fn [resp]
                 (assoc op :type :ok :key key
                        :length (p/cache-length resp) :t0-ns t0 :done-ns (System/nanoTime))))))

(defn- invoke-cache-sadd [this op]
  (let [{:keys [key member]} (:value op)
        t0 (System/nanoTime)
        res (try-nodes this (cache-tag key)
                       #(p/call % p/cache-sadd
                                (p/cache-sadd-req {:key key :member member})))]
    (result-op op res
               (fn [_]
                 (assoc op :type :ok :key key :member member :t0-ns t0 :done-ns (System/nanoTime))))))

(defn- invoke-cache-smembers [this op]
  (let [{:keys [key]} (:value op)
        t0 (System/nanoTime)
        res (try-nodes this (cache-tag key)
                       #(p/call % p/cache-smembers (p/cache-smembers-req key)))]
    (result-op op res
               (fn [resp]
                 (assoc op :type :ok :key key
                        :members (p/cache-values resp) :t0-ns t0 :done-ns (System/nanoTime))))))

;; --- MQ ------------------------------------------------------------------

(defn- mq-tag [topic] (str "mq/" topic))

(defn- invoke-mq-create-topic
  "CreateTopic 是**幂等意图但非幂等契约**：已存在时服务端返回 ALREADY_EXISTS，
  这里一律记 :ok/:info（不判分），只作为「主题真的建过」的证据。"
  [this op]
  (let [{:keys [topic partitions]} (:value op)
        res (try-nodes this (mq-tag topic)
                       #(p/call % p/mq-create-topic
                                (p/mq-create-topic-req {:topic topic
                                                        :partitions partitions})))]
    (result-op op res
               (fn [_] (assoc op :type :ok :topic topic
                              :partitions (long (or partitions 1)))))))

(defn- invoke-mq-publish
  "Publish → `:offset`（broker 分配的**分区内**序号）。

  `:dup?` = **刻意重发**：用同一个 (payload, idempotency-key) 连发两次，把两次
  返回的 offset 都记下来（`:offset` / `:dup-offset`）。这是 `idempotency_key`
  字段的证伪点 —— 同一个键重复发布应当只落一条（去重），否则调用方在「响应
  丢失后重试」时会给下游多一条消息。

  注意：`mq.rs` 的 publish 路径**不使用** `idempotency_key`（gRPC 层把 `None`
  当作 header 传下去），所以这条判据在实现修好之前必然是红的 —— checker 默认
  只**记录**不判红（`:expect-idem-dedupe?` 默认 false），并把它记成缺陷单
  （见 coord-findings.md）；契约一旦书面确认为「必须去重」，把这个开关打开即可。"
  [this op]
  (let [{:keys [topic partition payload idempotency-key dup?]} (:value op)
        t0 (System/nanoTime)
        pub (fn []
              (try-nodes this (mq-tag topic)
                         #(p/call % p/mq-publish
                                  (p/mq-publish-req {:topic topic
                                                     :partition partition
                                                     :payload payload
                                                     :idempotency-key idempotency-key}))))
        res (pub)]
    (result-op op res
               (fn [resp]
                 (assoc op :type :ok :topic topic
                        :partition (long (or partition 0))
                        :payload payload
                        :idempotency-key idempotency-key
                        :offset (p/mq-offset resp)
                        :dup? (boolean dup?)
                        ;; 重发的那一次（只在 dup? 时发）——它的 offset 与首次
                        ;; 相同（去重生效）还是不同（没去重），就是判据的全部。
                        :done-ns (System/nanoTime)
                        :dup-offset (when dup?
                                      (let [r2 (pub)]
                                        (when (:ok r2) (p/mq-offset (:ok r2)))))
                        :t0-ns t0)))))

(defn- invoke-mq-ack
  "Ack 一批 offset。**poll op 内部调用**（见 `invoke-mq-poll`）：契约里
  「poll + ack」是一对动作，把它们拆成两个独立 op 会让「ack 的到底是哪批
  offset」变成生成器的猜测（生成器不知道服务端分配了什么），从而把一条本该
  精确的 at-least-once 判据变成概率判据。

  全部 Ack 成功才推进游标（`mq-cursor`）：只确认过的消息才不会被再次投递，
  这是「静默丢失」判据的锚 —— 游标越过某 offset 而它从未被投递，就是丢消息。"
  [this {:keys [topic partition consumer-group offsets]}]
  (let [rs (mapv (fn [o]
                   (try-nodes this (mq-tag topic)
                              #(p/call % p/mq-ack
                                       (p/mq-ack-req {:topic topic
                                                      :partition partition
                                                      :consumer-group consumer-group
                                                      :offset o}))))
                 offsets)
        ok? (and (seq offsets) (every? :ok rs))]
    (when ok?
      (swap! (:mq-cursor this)
             (fn [m] (assoc m [topic (long (or partition 0))]
                            (inc (long (apply max offsets)))))))
    ok?))

(defn- invoke-mq-poll
  "Poll（unary）→ 立刻 Ack 返回的这批消息（契约：poll + ack = at-least-once）。

  start_offset 取本客户端在该 topic/partition 上的游标：游标**只在 Ack 全成
  功后推进**。checker 因此可以精确判「静默丢失」——某 offset 已确认发布、而
  某次 Poll 的起点已经越过它、它却从没被投递过。

  completion 里的 `:acked-offsets` 是本次 op 确认掉的 offset 列表；`:poll-ok?`
  与 `:ack-ok?` 分开记（**读成功但 Ack 失败**是一种独立形态：消费者会重复收到
  同一批消息，正是 at-least-once 的合法重复）。"
  [this op]
  (let [{:keys [topic partition consumer-group max-count]} (:value op)
        cur @(:mq-cursor this)
        start (long (get cur [topic partition] 0))
        t0 (System/nanoTime)
        res (try-nodes this (mq-tag topic)
                       #(p/call % p/mq-poll
                                (p/mq-poll-req {:topic topic :partition partition
                                                :consumer-group consumer-group
                                                :start-offset start
                                                :max-count max-count})))]
    (result-op op res
               (fn [resp]
                 (let [msgs   (p/mq-messages resp)
                       offs   (mapv (fn [m] (long (:offset m))) msgs)
                       ack-ok? (if (seq offs)
                                 (invoke-mq-ack this {:topic topic
                                                      :partition (long (or partition 0))
                                                      :consumer-group consumer-group
                                                      :offsets offs})
                                 ;; 空响应：什么都不用确认（但游标也不动）
                                 true)]
                   (assoc op :type :ok :topic topic
                          :partition (long (or partition 0))
                          :start-offset start
                          :messages msgs
                          :acked-offsets (if ack-ok? offs [])
                          :poll-ok? true
                          :ack-ok? ack-ok?
                          :t0-ns t0 :done-ns (System/nanoTime)))))))

(defn- invoke-coord!
  "Runs op, transparently re-authenticating and retrying when the cluster
  rejects our CCT as UNAUTHENTICATED (coord CCTs expire after ~1h; a rejected
  request is never applied, so retrying after a fresh login is safe)."
  [this test op]
  (let [attempt (fn []
                  (case (:f op)
                    :read             (invoke-read this op)
                    :write            (invoke-write this op)
                    :cas              (invoke-cas this op)
                    :delete           (invoke-delete this op)
                    :exists           (invoke-exists this op)
                    :scan             (invoke-scan this op)
                    :read-at          (invoke-read-at this op)
                    :watch-session    (invoke-watch-session this op)
                    :lease-ttl        (invoke-lease-ttl this op)
                    :lease-keepalive  (invoke-lease-keepalive this op)
                    :lease-revoke     (invoke-lease-revoke this op)
                    :txn-create       (invoke-txn-create this op)
                    :txn-write-set    (invoke-txn-write-set this op)
                    :txn-cas          (invoke-txn-cas this op)
                    :txn-cas-delete   (invoke-txn-cas-delete this op)
                    :txn-read         (invoke-txn-read this op)
                    :idem-put         (invoke-idem-put this op)
                    :idem-delete      (invoke-idem-delete this op)
                    :idem-range-delete (invoke-idem-range-delete this op)
                    ;; M5a agent 本地面
                    :lock-contend     (invoke-lock-contend this op)
                    :lock-abandon     (invoke-lock-abandon this op)
                    :lock-probe       (invoke-lock-probe this op)
                    :elect-campaign   (invoke-elect-campaign this op)
                    :election-probe   (invoke-election-probe this op)
                    :idgen            (invoke-idgen this op)
                    :registry-cycle   (invoke-registry-cycle this op)
                    :registry-discover (invoke-registry-discover this op)
                    ;; M5b agent 本地数据面
                    :cache-set        (invoke-cache-set this op)
                    :cache-get        (invoke-cache-get this op)
                    :cache-del        (invoke-cache-delete this op)
                    :cache-lpush      (invoke-cache-lpush this op)
                    :cache-lrange     (invoke-cache-lrange this op)
                    :cache-llen       (invoke-cache-llen this op)
                    :cache-sadd       (invoke-cache-sadd this op)
                    :cache-smembers   (invoke-cache-smembers this op)
                    :mq-create-topic  (invoke-mq-create-topic this op)
                    :mq-publish       (invoke-mq-publish this op)
                    :mq-poll          (invoke-mq-poll this op)
                    (assoc op :type :fail :error (str "unknown op " (:f op)))))]
    (loop [re-auths-left 2]
      (let [op' (attempt)]
        (if (and (pos? re-auths-left)
                 (= :fail (:type op'))
                 (= :unauthenticated (:error op')))
          (let [refreshed? (try
                             (info "CCT rejected as unauthenticated — refreshing session")
                             (reauthenticate! this)
                             true
                             (catch Exception e
                               (warn e "Session refresh failed; keeping op result")
                               false))]
            (if refreshed?
              (recur (dec re-auths-left))
              op'))
          op')))))

(defrecord CoordClient [nodes root-password cct channels raw-channels leader-idx
                        last-rev last-watch-rev parse-values?
                        ;; M5a：鉴权用的端点（server）可以与 ops 端点（agent 隧道）
                        ;; 不同 —— agent 不代理 Auth，见 reauthenticate! 的注释。
                        auth-nodes auth-raw-channels
                        ;; M5a / F-34：地面真值探针的端点（必须是 **server**）。
                        probe-nodes probe-raw-channels probe-channels
                        ;; M5b：MQ 消费者游标 {[topic partition] next-offset}。
                        ;; 只在 **Ack 成功** 后推进 —— 这是 at-least-once 判据的
                        ;; 锚（未确认的消息不得被跳过）。
                        mq-cursor
                        ;; M5b：需要在 setup! 里引导创建的主题（--workload mq）。
                        mq-topic]
  client/Client
  (open! [this test node]
    (info "Opening coord client on" node)
    (let [raw-channels (mapv p/channel nodes)
          anodes       (vec (or (seq auth-nodes) nodes))
          ;; 端点相同（直连 run）时共用同一批 channel，不多建连接。
          auth-raw     (if (= anodes (vec nodes))
                         raw-channels
                         (mapv p/channel anodes))
          ;; 探针端点的缺省 = 鉴权端点 = **server**（不是 ops 端点）：
          ;; --via-agent 时这两者不同，而探针必须走后者之外的这一条。
          pnodes       (vec (or (seq probe-nodes) anodes))
          probe-raw    (cond
                         (= pnodes anodes) auth-raw
                         (= pnodes (vec nodes)) raw-channels
                         :else (mapv p/channel pnodes))]
      (try
        (let [cct      (authenticate! auth-raw root-password)
              channels (mapv #(p/auth-channel % cct) raw-channels)]
          (assoc this :cct (atom cct)
                      :channels (atom channels)
                      :raw-channels (atom raw-channels)
                      :auth-raw-channels (atom auth-raw)
                      :probe-raw-channels (atom probe-raw)
                      :probe-channels (atom (mapv #(p/auth-channel % cct) probe-raw))
                      :leader-idx (atom {})
                      :last-rev (atom {})
                      ;; T2.1：本客户端每个 key 上**已观察到的最大 watch
                      ;; revision**（跨会话），`start-revision :last` 用它
                      ;; = 契约的续传起点。
                      :last-watch-rev (atom {})
                      ;; M5b：MQ 游标（见 invoke-mq-poll / invoke-mq-ack）
                      :mq-cursor (atom {})))
        (catch Exception e
          ;; Don't leak gRPC channels if auth fails partway through.
          (doseq [^ManagedChannel ch (distinct (concat raw-channels auth-raw probe-raw))]
            (.shutdownNow ch))
          (throw e)))))

  (setup! [this test]
    (info "coord client setup: waiting for a successful read (quorum + auth)")
    (let [res (loop [i 0]
                (if (< i 60)
                  (let [res (try
                              (try-nodes this register-key
                                         #(p/call % p/kv-range (p/range-req register-key)))
                              (catch Exception e {:err e}))]
                    (if (:ok res)
                      res
                      (do (Thread/sleep 1000)
                          (recur (inc i)))))
                  nil))]
      (when-not res
        ;; 把**最后一次失败原因**带进异常：没有它的话，「cluster did not become
        ;; ready」既可能是集群没起来、也可能是鉴权被拒 / 端点写错 / agent 没代理
        ;; 该 RPC —— 实测（M5a 接入时）为这一句话多花了一轮排查，因为真正的错
        ;; 误（agent 侧 UNAUTHENTICATED）被这条消息盖住了。
        (let [last-err (try
                         (try-nodes this register-key
                                    #(p/call % p/kv-range (p/range-req register-key)))
                         (catch Exception e {:err e}))]
          (throw (ex-info (str "coord cluster did not become ready (no successful read); "
                               "last error: " (:err last-err)
                               "; ops endpoints: " (pr-str (:nodes this))
                               "; auth endpoints: "
                               (pr-str (or (:auth-nodes this) (:nodes this))))
                          {:nodes (:nodes this)
                           :auth-nodes (:auth-nodes this)
                           :err (str (:err last-err))}))))
      (info "coord cluster is ready")
      ;; M5b：--workload mq 的主题引导。放在 setup! 而不是生成器里：CreateTopic
      ;; 不是幂等契约（已存在时返回 ALREADY_EXISTS），做成随机混进来的 op 会让
      ;; 「主题到底建没建」变成概率事件；这里建一次，失败也只在**真正的连接/
      ;; 鉴权问题**上（此时后面的 publish 会以显式错误暴露，而不是静默空跑）。
      (when-let [t (:mq-topic this)]
        (let [res (try
                    (try-nodes this (mq-tag t)
                               #(p/call % p/mq-create-topic
                                        (p/mq-create-topic-req {:topic t
                                                                :partitions 1})))
                    (catch Exception e {:err e}))]
          (info "coord client: mq topic" t
                (if (:ok res) "ready" (str "create failed: " (:err res)))))))
    this)

  (invoke! [this test op]
    (invoke-coord! this test op))

  (close! [this test]
    (doseq [^ManagedChannel ch (distinct (concat @(:raw-channels this)
                                                (when-let [a @(:auth-raw-channels this)]
                                                  a)
                                                (when-let [a @(:probe-raw-channels this)]
                                                  a)))]
      (.shutdownNow ch)))

  (teardown! [this test] this))

(defn coord-client
  "Builds the (template) coord client. Channels are opened per-worker in
  open!.

  `parse-values?`（F-14）：`:register` / `:cas-register` / `:multi-register` /
  `:idempotency` 的值是整数，knossos 模型用整数比较；`:map` / `:txn` / `:scan` /
  `:watch` / `:lease` 的值是任意字节串（lease 场景用点读拿 KeyValue，不走
  `:read`/`:write`），必须原样传（否则读到非数值就抛异常，整类 op 变成 :info）。"
  [opts]
  (reset-seen!)
  (->CoordClient (vec (:nodes opts))
                 (or (:root-password opts) "66c57bb56bce306f484344e4a8650836")
                 nil nil nil (atom {}) (atom {}) (atom {})
                 (not (contains? #{:map :txn :scan :watch :lease}
                                 (:workload opts)))
                 ;; M5a：:auth-nodes 缺省 = ops 端点（直连 run 行为不变）。
                 ;; --via-agent / agent 本地面时，coord.clj 传集群端点过来。
                 (:auth-nodes opts)
                 nil
                 ;; F-34 探针端点：缺省 = :auth-nodes = **server**。
                 (:probe-nodes opts)
                 nil
                 nil
                 ;; M5b：MQ 游标（运行时状态，open! 里初始化）
                 nil
                 ;; M5b：需要 setup! 引导创建的主题（--workload mq）
                 (:mq-topic opts)))
