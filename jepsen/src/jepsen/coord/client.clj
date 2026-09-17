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
  channels for every node. Returns the new CCT."
  [this]
  (let [raw-channels @(:raw-channels this)
        cct          (authenticate! raw-channels (:root-password this))
        channels     (mapv #(p/auth-channel % cct) raw-channels)]
    (reset! (:cct this) cct)
    (reset! (:channels this) channels)
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
             (if (or (unavailable? (:err res)) (forward-request? (:err res)))
               (recur (inc i) (:err res)
                      (and all-not-leader?
                           (or (not-leader? (:err res))
                               (forward-request? (:err res)))))
               res)))
         {:err last-err, :all-not-leader? all-not-leader?})))))

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
                        last-rev last-watch-rev parse-values?]
  client/Client
  (open! [this test node]
    (info "Opening coord client on" node)
    (let [raw-channels (mapv p/channel nodes)]
      (try
        (let [cct      (authenticate! raw-channels root-password)
              channels (mapv #(p/auth-channel % cct) raw-channels)]
          (assoc this :cct (atom cct)
                      :channels (atom channels)
                      :raw-channels (atom raw-channels)
                      :leader-idx (atom {})
                      :last-rev (atom {})
                      ;; T2.1：本客户端每个 key 上**已观察到的最大 watch
                      ;; revision**（跨会话），`start-revision :last` 用它
                      ;; = 契约的续传起点。
                      :last-watch-rev (atom {})))
        (catch Exception e
          ;; Don't leak gRPC channels if auth fails partway through.
          (doseq [^ManagedChannel ch raw-channels]
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
        (throw (ex-info "coord cluster did not become ready (no successful read)" {})))
      (info "coord cluster is ready"))
    this)

  (invoke! [this test op]
    (invoke-coord! this test op))

  (close! [this test]
    (doseq [^ManagedChannel ch @(:raw-channels this)]
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
                                 (:workload opts)))))
