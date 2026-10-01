(ns jepsen.coord.idem
  "T1.4 —— `request_id` 幂等专项 checker（倒逼 F-01/F-02/F-03）。

  契约锚点（R1）——`apis/contracts/proto/coord/kv/kv.proto`：

    PutRequest.request_id = 5
      「同一客户端身份下，重复提交相同 request_id 的请求不会重复生效，
        返回首次执行的结果。」

    DeleteRequest.request_id = 4
      「可选：幂等去重键（语义同 PutRequest.request_id）」

  这两句话带来**四条互相独立、都可被历史证伪**的承诺：

    (1) **不重复生效** —— 同一 rid 的 k 次重放最多生效一次。
        证据：k 次 `Put` 响应的 `revision` 必须全部相同；重放后该 key 的
        `version`（KeyValue.version，创建为 1、随写递增）必须 == 1。
        违反 ⇒ `:revision-advanced` / `:version-over-advance`。
        对 `Delete` 同理：k 次响应的 `deleted` 与被删集合必须相同。

    (2) **返回首次执行的结果** —— 命中缓存的响应必须逐字段等于首次响应。
        对 `Put{prev_kv:true}` 尤其致命：首次返回旧值，重放若返回 `None`
        （F-02：幂等命中路径 `prev_kv` 恒为 None，`server/mod.rs:1425`），
        依赖 `prev_kv` 做 CAS 后置校验 / 审计的上层会把 `None` 解读成
        「该 key 此前不存在」——**静默的错误结论**。
        违反 ⇒ `:prev-kv-mismatch`。

    (3) **不得放大破坏面** —— 范围删（`range_end` 非空）的重放若在「首次
        执行之后、重放之前」区间内被新写入补齐，重放会删掉**首次执行之后
        才写入的数据**（`delete` 处理器第 1650 行起没有任何 `*_idempotent*`
        调用 ⇒ F-01）。这是比重复计数严重得多的形态：重试放大破坏面。
        违反 ⇒ `:replay-deleted-new-write`。

    (4) **重放不得使已确认的删除失效** —— k 次 delete 全部 `:ok` 之后，
        该 key 必须仍然不存在。违反 ⇒ `:delete-lost`（P0：删除复活）。

  时间/节点维度（F-03）：幂等缓存是**单节点进程内**的（`server/mod.rs`
  113–160：`HashMap` + TTL 60s + 4096 FIFO，不进 raft 日志/快照）。所以
  「重放落到另一个节点」「重放发生在本节点重启之后」时，上述四条承诺都会
  被违反。这就是本 checker 在 nemesis 下必然变红的原因——**变红是结论，
  不是测试抖动**：要么 coord 把去重做成跟随 raft 复制的语义，要么把契约
  措辞限缩到「单节点 / 60s / 4096 容量」并同步更新 §5.4-① 的确认内容。

  漏检边界（R2，必须声明）：
    * 只在**全部重放尝试都 `:ok`** 的分组上判定 `version`/最终值——首次
      尝试是 `:info`（响应丢失，可能已应用）时，重放再次生效是合法的，
      此时唯一可判的是「响应内容是否自洽」，不判生效次数。
    * 不做历史回放式全序验证（那是 knossos 的活）；本 checker 只回答
      「幂等承诺是否被违反」，不回答「KV 是否线性一致」。
    * 客户端的重放次数与每次响应都在**一个 completion op** 里汇报
      （见 `jepsen.coord.client`），因此单次 RPC 级别的时序不在这份历史里；
      这对上述四条断言没有影响（它们只依赖响应内容与最终读）。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]))

;; ---------------------------------------------------------------------------
;; 参数
;; ---------------------------------------------------------------------------

(def default-min-replay-attempts
  "§5.1 对 `idempotency` workload 的样本门槛：重放尝试 ≥ 50 次。
  样本不足的 cell 视为**未执行**（不计绿也不计红），因此这里直接判 invalid
  并给出 `:reason :insufficient-sample`，避免「跑了 3 次就说幂等没问题」。"
  50)

;; ---------------------------------------------------------------------------
;; 工具
;; ---------------------------------------------------------------------------

(defn- completions
  "所有带数据的完成 op（`/idem-*` 各 op）。"
  [history]
  (filterv #(and (contains? #{:ok :info :fail} (:type %))
                 (contains? #{:idem-put :idem-delete :idem-range-delete} (:f %)))
           history))

(defn- replay-attempts
  "整个历史里的重放尝试总数（每次 RPC 记 1）。样本门槛用。"
  [comps]
  (reduce + 0 (map #(count (:attempts %)) comps)))

(defn- ok-attempts
  [op]
  (filterv :ok? (:attempts op)))

(defn- all-ok?
  [op]
  (= (count (:attempts op)) (count (ok-attempts op))))

(defn- distinct-vals
  [xs]
  (vec (distinct xs)))

(defn- group-by-rid
  "按 `[f rid]` 分组；`rid` 取自 completion op 的 `:request`（invoke 时写入）。"
  [comps]
  (group-by (fn [op] [(:f op) (get-in op [:request :rid])]) comps))

;; ---------------------------------------------------------------------------
;; 四条断言
;; ---------------------------------------------------------------------------

(defn- put-violations
  "断言 (1)(2)：同一 rid 的 k 次 `Put` 必须返回同一个 revision 与同一个
  `prev_kv`；全部 `:ok` 时，重放后该 key 的 version 必须不超过本分组的
  `:expected-version`（新建 key 为 1；带 setup 写的分组为 2），且值等于所写值。"
  [{:keys [request attempts final] :as op}]
  (let [oks     (ok-attempts op)
        revs    (distinct-vals (keep :revision oks))
        prevs   (distinct-vals (map :prev-kv oks))
        rid     (:rid request)
        key     (:key request)
        val     (:val request)
        exp-ver (long (or (:expected-version request) 1))
        v       []
        v       (cond-> v
                  (> (count revs) 1)
                  (conj {:type :revision-advanced
                         :rid rid :key key
                         :revisions revs
                         :note "同一 request_id 的重放返回了不同 revision：请求被重复生效"})

                  (> (count prevs) 1)
                  (conj {:type :prev-kv-mismatch
                         :rid rid :key key
                         :prev-kvs prevs
                         :note "幂等命中必须返回首次执行的结果；prev_kv 不一致（含 F-02 的 nil 退化）"}))]
    (if (and (all-ok? op) (:ok? final) (seq (:attempts op)))
      (let [kv (:kv final)]
        (cond-> v
          (and kv (> (long (or (:version kv) 0)) exp-ver))
          (conj {:type :version-over-advance
                 :rid rid :key key
                 :version (:version kv)
                 :expected-version exp-ver
                 :replays (count (:attempts op))
                 :note "重放后 version 超过本分组应有的写入次数：同一 request_id 生效了多次"})

          (and kv (not= (:value kv) (str val)))
          (conj {:type :final-value-mismatch
                 :rid rid :key key
                 :expected (str val) :got (:value kv)})

          (nil? kv)
          (conj {:type :final-missing
                 :rid rid :key key
                 :note "写完立刻读不到该 key"})))
      v)))

(defn- delete-violations
  "断言 (1)(2)(4)：同一 rid 的 k 次 `Delete` 必须返回相同的 `deleted` 与
  `prev_kvs`；全部 `:ok` 时该 key 必须仍然不存在。"
  [{:keys [request attempts final] :as op}]
  (let [oks   (ok-attempts op)
        dels  (distinct-vals (keep :deleted oks))
        prevs (distinct-vals (map :prev-kvs oks))
        rid   (:rid request)
        key   (:key request)]
    (cond-> []
      (> (count dels) 1)
      (conj {:type :delete-count-mismatch
             :rid rid :key key :deleted dels
             :note "同一 request_id 的重放返回了不同 deleted：删除被重复执行"})

      (> (count prevs) 1)
      (conj {:type :delete-prev-kvs-mismatch
             :rid rid :key key :prev-kvs prevs
             :note "幂等命中必须返回首次执行的 prev_kvs"})

      (and (all-ok? op) (:ok? final) (seq (:attempts op)) (some? (:kv final)))
      (conj {:type :delete-lost
             :rid rid :key key
             :final (:kv final)
             :note "全部 delete 返回 :ok 之后该 key 仍然可读：删除丢失（P0）"}))))

(defn- range-delete-violations
  "断言 (3)：范围删的首次执行 `:ok` 之后，区间内新写入的 key 必须存活到重放
  结束；且重放的 `deleted` 必须等于首次执行（返回首次执行的结果）。"
  [{:keys [request attempts post-write-final] :as op}]
  (let [rid (:rid request)
        a1  (first attempts)
        a2  (second attempts)]
    (cond-> []
      ;; 只有首次执行确定成功时，才能断言重放不得再次生效；且只有收尾读
      ;; **成功且读不到** post-write key 时才算「被重放删掉」——读失败是
      ;; 另一回事（可能是集群不可用），单列一类，不算删除放大。
      (and (:ok? a1) (:ok? post-write-final) (nil? (:kv post-write-final)))
      (conj {:type :replay-deleted-new-write
             :rid rid
             :range [(:range-start request) (:range-end request)]
             :post-write (:post-write request)
             :attempt-1 a1
             :note "重放的范围删删掉了首次执行之后新写入的 key：重试放大破坏面"})

      (and (:ok? a1) (:ok? a2)
           (not= (:deleted a1) (:deleted a2)))
      (conj {:type :range-delete-count-mismatch
             :rid rid
             :deleted-1 (:deleted a1) :deleted-2 (:deleted a2)
             :note "幂等命中必须返回首次执行的 deleted"})

      (and (:ok? a1) (not (:ok? post-write-final)))
      (conj {:type :post-write-read-failed
             :rid rid
             :final post-write-final
             :note "区间内新写入的 key 在重放后读不到（读失败，无法判定）"}))))

;; ---------------------------------------------------------------------------
;; Checker
;; ---------------------------------------------------------------------------

(defn checker
  "T1.4 checker。opts：

    :min-replay-attempts  重放尝试样本门槛（默认 50，§5.1）；不足 → invalid
                          （`:reason :insufficient-sample`，计入 summary 不判绿）

  注意：门槛用 `or` 而不是 destructuring `:or` 解析。`:or` 只在键**缺失**时
  生效，而调用方（coord.clj）总是显式传入 `:min-replay-attempts nil`，于是
  `:or` 不会接管——门槛会变成 nil 并让 `>=` 抛 NPE（或静默失效）。
  同类问题见 F-09（gates.clj 的四个门槛参数）。"
  ([] (checker {}))
  ([{:keys [min-replay-attempts]}]
   (let [min-replay-attempts (or min-replay-attempts default-min-replay-attempts)]
     (reify checker/Checker
       (check [_ _test history _opts]
       (let [comps   (completions history)
             by-key  (group-by-rid comps)
             puts    (filterv #(= :idem-put (:f %)) comps)
             deletes (filterv #(= :idem-delete (:f %)) comps)
             rds     (filterv #(= :idem-range-delete (:f %)) comps)
             fails   (vec (concat (mapcat put-violations puts)
                                  (mapcat delete-violations deletes)
                                  (mapcat range-delete-violations rds)))
             attempts  (replay-attempts comps)
             groups    (count by-key)
             all-atts  (mapcat #(or (:attempts %) []) comps)
             ok-atts   (count (filter :ok? all-atts))
             err-atts  (frequencies (keep #(when-not (:ok? %) (or (:kind %) :unknown))
                                          all-atts))
             ;; 「可判定」的分组：全部重放尝试都 :ok（首次尝试可能是 :info，
             ;; 此时重放再次生效合法，不能判生效次数）。
             judged    (count (filter all-ok? comps))
             by-class  (frequencies (map :type fails))
             sample-ok? (>= (long attempts) (long min-replay-attempts))
             stats   {:groups              groups
                      :completion-ops      (count comps)
                      :replay-attempts     attempts
                      :min-replay-attempts min-replay-attempts
                      :judged-groups       judged
                      :attempt-ok          ok-atts
                      :attempt-errors      err-atts
                      :violations-by-class by-class
                      :put-groups          (count puts)
                      :delete-groups       (count deletes)
                      :range-delete-groups (count rds)}
             reasons (cond-> []
                       (not sample-ok?) (conj {:type :insufficient-sample
                                               :attempts attempts
                                               :required min-replay-attempts
                                               :note "样本不足：该 cell 视为未执行（§5.1），不得判绿"})
                       (seq fails) (conj {:type :idempotency-violations
                                          :count (count fails)
                                          ;; 前 3 条明细放进 reason：fixture 运行器
                                          ;; 只打印 :failures/:soak/:gates，明细放这里
                                          ;; 才能被看见（results.edn 里同样可见）。
                                          :sample (vec (take 3 fails))}))
             valid?  (and sample-ok? (empty? fails))]
         (info "idem checker:" (pr-str (assoc stats :failures (take 10 fails))))
         (cond-> {:valid? valid?
                  :idem   stats
                  :failures reasons}
           (seq fails) (assoc :violations (vec (take 20 fails))))))))))
