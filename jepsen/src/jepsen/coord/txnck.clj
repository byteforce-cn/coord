(ns jepsen.coord.txnck
  "T1.2 —— txn 全形态 checker（覆盖缺口 A3：Txn 只测了约 5% 的形态）。

  ## 契约（`apis/contracts/proto/coord/kv/txn.proto` + dev.md §5.1）

  一次 txn 是一个**原子**的「比较 → 选分支 → 执行分支」：

    1. **无副作用** —— `succeeded=false` 时，**成功分支**的写一个都不许生效，
       永远不许出现在任何后续 `:ok` 读里（`:failure-branch-leak`，P0）。
       反向同理：`succeeded=false` 且失败分支含写时，**失败分支必须真的执行**
       （`:failure-branch-not-executed`）。
    2. **原子可见** —— 对写集 W 的 txn：任何 `:ok` 读要么完整看到 W（若它
       发生在 txn 完成之后），要么一个都看不到；**部分可见**（半应用）即
       `:txn-partial-visibility`（P0）。这是「多 key 写集不得被拆开观察」的
       判定，也直接覆盖 split/region 迁移下的原子性风险。
    3. **成功必可见** —— `succeeded=true` 且回读成功时，写集的每个 key 都必须
       读回写进去的值（`:txn-lost-write`，P0：提案返回成功但效果丢失）。
    4. **create-if-absent 的存在性语义** —— `Compare{VERSION, EQUAL, 0}` 在全
       **新** key 上必须成立（`:create-absent-failed`）。这条同时验证
       `mvcc.rs` 的「不存在 ⇒ version = 0」语义（§9-⑨）。
    5. **原子性在每个 key 上的投影** —— 把 txn 的写集当成该 key 的一次写，
       用 `jepsen.coord.windex` 的 fabricated / future / stale 判据检查所有
       「对 key 的观察」（点读、txn 内读、回读、delete 的 prev_kv）。
       这条抓的是：「读到了 txn 之前的旧值，而该 txn 已被强制排在该读之前」
       —— 即 §5.1 的「完成时间更晚的任何 :ok 读若读 W 中 key，不得看到 T
       之前的值」。

  ## 写集与观察从哪来

  客户端把每次 txn 的 `succeeded`、**实际生效**的写集 `:wrote`（失败时是
  失败分支）、`setup`（compare 用的旧值写）、以及回读 `:post-read` 全部写进
  completion op（`jepsen.coord.client/invoke-txn*`）。checker 不需要猜分支，
  也不需要解析 `TxnResponse.responses` —— 它只按 op 汇报的事实判定。

  响应丢失（`:info`）的 txn 无法归因分支，只能贡献「**可能**生效的写集」
  `:wrote-possible`：它们可以作为「读到这个值是合法的」依据（避免假红），
  但绝不作为「必须可见」的依据。

  ## 漏检边界（R2）

  * 只判 `:ok` 的 txn 与 `:ok` 的读；`:info`/`:fail` 的 completion 不判。
  * txn 的**隔离级别**（如果有读快照语义）不在这里判：coord 的 txn 是
    「compare + 单点提交」，没有多版本快照读承诺；本 checker 只判上面 5 条。
  * 不判 `TxnResponse.responses` 的元素个数（`:response-count` 只进报告）：
    契约未冻结「每个 op 一个 ResponseOp」这一条，先当观察项跟进。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]))

(def txn-write-fs
  "参与写索引的 txn op 类型。"
  #{:txn-create :txn-write-set :txn-cas :txn-cas-delete})

(def ^:private synthetic-f
  "checker 内部合成的写 entry 的 `:f`（只用于索引，不是真实 op 名）。"
  :txn-write)

;; ---------------------------------------------------------------------------
;; 历史展开
;; ---------------------------------------------------------------------------

(defn- txn-completions
  [ops]
  (filterv #(and (contains? #{:ok :info :fail} (:type %))
                 (contains? txn-write-fs (:f %)))
           ops))

(defn- synth-write
  "一条 txn 写集里的 (key, value) → 合成写 entry。

  `:type` 决定它是否算「已生效」：`:ok` 的 txn 只有**实际执行的那个分支**算
  已生效；`:info` 的 txn 两边都可能生效 → `:info`（可以被读到，但不能作为
  「必须可见」的依据）。

  value 为 nil 表示该写是 delete（失败分支的 delete）→ tombstone。"
  [op k v]
  {:type (if (= :ok (:type op)) :ok :info)
   :f synthetic-f
   :key k
   :value v
   :tombstone? (nil? v)
   :time (:time op)
   :invoke (:invoke op)
   :process (:process op)})

(defn- synthetic-writes
  "历史里所有 txn 的写 → 合成写 op 序列（含 setup 写）。

  `:ok` 的 txn 只有**实际执行的分支**（`:wrote`）进索引；`:info`/`:fail` 的 txn
  两边都可能生效（`:wrote-possible`），以 `:info` 进索引（可以被读到，但不能
  作为「必须可见」的依据）。

  setup 写（compare 用的旧值）：客户端在同一个 op 内先写 setup 再发 txn，所以它
  必然早于 txn 的写完成。历史里只有 op 的 invoke/complete 两个时刻可用，这里把
  setup 放在 op.invoke 之前的两个刻度上 —— 这不是绝对时间，而是**保序**
  （setup 完成 < txn 写 invoke ≤ txn 写完成）。保序只能让「读到 setup 旧值」这类
  陈旧读更容易被抓到，不会把合法历史判红。"
  [ops]
  (vec
    (mapcat
      (fn [op]
        (let [wrote (if (= :ok (:type op))
                      (get op :wrote)
                      (get op :wrote-possible))]
          (concat
            (for [[k v] wrote] (synth-write op k v))
            (when-let [{:keys [key value]} (:setup op)]
              [(assoc (synth-write (if (= :ok (:type op))
                                     op
                                     (assoc op :type :info))
                                   key value)
                      :invoke (- (long (or (:invoke op) (:time op))) 2)
                      :time   (- (long (or (:invoke op) (:time op))) 1))]))))
      (txn-completions ops))))

(defn- obs
  "一条「对 key 的读观察」→ `{:key k :value v ...}`（可直接当读 op 喂给
  `windex/check-reads`）。"
  [op key value time invoke]
  {:type :ok :f :txn-obs :key key :value value
   :time time :invoke invoke :process (:process op) :src (:f op) :op op})

(defn- read-observations
  "历史里所有对 key 的**观察**（点读、txn 内读、txn 的点回读、区间回读的每个
  kv、delete 的 prev_kvs）。"
  [ops]
  (let [post (fn [op]
               (when-let [{:keys [kv kvs]} (:post-read op)]
                 (if kvs
                   (for [e kvs] (obs op (:key e) (:value e) (:time op) (:invoke op)))
                   ;; 点回读：读不到是合法的 nil 观察，要进判据
                   [(obs op (:key kv) (:value kv) (:time op) (:invoke op))])))]
    (vec
      (mapcat
        (fn [op]
          (case (:f op)
            :read     (when (= :ok (:type op))
                        [(obs op (or (:key op) ::register) (:value op)
                              (:time op) (:invoke op))])
            :txn-read (when (= :ok (:type op))
                        [(obs op (:key op) (:value op)
                              (:time op) (:invoke op))])
            (:txn-create :txn-write-set :txn-cas :txn-cas-delete)
            (concat (post op)
                    (for [kv (:prev-kvs op)]
                      (obs op (:key kv) (:value kv) (:time op) (:invoke op))))
            nil))
        ops))))

;; ---------------------------------------------------------------------------
;; 断言
;; ---------------------------------------------------------------------------

(defn- poisoned-values
  "失败 txn 的**成功分支**写值 —— 它们永远不许出现在任何读里。

  只收 `:ok` 的 txn（`:info` 的 txn 无法归因分支，收进来会把「可能生效」的
  值当成禁区而误报）。用 `:success-wrote` 而不是 `:wrote` 的补集：cas-delete
  的两个分支写**同一个 key**，只有分分支带（`:success-wrote` /
  `:failure-wrote`）才看得出来成功分支写了什么。"
  [ops]
  (into {}
        (for [op (txn-completions ops)
              :when (and (= :ok (:type op)) (false? (:succeeded op)))
              [k v] (get op :success-wrote)]
          [v {:key k :form (:f op) :op op}])))

(defn- leak-fails
  "断言 1：任何 `:ok` 读都不得观察到「失败 txn 成功分支」的值。"
  [ops]
  (let [poison (poisoned-values ops)
        seen (into {} (for [o (read-observations ops)] [(:value o) o]))]
    (for [[v info] poison
          :when (contains? seen v)]
      {:type :failure-branch-leak
       :value v
       :key (:key info)
       :observed-by (:src seen)
       :observed-at (:time seen)
       :note "succeeded=false 的 txn 的成功分支写值被读到了：失败分支的副作用泄漏（P0）"})))

(defn- success-fails
  "断言 2/3：成功 txn 的回读必须看到全部写集；看到严格非空子集 = 半应用可见。"
  [ops]
  (for [op (txn-completions ops)
        :when (and (= :ok (:type op))
                   (true? (:succeeded op))
                   (:post-read-ok? op)
                   (seq (:wrote op)))]
    (let [wrote (:wrote op)
          n     (count wrote)
          seen  (if-let [kvs (get-in op [:post-read :kvs])]
                  (into {} (for [e kvs] [(:key e) (:value e)]))
                  (let [kv (get-in op [:post-read :kv])]
                    (if kv {(:key kv) (:value kv)} {})))
          hit   (filter (fn [[k v]] (= (get seen k) v)) wrote)]
      (cond
        (= (count hit) n) nil
        (pos? (count hit))
        {:type :txn-partial-visibility
         :key (:key (:value op))
         :wrote wrote :observed (select-keys seen (map first wrote))
         :op op
         :note "多 key 写集只被观察到一个非空真子集：半应用可见（P0）"}
        :else
        {:type :txn-lost-write
         :key (:key (:value op))
         :wrote wrote :observed seen
         :op op
         :note "succeeded=true 且回读成功，但写集一个都没读到：提案成功而效果丢失（P0）"}))))

(defn- branch-fails
  "断言 1（反向）/4：分支行为。

  * `:txn-create` 在全新 key 上 `succeeded=false` → create-if-absent 语义异常；
  * `:txn-cas-delete` 的失败分支是 delete：`succeeded=false` 时回读必须看不到
    该 key（失败分支被执行了）；看到 = 失败分支被静默跳过。"
  [ops]
  (for [op (txn-completions ops)
        :when (and (= :ok (:type op)) (false? (:succeeded op))
                   (true? (:setup-ok? op)))]
    (case (:f op)
      :txn-create
      {:type :create-absent-failed
       :key (:key (:value op))
       :op op
       :note "全新 key 上 Compare{VERSION, EQUAL, 0} 不成立（不存在 ⇒ version=0 语义被破坏，或别的东西写了这个 key）"}

      :txn-cas-delete
      (let [kv (get-in op [:post-read :kv])]
        (when (and (:post-read-ok? op) (some? kv))
          {:type :failure-branch-not-executed
           :key (:key (:value op))
           :observed kv
           :op op
           :note "succeeded=false 的失败分支是 delete，但回读仍能看到该 key：失败分支没执行"}))

      nil)))

;; ---------------------------------------------------------------------------
;; Checker
;; ---------------------------------------------------------------------------

(defn checker
  "T1.2 checker（无参）。"
  ([] (checker {}))
  ([_opts]
   (reify checker/Checker
     (check [_ _test history _opts]
       (let [{:keys [ops]} (wi/pair-invokes history)
             writes  (synthetic-writes ops)
             reads   (read-observations ops)
             by-key  (fn [xs] (group-by :key xs))
             wkeys   (by-key writes)
             rkeys   (by-key reads)
             all-keys (set (concat (keys wkeys) (keys rkeys)))
             ;; 断言 5：逐 key 的 fabricated / future / stale
             per-key (for [k all-keys
                           :let [w (get wkeys k [])
                                 r (get rkeys k [])
                                 idx (wi/write-index w [] #{synthetic-f} #{})
                                 prefix (wi/confirmed-prefix (:confirmed idx))
                                 pprefix (wi/producer-prefix (:producers idx))]]
                       (wi/check-reads r idx prefix pprefix (constantly true) :value))
             register-fails (vec (mapcat identity per-key))
             fails (vec (concat register-fails
                                (remove nil? (leak-fails ops))
                                (remove nil? (success-fails ops))
                                (remove nil? (branch-fails ops))))
             txn-ops (txn-completions ops)
             by-class (frequencies (map :type fails))
             ;; `:txn-read` 不是写集 txn，单独统计（否则报告里永远是 0）
             treads (filterv #(and (= :txn-read (:f %))
                                   (contains? #{:ok :info :fail} (:type %)))
                             ops)
             ok? (fn [f] (filterv #(and (= f (:f %)) (= :ok (:type %))) txn-ops))
             summary {:txn-ops      (count txn-ops)
                      :by-form      (into {} (map (fn [op]
                                                    [(:f op)
                                                     (count (filter #(= (:f op) (:f %)) txn-ops))])
                                                  txn-ops))
                      :ok            (count (filter #(= :ok (:type %)) txn-ops))
                      :info          (count (filter #(= :info (:type %)) txn-ops))
                      :fail          (count (filter #(= :fail (:type %)) txn-ops))
                      :succeeded     (count (filter #(true? (:succeeded %)) txn-ops))
                      :branch-taken  {:success (count (filter #(true? (:succeeded %)) txn-ops))
                                      :failure (count (filter #(false? (:succeeded %)) txn-ops))}
                      :observations  (count reads)
                      :write-entries (count writes)
                      :txn-reads     (count treads)
                      :txn-reads-ok  (count (filter #(= :ok (:type %)) treads))
                      :response-counts (frequencies (keep :response-count txn-ops))
                      :violations-by-class by-class
                      :read-writes   {:ok (count (ok? :txn-write-set))
                                      :cas (count (ok? :txn-cas))
                                      :cas-delete (count (ok? :txn-cas-delete))
                                      :create (count (ok? :txn-create))
                                      :read (count (ok? :txn-read))}}
             reasons (when (seq fails)
                       [{:type :txn-violations
                         :count (count fails)
                         :sample (vec (take 3 fails))}])]
         (info "txn checker:" (pr-str (assoc summary :failures (take 10 fails))))
         (cond-> {:valid? (empty? fails)
                  :txn    summary
                  :failures (vec reasons)}
           (seq fails) (assoc :violations (vec (take 20 fails)))))))))
