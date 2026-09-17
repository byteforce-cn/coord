(ns jepsen.coord.mapck
  "T1.1 —— map / delete workload 的 checker（覆盖缺口 A1：Delete 未测）。

  ## 模型

  每个 key 是一个**寄存器**，值域 = {写值} ∪ {nil}，其中 `nil` 表示
  「key 不存在」。在这个模型里 `delete` 就是「写 nil（tombstone）」，
  于是：

    * 短矩阵用 knossos：把 `:delete` 重写成 `{:f :write :value nil}` 后按 key
      分组跑 `model/register`（线性一致性搜索；小样本可行）。
    * 长跑（8h/24h/72h）用 O(n log n) 写索引：`jepsen.coord.windex` 的三条
      判据（fabricated / future / stale）+ 本文件额外的 delete 语义断言。

  两者对 delete 的口径必须一致：**delete 的完成 = 该 key 的线性化点上
  「值变成 nil」**。因此「tombstone 之后读到旧值」在两边都判红（§5.1 的
  `tombstone 后读旧值 = 0`）。

  ## 除了三条判据，本 checker 还断言什么（都对应契约条款）

  1. **`DeleteResponse` 自洽**（`DeleteRequest.prev_kv` 承诺填充被删旧值）：
     `deleted` ∈ {0,1}（单键删）；请求了 `prev_kv` 时
     `count(prev_kvs) == deleted`；`deleted=0` 必须 `prev_kvs` 为空。
     违反 ⇒ `:delete-response-inconsistent`。
  2. **`prev_kvs` 是被删时刻的真实值**：请求了 `prev_kv` 且 `deleted=1` 时，
     `prev_kvs[0].value` 是**一次读**——它必须满足与 `:read` 完全相同的三条
     判据（不能是 fabricated / future / stale）。违反 ⇒ 与读同一类
     （`:fabricated` / `:future` / `:stale`，`op` 是那条 delete）。
     这条是「delete 返回的旧值必须真的是当时的当前值」的直接检验。
  3. **存在性（`Compare{VERSION, EQUAL, 0}`）**：`jepsen.coord` 的 map workload
     里有 `:exists` op —— 它用 `Txn{compare=[VERSION EQUAL 0]}` 精确回答
     「该 key 是否存在」（`coord-server/src/storage/mvcc.rs:1232`：
     不存在的 key（含软删除）实际 version 取 0），这比「读不到就当不存在」
     精确，也是 §9-⑨ 的落地判据：

       * `exists?=false` 就是一次 **nil 读**，按 nil 判据检查
         （确认写被强制夹在其后、本读之前 ⇒ 违反 `:stale`）；
       * `exists?=true` 若**从来没有**任何非 nil 写在该 op 完成之前被 invoke
         过，则不可能存在 ⇒ `:exists-fabricated`（P0 级：凭空报告存在）。

  ## 漏检边界（R2，必须声明）

  * 只判 `:ok` 的读与 `:ok` 的 delete；`:info` 的读/删不判（响应丢失，
    可判性不足）。
  * `deleted=0` 只做「响应自洽」检查，**不**断言「该 key 当时确实不存在」：
    后者需要排除该 delete 自己的 tombstone 才能做强制序判定，见
    `windex` 的 nil 判据；强行断言会误报（并发 tombstone 会让 nil 合法）。
  * 不做 knossos 式的全序搜索；本 checker 回答「这些属性是否被违反」，
    不回答「是否线性一致」。短矩阵的线性一致性仍由 knossos 路径负责。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]
            [knossos.model :as model]))

(def write-fs #{:write :delete})
(def nil-fs   #{:delete})

;; ---------------------------------------------------------------------------
;; 短矩阵：knossos（delete 建模为 write nil）
;; ---------------------------------------------------------------------------

(defn- to-register-op
  "把 map workload 的 op 转成 knossos 单寄存器模型能吃的形状。

  `:delete` → `:write` 且 `:value nil`（tombstone 建模，见 ns 注释）；
  `:exists` 之类不参与线性化搜索的 op 直接丢掉。"
  [op]
  (case (:f op)
    :read   op
    :write  op
    :delete (assoc op :f :write, :value nil)
    nil))

(defn- register-history
  "按 key 分组后的 knossos 子历史（丢掉与线性化无关的 op）。"
  [ops]
  (->> ops
       (map to-register-op)
       (remove nil?)
       vec))

(defn linear-checker
  "短矩阵 checker：每个 key 独立跑 knossos `model/register`（delete = write nil）。

  有效当且仅当**每个** key 的子历史都线性一致。

  注意：这里必须用**原始历史**分组（而不是 `windex/pair-invokes` 的结果）——
  knossos 的 `history/complete` 要求每个 completion 都能找到配对的 invoke op，
  而 pair-invokes 只保留 completion（它只补一个 `:invoke` 时刻给 O(n) 引擎用）。
  丢了 invoke op 会直接报「Process completed an operation without a prior
  invocation」（实测：`make test WORKLOAD=map` 第一次跑就报）。"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [data  (filterv #(and (contains? #{:read :write :delete} (:f %))
                                 (contains? #{:invoke :ok :info :fail} (:type %)))
                           history)
            by-key (group-by :key data)
            results (for [[k kops] by-key]
                      [k (checker/check
                           (checker/linearizable {:model (model/register)
                                                  :algorithm :wgl})
                           test
                           (register-history kops)
                           opts)])
            valid? (every? (fn [[_ r]] (:valid? r)) results)
            ok-ops (filterv #(contains? #{:ok :info :fail} (:type %)) data)]
        (into {:valid? valid?
               :map {:keys    (count results)
                     :ops     (count ok-ops)
                     :deletes (count (filter #(and (= :delete (:f %))
                                                   (= :ok (:type %)))
                                            ok-ops))}}
              (for [[k r] results]
                [(str "key-" k) {:valid? (:valid? r)}]))))))

;; ---------------------------------------------------------------------------
;; 长跑：写索引（O(n log n)）
;; ---------------------------------------------------------------------------

(defn- delete-shape-fails
  "断言 (1)：`DeleteResponse` 自洽性（单键 delete）。

  `:value` 是客户端记录的请求（map workload 的 delete op 请求里没有范围信息，
  即单键删）。"
  [{:keys [deleted prev-kvs prev-kv-requested?] :as op}]
  (cond-> []
    (not (contains? #{0 1} deleted))
    (conj {:type :delete-response-inconsistent
           :op op :deleted deleted
           :note "单键 delete 的 deleted 必须 ∈ {0,1}"})

    (and (contains? #{0 1} deleted)
         prev-kv-requested?
         (not= (count prev-kvs) deleted))
    (conj {:type :delete-response-inconsistent
           :op op :deleted deleted :prev-kvs (count prev-kvs)
           :note "请求了 prev_kv 时 count(prev_kvs) 必须等于 deleted"})

    (and (zero? (long (or deleted 0))) (seq prev-kvs))
    (conj {:type :delete-response-inconsistent
           :op op :deleted deleted :prev-kvs (vec prev-kvs)
           :note "deleted=0 时 prev_kvs 必须为空"})))

(defn- exists-fails
  "断言 (3)：`:exists` op（`Txn{compare=[VERSION EQUAL 0]}`）。

  * `exists?=false` → 一次 nil 读，走 `windex` 的 nil 判据（在 `index-checker`
    里作为第三个 pass 传入）；
  * `exists?=true` → 若在本 op **完成之前**从来没有非 nil 写被 invoke 过，
    则「存在」不可能成立。"
  [ops {:keys [entries]}]
  (let [non-nil (filterv #(some? (:value %)) entries)]
    (for [op ops
          :when (and (= :ok (:type op))
                     (= :exists (:f op))
                     (true? (:exists? op))
                     (not-any? (fn [e] (< (long (:invoke e)) (long (:time op))))
                               non-nil))]
      {:type :exists-fabricated
       :op op
       :note "报告 key 存在，但在本 op 完成之前从来没有非 nil 写被 invoke 过"})))

(defn- read-value
  "读 op 观察到的值。

  真实 run 里客户端把原样字节串放在 `:raw-value`（F-14：`parse-values?` 为
  false 时其实 `:value` 也是原样字符串，但两条都兼容更安全）；手写 fixture
  只带 `:value`。"
  [op]
  (if (contains? op :raw-value) (:raw-value op) (:value op)))

(defn- key-fails
  "单个 key 的全部违反。"
  [kops inflight]
  (let [idx    (wi/write-index kops inflight write-fs nil-fs)
        prefix (wi/confirmed-prefix (:confirmed idx))
        pprefix (wi/producer-prefix (:producers idx))
        ;; pass 1：普通 :read（值 = 读到的值，nil = 不存在）
        reads  (wi/check-reads kops idx prefix pprefix
                               #(= :read (:f %))
                               read-value)
        ;; pass 2：delete 返回的 prev_kv = 一次读（值 = prev_kvs[0].value）
        prevs  (wi/check-reads kops idx prefix pprefix
                               #(and (= :delete (:f %))
                                     (= 1 (:deleted %))
                                     (seq (:prev-kvs %)))
                               #(-> % :prev-kvs first :value))
        ;; pass 3：exists?=false 就是一次 nil 读
        exists (wi/check-reads kops idx prefix pprefix
                               #(and (= :exists (:f %)) (false? (:exists? %)))
                               (constantly nil))
        shapes (mapcat delete-shape-fails (filter #(= :delete (:f %)) kops))
        exists2 (exists-fails kops idx)]
    (concat reads prevs exists shapes exists2)))

(defn- index-checker
  "O(n log n) checker（长跑用）。"
  [opts]
  (let [min-deletes (long (or (:min-deletes opts) 0))]
    (reify checker/Checker
      (check [_ _test history _opts]
        (let [{:keys [ops inflight]} (wi/pair-invokes history)
              data   (filterv #(contains? #{:read :write :delete :exists} (:f %)) ops)
              by-key (group-by :key data)
              per-key (for [[k kops] by-key]
                        {:key k
                         :fails (vec (key-fails kops inflight))
                         :summary {:ops     (count kops)
                                   :reads   (count (filter #(= :read (:f %)) kops))
                                   :writes  (count (filter #(= :write (:f %)) kops))
                                   :deletes (count (filter #(= :delete (:f %)) kops))
                                   :exists  (count (filter #(= :exists (:f %)) kops))}})
              fails  (vec (mapcat :fails per-key))
              by-class (frequencies (map :type fails))
              ;; §5.1：delete 占比 ≥ 10% 是矩阵的样本门槛（默认关，矩阵按需打开）
              deletes (count (filter #(= :delete (:f %)) data))
              reads-ok (count (filter #(and (= :read (:f %)) (= :ok (:type %))) data))
              summary {:keys              (count per-key)
                       :ops               (count data)
                       :reads-ok          reads-ok
                       :writes-ok         (count (filter #(and (= :write (:f %))
                                                              (= :ok (:type %))) data))
                       :deletes-ok        (count (filter #(and (= :delete (:f %))
                                                              (= :ok (:type %))) data))
                       :exists-ok         (count (filter #(and (= :exists (:f %))
                                                              (= :ok (:type %))) data))
                       :delete-ratio      (if (pos? (count data))
                                            (double (/ deletes (count data)))
                                            0.0)
                       :min-deletes       min-deletes
                       :violations-by-class by-class
                       ;; §5.1 专属断言的可报数：tombstone 之后读到旧值 = 0
                       :stale-after-tombstone (count (filter #(and (= :stale (:type %))
                                                                   (:producer-tombstone? %))
                                                             fails))
                       :per-key           (into {}
                                                (map (fn [{:keys [key summary]}]
                                                       [key summary]))
                                                per-key)}
              sample-ok? (>= deletes min-deletes)
              valid? (and sample-ok? (empty? fails))
              reasons (cond-> []
                        (not sample-ok?)
                        (conj {:type :insufficient-sample
                               :deletes deletes :required min-deletes
                               :note "delete 样本不足：该 cell 视为未执行（§5.1），不得判绿"})
                        (seq fails)
                        (conj {:type :map-violations
                               :count (count fails)
                               :sample (vec (take 3 fails))}))]
          (info "map checker:" (pr-str (assoc summary :failures (take 10 fails))))
          (cond-> {:valid? valid?
                   :map    summary
                   :failures reasons}
            (seq fails) (assoc :violations (vec (take 20 fails)))))))))

;; ---------------------------------------------------------------------------
;; 入口
;; ---------------------------------------------------------------------------

(defn checker
  "T1.1 checker。opts：

    :mode          :linear（默认，knossos 短矩阵）| :index（O(n)，长跑）
    :min-deletes   §5.1 的 delete 样本门槛（默认 0 = 不判；矩阵验收时设为
                   实际要求的下限 ×0.1，见 dev.md §5.1）"
  ([] (checker {}))
  ([{:keys [mode min-deletes]}]
   (let [mode (or mode :linear)
         min-deletes (or min-deletes 0)]
     (when (not (contains? #{:linear :index} mode))
       (throw (ex-info (str "map checker: unknown mode " mode) {:mode mode})))
     (if (= :index mode)
       (index-checker {:min-deletes min-deletes})
       (linear-checker)))))
