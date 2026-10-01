(ns jepsen.coord.scanck
  "T1.3 —— scan（RangeRequest 全形态）与历史 revision 读的 checker
  （覆盖缺口 A2：Range 只测了单形态）。

  ## 断言的来源

  `apis/contracts/proto/coord/kv/kv.proto` 的 `RangeRequest`：

    * `key` / `range_end`：`[key, range_end)` 半开区间（`range_end` 为空 = 仅
      精确查询 `key`）；
    * `limit`：返回条数上限；
    * `revision`：指定历史 Revision 读（0 = 最新）；被压缩清理的历史读必须
      返回 `OUT_OF_RANGE`，**不得**静默返回错值（P0-7 的契约）；
    * `keys_only`：只返回 Key；
    * `count_only`：只返回 `count`（命中总数）。

  这些条款可以写成**确定性**断言（不需要线性化搜索）：

    1. `:scan-range-violation` —— 返回的 key 必须全部落在 `[key, range_end)`；
    2. `:scan-order-violation` —— 必须严格字典序递增（无重复、无乱序）；
    3. `:scan-limit-violation`  —— 条数不得超过 `limit`；
    4. `:scan-count-inconsistent` —— `count` 与 `kvs` 自洽：
       `count >= (count kvs)`；当 `(count kvs) < limit`（说明扫完了）时必须
       `count == (count kvs)`；
    5. `:scan-keys-only-violation` —— `keys_only=true` 时不得返回 value；
    6. `:scan-values-only-violation` —— `count_only=true` 时不得返回 kvs；
    7. `:read-at-mismatch` —— 历史 revision 读的**精确**断言：本客户端某次写
       的响应 revision 为 r，则读 r 必须拿回那次写写入的值（revision 就是该
       key 的 `mod_revision`）。读到别的值（含更新的值）= 历史读错答；
    8. 值层面的寄存器判据（`jepsen.coord.windex`）：scan / read-at 返回的每个
       (key, value) 也是一次观察，必须满足 fabricated / future / stale 三条。

  第 7 条是本文件里最强的一条：它不依赖时间戳容差，只依赖「写响应的 revision」
  与「按该 revision 读回的值」必须一致。

  ## 漏检边界（R2）

  * 值层判据只对**真的带了 value 的返回项**生效：`keys_only=true`（或服务端
    只回 Key）时 value 字段为空，那不是「key 不存在」的观察 —— 把它当 nil 读
    会直接误报（实测：假红 3/3），所以只统计在 `:unjudged-items`。
  * 「扫全了」只在 `(count kvs) < limit`（或未设 limit 且区间内 key 数固定）时
    可判；存在并发写时，`count` 会随写变化，此时只判 `count >= (count kvs)`。
  * 「区间内应有多少 key」不做精确断言（并发写/删下不可判定）；漏项由
    T1.1/T1.2 的逐 key 判据负责。
  * 压缩水印以下的历史读必须是 `OUT_OF_RANGE`：本 checker 把它记为
    `:compacted-reads` 计数（白名单 `:fail`），不判定其具体阈值（阈值是
    §9-③ 的待确认项）。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]))

(def ^:private scan-f :scan-item)

;; ---------------------------------------------------------------------------
;; 结构断言
;; ---------------------------------------------------------------------------

(defn- in-range?
  "`k` 是否落在本次 RangeRequest 的合法返回集里。

  `range_end` 为空 = 精确查询（只允许 key 本身）；非空 = 半开区间
  `[key, range_end)`。`RangeSemantics::of` 就是这两态（见 coord-core）。"
  [k start end]
  (if (or (nil? end) (= "" end))
    (= k start)
    (and (>= (compare k start) 0) (> (compare end k) 0))))

(defn- range-violations
  "断言 1/2/3/4/5/6：scan 的结构性质。

  注意：响应字段名叫 `count`，**不能**直接 `:keys` 解构 —— 那会遮蔽
  `clojure.core/count`，把响应里的数字当成函数调用（ClassCastException）。"
  [{:keys [value kvs] :as op}]
  (let [{:keys [key range-end limit keys-only count-only]} value
        rcount    (:count op)
        ks        (mapv :key kvs)
        ;; 「扫完了」只在给了 limit 且返回条数 < limit 时可判；否则区间里还可能
        ;; 有没返回的 key（并发写下也不可判定），此时只判 count ≥ 返回条数。
        complete? (and limit (< (count kvs) (long limit)))]
    (cond-> []
      (some #(not (in-range? % (str key) range-end)) ks)
      (conj {:type :scan-range-violation :op op :keys ks
             :range [key range-end]
             :note "scan 返回了区间外的 key"})

      (not= ks (distinct ks))
      (conj {:type :scan-order-violation :op op :keys ks
             :note "scan 返回了重复 key"})

      (not= ks (vec (sort ks)))
      (conj {:type :scan-order-violation :op op :keys ks
             :note "scan 返回的 key 不是字典序递增"})

      (and limit (> (count kvs) (long limit)))
      (conj {:type :scan-limit-violation :op op
             :limit limit :returned (count kvs)
             :note "scan 返回条数超过 limit"})

      (and (some? rcount) (< (long rcount) (count kvs)))
      (conj {:type :scan-count-inconsistent :op op
             :count rcount :returned (count kvs)
             :note "RangeResponse.count 小于实际返回条数"})

      (and (some? rcount) complete? (not count-only)
           (not= (long rcount) (count kvs)))
      (conj {:type :scan-count-inconsistent :op op
             :count rcount :returned (count kvs) :complete? complete?
             :note "已扫完（返回条数 < limit）但 count 与 kvs 条数不一致"})

      (and keys-only (some (comp seq :value) kvs))
      (conj {:type :scan-keys-only-violation :op op
             :kvs kvs
             :note "keys_only=true 却返回了 value"})

      (and count-only (seq kvs))
      (conj {:type :scan-values-only-violation :op op
             :kvs kvs
             :note "count_only=true 却返回了 kvs"}))))

(defn- read-at-violations
  "断言 7：历史 revision 读必须拿回该 revision 写入的值，且与「读它之前的
  那次最新点读」一致。

  `rev->value` = {revision → {:key k :value v}}（由写 op 的响应 revision 建）。
  只有「本 run 里见过该 revision 的写」时才判 revision 对应；`:latest-kv` 是
  客户端在无缓存 revision 时先做的点读（F-15），两者都有时一并断言。"
  [{:keys [value kvs read-revision latest-kv] :as op} rev->value]
  (let [{:keys [key]} value
        expected (get rev->value (long (or read-revision 0)))
        got      (first (filter #(= key (:key %)) kvs))]
    (cond-> []
      ;; 历史读必须与「同一次操作里先做的最新点读」一致
      (and latest-kv
           (not= (:value latest-kv) (:value got)))
      (conj {:type :read-at-latest-mismatch :op op
             :revision read-revision
             :latest latest-kv :got got
             :note "按自己刚读到的 mod_revision 读历史，拿回的值与最新点读不一致"})

      ;; 按「写响应 revision」对应时的不一致
      (and expected
           (nil? got))
      (conj {:type :read-at-mismatch :op op
             :revision read-revision :expected expected :got nil
             :note "按写响应 revision 读回，却读不到该 key（历史读丢数据）"})

      (and expected
           got
           (not= (:value got) (:value expected)))
      (conj {:type :read-at-mismatch :op op
             :revision read-revision :expected expected :got got
             :note "按 revision r 读回的值不等于 r 那次写写入的值（历史读错答 / 读到更新的值）"}))))

;; ---------------------------------------------------------------------------
;; Checker
;; ---------------------------------------------------------------------------

(defn checker
  "T1.3 checker（无参）。"
  ([] (checker {}))
  ([_opts]
   (reify checker/Checker
     (check [_ _test history _opts]
       (let [{:keys [ops]} (wi/pair-invokes history)
             data (filterv #(contains? #{:write :scan :read-at} (:f %)) ops)
             ;; 写索引：本 workload 的写都是普通 Put。**必须把 `:info`（响应丢失，
             ;; 可能已生效）的写也喂进索引** —— `write-index` 自己会用 `:confirmed`
             ;; 区分「已生效」与「可能生效」；只喂 `:ok` 会让后续读到那个值的读被
             ;; 判 `:fabricated`（实测：`--nemesis kill` 的 run 一次就假红）。
             writes (filterv #(and (= :write (:f %))
                                   (contains? #{:ok :info} (:type %)))
                             data)
             ok-writes (filterv #(= :ok (:type %)) writes)
             ;; 值层观察：只收**真的带了 value** 的返回项，而且**不包括**
             ;; `:read-at` —— 历史读返回的就是「那个 revision 当时的值」，用
             ;; 「读到的值是否已被后续确认写覆盖」的强制序判据去判它，必然把
             ;; 正确的历史读判成 stale（实测：一次 60s run 就假红）。历史读由
             ;; `read-at-violations` 的**精确 revision 对应**断言负责。
             ;; keys_only / count_only 也不返回 value，那不是「key 不存在」的观察。
             obs-of (fn [op]
                      (when-not (or (get-in op [:value :keys-only])
                                    (get-in op [:value :count-only]))
                        (for [e (:kvs op)
                              :when (and (string? (:value e))
                                         (seq (:value e)))]
                          {:type :ok :f scan-f :key (:key e) :value (:value e)
                           :time (:time op) :invoke (:invoke op)
                           :process (:process op) :op op})))
             scan-obs (vec (for [op data
                                 :when (and (= :scan (:f op)) (= :ok (:type op)))
                                 o (obs-of op)]
                             o))
             rat-obs  []
             unjudged (count (for [op data
                                   :when (and (= :scan (:f op)) (= :ok (:type op)))
                                   e (:kvs op)
                                   :when (or (nil? (:value e))
                                             (not (seq (:value e))))]
                               e))
             wbykey (group-by :key writes)
             obykey (group-by :key (concat scan-obs rat-obs))
             all-keys (set (concat (keys wbykey) (keys obykey)))
             value-fails
             (vec
               (mapcat
                 (fn [k]
                   (let [w      (get wbykey k [])
                         r      (get obykey k [])
                         idx    (wi/write-index w [] #{:write} #{})
                         prefix (wi/confirmed-prefix (:confirmed idx))
                         pprefix (wi/producer-prefix (:producers idx))]
                     (wi/check-reads r idx prefix pprefix (constantly true) :value)))
                 all-keys))
             ;; revision → 写值（read-at 的期望值来源）
             rev->value (into {}
                              (for [w ok-writes
                                    :when (pos? (long (or (:revision w) 0)))]
                                [(long (:revision w)) {:key (:key w) :value (:value w)}]))
             struct-fails (vec (mapcat range-violations
                                       (filter #(= :scan (:f %)) data)))
             rat-fails (vec (mapcat #(read-at-violations % rev->value)
                                    (filter #(= :read-at (:f %)) data)))
             fails (vec (concat struct-fails rat-fails value-fails))
             by-class (frequencies (map :type fails))
             scans (filterv #(= :scan (:f %)) data)
             rats  (filterv #(= :read-at (:f %)) data)
             keys-only (count (filter #(get-in % [:value :keys-only]) scans))
             count-only (count (filter #(get-in % [:value :count-only]) scans))
             summary {:scan-ops      (count scans)
                      :scan-ok       (count (filter #(= :ok (:type %)) scans))
                      :read-at-ops   (count rats)
                      :read-at-ok    (count (filter #(= :ok (:type %)) rats))
                      :writes-ok     (count ok-writes)
                      :scan-items    (count scan-obs)
                      :unjudged-items unjudged
                      :keys-only     keys-only
                      :count-only    count-only
                      :compacted-reads (count (filter #(and (= :read-at (:f %))
                                                            (= :fail (:type %))
                                                            (= :out-of-range
                                                               (:error %)))
                                                       data))
                      :violations-by-class by-class}
             reasons (when (seq fails)
                       [{:type :scan-violations
                         :count (count fails)
                         :sample (vec (take 3 fails))}])]
         (info "scan checker:" (pr-str (assoc summary :failures (take 10 fails))))
         (cond-> {:valid? (empty? fails)
                  :scan   summary
                  :failures (vec reasons)}
           (seq fails) (assoc :violations (vec (take 20 fails)))))))))
