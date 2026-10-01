(ns jepsen.coord.windex
  "按 key 的写索引引擎（T1.1 / T1.2 / T1.3 共用）。

  为什么需要它：knossos 的线性一致性搜索在 72h（十万级 op）历史上不可行，
  而 `jepsen.coord.soak` 已经用一套 O(n log n) 的写索引判据证明了自己能抓住
  coord 的陈旧读（2026-09-05 的 stale-read 缺陷）。T1.1（map/delete）与
  T1.2（txn）需要**同一套判据**，但要能表达 delete 与 tombstone，因此把
  soak 的三条判据抽成参数化引擎，语义逐条对齐（soak.clj 的判据注释仍然是
  权威说明，本文件只做泛化）：

    1. **fabricated** —— 读返回了一个从来没有任何写（含 `:info` 写）写过的值。
    2. **future**     —— 读返回的值，其写在本读**完成之前还没被 invoke**：
                        任何线性化都排不出这种顺序。
    3. **stale**      —— 有确认写被「强制」夹在该读观察到的值之后、该读之前：
                        该写在本读观察值的写**完成之后**才被 invoke（所以任何
                        线性化都把它排在其后），又在本读**开始之前**完成（所以
                        任何线性化都把它排在本读之前）—— 读到的值已被覆盖。

  泛化点（T1.1）：
    * 写操作的集合可配置（`write-fs`）：map workload 里 `:delete` 就是「写
      tombstone（nil）」，与 `:write` 一起进同一个索引；
    * nil 读（key 不存在）的判据从 soak 的「有写确认过就不许读 nil」推广为
      「**可能存在**的 nil 生产者（tombstone，含 `:info`/未完成）必须晚于所有
      被强制夹在本读之前的确认写」。tombstone 存在时 nil 是合法状态，这条
      推广是 T1.1「tombstone 之后读旧值 = 0」的判定基础。

  为什么用 `max invoke` 而不是「最新完成的值」（soak 的 P0-4）：写值按
  **invoke** 顺序分配，但**提交**顺序不必跟随 invoke 顺序 —— 客户端在
  UNAVAILABLE 后轮换/重发会让一个较小值的写更晚提交。所以「读必须返回最新
  完成写的值」是不可靠的（会把合法历史判红）；只有上面的「强制序」论断是
  可靠的。见 soak.clj 里 2026-09-05 的复现记录。

  漏检边界（R2，必须声明）：
    * 只判 `:ok` 的读；`:info` 读（响应丢失）不判。
    * `:info` 的 tombstone 被当作**可能生效**的 nil 生产者（这样只可能漏报、
      不会误报）：它若其实没生效，我们可能少抓一个 stale。
    * 不做历史回放式全序搜索；本引擎回答「这三条属性是否被违反」，
      不回答「是否线性一致」。"
  (:import (java.util Arrays)))

;; ---------------------------------------------------------------------------
;; invoke 配对
;; ---------------------------------------------------------------------------

(defn pair-invokes
  "历史 → `{:ops [带 :invoke 的 completion ...] :inflight [invoke ...]}`。

  客户端对每个 invoke 恰好产出一个 completion（重试在客户端内部完成、只汇报
  一次），所以按 `[process f]` 维护一个栈就够；缺 invoke（手工 fixture）时
  退化为 completion 时间。

  `:inflight` = 有 invoke 但从没完成的 op。未完成的 tombstone 可以被线性化到
  读之前，因此它影响 nil 合法性；不带出来就可能误报（见 `producer-prefix`）。"
  [history]
  (let [pending (atom {})
        done    (reduce
                  (fn [acc op]
                    (let [k [(:process op) (:f op)]]
                      (case (:type op)
                        :invoke
                        (do (swap! pending update k (fnil conj []) op)
                            acc)

                        (:ok :info :fail)
                        (let [stack (get @pending k)
                              inv   (peek stack)]
                          (when (seq stack)
                            (swap! pending update k pop))
                          (conj acc (assoc op :invoke
                                           (long (or (:time inv)
                                                     (:invoke op)
                                                     (:time op))))))

                        acc)))
                  []
                  history)
        inflight (vec (for [[_ ops] @pending, op ops] op))]
    {:ops done :inflight inflight}))

;; ---------------------------------------------------------------------------
;; 写索引
;; ---------------------------------------------------------------------------

(defn entry-of
  "规范化一个写/删完成 op 为索引条目。

  `:value` 为 nil 视为 tombstone（map workload 的 delete）。`tombstone?` 由
  `:f` 是否属于 `nil-fs` 决定，而不是看值是否 nil —— `:write` 空值（coord
  允许零长 value）与 delete 在 KV 层不是一回事，判定上要区分。"
  [op nil-fs]
  {:value      (:value op)
   :time       (:time op)
   :invoke     (long (or (:invoke op) (:time op)))
   :ok?        (= :ok (:type op))
   :tombstone? (contains? nil-fs (:f op))
   :f          (:f op)
   :process    (:process op)
   :op         op})

(defn write-index
  "把 `ops` 里所有写/删完成 op 编成索引。

    :entries   —— 全部条目（`:ok` 与 `:info` 都算：`:info` 的写可能已生效，
                  所以它的值可以被后续读合法观察到）
    :values    —— {非 nil 值 -> 条目}（fabricated 判据用）
    :confirmed —— 仅 `:ok` 的条目（强制序判据用；`:info` 不能作为「已生效」
                  的证据）
    :producers —— nil 可能的来源：`:ok`/`:info` 的 tombstone 条目 + 未完成的
                  tombstone invoke（有效完成时间视为 +∞）

  `write-fs` = 参与索引的 `:f` 集合（map workload = #{:write :delete}），
  `nil-fs`   = 其中值语义为「删除/tombstone」的子集（#{:delete}）。

  `inflight` 为 `pair-invokes` 返回的未完成 invoke 序列。"
  [paired-ops inflight write-fs nil-fs]
  (let [entries (->> paired-ops
                     (filter #(and (contains? #{:ok :info} (:type %))
                                   (contains? write-fs (:f %))))
                     (map #(entry-of % nil-fs))
                     (sort-by :time)
                     vec)
        inflight-tombs (->> inflight
                            (filter #(and (= :invoke (:type %))
                                          (contains? nil-fs (:f %))))
                            (map #(assoc (entry-of % nil-fs) :completed? false)))]
    {:entries   entries
     :values    (into {} (for [e entries
                               :when (some? (:value e))]
                           [(:value e) e]))
     :confirmed (filterv :ok? entries)
     ;; nil 判据的探针只能用**确认的非 nil 写**：tombstone 自己是 nil 的
     ;; 生产者，把它算进「被强制夹在本读之前的写」会把自己的完成时间拿来
     ;; 跟自己的 invoke 比，必然误报（写值判据里不存在这个问题，因为
     ;; w.invoke ≤ w.complete 使 w 自证无害）。
     :confirmed-non-nil (filterv #(and (:ok? %) (some? (:value %))) entries)
     :producers (concat (filterv :tombstone? entries)
                        inflight-tombs)}))

(defn confirmed-prefix
  "按完成时间排序的确认写前缀表，供二分查询：

    :times      —— 完成时间（升序）
    :vals       —— 与 :times 对齐的**值**（只对单寄存器有意义，map 用不到，
                   保留以便复用 soak 的口径/输出）
    :invoke-max —— invoke-max[i] = 前 i 个确认写里最大的 invoke 时间（强制序探针）"
  [confirmed]
  (let [[ts vals imax]
        (reduce (fn [[ts vals imax] e]
                  [(conj ts (:time e))
                   (conj vals (:value e))
                   (conj imax (max (long (or (peek imax) 0)) (:invoke e)))])
                [[] [] []]
                (sort-by :time confirmed))]
    {:times      (long-array ts)
     :vals       (object-array vals)
     :invoke-max (long-array imax)}))

(defn- last-at-or-before
  "完成时间 ≤ t 的最后一个元素下标（无则 -1）。"
  ^long [^longs times t]
  (if (zero? (alength times))
    -1
    (let [i (Arrays/binarySearch times (long t))]
      (if (neg? i)
        (- (- -1 i) 1)          ; insertion point - 1
        i))))

(defn latest-before
  "完成时间 ≤ t 的最后一个确认写的**值**（无则 nil）。报告用（`:expected`）。"
  [{:keys [times vals]} t]
  (let [i (last-at-or-before times t)]
    (when (>= i 0)
      (aget ^objects vals i))))

(defn max-invoke-before
  "完成时间 ≤ t 的确认写里最大的 **invoke** 时间（无则 0）。强制序探针：
  它大于某个值的写完成时间 ⇒ 有确认写在该值提交之后才被 invoke，却在该读开始
  之前完成了 ⇒ 读不可能还返回那个旧值。"
  [{:keys [times invoke-max]} t]
  (let [i (last-at-or-before times t)]
    (if (>= i 0) (aget ^longs invoke-max i) 0)))

(defn producer-prefix
  "nil 生产者前缀表：

    :invoke-times —— 生产者（tombstone）的 invoke 时间（升序）
    :eff-max      —— eff-max[i] = 前 i 个生产者里**最大**的「有效完成时间」，
                     其中未完成的生产者取 +∞（Long/MAX_VALUE）：未完成的
                     tombstone 总可以被线性化到本读之前，所以它能让 nil 合法。

  查询语义（见 `nil-legality-threshold`）。"
  [producers]
  (let [sorted (sort-by :invoke producers)
        ts     (mapv :invoke sorted)
        ;; emax 长度 = n+1：emax[i] = 前 i 个生产者的最大有效完成时间
        emax   (reductions (fn [acc e]
                             (max (long acc)
                                  (if (false? (:completed? e))
                                    Long/MAX_VALUE
                                    (long (:time e)))))
                           0
                           sorted)]
    {:invoke-times (long-array ts)
     :eff-max      (long-array (vec emax))}))

(defn nil-legality-threshold
  "在本读之前「可能生效」的 nil 生产者里，最大的有效完成时间（无则 0）。

  生产者 p 可用的前提是 `p.invoke <= read-complete`（否则无法被线性化到本读
  之前）。返回的最大有效完成时间就是阈值 `thr`：若还有确认写在 `thr` 之后才
  被 invoke、又在本读开始前完成（即强制序探针 `F > thr`），那么这些写被强制
  排在本读之前且排在所有 nil 生产者之后 ⇒ 状态非 nil ⇒ 读返回 nil 违法。

  `read-complete` 传 nil（无完成时间）时视为 +∞。"
  [{:keys [invoke-times eff-max]} read-complete]
  (let [^longs ts invoke-times
        n (alength ts)]
    (if (zero? n)
      0
      (let [t   (long (or read-complete Long/MAX_VALUE))
            i   (Arrays/binarySearch ts t)
            cnt (if (neg? i) (- -1 i) (inc i))]  ; invoke ≤ t 的生产者个数
        (aget ^longs eff-max cnt)))))

;; ---------------------------------------------------------------------------
;; 读判据
;; ---------------------------------------------------------------------------

(defn check-reads
  "检查一组读（同一 key 的子历史）是否违反 fabricated / future / stale。

  `ops` 是该 key 的全部数据 op（含写/删/读），`idx`/`prefix`/`pprefix` 为上面
  的索引结构；`read?` 判定 op 是否是一条读，`read-value-of` 从读 op 取出
  「观察到的值」（nil = 「不存在 / tombstone」）。

  返回失败向量（空 = 无违反）。只判 `:ok` 的读。"
  [ops {:keys [values] :as idx} prefix pprefix read? read-value-of]
  (let [fails (atom [])]
    (doseq [op ops]
      (when (and (= :ok (:type op))
                 (some? (:invoke op))
                 (read? op))
        (let [v       (read-value-of op)
              done    (:time op)
              inv     (:invoke op)
              m       (latest-before prefix inv)
              forced  (max-invoke-before prefix inv)
              ;; nil 分支的探针：只算确认的非 nil 写（见 write-index）
              forced-nn (max-invoke-before
                          (confirmed-prefix (:confirmed-non-nil idx)) inv)
              w       (get values v)
              nilt?   (nil? v)
              nilthr  (when nilt? (nil-legality-threshold pprefix done))
              nil-bad? (and nilt? (> (long forced-nn) (long (or nilthr 0))))]
          (cond
            ;; 初始/空状态：nil 只有在「没有任何确认的非 nil 写被强制夹在
            ;; 本读之前」时才合法；有 tombstone 时阈值抬高到该 tombstone 的
            ;; 有效完成时间。
            nil-bad?
            (swap! fails conj
                   {:type :stale :op op :expected m :got nil
                    :forced-write-invoke forced-nn
                    :nil-threshold nilthr
                    :note "读返回 nil（key 不存在），但有确认写被强制夹在其后、本读之前"})

            ;; 值没人写过。
            (and (not nilt?) (nil? w))
            (swap! fails conj
                   {:type :fabricated :op op
                    :note "读返回了一个从未被任何写（含 :info 写）写过的值"})

            ;; 值所属的写在本读完成之后才被 invoke。
            (and (not nilt?) (< (long done) (long (or (:invoke w) (:time w)))))
            (swap! fails conj
                   {:type :future :op op
                    :write-invoke   (:invoke w)
                    :write-complete (:time w)})

            ;; 值已被后续确认写（含 tombstone）覆盖。
            (and (not nilt?) (> (long forced) (long (:time w))))
            (swap! fails conj
                   {:type :stale :op op :expected m :got v
                    :write-complete      (:time w)
                    :forced-write-invoke forced
                    :producer-tombstone? (:tombstone? w)
                    :note "读到的值已被后续确认写覆盖（tombstone 覆盖旧值 = delete 未生效）"})))))
    @fails))
