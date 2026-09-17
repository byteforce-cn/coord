(ns jepsen.coord.watchck
  "T2.1 —— watch workload 的 checker（覆盖缺口 A5：Watch 未测）。

  ## 契约（`apis/contracts/proto/coord/watch/watch.proto`）

    * **至少一次投递**（at-least-once）：断线重连后以
      `start_revision = 已确认最大 revision + 1` 重建 Watch 即可续传；
    * **同一流内事件 revision 严格单调递增**（客户端可据此去重）；
    * 同一 Key 的事件按发生顺序投递；
    * 缓冲区溢出（慢消费者背压保护）时必须**显式**发 `BUFFER_OVERFLOW`
      事件（客户端据其 revision 做 Range 全量重同步）；
    * 请求的 `start_revision` 历史已被压缩清理 ⇒ `HISTORY_UNAVAILABLE`。

  F-06 已判（`watch/mod.rs:228/267/90`）：实现是「缓冲区满**丢最旧** + 合成
  `BufferOverflow` + 订阅者按 revision 去重」。所以 checker 的默认语义是
  **overflow-marker**：事件序列要么完整，要么在缺口处出现过
  `BUFFER_OVERFLOW` / `HISTORY_UNAVAILABLE` 标记 —— coalescing / lossless
  两态是可参数化的**对照**口径（源码为准，见 dev.md §5.4-⑥）。

  ## 判据

  1. `:watch-order-violation` —— 同一会话内事件 revision 不严格递增 /
     `PUT`/`DELETE` 事件缺 `kvs` / 事件类型不在契约内；
  2. `:watch-event-before-start` —— `start-revision = R > 0` 的会话收到
     revision < R 的事件（resume 起点错，会**重复投递已确认段**）；
  3. `:watch-fabricated` —— 事件里的值从未被任何写（含 `:info` 写）写过；
  4. `:watch-event-loss`（**P0**）—— **确认写**的 revision 落在会话的有效区间
     内却没有作为事件出现，且缺口之后没有出现过溢出/历史标记（**静默丢事件**：
     客户端按契约既收不到事件、也收不到「去重同步」的提示）；
  5. 样本门槛：全会话事件总数 < `--watch-min-events`（默认 200）⇒ invalid
     （§5.1「每 watcher 事件 ≥ 200」；未执行既不算绿也不算红）。

  ## 有效区间怎么定（可判定性的关键）

  `start_revision = 0`（「从当前最新开始」）时，服务器**不告诉**客户端那一刻的
  revision，所以「应从哪个 revision 起必须收到事件」不可判定。做法是取
  **首个事件的 revision** 作为有效下界 `lower`：只要求「revision > lower 的
  确认写必须都出现」。这是**保守**的（可能漏报发生在会话刚打开那一瞬间的丢失），
  但绝不会把合法历史判红 —— 与 soak checker 的 P0-4 修正同一取向。
  `start_revision > 0` 时下界就是它（因此判据 2 可以严格判）。

  上界取会话汇报的 `:end-revision`（客户端收到的最后一个事件 revision）；写成
  revision ≤ 上界的都要求出现。

  ## 漏检边界（R2）

  * 「同一 Key 的事件按发生顺序投递」不做全序搜索：判据 1+4 是它的可判定近似
    （revision 序列的完整性 + 严格单调）。
  * `:info` 写（响应丢失、可能已生效）**不参与**判据 4（它可能根本没生效），
    但可以作为判据 3 的合法来源（F-17 口径）。
  * 事件到达的**实时性**（§5.2「最后一个已确认写 120s 内被观察到」）不在这里
    判：它需要「写完成 → 事件到达」的配对时间，属 T6 长跑门槛。
  * 会话**之内**的重复（同一 revision 出现两次）判红（契约要求严格递增）；
    **跨会话**的重复是 at-least-once 允许的，不判。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]))

(def default-min-events
  "§5.1：一个 watch run 至少要观察到的事件数（不足视为未执行）。"
  200)

(def ^:private markers
  "契约里的两个「空洞标记」事件类型。"
  #{:buffer-overflow :history-unavailable})

(def ^:private data-types
  "带 kvs 的事件类型。"
  #{:put :delete})

(defn- sessions
  "历史里的 watch 会话完成 op（任何一种 completion 都收：`:fail` 的会话也有
  诊断价值，但不参与判据 1/2/4）。"
  [ops]
  (filterv #(contains? #{:watch-session} (:f %)) ops))

(defn- confirmed-write-revisions
  "被监听 key 的**确认写** revision 集合（`{\"key\" #{rev ...}}`）。

  只用 `:ok` 的写：`:info` 写可能没生效，拿它当「必须出现」的依据会误报
  （F-17 口径）。没有 `:revision` 的写（响应缺字段）跳过。"
  [ops]
  (->> ops
       (filter #(and (= :write (:f %))
                     (= :ok (:type %))
                     (some? (:key %))
                     (pos? (long (or (:revision %) 0)))))
       (reduce (fn [m op]
                 (update m (:key op) (fnil conj #{}) (long (:revision op))))
               {})))

(defn- structure-fails
  "判据 1 + 3：流内结构 + 值层。"
  [op values]
  (let [evs  (:events op)
        revs (mapv (fn [e] (long (or (:revision e) 0))) evs)
        fabricated
        (vec (for [e evs
                   :when (contains? data-types (:type e))
                   kv (:kvs e)
                   :when (and (some? (:value kv))
                              (not (contains? values (:value kv))))]
               {:type :watch-fabricated :op op :event e :kv kv
                :note "事件里的值从未被任何写（含 :info 写）写过"}))]
    (cond-> []
      (not= revs (vec (sort (distinct revs))))
      (conj {:type :watch-order-violation :op op :revisions revs
             :note "同一流内事件 revision 必须严格单调递增（契约：客户端据此去重）"})

      (some #(not (contains? (into markers data-types) (:type %))) evs)
      (conj {:type :watch-order-violation :op op
             :events (vec (remove #(contains? (into markers data-types) (:type %)) evs))
             :note "事件类型不在契约内"})

      (some #(and (contains? data-types (:type %)) (empty? (:kvs %))) evs)
      (conj {:type :watch-order-violation :op op
             :events (vec (filter #(and (contains? data-types (:type %))
                                        (empty? (:kvs %))) evs))
             :note "PUT/DELETE 事件必须携带 kvs"})

      (seq fabricated)
      (into fabricated))))

(defn- concrete-start
  "会话**实际使用**的起始 revision。

  客户端把具体值记在 op 的 `:start-revision` 上（请求可能是 `:last`，即「取本
  客户端上次观察到的最大 revision」——那是个非数字值，不能直接当门槛用）。
  手写 fixture 只给请求里的数字时从 `:value` 回退。"
  [op]
  (let [v (:start-revision op)
        r (get-in op [:value :start-revision])]
    (long (cond
            (number? v) v
            (number? r) r
            :else 0))))

(defn- before-start-fails
  "判据 2：resume 起点。`start_revision = R > 0` 时不允许 revision < R 的事件。"
  [op]
  (let [start (concrete-start op)]
    (when (pos? start)
      (let [early (filterv #(< (long (or (:revision %) 0)) start) (:events op))]
        (when (seq early)
          {:type :watch-event-before-start
           :op op :start-revision start :events early
           :note "resume 会话收到了 start_revision 之前的事件（重复投递已确认段）"})))))

(defn- loss-fails
  "判据 4：静默丢事件。

  对每个会话：
    * `lower` = `start-revision`（>0）或首个事件的 revision（=0 时的保守下界）；
    * `upper` = 会话汇报的 `:end-revision`；
    * 要求 (lower, upper] 内的**确认写** revision 都作为事件出现；
    * 缺口处（第一个确实的 revision）之后必须存在标记事件（`marker.revision ≥
      缺失 revision`）——「丢最旧 + 合成 BufferOverflow」的语义下，标记的
      revision 是该空洞的重新同步起点，必 ≥ 被丢事件。
  `semantics`：`:overflow-marker`（默认，契约口径）/ `:lossless`（不允许任何
  缺口，连标记也不能豁免）/ `:coalescing`（允许合并丢事件，只保留结构判据）。"
  [op by-key semantics]
  (when-not (= :coalescing semantics)
    (let [evs        (:events op)
          key        (or (get-in op [:value :key]) (:key op))
          start      (concrete-start op)
          revs       (vec (map (fn [e] (long (or (:revision e) 0))) evs))
          ;; 覆盖区间的**上界只能从事件自推**（最后一个事件的 revision），不能读
          ;; 客户端汇报的 `:end-revision`：那个字段曾是「最后一次尝试的起点」的
          ;; 别名，让一个**完全没有事件**的会话看起来覆盖了 (start, start+2]，
          ;; 于是把区间内所有确认写都判成「丢失」（F-21 假红）。没有事件 ⇒ 没有
          ;; 覆盖区间 ⇒ 不判。
          upper      (long (or (peek revs) 0))
          lower      (if (pos? start) start (long (or (first revs) 0)))
          required   (filter #(and (> (long %) lower) (<= (long %) upper))
                             (get by-key key))
          missing    (sort (remove (set revs) required))
          marker-revs (map (fn [e] (long (or (:revision e) 0)))
                           (filter #(contains? markers (:type %)) evs))]
      (when (and (seq evs) (seq missing))
        (let [first-missing (long (first missing))
              explained?   (and (= :overflow-marker semantics)
                                (some #(>= (long %) first-missing) marker-revs))]
          (when-not explained?
            {:type :watch-event-loss
             :op op :key key
             :start-revision start :end-revision upper
             :missing (vec (take 20 missing))
             :missing-count (count missing)
             :marker-revisions (vec marker-revs)
             :semantics semantics
             :note "确认写的 revision 落在会话有效区间内却没有任何事件，且缺口之后没有溢出/历史标记：静默丢事件（P0）"}))))))

(defn checker
  "T2.1 checker。opts：

    :semantics    `:overflow-marker`（默认）/ `:lossless` / `:coalescing`
    :min-events   全会话事件总数门槛（默认 200，§5.1）"
  ([] (checker {}))
  ([{:keys [semantics min-events]}]
   (let [semantics (or semantics :overflow-marker)
         min-events (long (or min-events default-min-events))]
     (when-not (contains? #{:overflow-marker :lossless :coalescing} semantics)
       (throw (ex-info (str "watch checker: unknown semantics " semantics)
                       {:semantics semantics})))
     (reify checker/Checker
       (check [_ _test history _opts]
         (let [{:keys [ops]} (wi/pair-invokes history)
               sops   (sessions ops)
               done   (filterv #(contains? #{:ok :info :fail} (:type %)) sops)
               ;; 合法值的来源：所有写（含 :info）——「事件里的值被写过」即可
               writes (filterv #(and (= :write (:f %))
                                     (contains? #{:ok :info} (:type %))
                                     (some? (:value %)))
                               ops)
               values (set (map :value writes))
               by-key (confirmed-write-revisions ops)
               fails  (vec (concat
                             (mapcat #(structure-fails % values) done)
                             (remove nil? (map before-start-fails done))
                             (remove nil? (map #(loss-fails % by-key semantics) done))))
               by-class (frequencies (map :type fails))
               total-events (reduce + 0 (map (fn [o] (count (:events o))) done))
               sessions-with-events (count (filter #(seq (:events %)) done))
               stream-errors (reduce + 0 (map (fn [o] (count (:stream-errors o))) done))
               resumes (reduce + 0 (map (fn [o] (long (or (:resumes o) 0))) done))
               markers-seen (reduce + 0 (map (fn [o] (count (filter #(contains? markers (:type %))
                                                                    (:events o))))
                                             done))
               sample-ok? (>= total-events min-events)
               summary {:sessions       (count done)
                        :sessions-with-events sessions-with-events
                        :events         total-events
                        :min-events     min-events
                        :markers        markers-seen
                        :stream-errors  stream-errors
                        :resumes        resumes
                        :writes-ok      (count (filter #(= :ok (:type %)) writes))
                        :violations-by-class by-class
                        :semantics      semantics}
               reasons (cond-> []
                         (not sample-ok?)
                         (conj {:type :insufficient-sample
                                :events total-events :required min-events
                                :note "watch 事件样本不足：该 cell 视为未执行（§5.1），不得判绿"})
                         (seq fails)
                         (conj {:type :watch-violations
                                :count (count fails)
                                :sample (vec (take 3 fails))}))]
           (info "watch checker:" (pr-str (assoc summary :failures (take 10 fails))))
           (cond-> {:valid? (and sample-ok? (empty? fails))
                    :watch  summary
                    :failures reasons}
             (seq fails) (assoc :violations (vec (take 20 fails))))))))))

