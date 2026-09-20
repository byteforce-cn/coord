(ns jepsen.coord.mqck
  "M5b —— agent 本地消息队列面的 checker（缺口 AG-11：at-least-once）。

  ## 被测对象与**前提**

  `coord-agent/src/services/mq.rs` 的 MQ 是 **agent 本地**日志（可选 ISR 复制）。
  与 cache 同一条前提：多 agent 下同一个 topic 在不同 agent 上是**各自独立的
  日志**，所以「发布后必须能被投递」只在单 agent 拓扑里可判 —— `coord.clj` 会在
  `--workload mq` 且 `--agents > 1` 时拒绝起跑。

  用 **Poll + Ack**（unary）而不是流式 Subscribe：契约自己的注释写明
  「业务侧 poll + ack 即得 at-least-once」，所以 unary 面就足以证伪
  「丢消息 / 重复投递 / offset 回退」这三类承诺，不必引入新的流式读取器。

  ## 判据（全部以「已确认的发布」和「实际投递过的 offset」为证据）

  1. `:mq-silent-loss`（**P0**）—— 已确认发布的消息**被跳过**：
     * a) 某个 Poll 的 `start_offset = s` 返回了 offset > s 的消息，但 `s`
       已被某次发布确认过（或用 start < s 的 Poll 覆盖过）却从没被投递过；
     * b) 更一般的形态：`o` 已确认发布，某次 `start_offset ≤ o` 的 Poll 返回了
       `offset > o` 的消息，而 `o` 在整个历史里从没被任何 Poll 投递。
  2. `:mq-payload-mismatch`（**P0**）—— 投递的 payload 与「该 offset 上已确认
     发布的 payload」不一致（日志把内容串了/截断了）。
  3. `:mq-dup-offset`（**P0**）—— 两次已确认的发布拿到**同一个 offset**
     （broker 分配失败 ⇒ 一条消息被覆盖）。
  4. `:mq-dup-in-response`（**P1**）—— 同一个 Poll 响应里同一 offset 出现多次。
  5. `:mq-redelivered-after-ack`（**P1**）—— 已 Ack 的 offset 又被投递。
     at-least-once 允许**未确认**消息重投（这是合法语义），但已确认的重投说明
     Ack 没被记住（消费者会把业务重放一次）。
  6. `:mq-out-of-order`（**P1**）—— 单个 Poll 响应内 offset 不是严格递增。
  7. 样本门槛 —— 已确认发布数 < `:min-publishes` 即 **invalid**（未执行，不是绿）。

  ## 漏检边界（R2）

  * **流式 Subscribe 未覆盖**（订阅端「不丢」的判据在 Poll 面上是等价的，但
    push 侧的背压行为没有观测）；
  * **消费者组再平衡 / 多消费者**未覆盖（本 checker 只跑单消费者）；
  * **DLQ（PollDlq）与毒消息**未覆盖；
  * **ISR 复制的跨 agent 面**未覆盖（同 cache：需要 agent 间可达）。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]))

(defn- start-ns [op] (long (or (:t0-ns op) (:invoke op) (:time op) 0)))
;; 同 cacheck：用**绝对**完成时刻，避免与 `:t0-ns` 混用两个时间轴（jepsen 的
;; `:time` 是相对测试起点的量）。fixture 没有 `:done-ns` 时退到 `:time`
;; （fixture 自己用同一轴写）。
(defn- end-ns [op] (long (or (:done-ns op) (:time op) 0)))
(defn- tp [op] [(:topic op) (long (or (:partition op) 0))])

(defn- publishes [ops] (filterv #(= :mq-publish (:f %)) ops))
(defn- polls [ops] (filterv #(= :mq-poll (:f %)) ops))

(defn- ack-events
  "把 poll op 内部 Confirm 的 offset 展开成 `{[topic partition offset] ack-毫秒}`。

  Ack 发生在 poll 响应之后、op completion 之前，所以用 completion 时刻当上界是
  **保守**的：判「已确认的又被重投」时只会漏报、不会误报。

  同一个 offset 可能被多次 Ack（重复投递后的再次确认，完全合法）——这里保留
  **最早**那一次：判据问的是「有没有在某个时刻**已经**确认过」，用最后一次会把
  确认时刻往后推，从而漏掉「第一次确认之后又被投递」的形态（实测：那条
  fixture 就是因为这个 false negative 判成了 valid）。"
  [ops]
  (reduce (fn [m p]
            (let [t (quot (end-ns p) 1000000)]              (reduce (fn [m o]
                        (let [k (conj [( :topic p) (long (or (:partition p) 0))]
                                      (long o))
                              cur (get m k)]
                          (if (and cur (<= (long cur) t)) m (assoc m k t))))
                      m (:acked-offsets p))))
          {} (filterv #(= :ok (:type %)) (polls ops))))

(defn- delivered
  "所有被投递过的 `[topic partition offset]` → 第一次投递它的 poll op。"
  [ops]
  (reduce (fn [m p]
            (reduce (fn [m msg]
                      (if (contains? m [(:topic msg) (long (:partition msg))
                                        (long (:offset msg))])
                        m
                        (assoc m [(:topic msg) (long (:partition msg))
                                  (long (:offset msg))] p)))
                    m (:messages p)))
          {} (polls ops)))

;; --------------------------------------------------------------------------

(defn- silent-loss
  "判据 1：已确认的发布从未被投递，且有 Poll 的游标跃过了它。

  两类证据（都要求「游标确实越过了 o」）：
    a) 某次 Poll 的 start_offset ≤ o，且它的响应里有 offset > o 的消息；
    b) 某次 Poll 的 start_offset = o+1（消费者已经确认处理到 o）——这只能由
       Ack 推进游标而来，所以「o 从未被投递」与「游标跳过 o」矛盾。
  证据不足（没有任何 Poll 越过 o）时不判：长跑末尾刚发布的消息本来就可能还在
  in-flight（宁可漏报，不产假红）。"
  [ops]
  (let [pubs (publishes ops)
        dlv  (delivered ops)
        ps   (polls ops)
        crossed? (fn [p o]
                   (let [offs (map :offset (:messages p))]
                     (or (and (seq offs) (> (long (apply max offs)) (long o)))
                         (> (long (:start-offset p 0)) (long o)))))]
    (->> pubs
         (filter #(= :ok (:type %)))
         (keep (fn [pub]
                 (let [k (conj (tp pub) (long (:offset pub)))
                       o (long (:offset pub))]
                   (when (and (not (contains? dlv k))
                              (some #(and (= (tp %) (tp pub)) (crossed? % o)) ps))
                     {:type :mq-silent-loss
                      :topic (:topic pub) :partition (:partition pub) :offset o
                      :payload (:payload pub)
                      :note "已确认发布的消息从未被投递，而消费者的游标已经越过它（at-least-once 被破坏）"}))))
         vec)))

(defn- payload-mismatch
  "判据 2：投递内容的 payload 与该 offset 上已确认的发布不一致。"
  [ops]
  (let [by-offset (into {}
                        (for [p (publishes ops)
                              :when (and (= :ok (:type p)) (:payload p))]
                          [(conj (tp p) (long (:offset p))) (:payload p)]))]
    (->> (polls ops)
         (mapcat (fn [p]
                   (for [msg (:messages p)
                         :let [exp (get by-offset [( :topic msg)
                                                   (long (:partition msg))
                                                   (long (:offset msg))])]
                         :when (and exp (not= exp (:payload msg)))]
                     {:type :mq-payload-mismatch
                      :topic (:topic msg) :partition (:partition msg)
                      :offset (long (:offset msg))
                      :expected exp :observed (:payload msg)
                      :note "投递的 payload 与该 offset 上已确认的发布不一致"})))
         vec)))

(defn- dup-offset
  "判据 3：两次已确认发布拿到同一个 offset（后一条覆盖前一条）。"
  [ops]
  (->> (publishes ops)
       (filter #(= :ok (:type %)))
       (group-by #(conj (tp %) (long (:offset %))))
       (keep (fn [[k v]]
               (when (> (count v) 1)
                 {:type :mq-dup-offset
                  :topic (ffirst (map (fn [o] [(:topic o)]) v))
                  :partition (:partition (first v)) :offset (last k)
                  :payloads (mapv :payload v)
                  :note "两次已确认的发布返回同一 offset（日志条目被覆盖）"})))
       vec))

(defn- dup-in-response
  "判据 4：同一 Poll 响应里 offset 重复。"
  [ops]
  (->> (polls ops)
       (mapcat (fn [p]
                 (let [freq (frequencies (map (fn [m] (long (:offset m)))
                                              (:messages p)))]
                   (for [[o n] freq :when (> (long n) 1)]
                     {:type :mq-dup-in-response
                      :topic (:topic p) :partition (:partition p) :offset o
                      :deliveries n
                      :note "同一个 Poll 响应里同一 offset 出现多次"}))))
       vec))

(defn- out-of-order
  "判据 6：单个 Poll 响应内 offset 不是严格递增。"
  [ops]
  (->> (polls ops)
       (keep (fn [p]
               (let [offs (map (fn [m] (long (:offset m))) (:messages p))]
                 (when (and (> (count offs) 1)
                            (not (apply < offs)))
                   {:type :mq-out-of-order
                    :topic (:topic p) :partition (:partition p)
                    :offsets (vec offs)
                    :note "同一 Poll 响应内 offset 非严格递增"}))))
       vec))

(defn- redelivered-after-ack
  "判据 5：已 Ack 的 offset 之后又被投递。

  只在「Ack 完成于该次 Poll 起点之前」时判（P1：ack 语义被破坏）。"
  [ops]
  (let [acked (ack-events ops)]
    (->> (filterv #(= :ok (:type %)) (polls ops))
         (mapcat (fn [p]
                   (for [msg (:messages p)
                         :let [k (conj [( :topic msg) (long (:partition msg))]
                                       (long (:offset msg)))
                               at (get acked k)]
                         :when (and at (< (long at) (quot (start-ns p) 1000000)))]
                     {:type :mq-redelivered-after-ack
                      :topic (:topic msg) :partition (:partition msg)
                      :offset (long (:offset msg))
                      :acked-at-ms (long at)
                      :poll-start-ms (quot (start-ns p) 1000000)
                      :note "已确认（Ack）的消息被再次投递"})))
         vec)))

(defn- idem-not-deduped
  "判据 7：同一个 `idempotency_key` 两次发布拿到**不同 offset**（重复条目）。

  这是 `idempotency_key` 字段的证伪点。实测（2026-09-19）：`grpc_handlers.rs`
  的 `publish` 根本不读 `req.idempotency_key`（两条路径都传 `None` 当作 header）
  ⇒ 重发必产生重复条目。**契约措辞尚未书面确认**（proto 只声明了字段，没有
  写明 broker 侧去重承诺），所以默认只**记录**（`:dups`），不写进 `:valid?`；
  与 coord 团队确认后把 `:expect-idem-dedupe?` 置 true 即可转为硬判据
  （dev.md §5.4 的待确认表）。"
  [ops]
  (->> (publishes ops)
       (filterv #(= :ok (:type %)))
       (filter :dup?)
       (keep (fn [p]
               (when (and (:offset p) (:dup-offset p)
                          (not= (long (:offset p)) (long (:dup-offset p))))
                 {:type :mq-idem-not-deduped
                  :topic (:topic p) :partition (:partition p)
                  :idempotency-key (:idempotency-key p)
                  :first-offset (long (:offset p))
                  :second-offset (long (:dup-offset p))
                  :note "同一 idempotency_key 的两次发布落到两个 offset（未去重）"})))
       vec))

;; --------------------------------------------------------------------------

(defn checker
  "M5b MQ checker。opts：

    :min-publishes        已确认发布数的样本门槛（默认 0；§5.1 口径：不足 ⇒ invalid）
    :min-polls            完成的 Poll 数下限
    :expect-idem-dedupe?  把「同一 idempotency_key 必须去重」当**硬判据**
                          （默认 false = 只记录，见 `idem-not-deduped`）

  报告里按子面打印 `:by-op` 完成数（F-41 的纪律：只看 `:valid?` 会在「一条 op 都
  没成功」时也是 true）。"
  ([] (checker {}))
  ([{:keys [min-publishes min-polls expect-idem-dedupe?]}]
   (let [min-pubs (long (or min-publishes 0))
         min-polls (long (or min-polls 0))]
     (reify checker/Checker
       (check [_ _test history _opts]
         (let [{:keys [ops]} (wi/pair-invokes history)
               done (filterv #(= :ok (:type %)) ops)
               pubs (filterv #(= :ok (:type %)) (publishes ops))
               ps   (filterv #(= :ok (:type %)) (polls ops))
               dlv  (delivered ops)
               idem (idem-not-deduped ops)
               fails (vec (concat (silent-loss ops)
                                  (payload-mismatch ops)
                                  (dup-offset ops)
                                  (dup-in-response ops)
                                  (out-of-order ops)
                                  (redelivered-after-ack ops)
                                  (when expect-idem-dedupe? idem)))
               by-class (frequencies (map :type fails))
               acked (ack-events ops)
               sample-reasons (cond-> []
                                (< (count pubs) min-pubs)
                                (conj {:type :insufficient-sample :op :mq-publish
                                       :count (count pubs) :required min-pubs
                                       :note "发布样本不足：该 cell 视为未执行（§5.1）"})
                                (< (count ps) min-polls)
                                (conj {:type :insufficient-sample :op :mq-poll
                                       :count (count ps) :required min-polls
                                       :note "Poll 样本不足：该 cell 视为未执行（§5.1）"}))
               summary {:publishes (count pubs)
                        :polls (count ps)
                        :polls-with-messages (count (filterv #(seq (:messages %)) ps))
                        :empty-polls (count (filterv #(empty? (:messages %)) ps))
                        :poll-ack-failures (count (filterv #(false? (:ack-ok? %)) ps))
                        :acked-offsets (count acked)
                        :delivered-offsets (count dlv)
                        :published-offsets (count (distinct (map #(conj (tp %) (long (:offset %)))
                                                                 pubs)))
                        ;; idempotency_key 的观察（默认不判红，见 idem-not-deduped）
                        :idem-dups (count idem)
                        :by-op (frequencies (map :f done))
                        :violations-by-class by-class}
               reasons (into sample-reasons
                             (when (seq fails)
                               [{:type :mq-violations
                                 :count (count fails)
                                 :sample (vec (take 3 fails))}]))]
           (info "mq checker:" (pr-str (assoc summary :failures (take 5 fails))))
           (cond-> {:valid? (and (empty? sample-reasons) (empty? fails))
                    :mq summary
                    :failures reasons}
             (seq fails) (assoc :violations (vec (take 20 fails)))
             (seq idem) (assoc :observations (vec (take 20 idem))))))))))
