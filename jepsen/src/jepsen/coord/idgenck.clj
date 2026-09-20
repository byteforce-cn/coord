(ns jepsen.coord.idgenck
  "M5a —— IdGen 的 checker（缺口 AG-08：唯一性与时钟回拨边界）。

  ## 契约（`apis/contracts/proto/coord/idgen/v1/idgen.proto` + STATUS.md 台账）

    * 同名发号器内 ID **全局唯一**；趋势递增；
    * NextBatch 返回 **count 个互异 ID**；
    * 台账整改要点：「时钟回拨防护**落地或明确不承诺边界**」——即要么守住，
      要么把边界写成可复现的判据（本 checker 的 `:min-regressions` 就是这个开关）。

  ## 判据（唯一性是唯一不可让步的那条）

  1. `:idgen-duplicate-id`（**P0**）—— 同一发号器名下同一个 ID 出现两次。
     这是「ID 当业务主键」场景下的静默数据损坏，也是 snowflake nodeid 冲突
     （同名主机 / 容器主机名重复 / 显式 nodeid 撞车）的唯一外部可观测形态。
  2. `:idgen-batch-not-distinct`（**P0**）—— 单个 NextBatch 响应里出现重复。
     这条**不需要跨节点、跨时钟就能判**，是 idgen 面最便宜的高价值判据。
  3. `:idgen-batch-size-mismatch`（P1）—— 返回个数 ≠ 请求个数。
  4. `:idgen-regression`（P1，按台账口径可配置）—— 同一 `(发号器, 客户端进程)`
     的 ID 序列出现下降。snowflake 依赖墙钟，回拨窗口内**可能**违反；台账要求
     「落地防护或明确不承诺边界」，所以默认 `min-regressions = 0`（= 认为已落地
     防护）；若双方书面确认「不承诺」该边界，则用 `--idgen-min-regressions`
     显式放宽并把确认记录链接进 MANIFEST（**不允许**在跑红之后临时调参）。
  5. 样本门槛 `:min-ids` —— 不足 ⇒ invalid（未执行既不算绿也不算红）。

  ## 漏检边界（R2）

  * 不判分布均匀性/含义（snowflake 的位布局是内部实现）；
  * 不判「趋势递增」的**跨进程**全序（不同 agent 各发各的，本来就无全局序）；
  * 单调性只按**同一个客户端进程内**的观察序列判（最强且无歧义的口径）。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]))

(defn- idgen-ops [ops]
  (filterv #(= :idgen (:f %)) ops))

(defn- as-long
  "ID 在 client 侧存成十进制字符串（见 invoke-idgen 的注释）。解析失败返回 nil
  （不要抛：一个不可解析的 ID 不该让整个 checker 判 :unknown）。"
  [s]
  (try (Long/parseLong (str s)) (catch Exception _ nil)))

(defn- duplicate-ids
  "判据 1：全局重复（按发号器名分组）。"
  [ops]
  (->> (for [[name group] (group-by :name (idgen-ops ops))
             [id n] (frequencies (mapcat :ids group))
             :when (> (long n) 1)]
         {:type :idgen-duplicate-id
          :name name
          :id id
          :count (long n)
          :note "同一发号器下同一个 ID 被发了多次（snowflake nodeid 冲突 / 回拨 / 号段重叠）"})
       vec))

(defn- batch-not-distinct
  "判据 2：单个 batch 响应内部重复。"
  [ops]
  (->> (idgen-ops ops)
       (filter :batch?)
       (keep (fn [op]
               (when (and (seq (:ids op)) (not= (count (:ids op))
                                                (count (distinct (:ids op)))))
                 {:type :idgen-batch-not-distinct
                  :name (:name op)
                  :n (count (:ids op))
                  :distinct (count (distinct (:ids op)))
                  :sample (vec (take 12 (:ids op)))
                  :note "NextBatch 契约承诺返回 count 个互异 ID"})))
       vec))

(defn- batch-size-mismatch
  "判据 3：返回个数 ≠ 请求个数（`:requested-count` 由 client 的 invoke-idgen 记下）。"
  [ops]
  (->> (idgen-ops ops)
       (filter #(and (:batch? %) (:requested-count %)))
       (keep (fn [op]
               (when (not= (count (:ids op)) (long (:requested-count op)))
                 {:type :idgen-batch-size-mismatch
                  :name (:name op)
                  :requested (long (:requested-count op))
                  :returned (count (:ids op))
                  :note "NextBatch 返回个数与请求不符"})))
       vec))

(defn- regressions
  "判据 4：同一 (发号器, 客户端进程) 的观察序列出现下降。

  逐 op 比较「本 op 的第一个 ID」与「上一个 op 的最后一个 ID」（同进程、
  同发号器）：snowflake 在正常路径下应保证非递减。"
  [ops]
  (->> (idgen-ops ops)
       (filter #(seq (:ids %)))
       (group-by (juxt :name :process))
       (mapcat (fn [[[name proc] group]]
                 (let [seqd (sort-by :time group)]
                   (->> (partition 2 1 seqd)
                        (keep (fn [[a b]]
                                (let [la (as-long (last (:ids a)))
                                      fb (as-long (first (:ids b)))]
                                  (when (and la fb (> (long fb) 0) (< (long fb) (long la)))
                                    {:type :idgen-regression
                                     :name name
                                     :process proc
                                     :prev-id (str la)
                                     :next-id (str fb)
                                     :prev-at (:time a)
                                     :next-at (:time b)
                                     :note "同一进程观察到的 ID 序列下降（时钟回拨未防护）"}))))))))
       vec))

(defn checker
  "M5a idgen checker。opts：

    :min-ids          样本门槛：观察到的 ID 总数（默认 0）
    :min-regressions  允许的下降次数（默认 0 = 认为回拨防护已落地；
                      若书面确认「不承诺」，显式放宽并把确认记录进 MANIFEST）"
  ([] (checker {}))
  ([{:keys [min-ids min-regressions]}]
   (let [min-ids (long (or min-ids 0))
         min-regressions (long (or min-regressions 0))]
     (reify checker/Checker
       (check [_ _test history _opts]
         (let [{:keys [ops]} (wi/pair-invokes history)
               iops  (filterv #(= :ok (:type %)) (idgen-ops ops))
               all-ids (mapcat :ids iops)
               regs  (regressions iops)
               dup   (duplicate-ids iops)
               fails (vec (concat dup
                                  (batch-not-distinct iops)
                                  (batch-size-mismatch iops)
                                  (when (>= (count regs) (inc min-regressions))
                                    regs)))
               by-class (frequencies (map :type fails))
               sample-ok? (>= (count all-ids) min-ids)
               summary {:ops (count iops)
                        :ids (count all-ids)
                        :min-ids min-ids
                        :distinct-ids (count (distinct all-ids))
                        :batches (count (filter :batch? iops))
                        :regressions (count regs)
                        :min-regressions min-regressions
                        :violations-by-class by-class}
               reasons (cond-> []
                         (not sample-ok?)
                         (conj {:type :insufficient-sample
                                :ids (count all-ids)
                                :required min-ids
                                :note "idgen 样本不足：该 cell 视为未执行（§5.1），不得判绿"})
                         (seq fails)
                         (conj {:type :idgen-violations
                                :count (count fails)
                                :sample (vec (take 3 fails))}))]
           (info "idgen checker:" (pr-str (assoc summary :failures (take 10 fails))))
           (cond-> {:valid? (and sample-ok? (empty? fails))
                    :idgen summary
                    :failures reasons}
             (seq fails) (assoc :violations (vec (take 20 fails))))))))))
