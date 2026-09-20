(ns jepsen.coord.electck
  "M5a —— Leader Election 的 checker（缺口 AG-02/AG-10）。

  ## 契约（`apis/contracts/proto/coord/election/v1/election.proto` + 台账整改要点）

    * **竞选原子**：同一 group 同一时刻**至多一个 leader**；
    * TTL 内无续约 ⇒ leader 自动过期并广播 LEADER_EXPIRED；
    * Resign 之后旧 leader 不得再被 `GetLeader` 读出（台账明确列为整改要点：
      「续约（重新 Campaign）语义验证」）。

  ## 判据

  1. `:election-two-leaders`（**P0**）—— 同一 group 上两个**不同 candidate** 的
     leader 区间重叠。区间 = `[anchored(campaign-at-ms), anchored(gone-at-ms))`
     —— **绝对**毫秒（锚点 = op 自读的 `:t0-ns`，见 `abs-ms`）；Resign 失败且没有
     任何「已不在位」的观测时区间**不闭合**，不参与本判据（TTL 被动过期不是违约，
     用 fail-safe 延长的区间判会假红）。
  2. `:election-leader-after-resign`（P1）—— Resign 成功之后 `GetLeader` 仍返回
     **自己**（旧 leader 未真正退位）。
  3. 样本门槛 `:min-campaigns`（§5.1：campaign 轮次 ≥ 50）—— 不足 ⇒ invalid。
  4. `:election-server-truth-contradiction`（**P0**）—— **服务端**选举 key 的
     `leader_id` 与某条**其它 candidate** 的自述区间矛盾（`:election-probe` 绕开
     agent 直接 KV Range `/_election/{group}`）。这是 F-35「残留」段的补丁：判据
     1–2 全部基于客户端自述，而 F-34 证明单一自述源分诊不了「真违约 / agent
     汇报层与 server 不一致 / 度量偏差」三种解释。探针缺失 ⇒ 判未执行。
     边界 100ms 以内的样本记成 `:near-boundary`（不进 `valid?`，但一定出现在
     报告里）。

  ## 与 lockck 的关系

  两者共用「同一 JVM 单一时钟 ⇒ 区间比较精确」这一性质；这里同样的
  `tolerance-ms` 默认 0。选举的 leader 状态由 agent 内的 election 面维护
  （Lease + Watch + Txn 组合），跨 agent 的唯一性最终由 server 的一致性裁决 ——
  正是「多 agent 拓扑」才能证伪的东西（单 agent 下永远只有一个本地视图）。

  **两面的区间度量口径也必须一致**：第一版这里没有锚点（直接跨 op 比相对毫秒），
  实测 45s run 报出 124 条假「双 leader」，加上锚点后是 0（findings F-35）。
  守门员 fixture：`expect-valid-anchor-must-be-absolute.edn` 与
  `expect-valid-nanotime-anchor-wins-over-jepsen-time.edn`。

  ## 漏检边界（R2）

  * `elected=false`（别人在位）是合法业务结果，不参与区间判定；
  * Resign 语义之外的「被动过期」不直接判：如果 leader 因 TTL 过期而消失，
    它不会 Resign ⇒ 区间按 fail-safe 延长，可能把**合法**的过期当成「还在位」。
    为避免假红，这里只用 `:leader-after-resign` 判「旧 leader 可见性」，
    而**不**把 fail-safe 延长的区间当作「它真的还持有」去判双 leader；
    ⇒ 双 leader 判据只用**已 Resign** 的区间闭合 op（见 held-interval 的
    `:closed?` 字段与 mutual-exclusion 的 :when 过滤）。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]))

(def default-grace-ms 2000)

(defn- elect-ops [ops]
  (filterv #(= :elect-campaign (:f %)) ops))

(defn- abs-ms
  "把 op 内部的相对毫秒换成**绝对**毫秒。

  与 `lockck/abs-ms` 同一口径：优先用 op 自己读的 `:t0-ns`（同一个 JVM 的
  `System/nanoTime`，跨 op 精确）；缺它（手写 fixture / 旧历史）才退回 jepsen
  记录的 invoke 时刻。

  这里曾经**完全没有锚点**：`campaign-at-ms` / `resigned-at-ms` 都是「相对本 op
  起点」的量，直接跨 op 相减 ⇒ 把完全合法的先后关系报成双 leader。实测（M5a
  第二轮，45s run）：124 条；同一份历史只加上锚点就变成 **0**（findings F-35）。"
  [op rel-ms]
  (+ (quot (long (or (:t0-ns op) (:invoke op) (:time op) 0)) 1000000)
     (long rel-ms)))

(defn- leader-interval
  "成功当选者的区间（**绝对**毫秒）。

  * `start` = 锚点 + `:campaign-at-ms`；
  * `end`   = 锚点 + `:gone-at-ms`（「已不在 leader 位」的最早可举证时刻：
              Resign 成功，或 GetLeader 已不是自己）；
  * 一个证据都没有 ⇒ `:closed? false`（区间不参与双 leader 判定）。

  为什么只拿**已闭合**的区间判双 leader：未闭合的那些无法区分「它还在位」与
  「它已被动过期」，用它判会假红（TTL 到期不是违约）。这与 lock 的取向一致。"
  [op]
  (when (:campaign-at-ms op)
    (let [gone-at (:gone-at-ms op)
          closed? (boolean (and (:gone? op) gone-at))]
      {:group (:group-name op)
       :candidate (:candidate-id op)
       :process (:process op)
       :start (abs-ms op (:campaign-at-ms op))
       :end (when closed? (abs-ms op gone-at))
       :closed? closed?})))

(defn- two-leaders
  [ops tolerance-ms]
  (->> (group-by :group (filter :closed? (keep leader-interval (elect-ops ops))))
       (mapcat (fn [[_ ivs]]
                 (for [a ivs
                       b ivs
                       :when (and (neg? (compare (:candidate a) (:candidate b)))
                                  (< (long (:start a)) (- (long (:end b)) (long tolerance-ms)))
                                  (< (long (:start b)) (- (long (:end a)) (long tolerance-ms))))]
                   {:type :election-two-leaders
                    :group (:group a)
                    :left a :right b
                    :tolerance-ms (long tolerance-ms)
                    :note "同一 group 上两个不同 candidate 的 leader 区间重叠"})))
       vec))

(defn- leader-after-resign
  [ops]
  (->> (elect-ops ops)
       (filter :resigned?)
       (keep (fn [op]
               (let [l (:leader-after-resign op)]
                 (when (and (map? l) (:exists l)
                            (= (:leader-id l) (:candidate-id op)))
                   {:type :election-leader-after-resign
                    :group (:group-name op)
                    :candidate (:candidate-id op)
                    :leader l
                    :note "Resign 成功之后 GetLeader 仍返回自己（旧 leader 未退位）"}))))
       vec))

(defn- probe-ops [ops] (filterv #(= :election-probe (:f %)) ops))

(def default-probe-margin-ms
  "探针样本与自述区间边界之间的测量容差（默认 100ms，与 lock 面同一口径）。

  区间闭合时刻是「客户端**观察到**自己不再是 leader」（Resign 返回 / GetLeader
  已换人），永远是真实退位时刻的**上界** ⇒ 紧贴边界的样本可能只是测量误差。
  这类样本记成 `:near-boundary`（**不**静默丢弃），硬违约才进 `valid?`。"
  100)

(defn- server-truth-violations
  "判据 4（**服务端视角**的唯一 leader，F-35 的残留补丁）：探针在 T 时刻读到
  `/_election/{group}` 的 `leader_id` 是 A，而某条**其它 candidate** 的自述区间
  把 T 含在内部 ⇒ 那一刻服务端说有 A 在任，而 B 自述在位（同一 group 双主）。

  与判据 1 的分工：判据 1 只比客户端自述（无法区分「真违约 / agent 汇报层与
  server 不一致 / 度量偏差」三种解释，F-34 的教训）；判据 4 用**绕开 agent** 的
  服务端真相做交叉验证。两条同时为 0 才是「唯一 leader 成立」的完整证据。"
  [ops ivs margin-ms]
  (let [ivs-by-group (group-by :group ivs)
        margin (long margin-ms)
        samples (filterv :exists? (probe-ops ops))]
    (vec
      (for [p samples
            :let [leader (get-in p [:server :leader-id])
                  t      (abs-ms p (:server-at-ms p))]
            :when (seq leader)
            iv (get ivs-by-group (:group-name p))
            :when (not= leader (:candidate iv))
            :let [d-in  (- t (long (:start iv)))
                  d-out (- (long (:end iv)) t)]
            :when (and (pos? d-in) (pos? d-out))]
        {:type :election-server-truth-contradiction
         :group (:group-name p)
         :server-leader-id leader
         :claiming-candidate (:candidate iv)
         :at-ms t
         :claimed-interval [(:start iv) (:end iv)]
         :distance-to-edge-ms (min d-in d-out)
         :hard? (>= (min d-in d-out) margin)
         :note "服务端选举 key 写的是别人，而这条自述区间声称自己在任"}))))

(defn checker
  "M5a election checker。opts：

    :min-campaigns  样本门槛（默认 0；§5.1 = 50）
    :tolerance-ms   区间容差（默认 0，同 JVM 单一时钟）
    :probe?         是否要求服务端地面真值探针在场（默认 false；workload 传 true）
    :probe-margin-ms 服务端真相判据的边界容差（默认 100）"
  ([] (checker {}))
  ([{:keys [min-campaigns tolerance-ms probe? probe-margin-ms]}]
   (let [min-campaigns (long (or min-campaigns 0))
         tolerance-ms  (long (or tolerance-ms 0))
         probe?        (boolean probe?)
         margin-ms     (long (or probe-margin-ms default-probe-margin-ms))]
     (reify checker/Checker
       (check [_ _test history _opts]
         (let [{:keys [ops]} (wi/pair-invokes history)
               eops   (elect-ops ops)
               done   (filterv #(= :ok (:type %)) eops)
               won    (filterv :campaign-at-ms done)
               ivs    (vec (keep leader-interval eops))
               probes (probe-ops ops)
               probe-read-failures (count (filter #(= :info (:type %)) probes))
               truth  (server-truth-violations ops ivs margin-ms)
               truth-hard (filterv :hard? truth)
               fails  (vec (concat (two-leaders ops tolerance-ms)
                                   (leader-after-resign ops)
                                   truth-hard))
               by-class (frequencies (map :type fails))
               sample-ok? (>= (count won) min-campaigns)
               probe-ok? (or (not probe?) (pos? (count probes)))
               summary {:campaigns (count done)
                        :won (count won)
                        :not-won (count (filter #(false? (:elected? %)) done))
                        :min-campaigns min-campaigns
                        :resign-failures (count (filter #(and (:campaign-at-ms %)
                                                              (not (:resigned? %)))
                                                        done))
                        :unclosed-intervals (count (remove :closed?
                                                           (keep leader-interval done)))
                        ;; 服务端地面真值（F-35 残留）：探针样本与矛盾计数
                        :probe {:samples (count probes)
                                :read-failures probe-read-failures
                                :with-key (count (filter :exists? probes))
                                :distinct-leaders
                                (count (distinct (keep #(get-in % [:server :leader-id])
                                                       probes)))
                                :contradictions (count truth-hard)
                                :near-boundary (count (remove :hard? truth))}
                        :violations-by-class by-class}
               reasons (cond-> []
                         (not sample-ok?)
                         (conj {:type :insufficient-sample
                                :campaigns (count won)
                                :required min-campaigns
                                :note "election 样本不足：该 cell 视为未执行（§5.1），不得判绿"})
                         (not probe-ok?)
                         (conj {:type :election-probe-missing
                                :samples 0
                                :note "本 workload 声明了服务端地面真值探针却没有一条样本：唯一 leader 的服务端口径无证据，判未执行（不是绿）"})
                         (seq truth)
                         (conj {:type :election-server-truth
                                :hard (count truth-hard)
                                :near-boundary (count (remove :hard? truth))
                                :margin-ms margin-ms
                                :note "服务端选举 key 的 leader 与自述区间矛盾（硬违约计入 valid?；边界争议只报到报告里）"})
                         (seq fails)
                         (conj {:type :election-violations
                                :count (count fails)
                                :sample (vec (take 3 fails))}))]
           (info "election checker:" (pr-str (assoc summary :failures (take 10 fails))))
           (cond-> {:valid? (and sample-ok? probe-ok? (empty? fails))
                    :election summary
                    :failures reasons}
             (seq fails) (assoc :violations (vec (take 20 fails))))))))))
