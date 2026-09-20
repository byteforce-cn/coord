(ns jepsen.coord.lockck
  "M5a —— 分布式锁的 checker（缺口 AG-02：跨 agent 互斥）。

  ## 契约（`apis/contracts/proto/coord/lock/v1/lock.proto` + 台账整改要点）

    * **线性一致互斥**：同一时刻同一锁名**至多一个持有者**；
    * 仅 `(holder_id, lease_id)` 匹配者可 Release / Renew（非持有者 → 拒绝）；
    * TTL 内未 Renew ⇒ 锁自动释放（持有者崩溃不应留下死锁）。

  ## 判据

  1. `:lock-mutual-exclusion`（**P0**）—— 同一锁名上两个**不同 holder** 的持有区间
     重叠。这一条是 T5.3 的核心，也是 §5.1「重叠（>容差）= 0」的可判定化。
     区间的**结束**锚在客户端能举证的、最早的「锁已不在我名下」时刻
     （见 `held-interval`）—— 这一处曾是 F-34 假红的来源。
  2. `:lock-fencing-missing`（**P0**）—— 用**错误的 lease_id** 释放成功。契约只认
     `(holder_id, lease_id)`；若按 name 删，任何知道锁名的调用方都能破环互斥。
  3. `:lock-release-did-not-free`（P1）—— Release 返回 `released=true` 之后，
     `GetLockInfo` 仍显示锁挂在自己名下超过 `grace-ms`：「释放」没有真的释放。
  4. 样本门槛 `:min-acquires`（§5.1：acquire 成功 ≥ 200）—— 不足 ⇒ invalid
     （未执行既不算绿也不算红）。
  5. `:lock-server-truth-contradiction`（**P0**）—— **服务端**读到的 key 持有者
     与某条自述区间矛盾（`lock-probe` 绕开 agent 直接 KV Range `/_lock/{name}`）。
     判据 1 只看客户端自述，判据 5 是服务端真相的交叉验证：只有它能否证伪
     「agent 汇报层与 server 真相不一致」（F-34 的分支 2）。探针缺失时判
     invalid（`--workload lock` 的生成器一定会混探针，所以「一条都没有」= 接线
     断了）。边界 100ms 以内的样本记成 `:near-boundary`（不在 `valid?` 里，
     但一定出现在报告里）。
  6. `:lock-orphan-not-reclaimed`（**P0，AG-06**）—— **弃锁**的持有 agent 已经
     不可能再续期（被 kill / 暂停 / 与集群分区）之后，服务端 key 仍挂在它名下
     超过 `ttl + grace`：崩溃的持有者留下死锁。锚点是故障事件**完成**的时刻
     （保守：只可能漏报）。
  7. `:lock-phantom-loss`（**P1，AG-06 的另一半**）—— 持有 agent **活着**时，
     服务端 key 已经不挂在它名下了：自述与服务端真相不一致（「假丢锁」，
     `lock.rs` 记录的历史 P0 就是这一类）。**任何 TTL 都判**：实测（2026-09-19）
     主因是 agent 自发的保活流量不带凭据（F-50），而 TTL ≤ 续期节拍（10s）是第二个
     成因 —— 后者只作为违反里的 `:ttl-below-renew-cadence?` 诊断字段出现，不作豁免
     （否则一个小 TTL 就能把真缺陷一并赦免）。

  判据 6/7 是**成对**的（§6 第 13 条）：只有 6 会忽略「活着就把锁弄丢」，只有 7
  会忽略「死了都收不回来」。两者都靠「agent 什么时候不可能再续期」的时间窗
  （`jepsen.coord.faultwin`）来切分 —— 期望值在故障前后正好相反，没有时间窗就只能
  猜，而猜错的方向会产出假红或假绿。

  ## 时间基准（为什么不需要跨机容差）

  所有 jepsen 客户端跑在控制机的**同一个 JVM** 里，`:acquired-at-ms` /
  `:released-at-ms` 都是同一个 `System/nanoTime` 的相对毫秒 ⇒ **区间重叠判定是
  精确的**，`tolerance-ms` 默认 0（dev.md §5.4-① 的 500ms 跨机容差在这里用不上：
  那条是给「客户端时间戳跨机比较」留的）。

  注意：agent 自己的锁到期判定用**它自己的墙钟**（`lock.rs` 的 `unix_ts()`），
  但那不影响本判据 —— 本判据只依据**控制机上观测到的持有区间**。跨 agent 墙钟
  差会以「锁提前/延后释放」的形式**间接**体现在区间重叠上，正是要抓的东西。

  ## 漏检边界（R2）

  * 「未 release 且未到 ttl」的 op 不参与判据 3（它本来就应该还held）；
  * 锁名/持有者相同的重入不判（契约明确**不承诺可重入与其语义**）；
  * 公平性/等待队列顺序不判（契约不承诺）。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.faultwin :as fw]
            [jepsen.coord.windex :as wi]))

(def default-grace-ms
  "释放后「锁必须消失」的观察窗口兜底值（op 自带 :grace-ms 时以 op 为准）。"
  2000)

(defn- lock-ops [ops]
  (filterv #(= :lock-contend (:f %)) ops))

(defn- abs-ms
  "把 op 内部的相对毫秒换成**绝对**毫秒。

  `:at-ms` 系字段是「相对**本 op** 起点」的毫秒（客户端用 `System/nanoTime` 量的），
  所以两个 op 的相对值**不能直接比较** —— 每个 op 的零点都不一样。

  锚点优先用 op 自己读的 `:t0-ns`（同一个 JVM 的 `System/nanoTime`，
  **精确**）；缺它（手写 fixture / 旧历史）才退回 jepsen 记录的 invoke 时刻。

  为什么 `:t0-ns` 是必须的：jepsen 的 invoke 时刻是 worker 线程派发 op 时打的点，
  中间隔着队列与线程调度。实测（2026-09-18）同一 JVM 里
  `completion.time - (t0 + 最后一个 :at-ms)` 在不同 op 之间从 40ms 抖到 290ms
  —— 锚点自身就有几百毫秒噪声，而这份噪声会被算成「两个持有者的区间重叠」
  （实测 3/126 条假红，全部在 1–241ms 量级）。"

  [op rel-ms]
  (+ (quot (long (or (:t0-ns op) (:invoke op) (:time op) 0)) 1000000)
     (long rel-ms)))

(defn- held-interval
  "一个成功持有者的 `[start end)` 区间（**绝对**毫秒，同一时钟）：

  * `start` = invoke 时刻 + `:acquired-at-ms`；
  * `end`   = invoke 时刻 + `:gone-at-ms` —— 客户端给出的**最早的**「锁已不在我
              名下」可举证时刻（Release 成功 / fencing 探针删除成功 / 观测到
              holder 换人）；
              一个证据都没有 ⇒ fail-safe 取 `start + ttl + grace`。

  为什么不是 `:released-at-ms`：它是 Release RPC 返回的时刻，而客户端的观测
  循环在它**之后**还会 sleep/轮询，于是区间尾部被拉长（F-34 根因 ①，实测中位数
  +196ms、最长 +1.37s）。
  为什么不是「`exists=false` 才算结束」：观测窗口里另一个持有者**合法接管**时
  `exists=true`，区间会被延长整整 ttl+grace（=9s，F-34 根因 ②）。
  两条合起来曾产出「162 次持有中 99 次互斥重叠」的假红（findings F-34）。

  返回 nil 当 `:acquired-at-ms` 缺失（不是成功持有者）。"
  [op]
  (when-let [rel-start (:acquired-at-ms op)]
    (let [start   (abs-ms op rel-start)
          gone-at (:gone-at-ms op)
          closed? (boolean (:gone? op))
          end     (if (and closed? gone-at)
                    (abs-ms op gone-at)
                    (+ start (* 1000 (long (or (:holder-ttl-seconds op) 0)))
                       (long (or (:grace-ms op) default-grace-ms))))]
      {:name (:name op)
       :holder (:holder-id op)
       :process (:process op)
       :start start
       :end end
       :closed? closed?})))

(defn- overlaps? [a b tolerance-ms]
  (and (< (long (:start a)) (- (long (:end b)) (long tolerance-ms)))
       (< (long (:start b)) (- (long (:end a)) (long tolerance-ms)))))

(def default-overlap-tolerance-ms
  "区间重叠判据的**最小可分辨重叠**（默认 50ms）。

  区间闭合时刻是「客户端**观察到**锁不在我名下」，它永远是真实闭合时刻的**上界**
  （差一个 RPC 往返）。所以严格 0 容差会把「后继持有者在上一任的删除 RPC **返回
  之前** 1ms 拿到锁」这种完全合法的情况判成互斥破坏 —— 实测（M5a 第二轮）每个 run
  都会出现 0–1 条、量级 1–3ms，而且服务端探针在那段时间里**一次矛盾都没看到**。

  取向与探针的 `probe-margin-ms` 一致：≤ 容差的重叠记成
  `:lock-mutual-exclusion-near-boundary`（**出现在报告里**，不进 `valid?`），
  > 容差才是硬违约。「不静默丢弃」是这个取向成立的前提 —— 报告里永远能同时看到
  硬违约与边界争议两栏。"
  50)

(defn- mutual-exclusion-pairs
  "同一锁名上**不同 holder** 的两两持有区间里，重叠量 > 0 的全部对（带 `:overlap-ms`）。

  同 holder 的多次获取属重入，契约不承诺，不判。"
  [ops]
  (->> (group-by :name (keep held-interval (lock-ops ops)))
       (mapcat (fn [[_ ivs]]
                 (for [a ivs
                       b ivs
                       :when (neg? (compare (:holder a) (:holder b)))
                       :let [ov (- (min (long (:end a)) (long (:end b)))
                                    (max (long (:start a)) (long (:start b))))]
                       :when (pos? ov)]
                   {:type :lock-mutual-exclusion
                    :name (:name a)
                    :left a
                    :right b
                    :overlap-ms ov
                    :note "同一锁名上两个不同 holder 的持有区间重叠"})))
       vec))

(defn- fencing-violations
  "判据 2：错误 lease_id 的 Release 竟然成功。"
  [ops]
  (->> (lock-ops ops)
       (filter #(get-in % [:foreign-release :released?]))
       (mapv (fn [op]
               {:type :lock-fencing-missing
                :name (:name op)
                :holder (:holder-id op)
                :real-lease-id (:lease-id op)
                :lease-id-sent (inc (long (:lease-id op 0)))
                :foreign-release (:foreign-release op)
                :note "非匹配 lease_id 的 Release 成功了：契约只认 (holder_id, lease_id)" }))))

(defn- not-freed-violations
  "判据 3：Release 回 true 但锁**仍然挂在自己名下**（轮询到第 5 次依旧
  `exists=true` 且 holder 是自己）。

  判据是「还挂在我名下」而不是「exists=true」：后者会把「观测窗口里另一个
  持有者合法接管」误判成「没释放」（F-34 根因 ② 的同一处语义）。"
  [ops grace-ms]
  (->> (lock-ops ops)
       (filter :released?)
       (keep (fn [op]
               (when (true? (:still-mine-after-release op))
                 {:type :lock-release-did-not-free
                  :name (:name op)
                  :holder (:holder-id op)
                  :grace-ms (long grace-ms)
                  :lock-info (:lock-info-after-release op)
                  :note "Release 返回 released=true，但随后 GetLockInfo 仍显示锁挂在自己名下"})))
       vec))

;; --------------------------------------------------------------------------
;; 判据 5 —— 服务端地面真值探针（F-34 的决定性实验，AG-02 的服务端口径）
;; --------------------------------------------------------------------------

(def default-probe-margin-ms
  "探针样本与自述区间边界之间的**测量容差**（默认 100ms）。

  区间闭合时刻是「客户端**观察到**锁已不在我名下」，它永远是真实闭合时刻的
  **上界**（差一个 RPC 往返 + 观测抖动）。所以紧贴边界的样本可能只是测量误差，
  不能当铁证；判据把它们单独报成 `:near-boundary`（**不**静默丢弃），于是
  报告里永远能同时看到「硬违约」与「边界争议」两栏。"
  100)

(defn- probe-ops [ops] (filterv #(= :lock-probe (:f %)) ops))

(defn- server-truth-violations
  "判据 5（**服务端视角**的互斥）：探针在 T 时刻看到服务端 key 挂在 holder H
  名下，而某条**其它 holder** 的自述区间把 T 含在内部 ⇒ 那一刻有两个持有者。

  与判据 1 的分工：判据 1 只比较客户端的自述区间，判据 5 用**绕开 agent 的
  服务端真相**做交叉验证。两条同时为 0 才是「互斥成立」的完整证据；而只有
  判据 5 能证伪「agent 汇报层与 server 真相不一致」（F-34 的分支 2）。"
  [ops ivs margin-ms]
  (let [ivs-by-name (group-by :name ivs)
        margin      (long margin-ms)
        samples     (filterv :exists? (probe-ops ops))]
    (vec
      (for [p   samples
            :let [h (get-in p [:server :holder-id])
                  t (abs-ms p (:server-at-ms p))]
            :when (seq h)
            iv  (get ivs-by-name (:name p))
            :when (not= h (:holder iv))
            :let [d-in  (- t (long (:start iv)))
                  d-out (- (long (:end iv)) t)]
            :when (and (pos? d-in) (pos? d-out))]
        {:type :lock-server-truth-contradiction
         :name (:name p)
         :server-holder-id h
         :claiming-holder (:holder iv)
         :at-ms t
         :claimed-interval [(:start iv) (:end iv)]
         :distance-to-edge-ms (min d-in d-out)
         :hard? (>= (min d-in d-out) margin)
         :note "服务端 key 挂在别人名下，而这条自述区间声称自己持有"}))))

;; --------------------------------------------------------------------------
;; 判据 6/7 —— AG-06：弃锁（持有者进程消失）后的服务端回收，以及「不得假丢锁」
;; --------------------------------------------------------------------------

(def default-orphan-tolerance-ms
  "判据 6 的时刻容差（默认 0）。

  与重叠判据的 `tolerance-ms` 不同：那条是**测量分辨率**（区间端点是 RPC 上界），
  这条比的是「探针样本时刻 vs 故障时刻 + ttl + grace」，两边都由同一个时钟推进，
  而且 `grace` 本身已经是给到期清理留的余量 ⇒ 不需要再加容差。"
  0)

(def renew-cadence-ms
  "agent 后台续期任务的**节拍**（10 秒，硬编码在 `lock.rs` 的 `tokio::spawn` 循环里）。

  这个值在本 checker 里只做**诊断标注**，不做豁免：

    * 实测（2026-09-19）发现真正的主因是另一件事 —— agent **自发**的保活流量不带
      凭据（`missing CCT token`，详见 `coord-findings.md` 的 F-50），所以只要服务端
      开了鉴权，任何 TTL 下续期都不会成功；
    * 节拍本身是第二个边界（`ttl ≤ 10s` 时即使凭据正常，续期也跑不赢到期），但它只
      会**加刷**同一个症状，不是另一类问题。

  ⇒ 判据 7（活着就必须一直持有）在**所有** TTL 上都判（不拿这条当豁免，否则一个小
  TTL 就能把真缺陷也赦免掉）；这条常数只用来在违反里标一个
  `:ttl-below-renew-cadence?`，让读者一眼分开「TTL 太小」与「续期根本没跑」两种解释。

  如果 coord 团队书面确认「TTL < 续期节拍 不在自动续期承诺范围内」，那时才把它改成
  分档豁免（一行：`cadence?` 为假时跳过 H2 并记 `:ttl-below-renew-cadence`）。"
  10000)

(defn- abandon-ops [ops]
  (filterv #(and (= :lock-abandon (:f %)) (:abandoned? %)) ops))

(defn- host-of
  "这个 op 的锁落在哪个 agent 上（`agent-nodes` = 隧道 endpoint → 主机名）。"
  [op agent-nodes]
  (let [n (:acquire-node op)]
    (when (and n (map? agent-nodes)) (get agent-nodes n))))

(defn- probes-by-name [ops]
  (->> (probe-ops ops)
       (filter #(= :ok (:type %)))
       (group-by :name)))

(defn- abandon-analysis
  "弃锁（AG-06）的**判定 + 覆盖**一起算。

  对每条弃锁 op：

    * `held-samples` —— 落在「持有者一定还能续期」的窗口 `[acquired, min(下一个
      故障, run 结束))` 里的探针样本 ⇒ 判据 7（不得假丢锁）的证据；
    * `dead-samples` —— 落在「故障 + ttl + grace」之后的样本 ⇒ 判据 6（必须被
      服务端回收）的证据。

  一条 op **判过**（judged）的条件是上面两组样本至少有一组非空 —— 这是
  `:lock-abandon-unjudged` 门槛的依据（「没判过」既不等于通过也不等于红，
  §5.5 第 9 条）。

  例外（**显式记未判**，不是漏洞）：归因不到 agent 的弃锁 op（`:acquire-node` 不在
  映射表里）永远记 `:no-agent-attribution` —— 接线/映射断了的时候不许算绿。

  返回：`{:violations [...] :judged n :unjudged [...]}`。"
  [ops agent-nodes end-ms]
  (let [wins   (fw/windows ops)
        byname (probes-by-name ops)]
    (reduce
      (fn [acc op]
        (let [name   (:name op)
              holder (:holder-id op)
              start  (abs-ms op (:acquired-at-ms op))
              ttl    (* 1000 (long (or (:holder-ttl-seconds op) 0)))
              grace  (long (or (:grace-ms op) default-grace-ms))
              cadence? (> (long ttl) (long renew-cadence-ms))
              host   (host-of op agent-nodes)
              down   (when host (fw/first-down wins host start))
              before (min (long (or down Long/MAX_VALUE)) (long end-ms))
              samples (get byname name)
              t-of   (fn [p] (abs-ms p (:server-at-ms p)))
              ;; H2：持有 agent 活着（还没遇到下一个 kill/pause/分区）时，服务端 key
              ;; 必须一直挂在这个 holder 名下 —— **任何 TTL 都判**（见 renew-cadence-ms
              ;; 的说明：小 TTL 是第二个成因，不是豁免理由）。
              held   (filterv (fn [p] (and (>= (t-of p) start) (< (t-of p) before)))
                              samples)
              dead   (when down
                       (filterv (fn [p] (> (t-of p) (+ (long down) ttl grace
                                                       (long default-orphan-tolerance-ms))))
                                samples))
              phantom (first (filter #(not= holder (get-in % [:server :holder-id]))
                                     held))
              orphan  (first (filter #(= holder (get-in % [:server :holder-id]))
                                     dead))
              judged? (boolean (or (seq held) (seq dead)))
              ;; 每条 op 至多产出一条违反：H1（回收）优先于 H2（假丢锁）—— 两者
              ;; 不可能同时成立（一个要求 key 还在、一个要求 key 不在）。
              new-vios (cond
                         orphan
                         [{:type :lock-orphan-not-reclaimed
                           :name name
                           :holder holder
                           :lease-id (:lease-id op)
                           :agent host
                           :fault (fw/kind-of wins host start)
                           :fault-at-ms (long down)
                           :deadline-ms (+ (long down) ttl grace)
                           :seen-at-ms (t-of orphan)
                           :ttl-ms ttl :grace-ms grace
                           :note (str "持锁 agent 已不可能续期（" (fw/kind-of wins host start)
                                      "），但服务端 key 在 ttl+grace 之后仍挂在这个 holder "
                                      "名下：崩溃的持有者留下了死锁")}]

                         phantom
                         [{:type :lock-phantom-loss
                           :name name
                           :holder holder
                           :lease-id (:lease-id op)
                           :agent host
                           :acquired-at-ms start
                           :seen-at-ms (t-of phantom)
                           ;; 诊断字段：这个 TTL 是否也小于续期节拍（第二个成因）
                           :ttl-below-renew-cadence? (not cadence?)
                           :server (if (:exists? phantom)
                                     (get-in phantom [:server :holder-id])
                                     :absent)
                           :note (str "持有 agent 仍然活着（下一个故障在 "
                                      (or down :run-end) "），但服务端 key 已经不挂在"
                                      "这个 holder 名下了：自述与服务端真相不一致")}]

                         :else [])]
          (cond-> (update acc :violations into new-vios)
            judged?       (update :judged inc)
            (not judged?) (update :unjudged conj
                                  {:name name
                                   :holder holder
                                   :agent host
                                   :reason (cond
                                             (nil? host) :no-agent-attribution
                                             (empty? samples) :no-probe-sample
                                             (nil? down) :no-fault-window
                                             (not cadence?) :ttl-below-renew-cadence
                                             :else :no-sample-in-window)}))))
      {:violations [] :judged 0 :unjudged []}
      (abandon-ops ops))))

(defn checker
  "M5a lock checker。opts：

    :min-acquires   样本门槛（默认 0；§5.1 = 200）
    :tolerance-ms   重叠判据的**最小可分辨重叠**（默认 50，见
                    default-overlap-tolerance-ms；同一 JVM 单一时钟，但区间闭合
                    时刻本身是一个 RPC 上界）
    :grace-ms       释放后消失窗口（默认 2000；op 自带 :grace-ms 时以 op 为准）
    :probe?         是否要求地面真值探针在场（默认 false；lock workload 传 true）
    :probe-margin-ms 服务端真相判据的边界容差（默认 100，见 default-probe-margin-ms）
    :agent-nodes    隧道 endpoint → agent 主机名（AG-06 判据的归因桥；缺省如此
                    则弃锁判据全部记成未判，不会静默算绿）
    :abandon?       是否要求弃锁样本（默认 false；lock workload 传 true）——
                    生成器必然产出弃锁 op，所以「一条都没有」只能是接线断了
    :min-abandon    弃锁判定条数的下界（默认 1；只用于「真的判过」这条门槛）"
  ([] (checker {}))
  ([{:keys [min-acquires tolerance-ms grace-ms probe? probe-margin-ms
            agent-nodes abandon? min-abandon]}]
   (let [min-acquires (long (or min-acquires 0))
         tolerance-ms (long (or tolerance-ms default-overlap-tolerance-ms))
         grace-ms     (long (or grace-ms default-grace-ms))
         probe?       (boolean probe?)
         abandon?     (boolean abandon?)
         min-abandon  (long (or min-abandon 1))
         margin-ms    (long (or probe-margin-ms default-probe-margin-ms))]
     (reify checker/Checker
       (check [_ _test history _opts]
         (let [{:keys [ops]} (wi/pair-invokes history)
               lops     (lock-ops ops)
               ivs      (vec (keep held-interval lops))
               done     (filterv #(= :ok (:type %)) lops)
               acquired (filterv :acquired-at-ms done)
               contended (count (filter #(= :lock-held (:error %)) lops))
               probes   (probe-ops ops)
               probe-read-failures (count (filter #(= :info (:type %)) probes))
               ;; run 结束时刻必须与 `abs-ms` **同一个时间轴**：`abs-ms` 优先用 op
               ;; 自读的 `:t0-ns`（`System/nanoTime`，ms-since-boot），而 jepsen 记录
               ;; 的 `:time` 是**相对测试起点**的纳秒。混用两者的后果实测过一次
               ;; （2026-09-19 lock/none）：`before` 被算成 15_619ms 而样本时刻是
               ;; 744_614_580ms ⇒ 每条弃锁都变成「窗口内一条样本都没有」
               ;; （`:no-fault-window`）。这是 F-34 「两个时间轴混用」的第二次复发，
               ;; 教训：取 run 边界也要走同一个 `abs-ms`。
               end-ms   (long (or (last (sort (map #(abs-ms % 0) ops))) 0))
               abandon  (abandon-ops ops)
               aanalysis (abandon-analysis ops agent-nodes end-ms)
               orphan   (filterv #(= :lock-orphan-not-reclaimed (:type %))
                                 (:violations aanalysis))
               phantom  (filterv #(= :lock-phantom-loss (:type %))
                                 (:violations aanalysis))
               unjudged (:unjudged aanalysis)
               truth    (server-truth-violations ops ivs margin-ms)
               truth-hard (filterv :hard? truth)
               me-pairs (mutual-exclusion-pairs ops)
               me-hard  (filterv #(> (long (:overlap-ms %)) tolerance-ms) me-pairs)
               me-near  (filterv #(<= (long (:overlap-ms %)) tolerance-ms) me-pairs)
               fails    (vec (concat me-hard
                                     (fencing-violations ops)
                                     (not-freed-violations ops grace-ms)
                                     truth-hard
                                     orphan
                                     phantom))
               by-class (frequencies (map :type fails))
               sample-ok? (>= (count acquired) min-acquires)
               probe-ok? (or (not probe?) (pos? (count probes)))
               abandon-judged (:judged aanalysis)
               h2-cadence (count (filter :ttl-below-renew-cadence? phantom))
               abandon-ok? (or (not abandon?)
                               (and (pos? (count abandon))
                                    (>= abandon-judged min-abandon)))
               summary {:acquires (count acquired)
                        :min-acquires min-acquires
                        :not-acquired contended
                        :release-failures (count (filter #(and (:acquired-at-ms %)
                                                               (not (:released? %)))
                                                         done))
                        :foreign-release-rejected
                        (count (filter #(and (get-in % [:foreign-release :ok?])
                                             (false? (get-in % [:foreign-release :released?])))
                                       done))
                        :intervals (vec (take 50 ivs))
                        :mutual-exclusion {:hard (count me-hard)
                                           :near-boundary (count me-near)
                                           :tolerance-ms tolerance-ms
                                           :max-overlap-ms (long (or (last (sort (map :overlap-ms me-pairs))) 0))}
                        ;; AG-06：弃锁的服务端回收（H1）与不得假丢锁（H2）
                        :abandon {:ops (count abandon)
                                  :orphans (count orphan)
                                  :phantom-loss (count phantom)
                                  :judged abandon-judged
                                  :unjudged (vec (take 5 unjudged))
                                  :unjudged-count (count unjudged)
                                  ;; 违反里有多少条同时落在「TTL ≤ 续期节拍」这一档
                                  ;; （诊断用；不是豁免）
                                  :phantom-loss-below-cadence h2-cadence
                                  :renew-cadence-ms renew-cadence-ms
                                  :fault-windows (into {}
                                                       (map (fn [[h ws]]
                                                              [h (mapv #(select-keys % [:kind :from-ms :to-ms :closed?])
                                                                       ws)]))
                                                       (:by-host (fw/windows ops)))}
                        :probe {:samples (count probes)
                                :read-failures probe-read-failures
                                :with-key (count (filter :exists? probes))
                                :distinct-holders
                                (count (distinct (keep #(get-in % [:server :holder-id]) probes)))
                                :contradictions (count truth-hard)
                                :near-boundary (count (remove :hard? truth))}
                        :violations-by-class by-class}
               reasons (cond-> []
                         (not sample-ok?)
                         (conj {:type :insufficient-sample
                                :acquires (count acquired)
                                :required min-acquires
                                :note "lock 样本不足：该 cell 视为未执行（§5.1），不得判绿"})
                         (not probe-ok?)
                         (conj {:type :lock-probe-missing
                                :samples 0
                                :note "本 workload 声明了地面真值探针却没有一条探针样本：互斥的服务端口径无证据，判未执行（不是绿）"})
                         (not abandon-ok?)
                         (conj {:type :lock-abandon-unjudged
                                :abandon-ops (count abandon)
                                :judged abandon-judged
                                :required min-abandon
                                :unjudged (vec (take 5 unjudged))
                                :note (str "弃锁（AG-06）没有可判定的样本："
                                           "要么生成器没产出弃锁 op、要么缺故障窗 / "
                                           "缺 agent 归因 / 缺故障后的探针样本。"
                                           "未判定不等于通过（§5.5 第 9 条）")})
                         (seq orphan)
                         (conj {:type :lock-orphan-not-reclaimed
                                :count (count orphan)
                                :advisory? false
                                :sample (vec (take 3 orphan))
                                :note "持有者 agent 已不可能续期，但服务端锁 key 在 ttl+grace 后仍挂着：崩溃的持有者留下死锁（AG-06）"})
                         (seq phantom)
                         (conj {:type :lock-phantom-loss
                                :count (count phantom)
                                :below-renew-cadence h2-cadence
                                :sample (vec (take 3 phantom))
                                :note "持有 agent 活着，但服务端锁 key 已不在该 holder 名下：自述与服务端真相不一致（假丢锁）。若这里的 below-renew-cadence 等于 count，先看 TTL 是否 ≤ 续期节拍 10s；但两者都不是豁免理由"})
                         (seq truth)
                         (conj {:type :lock-server-truth
                                :hard (count truth-hard)
                                :near-boundary (count (remove :hard? truth))
                                :margin-ms margin-ms
                                :note "服务端 key 的持有者与自述区间矛盾（硬违约计入 valid?；边界争议只报到报告里）"})
                         (seq me-near)
                         (conj {:type :lock-mutual-exclusion-near-boundary
                                :count (count me-near)
                                :tolerance-ms tolerance-ms
                                :max-overlap-ms (long (apply max (map :overlap-ms me-near)))
                                :sample (vec (take 3 me-near))
                                :note "重叠量在测量分辨率以内（≤ tolerance）：只记录不判红 —— 区间闭合时刻是客户端**观察到**的时刻，本身就是一个 RPC 的上界"})
                         (seq fails)
                         (conj {:type :lock-violations
                                :count (count fails)
                                :sample (vec (take 3 fails))}))]
           (info "lock checker:" (pr-str (assoc summary :failures (take 10 fails))))
           (cond-> {:valid? (and sample-ok? probe-ok? abandon-ok? (empty? fails))
                    :lock summary
                    :failures reasons}
             (seq fails) (assoc :violations (vec (take 20 fails))))))))))
