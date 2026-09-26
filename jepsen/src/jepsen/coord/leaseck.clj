(ns jepsen.coord.leaseck
  "T2.2 —— lease workload 的 checker（覆盖缺口 A6：Lease 未测）。

  ## 契约（`apis/contracts/proto/coord/lease/lease.proto`）

    * Lease 提供 TTL 生命周期 + 保活，是「服务注册 / 分布式锁」的基础原语；
    * **级联删除**：Lease 过期或被 Revoke 时，所有绑定该 Lease 的 Key 被删除
      （删除事件正常投递给 Watch 订阅者）；
    * KeepAlive 是双向流：客户端持续发请求、服务端逐条回应；
      **流断开后应在 TTL 内重连续期**，TTL 内未完成续期的 Lease 被回收；
    * 回应里 **ttl = 0 表示 Lease 已不存在**（过期或被 Revoke），须重新 Grant；
    * **过期判定以服务端单调时钟为准**，不受节点系统时钟调整影响（F-08 已从
      源码确认：`coord-server/src/lease/mod.rs:43` 用 `tokio::time::Instant`）；
    * 【不承诺】精确过期时刻：实际回收可能略晚于 TTL（依赖 Leader 侧定时调度，
      Leader 切换后由新 Leader 重建）⇒ 判定必须带 grace。

  ## 判据（dev.md §5.1/§5.2 的可判定化）

  1. `:lease-safety-early-absence`（**P0**）——绑定的 Key 在 Lease **仍然存活**
     的时候被观察到不存在。存活期 = `ttl`（无续期）或**续期窗口 + ttl**
     （KeepAlive 场景），timeout 用响应里**实际授予**的 ttl（服务端可调整）；
  2. `:lease-write-not-visible` —— `Put{lease_id}` 返回 `:ok` 之后立刻点读看不到
     Key（写入未被观察到，或绑定写被过早清掉）；
  3. `:lease-not-expired`（活性）—— 停续期/到期后 Key 在 `ttl + grace` 内没有
     消失（§5.2「停止续租的 Key 在 ttl+grace 内消失率 100%」）；
  4. `:lease-revoke-not-cascaded`（**P0**）—— Revoke 之后 Key 在 `grace` 内没有
     消失（级联删除未兑现）；
  5. `:lease-keepalive-after-revoke` —— Revoke 之后对该 Lease 续期回了 `ttl > 0`
     （契约：ttl=0 = 已不存在）；
  6. `:lease-keepalive-not-extended` —— 续期场景里**没有一次** `ttl > 0` 的回应
     （KeepAlive 流根本没在续期），却要求「续期期内 Key 仍在」——那说明
     「Key 还在」不是续期换来的，样本不可信；
  7. 样本门槛：`grants < --lease-min-grants` 或 `expiries < --lease-min-expiries`
     ⇒ invalid（§5.1「grant ≥ 100；到期场景 ≥ 30」；未执行既不算绿也不算红）。

  ## 时间基准（可判定性的关键）

  所有 `:at-ms` 都是**客户端单调量**（`System/nanoTime` 相对 op 起点），
  **不跨机比较时钟** —— 所以 TTL 判定不受节点间时钟差影响，也不受墙钟跳变
  影响（与 F-08 的单调时钟口径一致）。服务端侧只提供「实际授予的 ttl」。

  ## 漏检边界

  * 精确过期**时刻**不判（契约明确「不承诺」）：只判 ≥ ttl 且 ≤ ttl+grace；
  * 读失败（`:read-ok? false`）**不**当作「Key 消失」：只有 `:read-ok? true`
    且 `:present? false` 的观测参与判据 1/3/4（否则一次 transient 读失败会产出
    一条像被测系统违约的假红）；
  * **F-71（2026-09-25）**：轮询**从未读成功**（`:ok-reads = 0`，典型形态是
    CCT 集中过期波期间读全部 `:unauthenticated`）时，判据 3/3'/4 **不判**，
    计入 summary 的 `:liveness-unjudged`（不判 ≠ 通过；客户端已在轮询内刷新
    会话重试，仍全失败则窗口内没有可信观测，不得据此报活性违反）；
  * **F-72（2026-09-26）**：**判定窗口末端采样断档**（新格式观测里
    `:last-ok-at-ms` 远早于窗口末端，形态：leader 被 SIGSTOP ⇒ 轮询读卡在
    冻结节点上）时，判据 3/3'/4 同样**不判**，计入 `:liveness-unjudged`；
    客户端已修（单节点读 500ms 超时）；旧格式观测（无 `:last-ok-at-ms`）
    语义不变；
  * Leader 切换后由新 Leader 重建 TTL（契约明确可能略晚）⇒ grace 是必要条件，
    不是宽容；`pause`（长冻结）与 `kill`（重启丢 deadline）是 T3.4 的证伪故障。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]))

(def default-grace-ms
  "§5.4-⑤ 的 lease grace：`ttl + grace` 内必须消失。默认 2×ttl 由 workload 传入；
  这里只做兜底（checker 侧不猜 ttl，用 op 汇报的 `:grace-ms`）。"
  2000)

(def default-tolerance-ms
  "客户端与服务端计时精度差 + 单调采样误差的容差（§5.4-① 同源的 500ms 口径）。
  只在「Key 提前消失」的安全判定上放宽这一点点——宁可漏报也不产假红。"
  500)

(defn- lease-ops
  "历史里的 lease 完成 op（任一种 completion 都收：失败的也有诊断价值，
  但只有 `:ok` 的参与判据）。"
  [ops]
  (filterv #(contains? #{:lease-ttl :lease-keepalive :lease-revoke} (:f %)) ops))

(defn- ok-obs
  "只有**读成功**的观测参与判定（读失败 ≠ Key 消失）。"
  [op & phases]
  (->> (:observations op)
       (filter #(and (:read-ok? %) (contains? (set phases) (:phase %))))))

(defn- ttl-ms [op] (* 1000 (long (get-in op [:lease :ttl] 0))))
(defn- grace-ms [op] (long (or (:grace-ms op) default-grace-ms)))

(defn- early-absence
  "判据 1：在 Lease 仍然存活的窗口内观察到 Key 不存在。

  存活窗口按场景算：
    * `:ttl`       —— [grant, grant+ttl)，只到 ttl（之后它**应该**消失）；
                     F-72b：锚在 grant ack（`:grant-at-ms`，旧数据回退 op 起点）——
                     故障注入下 setup 本身可能花数秒，从 op 起点起算会误判。
    * `:keepalive` —— [0, 停续期时刻 + ttl)，停续期后还有一个 ttl 的余寿；
    * `:revoke`    —— [0, revoke 时刻)，Revoke 之前一秒都不许少。
  容差 `tolerance-ms` 只用于「提前」这一侧。"
  [op tolerance-ms]
  (let [ttl (ttl-ms op)
        tol (long tolerance-ms)
        stop (cond
               (= :keepalive (:scenario op))
               (long (or (:at-ms (first (ok-obs op :during-keepalive))) 0))

               (= :revoke (:scenario op))
               (long (or (:revoke-at-ms op) 0))

               :else ttl)
        alive-until (case (:scenario op)
                      :keepalive (+ stop ttl)
                      :revoke    stop
                      (+ (long (or (:grant-at-ms op) 0)) ttl))]
    (vec
      (for [o (ok-obs op :after-write :during-keepalive :first-absent :after-revoke)
            :when (not (:present? o))
            :when (< (long (:at-ms o)) (- (long alive-until) tol))]
        {:type :lease-safety-early-absence
         :scenario (:scenario op)
         :lease (:lease op)
         :at-ms (:at-ms o)
         :alive-until-ms alive-until
         :tolerance-ms tol
         :observation o
         :note "绑定的 Key 在 Lease 存活期内被观察到不存在（级联删除过早 / 丢失绑定 / Lease 提前回收）"}))))

(defn- relative-absence
  "消失耗时（ms）相对**本场景的锚点**：

    * `:ttl`       —— 锚点就是 op 起点，故等于 `:absent-ms`；
    * `:keepalive` —— 锚点是「停续期」那一刻（关流之后的那次点读
                      `:during-keepalive` 的 `:at-ms`）；
    * `:revoke`    —— 锚点是 Revoke 请求发出的时刻 `:revoke-at-ms`。

  **锚点缺失时返回 nil**（例如 `:during-keepalive` 那次点读失败 —— 它被
  `ok-obs` 按 `:read-ok?` 过滤掉了）。F-26：第一版直接 `(long rel-absent)`，
  真实 run 里只要锚点缺失就 NPE ⇒ jepsen 把整个 checker 判成 `:unknown`
  （**既不是红也不是绿，退出码 2**）—— 那是比假绿更隐蔽的「没有结论」。"
  [op absent]
  (case (:scenario op)
    :keepalive
    (when (and absent (first (ok-obs op :during-keepalive)))
      (- (long absent)
         (long (:at-ms (first (ok-obs op :during-keepalive))))))

    :revoke
    (when (and absent (:revoke-at-ms op))
      (- (long absent) (long (:revoke-at-ms op))))

    absent))

(defn- poll-unjudged?
  "F-71：轮询期间**没有任何一次读成功**（`:ok-reads = 0`）⇒ 「没观察到消失」
  不是活性违反，而是**没判**（不判 ≠ 通过，计入 summary 的
  `:liveness-unjudged`）。

  读失败的典型来源：CCT 集中过期波（客户端侧已在轮询里刷新会话重试；
  仍全失败时说明窗口内根本没有可信观测）。旧格式观测（无 `:ok-reads` 字段）
  按旧口径判 —— 历史 fixture 语义不变。"
  [op]
  (let [o (first (filter #(= :first-absent (:phase %)) (:observations op)))]
    (and o (some? (:ok-reads o)) (zero? (long (:ok-reads o))))))

(defn- observation-truncated?
  "F-72：观测序列是否在**判定窗口末端之前就断了** —— 最后一次成功观测
  （`:last-ok-at-ms`）早于窗口末端减容差。

  形态（matrix-m2 lease/pause，seed 737037196）：n1 被 SIGSTOP 10.4s，轮询的
  下一次读卡在冻结节点上等满默认 5s 超时 ⇒ 判定窗口在 [22.7s, 27.7s] 内**零次
  成功观测**；旧口径把 `:absent-ms nil` 判成 `:lease-not-expired`，但那是采样
  断档，不是产品违约（客户端侧 F-72 修复后单节点读 500ms 超时，断档本身也应
  降到最小；本判据是兜底：断档 ⇒ 不判，计入 `:liveness-unjudged`）。

  窗口末端按场景推（锚点均用客户端单调量）：
    :ttl       → grant ack + ttl + grace（F-72b 锚；旧数据回退 op 起点）；
    :keepalive → 停续期时刻 + ttl + grace；
    :revoke    → revoke 时刻 + grace。

  只在新格式观测（带 `:last-ok-at-ms`）上生效：旧 fixture 语义不变；
  无法计算窗口（锚点缺失）时返回 nil（= 不管，保持旧口径）。"
  [op tolerance-ms]
  (let [o       (first (filter #(= :first-absent (:phase %)) (:observations op)))
        last-ok (when o (:last-ok-at-ms o))
        end     (case (:scenario op)
                  :ttl (+ (long (or (:grant-at-ms op) 0)) (ttl-ms op) (grace-ms op))
                  :keepalive (when-let [stop (:at-ms (first (ok-obs op :during-keepalive)))]
                               (+ (long stop) (ttl-ms op) (grace-ms op)))
                  :revoke (when-let [r (:revoke-at-ms op)]
                            (+ (long r) (grace-ms op)))
                  nil)]
    (when (and o (some? last-ok) (some? end))
      (boolean (< (long last-ok) (- (long end) (long tolerance-ms)))))))

(defn- late-absence-unattributable?
  "F-72c：**迟到性判据**（absent 晚于窗口末端）只在轮询采样**未断档**时成立。

  轮询期间有读失败（`:failed > 0`）时，观测到的 absent 时刻无法归因为删除时刻：
  实测（matrix-m2 lease/pause，2026-09-26）ka/109：服务端在停续期后 ~2.1s 就提交了
  revoke（http 日志 `revokes committed lease_ids=[78]`，早于窗口末端），但读数在
  13.7s 才恢复 ⇒ 拿 13.78s 当删除时刻会假红。

  注意：该规则只豁免**迟到性**，不豁免「从未观察到消失」（absent nil 走
  F-71/F-72 的轮询/断档分支）与「提前消失」（early-absence 是安全侧，不受影响）。"
  [op]
  (let [o (first (filter #(= :first-absent (:phase %)) (:observations op)))]
    (and o (pos? (long (or (:failed o) 0))))))

(defn- liveness-fails
  "判据 2/3/4/5/6。活性（3/4）只在**锚点存在、且采样未断档**时判；锚点缺失
  （F-26）、轮询全读失败（F-71）或窗口末端采样断档（F-72）的 op 记入 summary 的
  `:liveness-unjudged`（不判 ≠ 通过）。"
  [op tolerance-ms]
  (let [ttl   (ttl-ms op)
        grace (grace-ms op)
        absent (:absent-ms op)
        rel    (relative-absence op absent)
        rel?   (some? rel)
        ;; F-72b：ttl 场景的窗口锚在 grant ack（旧数据回退 op 起点）
        ttl-base (long (or (:grant-at-ms op) 0))
        ttl-end  (+ ttl-base ttl grace)]
    (cond-> []
      ;; 判据 2：写入后必须立刻可见
      (let [o (first (ok-obs op :after-write))]
        (and o (not (:present? o))))
      (conj {:type :lease-write-not-visible
             :scenario (:scenario op)
             :lease (:lease op) :observation (first (ok-obs op :after-write))
             :note "Put{lease_id} 返回 :ok 后立刻点读看不到该 Key"})

      ;; 判据 3：到期后必须在 ttl+grace 内消失（F-72b：锚 = grant ack）
      ;; F-71：轮询从未读成功（`:ok-reads = 0`）时不判（计入 :liveness-unjudged）
      ;; F-72：窗口末端采样断档（`:last-ok-at-ms` 远早于 ttl+grace）时同样不判
      ;; F-72c：迟到性只在采样未断档时可归因（有读失败 ⇒ 不判）
      (and (not (poll-unjudged? op))
           (not (and (nil? absent) (observation-truncated? op tolerance-ms)))
           (= :ttl (:scenario op))
           (or (nil? absent)
               (and (not (late-absence-unattributable? op))
                    (> (long absent) ttl-end))))
      (conj {:type :lease-not-expired
             :scenario (:scenario op) :lease (:lease op) :key (:key op)
             :absent-ms absent :ttl-ms ttl :grace-ms grace
             :note "停续期的 Key 未在 ttl+grace 内消失（§5.2 活性门槛）"})

      ;; 判据 3'：停续期后必须在 ttl+grace 内消失（锚点 = 停续期时刻）。
      ;; 注意 `(nil? absent)` 分支必须保留：「**从未消失**」本身就是活性违反，
      ;; 与锚点是否存在无关（重构时丢了它，守门员 fixture 立刻抓到）。
      ;; F-71：轮询从未读成功（`:ok-reads = 0`）时不判。
      ;; F-72：窗口末端采样断档时不判。
      (and (not (poll-unjudged? op))
           (not (and (nil? absent) (observation-truncated? op tolerance-ms)))
           (= :keepalive (:scenario op))
           (or (nil? absent)
               (and rel?
                    (not (late-absence-unattributable? op))
                    (> (long rel) (+ ttl grace)))))
      (conj {:type :lease-not-expired
             :scenario (:scenario op) :lease (:lease op) :key (:key op)
             :absent-ms absent :stop-relative-ms rel
             :ttl-ms ttl :grace-ms grace
             :note "停止续期后 Key 未在 ttl+grace 内消失"})

      ;; 判据 4：Revoke 级联删除（锚点 = Revoke 时刻）；F-71/F-72：不判条件同判据 3
      (and (not (poll-unjudged? op))
           (not (and (nil? absent) (observation-truncated? op tolerance-ms)))
           (= :revoke (:scenario op)) (:revoked? op)
           (or (nil? absent)
               (and rel?
                    (not (late-absence-unattributable? op))
                    (> (long rel) (long grace)))))
      (conj {:type :lease-revoke-not-cascaded
             :scenario (:scenario op) :lease (:lease op) :key (:key op)
             :absent-ms absent :revoke-relative-ms rel :grace-ms grace
             :note "LeaseRevoke 后绑定 Key 未在 grace 内被级联删除"})

      ;; 判据 5：Revoke 后 KeepAlive 必须回 ttl=0
      (and (= :revoke (:scenario op))
           (let [p (:post-revoke-keepalive op)]
             (and (map? p) (number? (:ttl p)) (pos? (long (:ttl p))))))
      (conj {:type :lease-keepalive-after-revoke
             :lease (:lease op)
             :ttl (:ttl (:post-revoke-keepalive op))
             :note "对已 Revoke 的 Lease 续期回了 ttl>0（契约：ttl=0 表示已不存在）"})

      ;; 判据 6：续期场景必须至少有一次 ttl>0 的回应
      (and (= :keepalive (:scenario op))
           (nil? (:keepalive-error op))
           (not (some #(and (number? (:ttl %)) (pos? (long (:ttl %))))
                      (:keepalives op))))
      (conj {:type :lease-keepalive-not-extended
             :lease (:lease op)
             :keepalives (vec (:keepalives op))
             :note "KeepAlive 从未回过 ttl>0：续期路径没真的生效（「Key 仍在」不可归因于续期）"}))))

(defn- unjudged-liveness?
  "该 op 的活性是否**无法判定**（必须计入 summary，不能静默通过）：
  ①锚点缺失（F-26 口径）；②F-71：轮询从未读成功（`:ok-reads 0`）；
  ③F-72：判定窗口末端采样断档；④F-72c：迟到性在采样断档时不可归因。"
  [op tolerance-ms]
  (or (and (contains? #{:keepalive :revoke} (:scenario op))
           (some? (:absent-ms op))
           (nil? (relative-absence op (:absent-ms op))))
      (poll-unjudged? op)
      (and (nil? (:absent-ms op))
           (observation-truncated? op tolerance-ms))
      (and (some? (:absent-ms op))
           (late-absence-unattributable? op))))

(defn checker
  "T2.2 checker。opts：

    :min-grants    样本门槛：授予成功的 Lease 数（默认 0 = 不门槛；§5.1 为 100）
    :min-expiries  样本门槛：真正观察到到期的场景数（默认 0；§5.1 为 30）
    :tolerance-ms  安全侧容差（默认 500，见 default-tolerance-ms）"
  ([] (checker {}))
  ([{:keys [min-grants min-expiries tolerance-ms]}]
   (let [min-grants   (long (or min-grants 0))
         min-expiries (long (or min-expiries 0))
         tolerance-ms (long (or tolerance-ms default-tolerance-ms))]
     (reify checker/Checker
       (check [_ _test history _opts]
         (let [{:keys [ops]} (wi/pair-invokes history)
               lops  (lease-ops ops)
               done  (filterv #(= :ok (:type %)) lops)
               ;; 样本口径：授予成功 = 完成 op 里带 lease id 的（无论场景）
               grants (count (filter #(pos? (long (or (get-in % [:lease :id]) 0)))
                                     done))
               by-scenario (frequencies (map :scenario done))
               expiries (count (filter #(and (= :ttl (:scenario %)) (:absent-ms %))
                                       done))
               fails (vec (concat
                            (mapcat #(early-absence % tolerance-ms) done)
                            (mapcat #(liveness-fails % tolerance-ms) done)))
               by-class (frequencies (map :type fails))
               sample-ok? (and (>= grants min-grants)
                               (>= expiries min-expiries))
               unjudged (count (filter #(unjudged-liveness? % tolerance-ms) done))
               summary {:grants grants
                        :expiries expiries
                        :min-grants min-grants
                        :min-expiries min-expiries
                        :by-scenario by-scenario
                        ;; 锚点缺失导致活性未判的数量（不判 ≠ 通过；F-26）
                        :liveness-unjudged unjudged
                        :keepalive-responses (reduce + 0 (map (fn [o] (count (:keepalives o)))
                                                               done))
                        :keepalive-stream-errors (count (filter :keepalive-error done))
                        :violations-by-class by-class}
               reasons (cond-> []
                         (not sample-ok?)
                         (conj {:type :insufficient-sample
                                :grants grants :expiries expiries
                                :required-grants min-grants
                                :required-expiries min-expiries
                                :note "lease 样本不足：该 cell 视为未执行（§5.1），不得判绿"})
                         (seq fails)
                         (conj {:type :lease-violations
                                :count (count fails)
                                :sample (vec (take 3 fails))}))]
           (info "lease checker:" (pr-str (assoc summary :failures (take 10 fails))))
           (cond-> {:valid? (and sample-ok? (empty? fails))
                    :lease  summary
                    :failures reasons}
             (seq fails) (assoc :violations (vec (take 20 fails))))))))))
