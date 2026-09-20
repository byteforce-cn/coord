(ns jepsen.coord.regck
  "M5a —— 服务注册发现的 checker（缺口 AG-07/AG-09 的实例面）。

  ## 契约与**刻意陈旧**（本 checker 最重要的背景）

  `coord-agent/src/services/registry.rs:6-9` 写明三件事：

    1. 本地缓存全量注册表（延迟 <1ms）；
    2. **与 Server 断连时保留最后已知实例快照（自我保护）**；
    3. 通过 Lease 绑定实现实例自动过期。

  第 2 条意味着「分区期间返回陈旧注册表」是**有意设计**，不是缺陷。所以本
  checker **不**把陈旧本身判红（那会产出假红）；它判的是契约真正承诺的两件事：

    * **健康态下不该有幽灵实例**：停止续约的实例必须在 `ttl + grace` 内从
      发现结果里消失（泄漏的实例会被上层一直调用 → 生产上是「调用已下线的
      实例」这一类故障）；
    * **注册后必须立即可见**（否则服务发现不可用）。

  至于「分区期间允许陈旧到多久 / 是否必须可区分」，属 §7 待书面确认项
  （staleness 上界 X 未确认前，本 checker 只判健康态与自身观测的那条链）。

  ## 判据

  1. `:registry-ghost-instance`（**P0**）—— 两个来源：
     a) **自身观测**：`registry-cycle` 的 op 自己停心跳后，在 `ttl+grace` 内
        仍能看到自己；
     b) **跨客户端快照**：某个 `:registry-discover` 探针在 `t` 时刻仍列出某个
        实例，而该实例的**属主 op** 早在 `t` 之前就已经确认它消失了
        （用 `:absent-ms` 与属主 op 的起点重建绝对时刻）。这是「自我保护快照」
        最可能露出来的形态：属主已经判它没了，别的客户端还能看见。
  2. `:registry-not-visible-after-register`（P1）—— 注册成功后立刻 Discover
     看不见自己（活性）。
  3. `:registry-not-gone-after-ttl`（P1）—— 停心跳后 `ttl+grace` 内没有消失。
  4. `:registry-dup-not-idempotent`（P1）—— 同 (service, instance) 连登两次后，
     发现结果里出现该实例**多条**记录（台账要求「重复注册幂等」）。
  5. 样本门槛 `:min-cycles`（§5.1：注册/到期循环 ≥ 30）—— 不足 ⇒ invalid。

  ## 时间基准

  同 lock/election：控制机单一时钟，`a` 类判据是精确的。`b` 类判据需要把
  「属主 op 的起点 + 相对毫秒」换算回探针 op 的时间轴 —— 两者都在同一个 JVM
  里，差异只来自 jepsen 记录 `:time` 的粒度，故用 `tolerance-ms`（默认 200）
  吸收。

  ## 漏检边界（R2）

  * 只判 `:exact` 过滤模式的 Discover（PREFIX/ALL 未覆盖）；
  * 不判 metadata 内容与 revision 单调性；
  * 跨 agent 的 **Watch 推送**未覆盖（M5b 的订阅面）。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]))

(def default-grace-ms 2000)
(def default-tolerance-ms 200)

(defn- cycle-ops [ops] (filterv #(= :registry-cycle (:f %)) ops))
(defn- discover-ops [ops] (filterv #(= :registry-discover (:f %)) ops))

(defn- obs-of [op phase]
  (first (filter #(= phase (:phase %)) (:observations op))))

(defn- self-detected-ghosts
  "判据 1a：op 自己停心跳后在 ttl+grace 外仍**看到**自己。

  两条证据路径，**都必须有成功读过作证据**：
    a) `:absent-ms` 存在但超出 `ttl+grace`（确实看见它消失得太晚）；
    b) `:after-ttl` 那次观测读成功且 `:present? true`（到点了还看得见）。

  若从未成功读到（分区/agent 不可用），**不判** —— 那属于 unjudged
  （与 leaseck 的 `:read-ok?` 纪律、F-26 的教训同源：把「读不到」当成
  「不存在」或「还活着」都会产出假红/假绿）。"
  [ops]
  (->> (cycle-ops ops)
       (filter #(= :ok (:type %)))
       (keep (fn [op]
               (let [ttl    (* 1000 (long (or (:holder-ttl-seconds op) 0)))
                     grace  (long (or (:grace-ms op) default-grace-ms))
                     absent (:absent-ms op)
                     o      (obs-of op :after-ttl)
                     late-read? (and o (:read-ok? o) (:present? o))]
                 (when (or (and absent (> (long absent) (+ ttl grace)))
                           late-read?)
                   {:type :registry-ghost-instance
                    :source :self
                    :name (:name op)
                    :instance-id (:instance-id op)
                    :absent-ms absent
                    :ttl-ms ttl
                    :grace-ms grace
                    :observation o
                    :note "停止续约的实例未在 ttl+grace 内从发现结果中消失"}))))
       vec))

(defn- unjudged-gone?
  "停心跳后的消失判定因**缺证据**而未判（所有读都失败）。不判 ≠ 通过。"
  [op]
  (let [o (obs-of op :after-ttl)]
    (and (nil? (:absent-ms op))
         (not (and o (:read-ok? o) (:present? o))))))

(defn- cross-client-ghosts
  "判据 1b：探针快照列出了「属主已确认消失」的实例。

  绝对时刻重建：属主 op 的 `:time`（jepsen 历史时刻，纳秒）是它的起点，
  `:absent-ms` 是相对起点的毫秒 ⇒ 消失时刻 = `:time/1e6 + absent-ms`。
  探针快照的时刻同样由 `:time/1e6` 得到。只当探针时刻**晚于**消失时刻
  超过 tolerance 时才算。"
  [ops tolerance-ms]
  (let [gone (into {}
                   (for [op (cycle-ops ops)
                         :when (and (:instance-id op) (:absent-ms op) (:time op))]
                     [[(:name op) (:instance-id op)]
                      (+ (quot (long (:time op)) 1000000) (long (:absent-ms op)))]))]
    (->> (discover-ops ops)
         (mapcat (fn [op]
                   (let [at (+ (quot (long (or (:time op) 0)) 1000000)
                               (long (or (:latency-ms 0) 0)))]
                     (for [inst (:instances op)
                           :let [g (get gone [(:name op) inst])]
                           :when (and g (> (long at) (+ (long g) (long tolerance-ms))))]
                       {:type :registry-ghost-instance
                        :source :other-client
                        :name (:name op)
                        :instance-id inst
                        :seen-at-ms at
                        :gone-at-ms g
                        :tolerance-ms (long tolerance-ms)
                        :note "别的客户端仍能发现「属主已确认消失」的实例（自我保护快照未收敛）"}))))
         vec)))

(defn- not-visible
  "判据 2：注册后立刻看不见自己。读失败（read-ok? false）不判。"
  [ops]
  (->> (cycle-ops ops)
       (filter #(= :ok (:type %)))
       (keep (fn [op]
               (let [o (obs-of op :after-register)]
                 (when (and o (:read-ok? o) (not (:present? o)))
                   {:type :registry-not-visible-after-register
                    :name (:name op)
                    :instance-id (:instance-id op)
                    :observation o
                    :note "Register 成功之后立刻 Discover 看不到自己"}))))
       vec))

(defn- not-gone
  "判据 3：停心跳后 ttl+grace 内未消失。"
  [ops]
  (->> (cycle-ops ops)
       (filter #(= :ok (:type %)))
       (keep (fn [op]
               (let [ttl   (* 1000 (long (or (:holder-ttl-seconds op) 0)))
                     grace (long (or (:grace-ms op) default-grace-ms))
                     o     (obs-of op :after-ttl)]
                 (when (and o (:read-ok o) (:present? o)
                            (let [at (long (:at-ms o))]
                              (> at (+ ttl grace))))
                   {:type :registry-not-gone-after-ttl
                    :name (:name op)
                    :instance-id (:instance-id op)
                    :observation o
                    :ttl-ms ttl :grace-ms grace
                    :note "停止续约后实例在 ttl+grace 内仍可被发现"}))))
       vec))

(defn- dup-not-idempotent
  "判据 4：同 (service, instance) 连登两次后出现重复条目。"
  [ops]
  (->> (cycle-ops ops)
       (filter :dup?)
       (keep (fn [op]
               (let [o (obs-of op :after-register)
                     insts (:instances o)
                     n (count (filter #(= (:instance-id op) %) insts))]
                 (when (> (long n) 1)
                   {:type :registry-dup-not-idempotent
                    :name (:name op)
                    :instance-id (:instance-id op)
                    :duplicate-entries (long n)
                    :instances insts
                    :note "重复注册同一 (service, instance) 后出现多条记录（台账要求幂等）"}))))
       vec))

(defn checker
  "M5a registry checker。opts：

    :min-cycles     样本门槛（默认 0；§5.1 = 30）
    :grace-ms       ttl 之后的宽限（默认 2000）
    :tolerance-ms   跨客户端快照判据的时刻容差（默认 200）"
  ([] (checker {}))
  ([{:keys [min-cycles grace-ms tolerance-ms]}]
   (let [min-cycles   (long (or min-cycles 0))
         tolerance-ms (long (or tolerance-ms default-tolerance-ms))]
     (reify checker/Checker
       (check [_ _test history _opts]
         (let [{:keys [ops]} (wi/pair-invokes history)
               cops  (cycle-ops ops)
               done  (filterv #(= :ok (:type %)) cops)
               fails (vec (concat (self-detected-ghosts ops)
                                  (cross-client-ghosts ops tolerance-ms)
                                  (not-visible ops)
                                  (not-gone ops)
                                  (dup-not-idempotent ops)))
               by-class (frequencies (map :type fails))
               sample-ok? (>= (count done) min-cycles)
               summary {:cycles (count done)
                        :min-cycles min-cycles
                        :instances (count (distinct (map :instance-id done)))
                        :discover-probes (count (discover-ops ops))
                        :deregistered (count (filter :deregistered? done))
                        ;; 缺证据而未判的消失判定（不判 ≠ 通过）
                        :gone-unjudged (count (filter unjudged-gone? done))
                        :violations-by-class by-class}
               reasons (cond-> []
                         (not sample-ok?)
                         (conj {:type :insufficient-sample
                                :cycles (count done)
                                :required min-cycles
                                :note "registry 样本不足：该 cell 视为未执行（§5.1），不得判绿"})
                         (seq fails)
                         (conj {:type :registry-violations
                                :count (count fails)
                                :sample (vec (take 3 fails))}))]
           (info "registry checker:" (pr-str (assoc summary :failures (take 10 fails))))
           (cond-> {:valid? (and sample-ok? (empty? fails))
                    :registry summary
                    :failures reasons}
             (seq fails) (assoc :violations (vec (take 20 fails))))))))))
