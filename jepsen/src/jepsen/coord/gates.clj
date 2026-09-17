(ns jepsen.coord.gates
  "T0.2 —— 长跑/矩阵的\"硬门槛\"checker（quiet 可用率 / RTO / 值唯一性前提）。

  与 `jepsen.coord.soak`（线性一致性）互补：线性一致性回答\"有没有数据错误\"，
  本 checker 回答三个**独立**的问题，且三者都必须计入 `:valid?`（C4：违例必须
  落到门禁上，不能只出一份报告）：

   1. **前提自检（G1）** —— soak/矩阵的 stale 判定把 write 的 value 当作全局
      序号（`jepsen.coord.soak` 的 forced-order 规则）。若同一个 value 被两次
      write *invoke* 使用（生成器复用、计数器重置、某个 workload 忘了用唯一
      值），那个判定就建立在错误前提上——宁可 invalid 也不能出假绿。
      同一 value 两次 write invoke → `:valid? false`。

   2. **quiet 窗口可用率（G2）** —— 每个 quiet 窗口（上一次 nemesis stop 完成 →
      下一次 nemesis start）内 write 的 `:ok` 率必须 ≥ 门槛。样本数 < 最小样本
      的窗口**记入 summary 但不参与判定**（窗口太短，偶然一次失败不代表可用性
      受损；把它当缺陷会制造假红）。

   3. **RTO（G4）** —— 每次 disruption stop 完成到**首个 `:ok` write** 的耗时，
      按 nemesis 分档判定 P95 上界，summary 同时输出 P95 与 max。没有任何后续
      `:ok` write 的 disruption 记为 `:unrecovered`（集群没回来），直接 invalid。

  时间基准：history 的 `:time` 是控制机**单调时钟纳秒**（history.txt 里的相对秒
  与之一致：一个 `--time-limit 60` 的 run 在 history.edn 里 max `:time` ≈
  1.2e11 = 120s。注意 `scripts/nemesis-timeline.clj` 把它当 epoch 毫秒、除以
  1000 打印成 `epoch 秒`——那个标签是错的，别被它误导），因此 RTO 的秒 =
  (t2 - t1) / 1e9。

   4. **空转检测 / op 级活性（G6）** —— 按 `:f` 分组的**每类**客户端 op，只要
      完成数 ≥ 最小样本，它的 `:ok` 率就必须 ≥ 门槛。这是为了抓「这个 workload
      根本没真的跑」：有一类 op 系统性失败（客户端构造报错 / RPC 不被支持 /
      服务端一致拒绝）而 knossos 对一份全是 `:info` 的历史照样判 valid —— 典型
      的**假绿**。2026-09-16 实测：`jepsen.coord.proto/txn-req` 用了不存在的
      `DynamicMessage$Builder.addAllField`，于是 cas / exists / txn-* 全部 op
      在客户端就抛异常、被 jepsen 记成 `:info`；CAS 矩阵与 map 矩阵都
        *看着是绿的*。第 4 条门槛就是为这一类缺陷装的（F-13）。

  默认值见 `jepsen/docs/dev.md` §5.4-②③，属**开发迭代**默认；验收级 run 必须用
  coord 团队书面确认的取值覆盖（`--soak-max-rto-seconds` 可全局覆盖 RTO 分档）。"
  (:require [jepsen.checker :as checker])
  (:import (java.util Arrays)))

;; ---------------------------------------------------------------------------
;; 参数（§5.4-②③ 计划默认值）
;; ---------------------------------------------------------------------------

(def default-quiet-availability-min
  "quiet 窗口 write `:ok` 率门槛（§5.4-③）。"
  0.95)

(def default-quiet-min-sample
  "quiet 窗口参与判定的最小 write 样本数；不足则仅记录不判定（§5.4-③）。"
  100)

(def default-rto-budgets
  "RTO 分档上界（秒），§5.4-② 计划默认值。"
  {:none        0
   :kill        120
   :pause       120
   :partition   120
   :membership  300
   :netem       600
   :disk        600
   ;; :all / :soak 是组合 nemesis（kill+pause+partition 轮换），按最紧的
   ;; 单步分档取上界
   :all         120
   :soak        120})

(def default-rto-budget
  "未知 nemesis 关键字时的保守 RTO 上界（秒）。"
  120)

(def default-min-op-ok-ratio
  "G6（F-13）：单类客户端 op 的 `:ok` 率下界。低于它的 op 类视为「没真的跑」。

  取 0.1 而不是 0.5：重扰动下真正的可用性下降会让 `:info` 变多（这是可接受的
  现象，由 quiet 窗口可用率与 RTO 分别负责）；本门槛只回答「这条路是不是
  压根没通」。

  分母里**扣除**§5.1 白名单里的合法确定性失败（如 `cas-miss`）：CAS 类 workload
  本来就大量 miss（实测 `cas-register` 一次 45s run：108 个 cas 里 95 个
  `cas-miss`），把它们计入分母会把合法工作负载判成「没跑」。`cas-miss` 是
  比较失败的**业务结果**，不是「路没通」。"
  0.1)

(def default-op-fail-whitelist
  "不计入 G6 分母的合法失败错误码（与 §5.1 的 `:fail` 白名单一致）。"
  #{:cas-miss :not-leader :lock-held :compacted :permission-denied})

(def default-min-op-sample
  "G6：单类 op 参与判定的最小完成数（不足只记录，不判定）。"
  10)

(def ^:private nanos-per-second
  "history 的 `:time` 单位：纳秒（见 ns docstring）。"
  1.0e9)

(defn rto-budget
  "RTO 上界（秒）：显式 `override` > `nemesis-key` 分档 > 默认。"
  [nemesis-key override]
  (or override
      (when nemesis-key (get default-rto-budgets nemesis-key))
      default-rto-budget))

;; ---------------------------------------------------------------------------
;; Nemesis 时间线
;; ---------------------------------------------------------------------------

(def ^:private start-fs
  "历史里表示\"扰动开始\"的 `:f`。jepsen 内置 nemesis 用 `:start`；本仓的组合
  nemesis（`jepsen.coord.nemesis/compose-all`）用 `:kill`/`:pause`/`:partition`。"
  #{:start :kill :pause :partition})

(def ^:private stop-fs
  "历史里表示\"扰动结束\"的 `:f`：jepsen 的 `:stop`，组合 nemesis 的
  `:kill-stop` / `:pause-stop` / `:partition-stop`（soak 收尾的全局 `:stop`）。"
  #{:stop :kill-stop :pause-stop :partition-stop})

(defn- nemesis-events
  "`[[time f] ...]`：历史里所有 nemesis start/stop op（`time` 升序）。"
  [history]
  (->> history
       (filter #(= :nemesis (:process %)))
       (keep (fn [op]
               (let [f (:f op)]
                 (when (and (contains? #{:info :ok} (:type op)) f
                            (or (contains? start-fs f)
                                (contains? stop-fs f)))
                   [(long (:time op)) f]))))
       (sort-by first)
       vec))

(defn- windows
  "把 nemesis 时间线切成两类窗口（都按 `:start`/`:end` 的毫秒时刻表示）：

    :disruptions —— `start` → `stop`（扰动在进行）
    :quiet       —— `stop` → 下一个 `start`（环境自愈期）

  第一个 quiet 窗口从 `t0`（数据面首个 op 的时刻）起算；最后一个 quiet 窗口
  延伸到 `t-end`（数据面最后一个 op 的时刻）。"
  [{:keys [events t0 t-end]}]
  (let [disruptions (loop [evs events, open nil, acc []]
                      (if-let [[t f] (first evs)]
                        (if (contains? start-fs f)
                          (recur (rest evs) t acc)
                          (recur (rest evs) nil
                                 (cond-> acc
                                   (some? open) (conj {:start open :end t}))))
                        acc))
        quiet (loop [evs events, last-stop t0, acc []]
                (if-let [[t f] (first evs)]
                  (if (contains? start-fs f)
                    (recur (rest evs) nil
                           (cond-> acc
                             (some? last-stop) (conj {:start last-stop :end t})))
                    (recur (rest evs) t acc))
                  (cond-> acc
                    (and (some? last-stop) (some? t-end)
                         (> t-end last-stop))
                    (conj {:start last-stop :end t-end}))))]
    {:disruptions disruptions
     :quiet quiet}))

;; ---------------------------------------------------------------------------
;; 数据面事实
;; ---------------------------------------------------------------------------

(defn- write-completions
  "`[[time type] ...]`：write op 的完成（`:ok`/`:info`/`:fail`），`time` 升序。"
  [history]
  (->> history
       (filter #(and (= :write (:f %))
                     (contains? #{:ok :info :fail} (:type %))))
       (map (fn [op] [(long (:time op)) (:type op)]))
       (sort-by first)
       vec))

(defn- first-at-or-after
  "已排序 long-array 中第一个 >= `t` 的元素；没有则 nil。"
  [^longs arr ^long t]
  (let [n (alength arr)]
    (when (pos? n)
      (let [i (Arrays/binarySearch arr t)]
        (cond
          (not (neg? i)) (aget arr i)
          :else (let [ins (- -1 i)]
                  (when (< ins n) (aget arr ins))))))))

(defn- percentile
  "`xs`（可为空）的线性插值百分位 `p` ∈ [0,1]；空集返回 nil。"
  [xs p]
  (let [v (vec (sort xs))
        n (count v)]
    (when (pos? n)
      (let [idx (* (double p) (dec n))
            lo  (long (Math/floor idx))
            hi  (long (Math/ceil idx))
            frac (- idx lo)]
        (if (= lo hi)
          (double (nth v lo))
          (+ (* (- 1.0 frac) (double (nth v lo)))
             (* frac (double (nth v hi)))))))))

;; ---------------------------------------------------------------------------
;; 三项断言
;; ---------------------------------------------------------------------------

(defn- value-uniqueness
  "G1 前提自检：每个 write *invoke* 的 `:value` 必须唯一。

  返回 `{:valid? bool :duplicate-values {...} :duplicate-value-count n
  :writes n}`。`:value` 为 nil 的写（不应出现在 soak 上）不计入。"
  [history]
  (let [by-val (->> history
                    (filter #(and (= :invoke (:type %))
                                  (= :write (:f %))
                                  (some? (:value %))))
                    (group-by :value))
        dups   (->> by-val
                    (filter (fn [[_ ops]] (> (count ops) 1)))
                    (sort-by (comp - count val))
                    vec)]
    {:valid? (empty? dups)
     :writes (count (filter #(and (= :invoke (:type %)) (= :write (:f %)))
                            history))
     :duplicate-value-count (count dups)
     :duplicate-values (into {} (take 20 dups))}))

(defn- availability-in
  "窗口内 write 完成的 `:ok` 率。`{:ok n :total n :ratio r}`；窗口内没有
  write 完成时 ratio = 1.0（无样本 ≠ 不可用）。"
  [writes {:keys [start end]}]
  (let [in-win (filter (fn [[t _]] (and (>= (long t) (long start))
                                       (< (long t) (long end))))
                       writes)
        total  (count in-win)
        ok     (count (filter (fn [[_ ty]] (= :ok ty)) in-win))]
    {:ok ok
     :total total
     :ratio (if (pos? total) (double (/ ok total)) 1.0)}))

(defn- availability
  "G2：逐窗口算可用率，只对样本 >= `min-sample` 的窗口判定。"
  [writes quiet-windows min-sample min-ratio]
  (let [scored (mapv (fn [w] (assoc w :avail (availability-in writes w)))
                     quiet-windows)
        judged (filterv #(>= (get-in % [:avail :total]) min-sample) scored)
        viols  (filterv #(< (get-in % [:avail :ratio]) min-ratio) judged)
        worst  (when (seq judged)
                 (apply min-key #(get-in % [:avail :ratio]) judged))]
    {:min-ratio min-ratio
     :min-sample min-sample
     :windows (count scored)
     :judged (count judged)
     :skipped-small-sample (- (count scored) (count judged))
     :worst-window (when worst
                     (select-keys worst [:start :end :avail]))
     :violations (mapv #(select-keys % [:start :end :avail]) viols)}))

(defn- rto
  "G4：每次 disruption stop 完成 → 首个 `:ok` write 完成的耗时（秒）。

  返回 `{:budget-seconds b :n n :p95 s :max s :samples [...] :unrecovered n
  :not-measured n :details [...]}`。

  只有**写阶段仍在进行**（disruption 结束早于最后一次 write *invoke*）的
  disruption 才可测。这很重要：soak 的生成器让 nemesis 在 time-limit 到点时
  收尾，`{:f :stop}` 落在写阶段之后，此时\"stop 后没有 :ok write\"是结构使然，
  不是集群没恢复——把它算作 unrecovered 会让每个 soak run 都假红。真正的
  \"集群没回来\"由 soak checker 的收敛门禁（finale 读必须 :ok）判定。"
  [writes last-write-invoke disruptions budget]
  (let [times   (long-array (map first writes))
        first-ok-after (fn [^long t]
                         (loop [i (Arrays/binarySearch times t)]
                           (let [idx (if (neg? i) (- -1 i) i)]
                             (when (< idx (alength times))
                               (let [w (nth writes idx)]
                                 (if (= :ok (second w))
                                   (first w)
                                   (recur (inc idx))))))))
        details (mapv (fn [{:keys [start end]}]
                        (let [t (first-ok-after (long end))
                              measurable? (and (some? last-write-invoke)
                                               (< (long end)
                                                  (long last-write-invoke)))]
                          {:disruption {:start start :end end}
                           :measurable? measurable?
                           :first-ok-after t
                           :seconds (when (and measurable? t)
                                      (/ (- (double t) (double end))
                                         nanos-per-second))}))
                      disruptions)
        rtos    (keep :seconds details)]
    {:budget-seconds budget
     :n (count rtos)
     :p95 (percentile rtos 0.95)
     :max (when (seq rtos) (apply max rtos))
     :samples (vec rtos)
     :unrecovered (count (filter #(and (:measurable? %) (nil? (:seconds %)))
                                 details))
     :not-measured (count (remove :measurable? details))
     :details details}))

(defn- op-liveness
  "G6：按 `:f` 分组统计客户端 op 的 `:ok` 率，找出「系统性失败」的 op 类。

  只算客户端 op（`:process` 为数字）：nemesis 的 `:start`/`:stop`/`:kill` … 与
  生成器 op 不参与。缺 `:process`（手写 fixture）时视为不可判定。

  返回 `{:by-op {...} :judged n :skipped-small-sample n :violations [...]}`。"
  [ops min-sample min-ratio]
  (let [data (filterv #(and (contains? #{:ok :info :fail} (:type %))
                            (number? (:process %)))
                      ops)
        ;; 合法确定性失败：既不算「成功」也不算「路没通」，从分母里扣除
        legit? (fn [op] (and (= :fail (:type op))
                             (contains? default-op-fail-whitelist (:error op))))
        by-f (group-by :f data)
        by-op (into {}
                    (for [[f group] by-f]
                      (let [legit (count (filter legit? group))]
                        [f {:total       (count group)
                            :ok          (count (filter #(= :ok (:type %)) group))
                            :info        (count (filter #(= :info (:type %)) group))
                            :fail        (count (filter #(= :fail (:type %)) group))
                            :legit-fail  legit
                            :judged      (- (count group) legit)
                            :errors      (frequencies (keep :error group))}])))
        judged (filter #(>= (get-in by-op [% :judged]) min-sample)
                       (keys by-op))
        viols  (filterv (fn [f]
                          (< (double (/ (get-in by-op [f :ok])
                                        (get-in by-op [f :judged])))
                             min-ratio))
                        judged)]
    {:min-ok-ratio min-ratio
     :min-sample   min-sample
     :ops          (count by-op)
     :judged       (count judged)
     :skipped-small-sample (- (count by-op) (count judged))
     :ok-ratio     (when (seq data)
                     (double (/ (count (filter #(= :ok (:type %)) data))
                                (count data))))
     :by-op        by-op
     :violations   (mapv (fn [f] (assoc (get by-op f)
                                        :f f
                                        :ratio (double (/ (get-in by-op [f :ok])
                                                          (get-in by-op [f :judged])))))
                         viols)}))

;; ---------------------------------------------------------------------------
;; Checker
;; ---------------------------------------------------------------------------

(defn checker
  "T0.2 硬门槛 checker。opts：

    :quiet-availability-min  quiet 窗口 write `:ok` 率门槛（默认 0.95）
    :quiet-min-sample        参与判定的最小样本（默认 100；不足只记录）
    :max-rto-seconds         全局 RTO 上界覆盖（默认按 `:nemesis` 分档）
    :nemesis                 本 run 的 nemesis 关键字（决定 RTO 分档）
    :check-premise?          值唯一性前提自检开关（默认 true；仅 T1.4 幂等
                             workload 这种「故意重放同 value」的场景才关掉）
    :min-op-ok-ratio         G6 单类 op 的 `:ok` 率下界（默认 0.1）
    :min-op-sample           G6 单类 op 参与判定的最小完成数（默认 10）"
  ([] (checker {}))
  ([{:keys [quiet-availability-min quiet-min-sample max-rto-seconds
            nemesis check-premise? min-op-ok-ratio min-op-sample]}]
   ;; F-09：门槛必须用 `or` 解析，**不能**用 destructuring 的 `:or`。
   ;; `:or` 只在键缺失时生效；而调用方（coord.clj 的 checker 组合）总是显式
   ;; 传入 `{:quiet-availability-min (:quiet-availability-min opts) ...}`，
   ;; 于是 `:or` 不会接管，门槛会变成 nil —— 轻则 `(< ratio nil)` /
   ;; `(>= total nil)` 抛 NPE，重则（窗口为空时）静默失效、报告里
   ;; `:min-ratio nil` 而 `:valid?` 仍然是 true。这正是 T0.2 想避免的
   ;; 「门槛看着在、其实没判」，因此按缺陷处理并在 fixture 里固定住。
   (let [quiet-availability-min (or quiet-availability-min
                                    default-quiet-availability-min)
         quiet-min-sample       (or quiet-min-sample default-quiet-min-sample)
         min-op-ok-ratio        (or min-op-ok-ratio default-min-op-ok-ratio)
         min-op-sample          (or min-op-sample default-min-op-sample)
         check-premise?         (if (nil? check-premise?) true check-premise?)]
     (reify checker/Checker
       (check [_ _test history _opts]
         (let [ops    (vec history)
             data   (filterv #(contains? #{:read :write} (:f %)) ops)
             t0     (some-> ops first :time long)
             t-end  (when (seq data) (apply max (map (comp long :time) data)))
             events (nemesis-events ops)
             {:keys [quiet disruptions]} (windows {:events events
                                                   :t0 (or t0 0)
                                                   :t-end t-end})
             writes (write-completions ops)
             last-write-invoke
             (let [ts (->> ops
                           (filter #(and (= :invoke (:type %))
                                         (= :write (:f %))))
                           (map (comp long :time)))]
               (when (seq ts) (apply max ts)))
             budget (rto-budget nemesis max-rto-seconds)
             premise (if check-premise?
                       (value-uniqueness ops)
                       {:valid? true :disabled? true})
             avail  (availability writes quiet quiet-min-sample
                                  quiet-availability-min)
             rto-r  (rto writes last-write-invoke disruptions budget)
             liveness (op-liveness ops min-op-sample min-op-ok-ratio)
             rto-fail? (or (pos? (:unrecovered rto-r))
                           (and (:p95 rto-r) (> (:p95 rto-r) budget)))
             failures (cond-> []
                        (not (:valid? premise))
                        (conj {:type :duplicate-values
                               :count (:duplicate-value-count premise)
                               :sample (:duplicate-values premise)})

                        (seq (:violations avail))
                        (conj {:type :quiet-availability
                               :min-ratio quiet-availability-min
                               :violations (:violations avail)})

                        (seq (:violations liveness))
                        (conj {:type :op-liveness
                               :min-ok-ratio min-op-ok-ratio
                               :violations (:violations liveness)
                               :note "有 op 类几乎从不成功：该路径没真的跑（F-13 类假绿）"})

                        (pos? (:unrecovered rto-r))
                        (conj {:type :rto-unrecovered
                               :count (:unrecovered rto-r)
                               :budget-seconds budget})

                        (and (:p95 rto-r) (> (:p95 rto-r) budget))
                        (conj {:type :rto-exceeded
                               :p95 (:p95 rto-r)
                               :budget-seconds budget}))]
         {:valid? (empty? failures)
          :gates  {:premise premise
                   :availability avail
                   :rto rto-r
                   :liveness liveness}
          :failures failures}))))))
