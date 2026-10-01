(ns jepsen.coord.cacheck
  "M5b —— agent 本地缓存面的 checker（缺口 AG-09：TTL / 重启后一致性）。

  ## 被测对象与**前提**（错了会得到假红）

  `coord-agent/src/services/cache.rs` 的缓存是 **agent 本地 redb**：
    * 数据面在 agent 进程里，不是 server 上的共享状态；
    * TTL 是**绝对到期时间戳**，与数据一起持久化（并随 ISR 复制条目传播）；
    * ISR 复制**可选**（`services.replication`，默认关）——本 lab 的 agent 绑
      loopback、彼此不可达，所以复制面**未覆盖**（见下「漏检边界」）。

  推论：一次 `Set` 之后 `Get` 必须在**同一个 agent** 上才可能读到。多 agent
  拓扑下「读不到」是合法的（数据在另一个 agent 上），所以本 checker 的判据
  只在 `--agents 1`（单一 agent）时有意义 —— `coord.clj` 会在
  `--workload cache` 且 `--agents > 1` 时**拒绝起跑**，而不是让 checker 去猜。

  ## 判据

  1. `:cache-fabricated`（**P0**）—— 读回来的值/成员**从来没被写过**。判定用
     「全历史写集合」，所以它只抓「凭空出现的值」（缓存把别人的值串了 / 复用
     了脏槽位），不会因并发而误报。
  2. `:cache-lost-write`（**P0**）—— 只针对 `ttl=0`（**持久**）的 Set：写已确认
    完成，之后的第一次 Get 却 `found=false`，且**中间没有任何写/删能解释它**。
     区间类判据的通用纪律：只有当「中间没有别的合法动作」时才算违反（F-34 的
     教训；宽度不设容差，用「有无干扰 op」当闸门更可靠）。
  3. `:cache-ttl-early`（**P0**）—— `ttl=T` 的写已确认，读在
     `写起点 + T` **之前**就 `found=false`。用写**起点**（不是完成时刻）当界是
     **保守**的：TTL 只可能在写生效之后才开始计，所以在 `起点+T` 之前不可能
     到期 —— 宁可漏报，不产假红。
  4. `:cache-ttl-ghost`（**P1**）—— `ttl=T` 的写已确认完成于 `c`，读在
     `c + T + grace` 之后仍 `found=true` 且**值仍是那次写的值**（TTL 没生效）。
  5. `:cache-list-lost`（**P0**）—— List 面：读（LLen/LRange）时，**所有在它
     开始前已确认完成**的 LPush 值都必须在 LRange 里出现（LLen 只判长度下界）。
     下界取「完成时刻早于读起点」的推入，对并发免疫。
  6. `:cache-set-lost-member`（**P0**）—— Set 面同构（SMembers 必须包含已确认
     SAdd 的成员）。
  7. `:cache-restart-loss`（**P0**）—— 重启（`kill-agent` 的 `:stop` 半场 =
     保留 `data_dir` 重启）后，重启前已确认的**持久**（ttl=0）写在重启后的
     第一次成功读里必须还在。这是 redb 持久化承诺的直接证伪点。
  8. 样本门槛（§5.1 口径）—— 每类 op 的完成数低于门槛即 **invalid**（不是绿）。

  ## 漏检边界（R2）

  * **ISR 复制面未覆盖**：需要 agent 之间可达（当前 lab 绑 loopback + SSH 隧道；
    跨 agent 的 `replication_peers` 拓扑是独立的 lab 工作）。因此「复制日志 /
    持久化幂等键 / 本地序列号」这三条承诺只覆盖到**单 agent 内**的持久化。
  * **Hash 面未覆盖**（HGet/HSet/HGetAll 需要 `map<string,bytes>` 描述符）。
  * `RPop` 未覆盖（原子出队需要「消费不重不漏」的业务语义，属另一类判据）。
  * 不判 metadata / 分片元数据（`CacheShardMeta`）。

  ## 时间基准

  一律用 op 自己读的**绝对**时刻：起点 `:t0-ns`、完成 `:done-ns`（都是
  `System/nanoTime`，同一 JVM 同域）。

  **不要**用 jepsen 的 `:time`：那是**相对测试起点**的量，而 `:t0-ns` 是
  boot 相对量 —— 两个轴混用会让「写完成 < 读开始」这种比较**恒真**。实测
  （2026-09-19 第八轮首次 lab 跑）：所有「丢写 / list 丢推入」判据把
  **读之后才发生的写**也算进了下界，37 条违反全是这一个原因（F-53 的同型
  复发）。`end-ns` 因此优先取 `:done-ns`，只在没有它（fixture）时才退到
  `:time`（fixture 自己用同一轴写就一致）。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.windex :as wi]))

(def default-start-tolerance-ms 0)
(def default-ttl-grace-ms 3000)

(defn- start-ns [op] (long (or (:t0-ns op) (:invoke op) (:time op) 0)))
(defn- end-ns [op] (long (or (:done-ns op) (:time op) 0)))

(defn- ops-of [ops f] (filterv #(= f (:f %)) ops))
(defn- done? [op] (= :ok (:type op)))

;; --------------------------------------------------------------------------
;; 判据 1：fabricated（值从来没被写过）
;; --------------------------------------------------------------------------

(defn- fabricated
  "读回来的值不在「全历史写集合」里。

  写集合收 `:ok` + `:info`（**响应丢失的写可能已经生效**，与 windex 的 F-17
  同一条口径）：把 `:info` 写漏掉会把合法观察判成 fabricated（假红）。"
  [ops]
  (let [sets    (ops-of ops :cache-set)
        lpushes (ops-of ops :cache-lpush)
        sadds   (ops-of ops :cache-sadd)
        ;; 每个 key 上**所有**被写过的字符串值（不是「最后一次写的值」）：
        ;; 只留最后一次会把「读到自己早先写的旧值」判成 fabricated —— 实测
        ;; （2026-09-19 的 lab cell）一次 run 里 9 条假红。写集合收 `:ok` 与
        ;; `:info`（响应丢失的写可能已生效，同 windex 的 F-17 口径）。
        written-str (into {} (for [[k os] (group-by :key (filter :value sets))]
                               [k (set (map :value os))]))
        written-lst (group-by :key (filter :value lpushes))
        written-set (group-by :key (filter :member sadds))
        vals (fn [o] (cond (:values o) (:values o)
                           (:members o) (:members o)
                           :else []))]
    (->> (filterv #(contains? #{:cache-get :cache-lrange :cache-smembers} (:f %)) ops)
         (keep (fn [op]
                 (let [k (:key op)
                       bad (case (:f op)
                             ;; 字符串读：found=true 且值不在「该 key 写过的值」集合里
                             :cache-get
                             (when (and (:found op) (:value op)
                                        (not (contains? (get written-str k #{}) (:value op))))
                               [(:value op)])
                             ;; list 读：每个元素都必须是该 key 上推过的值
                             :cache-lrange
                             (let [allowed (set (map :value (get written-lst k)))]
                               (vec (remove allowed (:values op))))
                             ;; set 读：每个成员都必须是该 key 上加过的
                             :cache-smembers
                             (let [allowed (set (map :member (get written-set k)))]
                               (vec (remove allowed (:members op))))
                             nil)]
                   (when (seq bad)
                     {:type :cache-fabricated
                      :f (:f op) :key k :observed bad
                      :note "读回来的值从未被写过（写集合含 :ok 与 :info 写）"}))))
         vec)))

;; --------------------------------------------------------------------------
;; 干扰闸门（区间类判据的通用前提）
;; --------------------------------------------------------------------------

(defn- mutators
  "能**移除/覆盖**该 key 上值的 op：Set/Delete 对所有类型有效，LPush/SAdd 只
  增不减（不算干扰）。"
  [ops]
  (filterv #(contains? #{:cache-set :cache-del} (:f %)) ops))

(defn- op-id
  "op 的身份（拿来做「排除自己」的比较）。

  不能用 `identical?`：op 在不同阶段会被 `filterv`/`map` 重新包一层，身份
  不稳；也不能只看 `:f`（同一个 key 上会有多次 Set）。用「函数 + 起点 + 完成
  时刻」三元组就够唯一（同一 worker 的同一函数在同一纳秒上不可能有两个 op）。"
  [op]
  [( :f op) (:t0-ns op) (:time op) (:key op)])

(defn- intervened?
  "在 [from-ns, to-ns] 窗口里有没有**别的** op 可能改变了这个 key 的值。

  区间相交（不是包含）：一个 **in flight** 的写（起点在窗口里、完成在窗口外）
  同样足以解释「读不到」——把它漏掉就会产出假红。

  务必排除**被审的那个写自己**：它的完成时刻就是窗口的左端点，不排除的话它
  总是「与自己相交」，于是所有「丢写 / TTL 提前到期」判据都会被静默关掉
  （实测：第一版就是如此，三条负控制 fixture 全部假绿）。"
  [ms k from-ns to-ns exclude]
  (boolean (some (fn [m] (and (= k (:key m))
                              (not= (op-id m) (op-id exclude))
                              (<= (start-ns m) (long to-ns))
                              (>= (end-ns m) (long from-ns))))
                 ms)))

;; --------------------------------------------------------------------------
;; 判据 2/3/4：字符串面的写可见性 / TTL 两侧
;; --------------------------------------------------------------------------

(defn- string-violations
  [ops {:keys [ttl-grace-ms]}]
  (let [sets (filter :key (ops-of ops :cache-set))
        gets (filter :key (ops-of ops :cache-get))
        ms   (mutators ops)
        grace-ms (long (or ttl-grace-ms default-ttl-grace-ms))]
    (vec
     (mapcat
      (fn [g]
        (let [k (:key g)]
          ;; 只看**在本次读之前**已完成的最后一次写（同 key）
          (when-let [w (last (filter #(and (= k (:key %))
                                           (done? %)
                                           (< (end-ns %) (start-ns g)))
                                     sets))]
            (let [ttl-ms (* 1000 (long (or (:ttl-seconds w) 0)))
                  ;; 时间轴换算：`:t0-ns` / `:time` 是**纳秒**，TTL 是毫秒 ⇒ 一律
                  ;; 换成 ns 再比。第一版把两者直接相减，于是「TTL 提前到期」判据
                  ;; 的门槛变成 0 ns（永远不成立），而「TTL 幽灵」判据却对所有正常
                  ;; 读取都成立（`get` 在 3s 读一个 ttl=5s 的值被判幽灵）——
                  ;; 两个方向同时错，而且**只有 fixture 能看出来**（lab 里那一档
                  ;; 会红成一片，看起来像被测系统崩了）。
                  ttl-ns (* 1000000 ttl-ms)
                  grace-ns (* 1000000 (long (or grace-ms default-ttl-grace-ms)))
                  ws  (start-ns w)   ; 保守：TTL 最早从写**起点**开始计
                  we  (end-ns w)
                  gs  (start-ns g)
                  clean? (not (intervened? ms k we gs w))]
              (cond
                ;; 判据 2：持久写看不到（found=false 且无干扰）
                (and clean? (zero? ttl-ms) (not (:found g)))
                [{:type :cache-lost-write
                  :key k :value (:value w) :read g
                  :set-invoke-ms (quot ws 1000000) :set-complete-ms (quot we 1000000)
                  :get-start-ms (quot gs 1000000)
                  :note "持久（ttl=0）写已确认完成，之后第一次读却 found=false，且中间无写/删"}]

                ;; 判据 3：TTL 提前消失（读起点早于「起点+ttl」）
                (and (pos? ttl-ms) (not (:found g)) (< gs (+ ws ttl-ns)) clean?)
                [{:type :cache-ttl-early
                  :key k :value (:value w) :ttl-seconds (:ttl-seconds w)
                  :age-ms (quot (- gs ws) 1000000)
                  :ttl-ms ttl-ms
                  :read g
                  :note "在写起点+ttl 之前就读不到（TTL 不可能已到期）"}]

                ;; 判据 4：TTL 幽灵（到期后仍在，且值还是那次写的）
                (and (pos? ttl-ms) (:found g)
                     (or (nil? (:value g)) (= (:value g) (:value w)))
                     (> gs (+ we ttl-ns grace-ns)) clean?)
                [{:type :cache-ttl-ghost
                  :key k :value (:value g) :ttl-seconds (:ttl-seconds w)
                  :overdue-ms (quot (- gs (+ we ttl-ns)) 1000000)
                  :grace-ms (quot grace-ns 1000000) :read g
                  :note "TTL 到期 + grace 之后仍能读到"}]
                :else nil)))))
      gets))))

;; --------------------------------------------------------------------------
;; 判据 5/6：List / Set 面的下界包含
;; --------------------------------------------------------------------------

(defn- pushes-before
  "在 `t` 之前**已确认完成**的推入/加入（对并发免疫的下界）。"
  [writes t field]
  (let [resets (filterv #(and (done? %) (< (end-ns %) t)) (ops-of writes :cache-del))]
    ;; 最后一次 Delete 之前的写才算数（Delete 会清掉该 key）
    (let [cut (if (seq resets) (end-ns (last resets)) 0)]
      (remove #(< (end-ns %) cut)
              (filterv #(and (done? %) (< (end-ns %) t)) writes)))))

(defn- list-violations
  [ops]
  (let [lpushes (filter :key (ops-of ops :cache-lpush))]
    (vec
     (mapcat
      (fn [r]
        (let [k (:key r)
              t (start-ns r)
              pushed (pushes-before (filterv #(= k (:key %)) lpushes) t :value)
              want (set (map :value pushed))]
          (cond
            (= :cache-lrange (:f r))
            (let [got (set (:values r))
                  missing (vec (remove got want))]
              (when (seq missing)
                [{:type :cache-list-lost :key k :missing missing
                  :expected-count (count want)
                  :observed-count (count (:values r))
                  :read-start-ms (quot t 1000000)
                  :note "读起点之前已确认的 LPush 值在读结果里缺失"}]))
            (= :cache-llen (:f r))
            (when (and (:length r) (< (long (:length r)) (count want)))
              [{:type :cache-list-lost :key k
                :expected-count (count want) :observed-count (:length r)
                :read-start-ms (quot t 1000000)
                :note "LLen 小于「读起点前已确认的 LPush 数」（丢失推入）"}])
            :else nil)))
     (filterv #(contains? #{:cache-lrange :cache-llen} (:f %)) ops)))))

(defn- set-violations
  [ops]
  (let [sadds (filter :key (ops-of ops :cache-sadd))]
    (vec
     (mapcat
      (fn [r]
        (let [k (:key r)
              t (start-ns r)
              added (pushes-before (filterv #(= k (:key %)) sadds) t :member)
              want (set (map :member added))
              got (set (:members r))
              missing (vec (remove got want))]
          (when (seq missing)
            [{:type :cache-set-lost-member :key k :missing missing
              :expected-count (count want) :observed-count (count (:members r))
              :read-start-ms (quot t 1000000)
              :note "读起点之前已确认的 SAdd 成员在读结果里缺失"}])))
      (ops-of ops :cache-smembers)))))

;; --------------------------------------------------------------------------
;; 判据 7：重启后的持久化
;; --------------------------------------------------------------------------

(defn restart-times-ms
  "从 nemesis 历史里取「agent 重启完成」的时刻（毫秒）。

  `kill-agent` 的 `:stop` 半场 = 保留 `data_dir` 重启（`agent/restart-one!`），
  其 completion 的 `:value` 形如 `[:restarted-agent \"n4\"]`（见
  `jepsen.coord.agent`）。取 completion 的 `:time` 而不是 invoke：重启**完成**
  之后的读才有意义。"
  [history]
  (->> history
       (filter #(= :nemesis (:process %)))
       (filter #(some (fn [v] (and (vector? v) (= :restarted-agent (first v))))
                      (let [v (:value %)] (if (vector? (first v)) v [v]))))
       (map #(quot (long (or (:time %) 0)) 1000000))
       vec))

(defn- restart-violations
  [ops restarts]
  (if (empty? restarts)
    []
    (let [sets (filterv :key (ops-of ops :cache-set))
          gets (filter :key (ops-of ops :cache-get))
          ms   (mutators ops)]
      (vec
       (for [rt restarts
             :let [rt-ns (* 1000000 (long rt))]
             [k ks] (group-by :key sets)
             ;; 重启前的最后一次**持久**写
             :let [w (last (filter #(and (done? %) (zero? (long (or (:ttl-seconds %) 0)))
                                         (< (end-ns %) rt-ns))
                                   ks))]
             :when w
             ;; 重启后该 key 的第一次成功读（中间无写/删）
             :let [g (first (filter #(and (done? %) (> (start-ns %) rt-ns))
                                    (filter #(= k (:key %)) gets)))]
             :when (and g (not (intervened? ms k rt-ns (start-ns g) w))
                        (not (:found g)))]
         {:type :cache-restart-loss
          :key k :value (:value w) :restart-at-ms rt
          :read-start-ms (quot (start-ns g) 1000000)
          :note "重启（保留 data_dir）后，重启前已确认的持久写在第一次读里消失（redb 持久化承诺）"})))))

;; --------------------------------------------------------------------------

(defn checker
  "M5b cache checker。opts：

    :min-sets        样本门槛：确认的 Set 数（默认 0）
    :min-gets        样本门槛：完成的 Get 数（默认 0）
    :min-list-ops    List 面（LPush+LRange/LLen）完成数下限
    :min-set-ops     Set 面（SAdd+SMembers）完成数下限
    :ttl-grace-ms    TTL 幽灵判据的宽限（默认 3000；服务端清理节拍）

  门槛取「样本不足 ⇒ invalid」而不是「绿」：45s 短跑里某一面一条 op 都没跑完
  时，报告必须是「未执行」（§5.1），不能是绿的。"
  ([] (checker {}))
  ([{:keys [min-sets min-gets min-list-ops min-set-ops ttl-grace-ms]}]
   (let [min-sets (long (or min-sets 0))
         min-gets (long (or min-gets 0))
         min-list (long (or min-list-ops 0))
         min-set  (long (or min-set-ops 0))]
     (reify checker/Checker
       (check [_ _test history _opts]
         (let [{:keys [ops]} (wi/pair-invokes history)
               done (filterv #(= :ok (:type %)) ops)
               n-sets (count (filterv done? (ops-of ops :cache-set)))
               n-gets (count (filterv done? (ops-of ops :cache-get)))
               n-list (count (filterv done? (filterv #(contains? #{:cache-lpush :cache-lrange :cache-llen} (:f %)) ops)))
               n-set  (count (filterv done? (filterv #(contains? #{:cache-sadd :cache-smembers} (:f %)) ops)))
               restarts (restart-times-ms history)
               fails (vec (concat (fabricated ops)
                                  (string-violations ops {:ttl-grace-ms ttl-grace-ms})
                                  (list-violations ops)
                                  (set-violations ops)
                                  (restart-violations ops restarts)))
               by-class (frequencies (map :type fails))
               sample-reasons (cond-> []
                                (< n-sets min-sets)
                                (conj {:type :insufficient-sample :op :cache-set
                                       :count n-sets :required min-sets
                                       :note "Set 样本不足：该 cell 视为未执行（§5.1）"})
                                (< n-gets min-gets)
                                (conj {:type :insufficient-sample :op :cache-get
                                       :count n-gets :required min-gets
                                       :note "Get 样本不足：该 cell 视为未执行（§5.1）"})
                                (< n-list min-list)
                                (conj {:type :insufficient-sample :op :cache-list
                                       :count n-list :required min-list
                                       :note "List 面样本不足：该 cell 视为未执行（§5.1）"})
                                (< n-set min-set)
                                (conj {:type :insufficient-sample :op :cache-set-type
                                       :count n-set :required min-set
                                       :note "Set 面样本不足：该 cell 视为未执行（§5.1）"}))
               summary {:sets n-sets :gets n-gets :list-ops n-list :set-ops n-set
                        :completed (count done)
                        :restarts (count restarts)
                        ;; 各面「真的跑了吗」——F-41/F-13 的纪律：报告必须按子面
                        ;; 打印完成数，不能只打总数（:valid? 在一条 op 都没成功时
                        ;; 也可能为 true）。
                        :by-op (frequencies (map :f done))
                        :ttl-modes (frequencies (map #(long (or (:ttl-seconds %) 0))
                                                     (filterv done? (ops-of ops :cache-set))))
                        :violations-by-class by-class}
               reasons (into sample-reasons
                             (when (seq fails)
                               [{:type :cache-violations
                                 :count (count fails)
                                 :sample (vec (take 3 fails))}]))]
           (info "cache checker:" (pr-str (assoc summary :failures (take 5 fails))))
           (cond-> {:valid? (and (empty? sample-reasons) (empty? fails))
                    :cache summary
                    :failures reasons}
             (seq fails) (assoc :violations (vec (take 20 fails))))))))))
