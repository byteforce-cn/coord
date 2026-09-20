(ns jepsen.coord.faultwin
  "M5a 第二轮 —— 把 nemesis 的历史事件还原成「每个 agent 什么时候不可用」的
  时间窗。

  为什么需要它（AG-06）：**弃锁 / 弃选**（拿到就再也不放的 op）的判定必须知道
  「持有者还有没有可能在续期」。持锁的 agent 活着时，它的后台续期任务会一直给
  lease 续命（`lock.rs` / `leader_election.rs` 的 `tokio::spawn` 续期循环），
  于是期望是「服务端 key 一直在」；一旦 agent 被 kill（或与集群分区），续期停止，
  期望翻转为「服务端必须在 `ttl + grace` 内回收」。

  没有这个时间窗，判据就只能靠猜（例如拿「run 结束时间」当界），而两种期望正好
  相反 —— 猜错的方向会产出假红 **或** 假绿。

  ## 事件的形状（实测）

  agent nemesis 的 completion op 的 `:value` 有四种形态：

    * `[:killed-agent \"n4\"]` —— 单个事件；
    * `[[:killed-agent \"n4\"] [:killed-agent \"n5\"]]` —— `:kill-agent-all`；
    * **内层 op map** —— `:agent-all`（`compose-agent-all` 把
      `nemesis/invoke!` 的返回值整个塞进 `:value`，所以真正的标记又深一层）；
    * `[:agent-all-stopped]` 之类的收尾标记（无主机名，忽略）。

  `flatten-markers` 递归地把它们统一成 `[marker host]` 序列，于是判据侧不必关心
  nemesis 的具体组合方式。

  ## 判定的取向

  时间窗只用于**放宽**判据（保守锚点）：`from-ms` = 不可用事件**完成**的时刻
  （kill 完成时进程已经死了），`to-ms` = 恢复事件完成的时刻（重启/愈合完成）。
  未闭合的窗口（run 在故障中结束）按 `to-ms = nil` 处理，调用方自己决定。

  刻意**不**在这里做任何判红：这个 ns 只回答「什么时候谁不可用」，判定留在各面
  自己的 checker 里（每个面的期望值不同）。")

(def down-markers
  "「agent 从此不再可能续期」的标记。"
  #{:killed-agent :paused-agent :partitioned-agent})

(def up-markers
  "「agent 又能续期了」的标记。"
  #{:restarted-agent :resumed-agent :healed-agent})

(defn- marker-kind [m]
  (case m
    :killed-agent      :kill
    :paused-agent      :pause
    :partitioned-agent :partition
    nil))

(defn- marker->edn
  "递归展开一个 nemesis completion 的 `:value`，取出全部 `[marker host]` 对。"
  [v]
  (cond
    (nil? v) nil
    (map? v) (marker->edn (:value v))
    (sequential? v)
    (if (and (= 2 (count v)) (keyword? (first v)) (string? (second v)))
      [(vec (take 2 v))]
      (mapcat marker->edn v))
    :else nil))

(defn- ns->ms [t] (quot (long t) 1000000))

(defn events
  "历史 → 按时间排序的 agent 事件序列（`{:marker :host :at-ms :type}`）。

  只看 nemesis completion（`:process :nemesis`，`:type :info`）。"
  [ops]
  (->> ops
       (filter #(and (= :nemesis (:process %))
                     (not= :invoke (:type %))))
       (mapcat (fn [op]
                 (for [[marker host] (marker->edn (:value op))
                       :when (or (down-markers marker) (up-markers marker))]
                   {:marker marker
                    :host   host
                    :at-ms  (ns->ms (or (:time op) 0))
                    :type   (if (down-markers marker) :down :up)
                    :op     op})))
       (sort-by :at-ms)
       vec))

(defn windows
  "历史 → `{:by-host {host [{:kind .. :from-ms .. :to-ms .. :closed? ..}]}
             :events  [...]}`。

  窗口 = 从某个 down 事件（kill / pause / partition）到**下一个**同主机的 up
  事件。同一主机可以有多段窗口（`kill-agent` 会反复杀同一个 agent）；一段未闭合
  的窗口表示 run 在故障中结束（`:closed? false`，`:to-ms nil`）。窗口按时间升序。"
  [ops]
  (let [evs (events ops)
        acc (reduce
              (fn [acc e]
                (let [h (:host e)]
                  (if (= :down (:type e))
                    (update acc h (fnil conj [])
                            {:kind       (marker-kind (:marker e))
                             :from-ms    (:at-ms e)
                             :to-ms      nil
                             :closed?    false
                             :started-by (:op e)})
                    ;; up：闭合该主机**最后一个尚未闭合**的窗口
                    (update acc h
                            (fn [ws]
                              (if-let [i (last (keep-indexed
                                                 (fn [i w] (when-not (:closed? w) i))
                                                 ws))]
                                (update ws i assoc
                                        :to-ms (:at-ms e) :closed? true
                                        :ended-by (:op e))
                                ws))))))
              {}
              evs)]
    {:events evs
     :by-host (update-vals acc #(vec (sort-by :from-ms %)))}))

(defn first-down
  "某主机在 `after-ms` 之后（含）的**最早** down 时刻；没有 ⇒ nil。"
  [wins host after-ms]
  (->> (get-in wins [:by-host host])
       (keep :from-ms)
       (filter #(>= (long %) (long after-ms)))
       sort
       first))

(defn kind-of
  "某主机在 `after-ms` 之后最早的故障类型（:kill / :pause / :partition）。"
  [wins host after-ms]
  (->> (get-in wins [:by-host host])
       (filter #(and (:from-ms %)
                     (>= (long (:from-ms %)) (long after-ms))))
       (sort-by :from-ms)
       first
       :kind))
