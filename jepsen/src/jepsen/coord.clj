(ns jepsen.coord
  "Jepsen tests for coord, a strongly-consistent (linearizable) KV store.

  Workloads:
    :register       linearizable read/write register on a single key
    :cas-register   atomic compare-and-set register on a single key
    :multi-register N independent registers, one per region (--regions N):
                    ops carry :key and route to that key's region; each
                    region is its own raft group (multi_raft mode, T4.1).
    :map            T1.1: multi-key register with **delete** (tombstone) +
                    exact existence reads (Txn Compare{VERSION, EQUAL, 0});
                    value sizes via --value-size (F3).
    :txn            T1.2: transactions -- create-if-absent, multi-key write
                    sets with range read-back, value CAS, CAS/delete both
                    branches, and read-inside-txn.
    :scan           T1.3: RangeRequest variants (range_end / limit /
                    keys_only / count_only) + historical revision reads.
    :watch          T2.1: watch event streams (sessions + resume; the
                    checker pins the overflow-marker semantics of F-06).
    :lease          T2.2: lease TTL expiry / KeepAlive renewal / Revoke
                    cascade-delete scenarios (jepsen.coord.leaseck).
    :soakfull       T6.1: the long-run acceptance entry point -- several
                    surfaces interleaved at declared weights (--soak-mix),
                    composed checker = per-surface checkers + a routing/
                    liveness gate. Requesting an unimplemented surface fails
                    at build time (no silent drop).
    :mixture        T1.5: map + txn + scan interleaved at a configurable
                    weight (--mixture-ratio), checked by the composed
                    jepsen.coord.mixck checker (routing + the three
                    surface checkers).
    :lock,          M5a agent-local control plane (mutual exclusion /
    :election,      unique leader / global ids / service discovery). They
    :idgen,         need --agents N > 0 and >= 2 agents for the mutual
    :registry       exclusion surfaces.
    :cache, :mq     M5b agent-local **data plane** (redb cache / message
                    queue; AG-09 / AG-11). Their data lives in the agent
                    process, so read-your-write judgements require
                    --agents 1 (enforced at build time).

  Nemeses (one per run for clean attribution, or :all):
    :none, :kill, :kill-all, :pause, :partition, :partition-halves,
    :partition-ring, :all, :soak

  :soak mode drives a slow, rotating fault cycle (kill / pause / partition
  with long quiet windows between disruptions) at a fixed low rate and runs a
  dedicated O(n log n) soak checker that stays tractable on 72h histories
  (see jepsen.coord.soak). Run it with --workload register --nemesis soak
  --time-limit 259200. Short runs: --checker linear keeps the knossos check.
  Multi-raft soak: --workload multi-register --regions N --nemesis soak
  (per-region soak checker grouping; T4.3)."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [cli :as cli]
                    [client :as jclient]
                    [checker :as checker]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [os :as os]
                    [random :as rand]
                    [tests :as tests]]
            [clojure.string :as str]
            [jepsen.coord [agent :as agent]
                          [cacheck :as cacheck]
                          [client :as client]
                          [db :as db]
                          [electck :as electck]
                          [gates :as gates]
                          [idem :as idem]
                          [idgenck :as idgenck]
                          [leaseck :as leaseck]
                          [lockck :as lockck]
                          [mapck :as mapck]
                          [mixck :as mixck]
                          [mqck :as mqck]
                          [nemesis :as n]
                          [regck :as regck]
                          [regions :as regions]
                          [scanck :as scanck]
                          [soak :as soak]
                          [txnck :as txnck]
                          [watchck :as watchck]]
            [knossos.model :as model])
  (:import [jepsen.coord CoordRpc]))

;; --------------------------------------------------------------------------
;; Workload generators
;; --------------------------------------------------------------------------

(defn- r [_ _]
  {:type :invoke, :f :read})

(defn- w [_ _]
  {:type :invoke, :f :write, :value (rand-int 1000000)})

(defn- cas
  "T1.5 workload 质量改进：`old` 取**最近一次观察到的值**（read-then-CAS）。

  原来是从 `client/seen`（200 元集合）里 `rand-nth`：命中率约 1/200，实测
  45s 只有 13 个 `:ok` / 108 个 cas —— knossos 的 `:cas` 前提是 `old` 必须
  等于当时的寄存器值，miss 的 op 会被丢掉，于是有效线性化样本少了一个量级。
  改用 `client/last-seen`（客户端每次读/写成功都更新）后，CAS 针对的就是
  「刚读到的值」，命中率由真实并发冲突决定。正确性不受影响：CAS 成功 ⇒ 该
  时刻寄存器确实是 `old` ⇒ 它的线性化点就在自己的区间内，knossos 一定找得到。"
  [_ _]
  (let [old @client/last-seen
        new (inc (long (rand 1000000)))]
    {:type :invoke, :f :cas, :value [old new]}))

;; Soak mode writes unique, monotonically increasing values so the O(n) soak
;; checker (jepsen.coord.soak) is exact: a read must never return a value
;; older than the newest confirmed write that completed before it began.
;; For :multi-register the counter is per register key (:key on the op), so
;; each region's values are unique within that region.
(defonce ^:private soak-write-counter (atom 0))
(defonce ^:private multi-write-counter (atom {}))

;; --------------------------------------------------------------------------
;; T1.1 map/delete 与 T1.2 txn / T1.3 scan：写值与 key 空间
;;
;; 共同约束：**每次写都用一个全局唯一的值**。检查器（jepsen.coord.windex）
;; 靠值的唯一性把「读到的值」归属到「某一次写」，从而做 fabricated / future /
;; stale 判定；值不唯一就没法判。
;; --------------------------------------------------------------------------

(defonce ^:private op-counter (atom 0))

(defn- next-n [] (swap! op-counter inc))

;; T1.1/T1.3 的 key 空间（各自一段前缀，互不干扰，便于扫区间）
(def ^:private map-key-prefix "/jepsen/map/k")
(def ^:private scan-key-prefix "/jepsen/scan/")
(def ^:private txn-key-prefix "/jepsen/txn/")

(defn- run-tag
  "本 run 的标签（用种子），拼进 key 与写值。

  为什么必要：lab 的节点数据目录**不保证每次 run 前清空**（`make test`
  不 wipe）。上一个 run 留在 `/jepsen/map/k0` 里的值对本 run 的写索引来说
  「从来没人写过」—— 一条读命中它就会被判 `:fabricated`（**假红**）。
  把 run 标签拼进 key/值，新 workload 就与历史数据完全隔离。"
  [opts]
  (str "s" (long (or (:seed opts) 0))))

(defn- map-keys
  "T1.1 的 key 集合：`--map-keys`（默认 8）个固定 key，制造真实争用。"
  [opts]
  (mapv #(str map-key-prefix (run-tag opts) "-" %)
        (range (max 1 (long (or (:map-keys opts) 8))))))

(defn- scan-keyspace
  "T1.3 的扫描区间 `[start, end)`：只覆盖本 run 的 key。

  tag 以数字结尾，`end` 把最后一个字符 +1（`s42-` → `s42.`，`-` 0x2d <
  `.` 0x2e），于是 `s42-*` 全部命中、其他 run 的 `s43-*` 全部排除。"
  [opts]
  [(str scan-key-prefix (run-tag opts) "-")
   (str scan-key-prefix (run-tag opts) ".")])

(defn- sized-value
  "写值：全局唯一前缀 + 定长填充到 `size` 字节（F3：值大小扫描）。

  `n` 保证 run 内唯一，`tag` 保证 run 间唯一（见 `run-tag`），`size`
  （--value-size，默认 16）保证长度可控；`size` 小于唯一前缀长度时
  **前缀优先**（唯一性 > 长度）。"
  [n size tag]
  (let [p (str "v" tag "-" n "-")]
    (if (<= (long size) (count p))
      p
      (str p (apply str (repeat (- (long size) (count p)) \x))))))

;; --------------------------------------------------------------------------
;; T0.5 随机种子 / T0.6 nemesis 抖动
;; --------------------------------------------------------------------------

(defonce ^:private rng (atom (java.util.Random.)))

(defn- init-rng!
  "Seeds the run's RNG (T0.5). Returns the seed, which is stored in the test
  map (and hence results.edn) so a failing history can be replayed with
  `scripts/replay.clj` at the same seed."
  [seed]
  (let [seed (long (or seed (rand-int Integer/MAX_VALUE)))]
    (reset! rng (java.util.Random. seed))
    seed))

(defn- jittered
  "`seconds` ±20% uniform jitter (T0.6/G3), drawn from the seeded RNG so the
  disruption schedule is reproducible."
  [seconds]
  (* (double seconds) (+ 0.8 (* 0.4 (.nextDouble ^java.util.Random @rng)))))

(defn- jitter? [opts] (not (false? (:jitter opts))))

(defn- nemesis-beat
  "Beat between nemesis start/stop ops: a *jittered* 3–8s (T0.6/G3) by default.
  A fixed 5s beat lets the fault schedule resonate with the cluster's own
  periodic work (compaction, heartbeats) in a way no real deployment does;
  `--no-jitter` restores the fixed beat when reproducing old results."
  [opts]
  (if (jitter? opts)
    (gen/sleep (+ 3.0 (* 5.0 (.nextDouble ^java.util.Random @rng))))
    (gen/sleep 5)))

(defn- soak-w
  [_ _]
  {:type :invoke, :f :write, :value (swap! soak-write-counter inc)})

;; --------------------------------------------------------------------------
;; T1.4 request_id 幂等专项（workload :idempotency）
;;
;; 三种分组轮换，每组的 key / rid 都是新的（这样 version 判定有一个已知的
;; 期望值，见 jepsen.coord.idem）：
;;
;;   :idem-put          同一 rid 紧连着 Put k 次（prev_kv=true）+ 收尾点读；
;;                      每 3 组有 1 组先写一个 setup 值，让首次响应真的带
;;                      prev_kv —— 这是纯 F-02（命中丢 prev_kv）的复现路径。
;;   :idem-delete       先写一个独占 key，再用同一 rid 删 k 次 + 收尾点读；
;;                      F-01 的直接复现（delete 无去重）。
;;   :idem-range-delete 范围删 → 在区间内写新 key → 用同一 rid 重放；
;;                      F-01 最严重形态（重试放大破坏面）。
;; --------------------------------------------------------------------------

(defonce ^:private idem-counter (atom 0))

(defn- idem-gen
  "`request_id` 幂等专项 workload（T1.4）。

  `--idem-replay-delay-ms` 用延迟把「重放」推到一次 kill/重启之后，让重放落到
  没有该 rid 缓存的节点上（F-03）；0（默认）时全部重放在同一 leader 上完成，
  是对照组（契约成立）。`--nemesis kill-all` + 非零延迟 = 复现 F-03 的组合。"
  [opts]
  (let [delay (:idem-replay-delay-ms opts)
        off   (long (or (:idem-replay-node-offset opts) 0))
        tag   (run-tag opts)
        n*    (fn [] (swap! idem-counter inc))
        put-op (fn [_ _]
                 (let [n (n*)]
                   {:type :invoke, :f :idem-put
                    :value {:rid (str "idem-put-" tag "-" n)
                            :key (str "/jepsen/idem/put/" tag "/" n)
                            :val (str n)
                            :replays (if (zero? (mod n 2)) 3 2)
                            :setup-val (when (zero? (mod n 3)) (str "OLD-" n))
                            :replay-delay-ms delay
                            :replay-node-offset off}}))
        del-op (fn [_ _]
                 (let [n (n*)]
                   {:type :invoke, :f :idem-delete
                    :value {:rid (str "idem-del-" tag "-" n)
                            :key (str "/jepsen/idem/del/" tag "/" n)
                            :val (str n)
                            :replays (if (zero? (mod n 2)) 3 2)
                            :replay-delay-ms delay
                            :replay-node-offset off}}))
        rd-op  (fn [_ _]
                 (let [n (n*)]
                   {:type :invoke, :f :idem-range-delete
                    :value {:rid (str "idem-rdel-" tag "-" n)
                            :range-start (str "/jepsen/idem/rd/" tag "/" n "/a")
                            :range-end   (str "/jepsen/idem/rd/" tag "/" n "/z")
                            :post-write-key (str "/jepsen/idem/rd/" tag "/" n "/m")
                            :post-write-val n}}))]
    (gen/mix [put-op del-op rd-op])))

(defn- soak?
  [opts]
  (= :soak (:nemesis opts)))

;; --------------------------------------------------------------------------
;; T1.1 map workload（delete/tombstone + 存在性）
;;
;; 混合比 r:r:r:w:w:w:d:e = 1/8 delete（12.5% ≥ §5.1 的 10%）。读/写/删都带
;; `:key`（随机选一个固定 key，制造真实的读写冲突）；写值全局唯一
;; （`--value-size` 控制长度，F3）。`--map-keys N` 控制 key 数（默认 8）。
;; --------------------------------------------------------------------------

(defn- map-gen
  [opts]
  (let [ks   (map-keys opts)
        size (long (or (:value-size opts) 16))
        tag  (run-tag opts)
        r    (fn [_ _] {:type :invoke, :f :read, :key (rand-nth ks)})
        w    (fn [_ _] {:type :invoke, :f :write, :key (rand-nth ks)
                        :value (sized-value (next-n) size tag)})
        d    (fn [_ _] {:type :invoke, :f :delete, :key (rand-nth ks)})
        e    (fn [_ _] {:type :invoke, :f :exists, :key (rand-nth ks)})]
    (gen/mix [r r r w w w d e])))

;; --------------------------------------------------------------------------
;; T1.2 txn 全形态
;;
;; 五个 txn 形态，每个用**独占**的 key/前缀（这样断言有确定的期望值）：
;;
;;   :txn-create     Compare{VERSION, EQUAL, 0} + Put；失败分支为空。
;;                   全新 key 上 succeeded=false 本身就是异常
;;                   （version=0 语义错 / 另一分支泄漏）。
;;   :txn-write-set  3 个 Put（同区间）+ **区间回读** → 原子可见性：
;;                   要么三个都在，要么一个都不在（半应用可见 = P0）。
;;   :txn-cas        Compare{VALUE, EQUAL, old} + Put{new}；失败分支为空
;;                   → 失败时 new 必不得出现（失败分支泄漏 = P0）。
;;   :txn-cas-delete 同 compare，但失败分支是 Delete → 同时探「失败分支必须
;;                   执行」与「失败分支不得越界」。
;;   :txn-read       txn 内 Range（ReadOp 路径），本身是一次寄存器读。
;; --------------------------------------------------------------------------

(defn- txn-gen
  [opts]
  (let [tag    (run-tag opts)
        n*     (fn [] (next-n))
        create (fn [_ _]
                 (let [n (n*)]
                   {:type :invoke, :f :txn-create
                    :value {:key (str txn-key-prefix tag "-create-" n)
                            :val (str n)}}))
        wset   (fn [_ _]
                 (let [n (n*)
                       p (str txn-key-prefix tag "-set-" n "/")]
                   {:type :invoke, :f :txn-write-set
                    :value {:prefix p
                            :vals [(str n "-a") (str n "-b") (str n "-c")]
                            :range-start p
                            :range-end (str p "z")}}))
        cas    (fn [_ _]
                 (let [n (n*)]
                   {:type :invoke, :f :txn-cas
                    :value {:key (str txn-key-prefix tag "-cas-" n)
                            :old (str "old-" n)
                            :val (str "new-" n)
                            ;; 1/3 的分组故意用错的 compare 值 → 比较失败 →
                            ;; **失败分支被执行**（空分支 / delete 分支）。
                            ;; 两种分支都覆盖到，才能把「无副作用」与「失败分支
                            ;; 必须执行」两条断言都真正跑一遍。
                            :stale? (zero? (mod n 3))}}))
        casdel (fn [_ _]
                 (let [n (n*)]
                   {:type :invoke, :f :txn-cas-delete
                    :value {:key (str txn-key-prefix tag "-casd-" n)
                            :old (str "old-" n)
                            :val (str "new-" n)
                            :stale? (zero? (mod n 3))}}))
        tread  (fn [_ _]
                 (let [n (n*)]
                   {:type :invoke, :f :txn-read
                    :value {:key (str txn-key-prefix tag "-create-" n)}}))]
    (gen/mix [create wset cas casdel tread])))

;; --------------------------------------------------------------------------
;; T1.3 scan / revision 读
;;
;; 在 `[scan-key-prefix, scan-key-end)` 里写、扫、按历史 revision 读：
;;   :write    递增写（全局唯一值），让区间里有内容；
;;   :scan     区间扫描，随机带 limit（无 / 2 / 5）与 keys_only/count_only；
;;   :read-at  读「本客户端上一次写该 key 拿到的 revision」（历史读）。
;; --------------------------------------------------------------------------

(defn- scan-gen
  [opts]
  (let [size  (long (or (:value-size opts) 16))
        tag   (run-tag opts)
        [st en] (scan-keyspace opts)
        n*    (fn [] (next-n))
        w     (fn [_ _]
                (let [n (n*)]
                  {:type :invoke, :f :write
                   :key (str st (mod n 16))
                   :value (sized-value n size tag)}))
        scan  (fn [_ _]
                (let [lim (rand-nth [nil 2 5 100])]
                  {:type :invoke, :f :scan
                   :value (cond-> {:key st :range-end en}
                            lim (assoc :limit lim)
                            (zero? (rand-int 4)) (assoc :keys-only true)
                            (zero? (rand-int 4)) (assoc :count-only true))}))
        rat   (fn [_ _]
                (let [n (n*)]
                  {:type :invoke, :f :read-at
                   :value {:key (str st (mod n 16)) :revision :last}}))]
    (gen/mix [w w scan scan rat])))

;; --------------------------------------------------------------------------
;; T2.1 watch workload（覆盖缺口 A5）
;;
;; 一个 watch key + 普通写：写产生事件，会话收事件。两种会话：
;;   `:watch-session`（`start-revision 0`，从最新开始）
;;   `:watch-session` + `start-revision :last`（从**本客户端上一次会话观察到的
;;   最大 revision**开始 = 契约「断线重连后以已确认最大 revision + 1 重建即可
;;   续传」的对照路径）。
;; 一次会话内部流被中断（kill/partition）时，客户端按契约重开到
;; `last-revision + 1`（`--watch-resumes` 次），checker 据此判「静默丢事件」。
;; --------------------------------------------------------------------------

(def ^:private watch-key-prefix "/jepsen/watch/")

(defn- watch-gen
  [opts]
  (let [tag  (run-tag opts)
        key  (str watch-key-prefix tag)
        size (long (or (:value-size opts) 16))
        n*   (fn [] (next-n))
        w    (fn [_ _]
               {:type :invoke, :f :write, :key key
                :value (sized-value (n*) size tag)})
        sess (fn [start]
               (fn [_ _]
                 {:type :invoke, :f :watch-session
                  :value {:key key
                          :start-revision start
                          :prev-kv? (zero? (rand-int 2))
                          ;; 默认值必须在这里解析：CLI 选项的 :default 是 nil
                          ;; （见其它 workload 的同一约定），而
                          ;; `(long (or nil 0))` 会把 resumes 变成 **0** ——
                          ;; 那正好关掉 T2.1 的核心路径（分区/重启后 resume）。
                          :window-ms (long (or (:watch-window-ms opts) 2000))
                          :max-events (long (or (:watch-max-events opts) 10000))
                          :resumes (long (or (:watch-resumes opts) 2))}}))]
    ;; 写 : 会话 = 2 : 2（会话是长 op：window-ms 内一直占着一个 worker）
    (gen/mix [w w (sess 0) (sess :last)])))

;; --------------------------------------------------------------------------
;; T2.2 lease workload（覆盖缺口 A6：Lease 未测）
;;
;; 三个场景混跑（到期 2 : 续期 1 : Revoke 1）：
;;   :lease-ttl        grant(ttl) → Put{lease_id} → ttl+grace 内必须消失；
;;                     存活期内不得提前消失（安全 + 活性，§5.1/§5.2）；
;;   :lease-keepalive  续期窗口 > ttl 内持续 KeepAlive → 期内 Key 仍在
;;                     （证明 TTL 真被延长）→ 停续期 → ttl+grace 内消失；
;;   :lease-revoke     grant(长 ttl) → Revoke → grace 内级联删除 +
;;                     对已失效 Lease 续期必须回 ttl=0。
;;
;; key 一律带 run tag（`run-tag`）且每 op 唯一：lab 的节点数据不保证每 run 清空，
;; 复用 key 会把上一个 run 的残留值当成「本 run 写的」（假红）。
;; --------------------------------------------------------------------------

(def ^:private lease-key-prefix "/jepsen/lease/")

(defn- lease-gen
  [opts]
  (let [tag    (run-tag opts)
        ttl    (long (or (:lease-ttl-seconds opts) 2))
        grace  (long (or (:lease-grace-ms opts) 4000))
        rev-ttl (long (or (:lease-revoke-ttl-seconds opts) 30))
        ka-ms  (long (or (:lease-keepalive-ms opts)
                         (* 2 (max 1 ttl) 1000)))
        k      (fn [scene] (str lease-key-prefix tag "/" scene "/" (next-n)))
        ttl-op (fn [_ _] {:type :invoke, :f :lease-ttl
                          :value {:key (k "ttl") :ttl ttl :grace-ms grace}})
        ka-op  (fn [_ _] {:type :invoke, :f :lease-keepalive
                          :value {:key (k "ka") :ttl ttl :grace-ms grace
                                  :keepalive-ms ka-ms}})
        rev-op (fn [_ _] {:type :invoke, :f :lease-revoke
                          :value {:key (k "rev") :ttl rev-ttl
                                  :grace-ms grace}})]
    ;; 到期场景权重翻倍：§5.1 要求「到期场景 ≥ 30」，而它同时承载安全与活性两条断言。
    (gen/mix [ttl-op ttl-op ka-op rev-op])))

;; --------------------------------------------------------------------------
;; M5a —— agent 本地面（lock / election / idgen / registry）
;;
;; 这四个面只在 **agent** 上存在（server 的 router 里没有 coord.agent.*），
;; 所以它们的「多实例」语义天然要求 ≥ 2 个 agent：单 agent 下 lock/election 的
;; 互斥判定只看得到一份本地缓存，结构性不可判（见 jepsen.coord.agent 的 ns 注释 3）。
;;
;; 生成器参数一律在**生成器里**解析默认值（不用 CLI 的 :default = nil 直接算），
;; 与 watch/lease 的同一约定：`(long (or nil 0))` 会把一个参数静默变成 0。
;; --------------------------------------------------------------------------

(def ^:private default-lock-names ["lock-a" "lock-b" "lock-c"])
(def ^:private default-elect-groups ["group-a" "group-b"])
(def ^:private default-idgen-names ["orders" "events"])
(def ^:private default-registry-services ["svc-a" "svc-b"])

(defn- lock-gen
  "`--workload lock`：多个客户端抢同一把锁 → 持有 → 释放。

  holder-id 每 op 随机：每次获取都是**独立的持有者身份**，于是任何跨持有者的
  重叠都是硬违约（真实系统里多数互斥缺陷都长这样）。

  **混入服务端地面真值探针**（`:f :lock-probe`）：它绕开 agent 直接读
  `/_lock/{name}`，把「服务端 key 挂在谁名下」按时间采样下来。这是 F-34 的决定
  性实验，也是 AG-02 里唯一**不依赖客户端自述**的互斥证据 —— 探针是**必然**混进
  来的，所以 checker 开了 `:probe?` 之后「一条探针都没有」会判未执行（见
  lockck 判据 5 与 expect-invalid-probe-missing.edn）。

  **混入弃锁 op**（`:f :lock-abandon`，AG-06）：拿到锁之后故意不释放，由
  `lockck` 用探针判「持锁 agent 活着时不得假丢锁 / 被 kill 后必须在 ttl+grace 内
  被服务端回收」。弃锁用**独立的锁名池**（`--lock-*` 的名字加 `-abandon` 后缀）：
  与 contend 的锁名分开，否则一把弃锁会永久占住争抢池里的一个名字，把
  `:acquires` 样本压到门槛以下（那是判据自身制造的假红）。"
  [opts]
  (let [names (vec (or (:lock-names opts) default-lock-names))
        anames (mapv #(str % "-abandon") names)
        ttl   (long (or (:lock-ttl-seconds opts) 5))
        hold  (long (or (:lock-hold-ms opts) 200))
        dl    (long (or (:lock-deadline-ms opts) 5000))
        grace (long (or (:lock-grace-ms opts) 4000))
        op    (fn [_ _]
                {:type :invoke, :f :lock-contend
                 :value {:name (rand-nth names)
                         :holder-id (str "h-" (rand-int 1000000))
                         :ttl-seconds ttl
                         :hold-ms hold
                         :deadline-ms dl
                         :grace-ms grace}})
        aband (fn [_ _]
                {:type :invoke, :f :lock-abandon
                 :value {:name (rand-nth anames)
                         :holder-id (str "a-" (rand-int 1000000))
                         :ttl-seconds ttl
                         :deadline-ms dl
                         :grace-ms grace}})
        probe (fn [_ _]
                {:type :invoke, :f :lock-probe
                 :value {:name (rand-nth (into names anames))}})]
    ;; 三个 contend + 一个 abandon + 两个 probe 实例：争抢密度高一些（否则
    ;; 「重叠」要等很久才碰得上）；探针份额 1/3（≈ 每个锁名每 40ms 一次采样，
    ;; AG-06 的两条判据都要靠它，短跑也够覆盖）。
    (gen/mix [op op op aband probe probe])))

(defn- elect-gen
  "`--workload election`：多个 candidate 竞选同一 group → 持有 → Resign。

  **混入服务端地面真值探针**（`:f :election-probe`）：绕开 agent 读
  `/_election/{group}`，把「服务端认为谁是 leader」按时间采样下来。此前 election
  的判据**全部**基于客户端自述，而 F-34 证明这种单一来源分诊不了「真违约 /
  agent 汇报层不一致 / 度量偏差」三种解释（F-35 的『残留』段把它列为明示待补）。
  探针是**必然**混进来的 ⇒ checker 开了 `:probe?` 之后「一条都没有」判未执行。"
  [opts]
  (let [groups (vec (or (:election-groups opts) default-elect-groups))
        ttl    (long (or (:election-ttl-seconds opts) 5))
        hold   (long (or (:election-hold-ms opts) 200))
        dl     (long (or (:election-deadline-ms opts) 5000))
        op     (fn [_ _]
                 {:type :invoke, :f :elect-campaign
                  :value {:group-name (rand-nth groups)
                          :candidate-id (str "c-" (rand-int 1000000))
                          :ttl-seconds ttl
                          :hold-ms hold
                          :deadline-ms dl}})
        probe  (fn [_ _]
                 {:type :invoke, :f :election-probe
                  :value {:group-name (rand-nth groups)}})]
    (gen/mix [op op probe])))

(defn- idgen-gen
  "`--workload idgen`：NextId / NextBatch 混跑。

  唯一性判定**不需要**跨节点比较：同一份 run 里所有 ID 放一起看重复即可
  （batch 内部重复更是单响应就能判）。"
  [opts]
  (let [names (vec (or (:idgen-names opts) default-idgen-names))
        op    (fn [_ _]
                {:type :invoke, :f :idgen
                 :value {:name (rand-nth names)
                         :batch? (zero? (rand-int 2))
                         :count (inc (rand-int 16))}})]
    (gen/mix [op op])))

(defn- registry-gen
  "`--workload registry`：注册 → 停心跳 → 等消失（+ 跨客户端 Discover 探针）。

  instance-id 带 run tag 且 run 内唯一：lab 的数据不保证每 run 清空，复用
  instance-id 会把上一个 run 的残留当成「本 run 注册的」（假红）—— 与 lease
  的 key 纪律一致。探针 op（:registry-discover）在**别的**客户端眼里观察，
  是「自我保护快照未收敛」唯一能被抓住的窗口。"
  [opts]
  (let [svcs  (vec (or (:registry-services opts) default-registry-services))
        ttl   (long (or (:registry-ttl-seconds opts) 5))
        grace (long (or (:registry-grace-ms opts) 4000))
        tag   (run-tag opts)
        op    (fn [_ _]
                {:type :invoke, :f :registry-cycle
                 :value {:name (rand-nth svcs)
                         :instance-id (str tag "-" (next-n))
                         :ttl-seconds ttl
                         :grace-ms grace
                         :dup? (zero? (rand-int 3))}})
        probe (fn [_ _]
                {:type :invoke, :f :registry-discover
                 :value {:name (rand-nth svcs)}})]
    (gen/mix [op op probe])))

;; --------------------------------------------------------------------------
;; M5b —— agent 本地**数据面**（cache / mq）
;;
;; 与 M5a 四个本地面（lock/election/idgen/registry）的区别：这四个是**控制面**
;; （互斥/唯一性/生命周期），cache/mq 是**数据面**（读写/交付）。共同点是数据都在
;; agent 进程本地（cache = redb；mq = agent 本地日志）⇒ 「读到自己刚写的值」
;; 只在**单 agent**（或无跨 agent 路由）时成立，所以这两个 workload 由
;; `local-consistency-workloads` 强制要求 `--agents 1`。
;; --------------------------------------------------------------------------

(def ^:private default-cache-keys
  "三类键空间互不相交（string / list / set）：每类 op 只碰自己那一组 key，于是
  一个面的丢失/幽灵不会把另一个面的判据带脏（与 M1 三个面的 key 空间纪律一致）。"
  {:str  (mapv #(str "cache-str-" %) (range 8))
   :list (mapv #(str "cache-list-" %) (range 4))
   :set  (mapv #(str "cache-set-" %) (range 4))})

(defn- cache-gen
  "`--workload cache`：string（Set/Get/Delete）+ list（LPush/LRange/LLen）+
  set（SAdd/SMembers）三类操作混跑。

  * **key 与值都带 run 标签**：cache 的数据在 agent 本地 redb 里，而 lab 不保证
    每次 run 前清空（`agent/setup!` 的清理只在真的需要重启时做）—— 不带标签时
    上一个 run 留下的值会被本 run 读回来，于是「读到的值本 run 从没写过」被
    判 `:fabricated`、而且**一次 run 的历史里会出现两个 run 标签**（实测：
    2026-09-19 的 `cache/partition-agent-server` cell）。与 map/scan/txn 同一
    纪律（`run-tag` 的 docstring）。
  * 每个值/成员全局唯一（run tag + 计数器）：fabricated 判据靠「这个值从没被
    写过」成立，复用值会让它失效；
  * string 的 Set 一半用 `ttl=0`（**持久**，判「写可见」与「重启后仍在」），
    一半用 `--cache-ttl-seconds`（判 TTL 两侧）—— `--cache-ttl-seconds 0`
    时全部持久；
  * list/set 不加 TTL（它们的判据是包含关系，不需要时间轴）。"
  [opts]
  (let [ttl  (long (or (:cache-ttl-seconds opts) 0))
        tag  (run-tag opts)
        ks   (merge-with (fn [a b] a) default-cache-keys
                         (select-keys opts [:cache-str-keys :cache-list-keys :cache-set-keys]))
        kstr (mapv #(str tag "-" %) (vec (:str ks)))
        klist (mapv #(str tag "-" %) (vec (:list ks)))
        kset (mapv #(str tag "-" %) (vec (:set ks)))
        v    (fn [p] (str tag "-" p "-" (next-n)))
        w-ttl (fn [] (if (pos? ttl)
                       (if (zero? (rand-int 2)) 0 ttl)
                       0))
        set-op (fn [_ _]
                 {:type :invoke, :f :cache-set
                  :value {:key (rand-nth kstr) :value (v "s") :ttl-seconds (w-ttl)}})
        get-op (fn [_ _]
                 {:type :invoke, :f :cache-get :value {:key (rand-nth kstr)}})
        del-op (fn [_ _]
                 {:type :invoke, :f :cache-del :value {:key (rand-nth kstr)}})
        lpush-op (fn [_ _]
                   {:type :invoke, :f :cache-lpush
                    :value {:key (rand-nth klist) :value (v "l")}})
        lrange-op (fn [_ _]
                    {:type :invoke, :f :cache-lrange
                     :value {:key (rand-nth klist) :start 0 :stop -1}})
        llen-op (fn [_ _]
                  {:type :invoke, :f :cache-llen :value {:key (rand-nth klist)}})
        sadd-op (fn [_ _]
                  {:type :invoke, :f :cache-sadd
                   :value {:key (rand-nth kset) :member (v "m")}})
        smem-op (fn [_ _]
                  {:type :invoke, :f :cache-smembers :value {:key (rand-nth kset)}})]
    (gen/mix [set-op set-op get-op get-op del-op
              lpush-op lpush-op lrange-op llen-op
              sadd-op sadd-op smem-op])))

(def ^:private default-mq-topic "jepsen-mq")

(defn- mq-topic-name
  "本 run 的 MQ 主题名。

  **必须只有一个来源**：生成器用它发布/拉取，客户端的 `setup!` 用它引导创建
  （`CreateTopic` 不是幂等契约，所以只能在启动期建一次）。实测（第八轮）：两边
  各算各的 ⇒ 生成器拼了 run 标签而客户端拿的是 `nil` ⇒ **主题从未被创建**，
  表现为「发布一条都不成功、每轮 Poll 都是空」（看起来像 MQ 坏了）。"
  [opts]
  (or (:mq-topic opts) (str default-mq-topic "-" (run-tag opts))))

(defn- mq-gen
  "`--workload mq`：Publish / Poll（poll 内部顺带 Ack）混跑。

  * payload 全局唯一（run tag + 计数器）⇒ 「投递内容与发布不一致」可精确判定；
  * 1/4 的发布是**刻意重发**（同一 payload + 同一 idempotency-key 连发两次）：
    这是 `idempotency_key` 去重承诺的证伪点；
  * 主题在客户端 `setup!` 里引导创建（CreateTopic 不是幂等契约，做成随机 op 会
    让「主题到底建没建」变成概率事件）。"
  [opts]
  (let [tag   (run-tag opts)
        ;; 主题名与客户端的 `setup!` **共用同一个来源**（见 mq-topic-name）
        topic (mq-topic-name opts)
        pub (fn [_ _]
              (let [m (next-n)]
                {:type :invoke, :f :mq-publish
                 :value {:topic topic :partition 0
                         :payload (str tag "-m" m)
                         :idempotency-key (str tag "-idem-" m)
                         :dup? (zero? (rand-int 4))}}))
        poll (fn [_ _]
               {:type :invoke, :f :mq-poll
                :value {:topic topic :partition 0
                        ;; 消费组名默认 **run 级**：agent 的消费组偏移是**本地
                        ;; 持久**状态，跨 run 复用同一个组名会让上一个 run 的已
                        ;; 提交偏移混进来（判据再干净也会被历史噪声污染）。
                        :consumer-group (or (:mq-consumer-group opts)
                                            (str "cg-" tag))
                        :max-count 64}})]
    (gen/mix [pub pub pub poll poll])))

;; --------------------------------------------------------------------------
;; T1.5 mixture workload（M1 收口）
;;
;; 三个面（map / txn / scan）混在同一条历史里跑。每一份 op 都打上 `:sub`
;; 标记（`:map` / `:txn` / `:scan`），客户端 completion 原样保留该字段，于是
;; `jepsen.coord.mixck` 可以把历史切片：每个面的专属 checker 只看自己那一份，
;; 而 `routing-checker` 负责证明「三个面都真的跑了」。
;;
;; 混合比：`gen/mix` 的行为是「**在给定生成器里均匀抽一个，从它取 1 个 op**」
;; （jepsen docstring：*A random mixture of several generators … chooses between
;; them uniformly*，被抽中的生成器是 one-time 的）。因此一个面的 op 份额
;; **等于它的实例数占比**，与子生成器内部的槽位数无关 —— 权重直接就是实例数：
;; `--mixture-ratio 4,2,2` ⇒ 4/2/2 个实例 ⇒ 目标份额 50%/25%/25%。
;; 这条口径是被实测钉住的：4/2/2 实例的一轮 120s run（1198 个 client op）实测
;; 份额 0.517/0.255/0.228（目标 0.50/0.25/0.25，标准差 ±0.015）；而「按内部槽位」
;; 的模型预测 0.615/0.192/0.192，与之相差 10 个百分点 —— 已被这轮实测否掉。
;; 每个 run 的 checker 都会报 `:routing {:share ...}` 实测份额，可随时核对。
;; 每个面都新建**独立**的生成器实例（不共享同一个对象），避免依赖
;; `gen/mix` 是否会把同一个生成器对象当作多个独立状态机的实现细节。
;;
;; 三个面的 key 空间本来就互不相交（`/jepsen/map/` · `/jepsen/txn/` ·
;; `/jepsen/scan/`），所以混合不会把另一个面的观察带进本面的写索引。
;; --------------------------------------------------------------------------

(def ^:private default-mixture-ratio
  "T1.5 默认混合比 map:txn:scan = 4:2:2（= 目标 op 份额 50%/25%/25%）。"
  [4 2 2])

(defn- mixture-ratio
  [opts]
  (let [w (or (:mixture-ratio opts) default-mixture-ratio)]
    (mapv long w)))

(defn- tag-sub
  "给一个子生成器的每个 op 打上 `:sub` 标记（见 mixture-gen）。"
  [sub g]
  (gen/map #(assoc % :sub sub) g))

(defn- mixture-gen
  [opts]
  (let [[wm wt ws] (mixture-ratio opts)]
    (info "mixture: target op share map:txn:scan" (str wm ":" wt ":" ws)
          "-> instances" (str wm "/" wt "/" ws))
    (gen/mix
      (concat
        (for [_ (range wm)] (tag-sub :map (map-gen opts)))
        (for [_ (range wt)] (tag-sub :txn (txn-gen opts)))
        (for [_ (range ws)] (tag-sub :scan (scan-gen opts)))))))

;; --------------------------------------------------------------------------
;; T6.1 soak-full workload（组合浸泡：多个面按**声明的比例**混跑）
;;
;; 与 T1.5 `mixture` 的区别：mixture 是三个数据面（map/txn/scan）的固定组合，
;; 而 soak-full 是**验收长跑的入口** —— 它的面集合与权重由 `--soak-mix`
;; 声明（dev.md T6.1 的比例：map 40 / txn 20 / watch 15 / lease 10 / lock 10 /
;; election 3 / registry 2）。
;;
;; **拒绝静默丢弃**：只声明了**已实现**的面；一旦 `--soak-mix` 里出现还没接的
;; 面（lock / election / registry 需要 M5 的 agent 插件面），测试在**构造期**
;; 就抛异常。理由是 soak 的价值全在「覆盖面可核对」：一个「少跑了 3 个面但报告
;; 全绿」的 72h soak 比不跑更糟（它会让评审以为那 3 个面已经被覆盖）。
;;
;; 权重会被 gcd 约简成实例数（gen/mix 的面份额 = 实例数占比，见 mixture-gen
;; 的口径注释）；报告里的实测份额 `:routing {:share ...}` 可以随时核对目标 vs 实际。
;; --------------------------------------------------------------------------

(def ^:private soakfull-implemented
  "T6.1 组合浸泡里**已经实现**的面。

  M5a 落地后 lock / election / registry / idgen 四个 **agent 本地面**也进来了
  —— 这是 T6.1 的关键解锁项：它们的契约判据本来就是跨 agent 的（互斥、唯一
  leader、全局唯一 ID），与数据面共用同一条历史正合适。"
  #{:map :txn :scan :watch :lease :lock :election :registry :idgen})

(def ^:private soakfull-default-mix
  "默认面权重。取 T6.1 比例中「数据面 + M5a agent 面」的一个可用组合；
  weight 只用于**相对份额**，不要求总和为 100。

  验收级 T6.1 长跑请显式传完整的 T6.1 比例：
  `--soak-mix map=40,txn=20,scan=5,watch=15,lease=10,lock=10,election=3,registry=2,idgen=5`"
  {:map 40 :txn 20 :scan 5 :watch 15 :lease 10 :lock 10 :election 3 :registry 2})

(defn- parse-mix
  "解析 `--soak-mix 「map=40,txn=20,watch=15」` → `{:map 40 :txn 20 :watch 15}`。

  注意 docstring 里不能用 ASCII 双引号（会把字符串提前截断，报错位置还很难懂）——
  F-23：这里第一版写了 `\"map=40,...\"`，编译直接失败在 `defn-` 上。"
  [s]
  (let [m (into {}
                (for [part (str/split (str s) #",")
                      :let [part (str/trim part)]
                      :when (seq part)]
                  (let [[k v] (str/split part #"=" 2)]
                    [(keyword (str/trim k)) (Long/parseLong (str/trim v))])))]
    (when (empty? m)
      (throw (ex-info "--soak-mix 解析为空" {:input s})))
    (when (some #(not (pos? (long %))) (vals m))
      (throw (ex-info "--soak-mix 的权重必须都是正整数" {:input s :parsed m})))
    m))

(defn- agent-node-map
  "agent 隧道 endpoint → agent 主机名。

  客户端 op 的 `:acquire-node` 是**隧道地址**（`127.0.0.1:<port>`），而 agent
  nemesis 的事件（`:killed-agent` / `:partitioned-agent` …）用**主机名**。lock 的
  AG-06 判据要把「我持有了」归因到具体 agent，再和 nemesis 的时间窗对上，所以
  需要在两边之间搭一座桥（端口分配是 run 级确定的，见 agent/local-port）。"
  [aplan]
  (when aplan
    (into {}
          (map (fn [i]
                 [(agent/endpoint aplan i) (agent/agent-host aplan i)]))
          (range 1 (inc (long (:count aplan)))))))

(defn- soakfull-mix
  "解析并校验面集合：未实现的面**硬失败**（拒绝静默丢弃）。"
  [opts]
  (let [m (or (:soak-mix opts) soakfull-default-mix)
        missing (vec (remove soakfull-implemented (keys m)))]
    (when (seq missing)
      (throw (ex-info
               (str "soakfull: 面 " missing " 尚未实现（lock/election/registry "
                    "需要 M5 的 agent 插件面）—— 组合 soak 不得以「少跑几个面」的"
                    "方式判绿。已实现：" (vec (sort soakfull-implemented)))
               {:missing missing
                :implemented (vec (sort soakfull-implemented))
                :requested (vec (sort (keys m)))})))
    m))

(defn- gcd-of
  [ns]
  (reduce (fn [a b] (long (let [a (long a) b (long b)]
                            (loop [x (max a b) y (min a b)]
                              (if (zero? y) x (recur y (mod x y)))))))
          ns))

(defn- soakfull-gen
  [opts]
  (let [mix (soakfull-mix opts)
        g   (gcd-of (vals mix))
        ;; 实例数 = 权重 / gcd（保持比例、把实例数压到最小）
        inst (into {} (map (fn [[k v]] [k (quot (long v) g)]) mix))
        sub-gen (fn [k]
                  (case k
                    :map   (map-gen opts)
                    :txn   (txn-gen opts)
                    :scan  (scan-gen opts)
                    :watch (watch-gen opts)
                    :lease (lease-gen opts)
                    ;; M5a agent 本地面
                    :lock      (lock-gen opts)
                    :election  (elect-gen opts)
                    :registry  (registry-gen opts)
                    :idgen     (idgen-gen opts)
                    (throw (ex-info (str "soakfull: no generator for " k) {:surface k}))))]
    (info "soakfull: surface mix" (pr-str mix) "-> instances" (pr-str inst))
    (gen/mix
      (concat
        (for [k (sort (keys inst))
              _ (range (long (get inst k)))]
          (tag-sub k (sub-gen k)))))))

(defn- soakfull-checker
  "T6.1 组合 checker：每个面用**它自己的** checker（不新造判据），外加
  mixck 的路由/样本门槛（每个面必须真的跑到，且不存在未路由的客户端 op）。"
  [opts]
  (let [mix (soakfull-mix opts)]
    (mixck/checker
      {:surfaces        (vec (sort-by name (keys mix)))
       :map-mode        (if (= :soak (:checker opts)) :index :linear)
       :min-deletes     (or (:map-min-deletes opts) 0)
       :min-sample      (:min-op-sample opts)
       :watch-semantics (:watch-semantics opts)
       :watch-min-events (:watch-min-events opts)
       :min-grants      (:lease-min-grants opts)
       :min-expiries    (:lease-min-expiries opts)
       :tolerance-ms    (:lease-tolerance-ms opts)
       ;; M5a agent 本地面
       :lock-min-acquires      (:lock-min-acquires opts)
       :lock-grace-ms          (:lock-grace-ms opts)
       ;; AG-06：弃锁判据的 agent 归因表（与 `:abandon?` 必须成对出现）
       :agent-nodes            (agent-node-map (:agent-plan opts))
       :election-min-campaigns (:election-min-campaigns opts)
       :registry-min-cycles    (:registry-min-cycles opts)
       :registry-grace-ms      (:registry-grace-ms opts)
       :idgen-min-ids          (:idgen-min-ids opts)
       :idgen-min-regressions  (:idgen-min-regressions opts)})))

(defn- client-gen
  "Client generator for the given opts. For :multi-register, ops target one
  of the N region register keys (--regions N), each routing to its own region."
  [opts]
  (let [workload (:workload opts)
        soak?    (soak? opts)]
    (gen/mix
      (case workload
        :register     [r (if soak? soak-w w)]
        :cas-register [r cas cas]
        :idempotency  [(idem-gen opts)]
        :map          [(map-gen opts)]
        :txn          [(txn-gen opts)]
        :scan         [(scan-gen opts)]
        :mixture      [(mixture-gen opts)]
        :watch        [(watch-gen opts)]
        :lease        [(lease-gen opts)]
        ;; M5a：agent 本地面（需要 --agents N 才有地方跑）
        :lock         [(lock-gen opts)]
        :election     [(elect-gen opts)]
        :idgen        [(idgen-gen opts)]
        :registry     [(registry-gen opts)]
        ;; M5b：agent 本地数据面（需要 --agents 1：见 local-consistency-workloads）
        :cache        [(cache-gen opts)]
        :mq           [(mq-gen opts)]
        :soakfull     [(soakfull-gen opts)]
        :multi-register
        (let [keys (vec (regions/region-keys (or (:regions opts) 1)))
              next-val (fn [k]
                         ;; Per-region monotonic counter: swap! returns the new
                         ;; atom state (the whole map) -- return the per-key
                         ;; value instead.
                         (let [v (inc (get @multi-write-counter k 0))]
                           (swap! multi-write-counter assoc k v)
                           v))
              w    (fn [_ _]
                     (let [k (rand-nth keys)]
                       {:type :invoke, :f :write, :key k
                        :value (if soak?
                                 (next-val k)
                                 (rand-int 1000000))}))
              rd   (fn [_ _]
                     {:type :invoke, :f :read, :key (rand-nth keys)})]
          [rd w])))))

;; --------------------------------------------------------------------------
;; Nemesis generators
;; --------------------------------------------------------------------------

(defn nemesis-schedule
  "T0.5/T0.6: re-derives the *jittered* nemesis schedule for `opts` from its
  `:seed` — the same draws, in the same order, that `soak-nemesis-gen` makes
  when the generator is built. Pure and side-effect free (no cluster, no
  sleeps), so `scripts/replay.clj` can prove that a stored run's fault
  schedule is reproducible from the seed recorded in its test map.

  `opts`: `:seed`, `:soak-quiet`, `:soak-disrupt`, `:jitter`, `:steps`.
  Returns `[{:kind :kill|:pause|:partition, :quiet-seconds s,
  :disrupt-seconds s} ...]`."
  [opts]
  (init-rng! (:seed opts))
  (let [quiet   (get opts :soak-quiet 1800)
        disrupt (get opts :soak-disrupt 600)
        jit?    (jitter? opts)
        steps   (get opts :steps 6)]
    (vec (for [[kind _stop] (take steps (cycle [[:kill      :kill-stop]
                                                [:pause     :pause-stop]
                                                [:partition :partition-stop]]))]
           {:kind kind
            ;; Draw order must mirror soak-nemesis-gen: quiet then disrupt,
            ;; per rotation step.
            :quiet-seconds   (if jit? (jittered quiet) quiet)
            :disrupt-seconds (if jit? (jittered disrupt) disrupt)}))))

(defn- single-nemesis-gen
  "Start/stop disruption every 3–8s (jittered, T0.6).

  F-11：**不能**写成 `(cycle [(beat) {:f :start} (beat) {:f :stop}])`。
  `clojure.core/cycle` 先求值那个向量**一次**，再无限重复它的元素 —— 于是
  整个 run 的节拍是常数（实测：gap 恒为 6.42s / 6.64s，看着落在 [3,8] 区间
  里，所以“间隔 ∈ [3,8]”这种判据抓不到）。用 `(mapcat ... (iterate inc 0))`
  逐个周期惰性求值：每个周期都重新抽一次抖动。
  用 `iterate` 而不是 `range`：`range` 是分块的，一次会预取 32 个周期。"
  [opts]
  (mapcat (fn [_] [(nemesis-beat opts) {:type :info, :f :start}
                   (nemesis-beat opts) {:type :info, :f :stop}])
          (iterate inc 0)))

(defn- combined-nemesis-gen
  "Drives the composed :all nemesis: kill, pause, partition, in rotation.
  （同样必须惰性构造，原因见 single-nemesis-gen 的 F-11 注释。）"
  [opts]
  (mapcat (fn [_] [(nemesis-beat opts) {:type :info, :f :kill}
                   (nemesis-beat opts) {:type :info, :f :kill-stop}
                   (nemesis-beat opts) {:type :info, :f :pause}
                   (nemesis-beat opts) {:type :info, :f :pause-stop}
                   (nemesis-beat opts) {:type :info, :f :partition}
                   (nemesis-beat opts) {:type :info, :f :partition-stop}])
          (iterate inc 0)))

(defn- soak-nemesis-gen
  "Slow rotating fault cycle for long soak runs: a quiet window, then a single
  disruption (kill / pause / single-node partition), then a recovery window;
  repeats forever (bounded by the surrounding gen/time-limit).

  F-11：静/动窗口必须**每个周期重抽**（`(q)`/`(d)` 在惰性构造里调用），否则
  72h soak 的 ±20% 抖动作废、所有周期等长。抽签顺序（每个周期先 quiet 后
  disrupt）必须与 `nemesis-schedule` 一致，否则 T0.5 的种子回放对不上。"
  [opts]
  (let [quiet   (get opts :soak-quiet 1800)
        disrupt (get opts :soak-disrupt 600)
        jit?    (jitter? opts)
        q       (fn [] (gen/sleep (if jit? (jittered quiet) quiet)))
        d       (fn [] (gen/sleep (if jit? (jittered disrupt) disrupt)))]
    (mapcat
      (fn [_]
        [(q)
         {:type :info, :f :kill}
         (d)
         {:type :info, :f :kill-stop}
         (q)
         {:type :info, :f :pause}
         (d)
         {:type :info, :f :pause-stop}
         (q)
         {:type :info, :f :partition}
         (d)
         {:type :info, :f :partition-stop}])
      (iterate inc 0))))

(defn- client-part
  "Client generator for the given opts. With --rate N, ops are issued at a
  fixed *global* rate of N ops/sec (gen/delay -- soak-friendly); otherwise
  the default exponential stagger (mean 0.3s) is used."
  [opts]
  (let [base (gen/clients (client-gen opts))
        rate (:rate opts)]
    (if (and rate (pos? rate))
      (gen/delay (/ 1.0 (double rate)) base)
      (gen/stagger 0.3 base))))

(defn- workload-gen
  [opts]
  (let [nemesis-key (:nemesis opts)
        soak?       (soak? opts)
        nemesis-gen (case nemesis-key
                      :none nil
                      :soak (soak-nemesis-gen opts)
                      :all  (combined-nemesis-gen opts)
                      (single-nemesis-gen opts))
        clients     (client-part opts)
        recovery    (if soak? 60 30)]
    (gen/phases
      (gen/time-limit (:time-limit opts)
        (if nemesis-gen
          (gen/nemesis nemesis-gen clients)
          clients))
      (when nemesis-gen
        (gen/nemesis {:type :info, :f :stop}))
      (gen/log "recovery: letting cluster converge")
      (gen/sleep recovery)
      (when soak?
        ;; Final verification reads: every client reads every register key
        ;; (single register: the one legacy key) until it observes an :ok
        ;; (bounded), which lets the soak checker confirm the cluster
        ;; recovered and converged on the latest committed value per region.
        ;;
        ;; 只对「key 集合有限且已知」的 workload 做收尾验证读：register /
        ;; multi-register / map / mixture（map 面）。txn / scan / idempotency 的
        ;; key 空间是每 op 新建的，收尾读 legacy register key 只会读到一个无关的
        ;; key（让人误以为集群没收敛）——那些 workload 的收敛由各自 checker 判定。
        ;;
        ;; §5.2 的「最终收敛」：收尾读必须能拿到「最新已确认值」，且不得是
        ;; fabricated / future / stale —— 这几条由本 workload 的 checker 判
        ;; （map = 写索引；mixture = map 面的写索引）。因此收尾读必须带 `:sub`
        ;; 标记，否则 mixck 会把它当成「未路由的客户端 op」直接判红（这正是
        ;; routing-checker 存在的意义）。
        (let [read-ops (cond
                         (= :multi-register (:workload opts))
                         (mapv (fn [k] {:f :read, :key k})
                               (regions/region-keys (or (:regions opts) 1)))

                         (= :map (:workload opts))
                         (mapv (fn [k] {:f :read, :key k}) (map-keys opts))

                         (= :mixture (:workload opts))
                         (mapv (fn [k] {:f :read, :key k, :sub :map})
                               (map-keys opts))

                         ;; T6.1：组合浸泡也做 §5.2 的最终收敛读（map 面的 key）。
                         ;; 必须带 `:sub :map`，否则路由门槛会把它们当成
                         ;; 「未路由的客户端 op」直接判红 —— 那正是这道门槛的用意。
                         (= :soakfull (:workload opts))
                         (mapv (fn [k] {:f :read, :key k, :sub :map})
                               (map-keys opts))

                         (= :register (:workload opts))
                         [{:f :read, :key nil}]

                         :else nil)]
          (when (seq read-ops)
            (gen/log "soak: final verification reads")
            (->> (gen/mix
                   (for [op read-ops]
                     (gen/until-ok
                       (gen/limit 20 (gen/repeat op)))))
                 gen/clients)))))))

;; --------------------------------------------------------------------------
;; Checker
;; --------------------------------------------------------------------------

(def ^:private local-surface-workloads
  "只存在于 agent 上的面（server 的 router 里没有 coord.agent.*）。

  对这几个 workload，客户端**必须**连 agent（否则是 UNIMPLEMENTED），所以
  `--via-agent` 是**隐含**的：不需要用户再写一遍。数据面的差分（direct vs
  --via-agent）才需要显式开关。"
  #{:lock :election :idgen :registry :cache :mq})

(def ^:private local-consistency-workloads
  "数据**只在 agent 进程本地**、却要求「读到自己刚写的值」的面。

  cache 是 agent 本地 redb；mq 是 agent 本地日志（ISR 复制默认关，且本 lab 的
  agent 绑 loopback + SSH 隧道，彼此不可达）。这些 workload 跑在 ≥2 个 agent 上
  时，「读不到」是**合法**的（数据在另一个 agent 上），于是任何「写后读」判据
  都会变成假红或假绿 —— 所以我们**在起跑前拒绝** `--agents > 1`，而不是让
  checker 去猜。跨 agent 的复制面是独立的一轮 lab 工作（见
  coord-agent-coverage-plan.md §11 的漏检边界）。"
  #{:cache :mq})

(defn- per-key-linear
  "Independent linearizable register check per key for :multi-register: ops
  carry :key, and each key is its own register (region). Valid iff every
  key's sub-history is linearizable."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [by-key (group-by (fn [op] (or (:key op) :register)) history)
            checks (for [[k h] by-key]
                     [k (checker/check (checker/linearizable
                                        {:model (model/register)
                                         :algorithm :wgl})
                                       test h opts)])]
        (into {:valid? (every? (fn [[_ r]] (:valid? r)) checks)}
              (map (fn [[k r]]
                     [(str "key-" k) {:valid? (:valid? r)}])
                   checks))))))

(defn- checker
  [opts]
  (let [workload (:workload opts)
        ck       (:checker opts)
        linear   (cond
                   ;; T1.4：幂等专项有专用 checker（op 形状不是 register）。
                   (= :idempotency workload)
                   (idem/checker {:min-replay-attempts (:idem-min-replay-attempts opts)})

                   ;; T1.1：map/delete。短矩阵 knossos（delete 建模为 write nil），
                   ;; 长跑用 O(n log n) 写索引（--checker soak）。
                   (= :map workload)
                   (mapck/checker
                     {:mode (if (= :soak ck) :index :linear)
                      :min-deletes (or (:map-min-deletes opts) 0)})

                   ;; T1.2：txn 全形态（无副作用 / 原子可见 / 几何断言）。
                   (= :txn workload)
                   (txnck/checker {})

                   ;; T1.3：scan/revision 读。
                   (= :scan workload)
                   (scanck/checker {})

                   ;; T1.5：mixture（map+txn+scan 混合）。组合 checker 里三个面
                   ;; 都复用各自的专属判据，另外加路由/样本门槛（每个面必须达到
                   ;; `--min-op-sample`，否则该 run 不能当 M1 收口证据）。
                   ;; 长跑（--checker soak）时 map 面切到 O(n log n) 索引路径。
                   (= :mixture workload)
                   (mixck/checker {:map-mode    (if (= :soak ck) :index :linear)
                                   :min-deletes (or (:map-min-deletes opts) 0)
                                   :min-sample  (:min-op-sample opts)})

                   ;; T6.1：soak-full 组合浸泡（面集合/权重由 --soak-mix 声明；
                   ;; 未实现的面在构造期硬失败，见 soakfull-mix）。
                   (= :soakfull workload)
                   (soakfull-checker opts)

                   ;; T2.1：watch 事件流（结构 / 续传起点 / fabricated / 静默
                   ;; 丢事件 + §5.1 样本门槛）。语义档由 --watch-semantics 选。
                   (= :watch workload)
                   (watchck/checker {:semantics  (:watch-semantics opts)
                                     :min-events (:watch-min-events opts)})

                   ;; T2.2：lease（TTL 到期 / 续期 / Revoke 级联删除）。
                   ;; 判定基准是客户端单调量，不受节点时钟差影响（F-08）。
                   (= :lease workload)
                   (leaseck/checker {:min-grants   (:lease-min-grants opts)
                                     :min-expiries (:lease-min-expiries opts)
                                     :tolerance-ms (:lease-tolerance-ms opts)})

                   ;; M5a：agent 本地面（lock / election / idgen / registry）。
                   ;; 四个面的判据都是跨 agent 的（互斥 / 唯一 leader / 全局唯一
                   ;; ID / 幽灵实例），需要 ≥2 个 agent 拓扑才有意义。
                   ;; lock 的生成器**必然**混地面真值探针 ⇒ checker 要求探针在场
                   ;; （缺了判未执行；见 lockck 判据 5 / F-34）。
                   (= :lock workload)
                   (lockck/checker {:min-acquires (:lock-min-acquires opts)
                                    :probe?       true
                                    :abandon?     true
                                    :grace-ms     (:lock-grace-ms opts)
                                    ;; AG-06：弃锁 op 的 kill/分区窗口归因
                                    :agent-nodes  (agent-node-map (:agent-plan opts))})

                   (= :election workload)
                   (electck/checker {:min-campaigns (:election-min-campaigns opts)
                                     ;; F-35 残留：服务端地面真值探针（与 lock 同构）
                                     :probe?       true})

                   (= :idgen workload)
                   (idgenck/checker {:min-ids (:idgen-min-ids opts)
                                     :min-regressions (:idgen-min-regressions opts)})

                   (= :registry workload)
                   (regck/checker {:min-cycles (:registry-min-cycles opts)
                                   :grace-ms   (:registry-grace-ms opts)})

                   ;; M5b：agent 本地数据面（cache = AG-09 / mq = AG-11）。
                   ;; 两者的数据都在 agent 本地 ⇒ 一致性判据只在单 agent 下成立
                   ;; （coord-test 已强制 --agents 1，见 local-consistency-workloads）。
                   (= :cache workload)
                   (cacheck/checker {:min-sets       (:cache-min-sets opts)
                                     :min-gets       (:cache-min-gets opts)
                                     :min-list-ops   (:cache-min-list-ops opts)
                                     :min-set-ops    (:cache-min-set-ops opts)
                                     :ttl-grace-ms   (:cache-ttl-grace-ms opts)})

                   (= :mq workload)
                   (mqck/checker {:min-publishes (:mq-min-publishes opts)
                                  :min-polls     (:mq-min-polls opts)
                                  :expect-idem-dedupe? (:mq-expect-idem-dedupe? opts)})

                   ;; Soak mode: O(n) checker groups by :key (per region).
                   (= :soak ck)
                   (if (= :register workload)
                     (soak/checker)
                     (if (= :multi-register workload)
                       (soak/checker)
                       (do (warn "soak checker requires --workload register, "
                                 "multi-register or map; falling back to knossos linearizable")
                           (checker/linearizable
                             {:model (model/register), :algorithm :wgl}))))

                   ;; Multi-register, non-soak: each :key is its own register.
                   (= :multi-register workload)
                   (per-key-linear)

                   :else
                   (checker/linearizable
                     {:model (case workload
                               :cas-register (model/cas-register)
                               (model/register))
                      ;; WGL is more memory-efficient than the competition
                      ;; checker for histories with many :info ops.
                      :algorithm :wgl}))]
    (checker/compose
      {:linear linear
       ;; T0.2: quiet-window availability + RTO + value-uniqueness premise.
       ;; Independent of the linearizability check, and gated (C4): these
       ;; failures must reach :valid?, not just a report.
       :gates  (gates/checker
                 {:nemesis (:nemesis opts)
                  :quiet-availability-min (:quiet-availability-min opts)
                  :quiet-min-sample       (:quiet-min-sample opts)
                  :max-rto-seconds        (:soak-max-rto-seconds opts)
                  ;; F-13：单类 op 的 :ok 率下界（防御「这个 workload 根本没跑」）
                  :min-op-ok-ratio        (:min-op-ok-ratio opts)
                  :min-op-sample          (:min-op-sample opts)
                  ;; A dedicated idempotency workload deliberately replays the
                  ;; same value, so the uniqueness premise does not hold there.
                  :check-premise?         (not= :idempotency (:workload opts))
                  ;; AG-01：--via-agent 的 run 必须能回答「这次真的经过 agent 了吗」
                  ;; （否则端点写错 = 直连假绿）。抓取在 check 时做：agent 每 run
                  ;; 新进程，计数从 0 起，run 后抓到 > 0 才算证明。
                  ;;
                  ;; **只有 `via?` 的 run 才挂这条门禁**：`matrix-m5-diff` 的 direct
                  ;; 跑是**对照组**（客户端直连 server），它本来就不该有 agent 流量
                  ;; —— 给它挂「代理计数 > 0」等于把对照组判红，于是 T5.2 的归因
                  ;; 基线根本建不起来。实测（M5a 第二轮）：diff 矩阵 4 个 direct cell
                  ;; 全部红在 `:agent-route-not-proven`（`:total 0`），而它们只是在
                  ;; 正确地直连。
                  :agent-scrape           (when (and (:agent-plan opts)
                                                     (or (:via-agent opts)
                                                         (contains? local-surface-workloads
                                                                    (:workload opts))))
                                            (let [p (:agent-plan opts)]
                                              (fn [] (agent/routing-proof! p))))
                  :min-agent-requests     (:min-agent-requests opts)
                  :agent-local-surface?   (contains? local-surface-workloads
                                                     (:workload opts))})
       :perf   (checker/perf)})))

;; --------------------------------------------------------------------------
;; Nemesis selection
;; --------------------------------------------------------------------------

(defn- build-nemesis
  [nemesis-key db aplan]
  (case nemesis-key
    :none             nemesis/noop
    :kill            (n/kill-one db)
    :kill-all        (n/kill-all db)
    :pause           (n/pause-one)
    :partition       (nemesis/partition-random-node)
    :partition-halves (nemesis/partition-random-halves)
    :partition-ring  (nemesis/partition-majorities-ring)
    :all             (n/compose-all db)
    :soak            (n/compose-all db)
    ;; M5a：agent 侧故障注入（需要 --agents N>0；否则构造期抛，不静默变成 :none）
    :kill-agent      (n/kill-agent db aplan)
    :kill-agent-all  (n/kill-agent-all db aplan)
    :pause-agent     (n/pause-agent db aplan)
    :partition-agent-server (n/partition-agent-server db aplan)
    :agent-all       (n/compose-agent-all db aplan)
    nemesis/noop))

(defn- with-agent
  "包装 client：把 agent 的启停挂到客户端的生命周期上。

  **为什么挂在 open! 上（而不是 setup!）**：jepsen 的顺序是
  `db/setup!`（所有节点）→ `client/open!`（每个节点一次）→ `client/setup!`
  （只对第一个 client）—— 见 `jepsen/core.clj:196-224`。`open!` 里就会
  连隧道并认证，所以 agent 必须在**第一次 open!** 之前就起来。用原子量保证
  只起一次（顺序不依赖 jepsen 版本的细节：两个钩子都调 bring-up!，谁先来谁做）。

  另：db/setup! 已经把整个集群起好（agent 的 static_peers 要连的就是它），
  所以这里做「集群级」的 agent 启动是安全的，包括幂等的
  `coord security bootstrap-role`。"
  [client agent-plan]
  (if-not agent-plan
    client
    (let [up?       (atom false)
          bring-up! (fn []
                      (when (compare-and-set! up? false true)
                        (agent/bootstrap-role! agent-plan)
                        ;; agent 侧没有 server 那种「root 全能力放行」旁路，
                        ;; 所以客户端用到的能力必须显式授给 root（幂等；见
                        ;; agent.clj 的 grant-client-capabilities! 与 F-28）。
                        (agent/grant-client-capabilities! agent-plan)
                        (agent/setup! agent-plan)))]
      (reify jclient/Client
        (open! [this test node]
          (bring-up!)
          (jclient/open! client test node))
        (setup! [this test]
          (bring-up!)
          (jclient/setup! client test)
          ;; AG-01：run **期间**的第一次路由采样（此时 agent 刚起、计数为 0）。
          ;; 只在 setup! 采一次 + teardown! 采一次，是因为 nemesis 会故意 kill
          ;; agent —— 只抓 run 结束那一次会把「刚才被杀、还没起回来」误判成
          ;; 「路由证不出来」（见 agent.clj 的 scrape-log 注释）。
          (agent/sample-routing! agent-plan)
          this)
        (invoke! [this test op] (jclient/invoke! client test op))
        (close! [this test] (jclient/close! client test))
        (teardown! [this test]
          (jclient/teardown! client test)
          (agent/sample-routing! agent-plan)
          (agent/teardown! agent-plan))))))

(defn- agent-plan
  "解析 agent 计划（`--agents N`）；未启用时 nil。

  `all-nodes` 是 **CLI 给的完整节点列表**（可能比集群大）—— agent 默认跑在
  集群之外的那几个节点上，这样 `:partition-agent-server` 才能把 agent 与整个
  集群隔开（见 jepsen.coord.agent 的 ns 注释 3）。"
  [opts all-nodes]
  (when (agent/enabled? opts)
    (agent/plan opts {:nodes all-nodes})))

;; --------------------------------------------------------------------------
;; Test assembly
;; --------------------------------------------------------------------------

(defn coord-test
  "Constructs a coord test from CLI options."
  [opts]
  (let [soak?  (soak? opts)
        ;; T0.5: resolve the seed *before* the generator is built (the jittered
        ;; nemesis schedule draws from this RNG at construction time).
        seed   (init-rng! (:seed opts))
        _      (println (str "coord: seed=" seed))
        opts   (assoc opts :seed seed)
        ;; Soak defaults: fixed low rate + the O(n) soak checker. Everything
        ;; is still overridable via --rate / --checker.
        opts   (cond-> opts
                 soak? (update :rate #(or % 0.5))
                 true  (update :checker #(or % (if soak? :soak :linear))))
        ;; :multi-register needs a region count; default to 1 (multi_raft
        ;; enabled, single full-keyspace region -- the T5.1 greyscale shape).
        opts   (cond-> opts
                 (and (= :multi-register (:workload opts))
                      (nil? (:regions opts)))
                 (assoc :regions 1))
        ;; M5a：agent 的计划必须在 :nodes 被截断**之前**算出来 ——
        ;; agent 默认跑在「集群之外的节点」（第 4 个起），所以需要 CLI 给的
        ;; 完整节点列表（all-nodes），而 :nodes 本身要截断成 3 节点集群。
        all-nodes (vec (:nodes opts))
        nodes  (vec (take db/server-count all-nodes))
        peers  (db/agent-peers all-nodes)
        opts   (cond-> (assoc opts :nodes nodes)
                 (agent/enabled? opts) (assoc :agent-peers peers))
        ;; M5b：cache/mq 两个数据面需要在 agent 配置里**显式开启**（agent 的
        ;; default-services 只有 M5a 的四个控制面）。按 workload 开，避免所有 run
        ;; 都多起两个服务、也多两个未判定的面。
        opts   (cond-> opts
                 (contains? #{:cache :mq} (:workload opts))
                 (assoc :agent-services (conj agent/default-services (:workload opts)))
                 ;; M5b：主题名必须在**构造客户端之前**定下来 —— 客户端的
                 ;; `setup!` 要用它建主题，而生成器要用同一个名字发布。
                 (= :mq (:workload opts))
                 (assoc :mq-topic (mq-topic-name opts)))
        db     (db/coord opts)
        aplan  (agent-plan opts all-nodes)
        opts   (cond-> opts aplan (assoc :agent-plan aplan))
        ;; --via-agent：客户端连控制机上的**隧道端口**（每个 agent 一个），
        ;; 而不是节点的 50051。其余一切不变：agent 只是传输路径。
        ;; agent 本地面（lock/election/idgen/registry）**必须**经 agent，
        ;; 所以对它们而言 via-agent 是隐含的。
        via?   (or (:via-agent opts)
                   (contains? local-surface-workloads (:workload opts)))
        client-nodes (if (and aplan via?)
                       (agent/endpoints aplan)
                       nodes)
        nemesis (build-nemesis (:nemesis opts) db aplan)]
    (when (and aplan via?)
      (info "M5a: client endpoints" (pr-str client-nodes)
            "-> agents on" (pr-str (:hosts aplan))
            (if (:via-agent opts) "(--via-agent)" "(implied: agent-local workload)")))
    (when (and (not aplan) (contains? local-surface-workloads (:workload opts)))
      (throw (ex-info (str "workload " (:workload opts) " 需要 agent："
                           "请加 --agents N（lock/election/idgen/registry/cache/mq 是 "
                           "agent 本地面，server 上不存在这些服务）")
                      {:workload (:workload opts)})))
    ;; M5b：cache/mq 的数据在 agent 进程本地 ⇒ 「写后读」判据只在单 agent 下成立。
    ;; 与其让 checker 在「另一份本地存储上读不到」时产出假红（或把真丢数据当噪声），
    ;; 不如在**起跑前**拒绝：多 agent 的 cache/mq 需要 ISR 复制拓扑，那是另一轮 lab
    ;; 工作（见 coord-agent-coverage-plan.md §11 的漏检边界）。
    (when (and aplan (contains? local-consistency-workloads (:workload opts))
               (not= 1 (long (:count aplan))))
      (throw (ex-info (str "workload " (:workload opts) " 的数据只在 agent 本地，"
                           "读-己-写判据要求 --agents 1（当前 " (:count aplan) " 个 agent）。"
                           "多 agent 同跑会把「数据在另一个 agent 上」误判成丢数据")
                      {:workload (:workload opts) :agents (:count aplan)})))
    (when soak?
      (reset! soak-write-counter 0)
      (reset! multi-write-counter {}))
    ;; T1.1/T1.2/T1.3：写值的全局唯一计数器每个 run 从 0 开始（同一种子 ⇒
    ;; 同一个值序列，便于回放）。
    (reset! op-counter 0)
    ;; Merge order matters: opts carries the *selection* keywords (:nemesis
    ;; :workload) which must not clobber the computed values below.
    (merge tests/noop-test
           opts
           {:name      "coord"
            :os        os/noop
            :db        db
            :client    (with-agent (client/coord-client
                                     ;; ops 走 client-nodes（可能是 agent 隧道）；
                                     ;; 鉴权必须走集群端点 —— agent 不代理 Auth
                                     ;; （它只代理 6 个数据面服务），见
                                     ;; client.clj 的 reauthenticate!。
                                     (assoc opts
                                            :nodes client-nodes
                                            :auth-nodes nodes))
                                   aplan)
            :nemesis   nemesis
            :generator (workload-gen opts)
            :checker   (checker opts)})))

;; --------------------------------------------------------------------------
;; CLI
;; --------------------------------------------------------------------------

(def cli-opts
  [[nil "--workload WORKLOAD" "Workload: register, cas-register, idempotency (request_id 幂等专项, T1.4), map (delete/tombstone + 存在性, T1.1), txn (txn 全形态, T1.2), scan (range/revision 读, T1.3), mixture (map+txn+scan 混合, T1.5), watch (watch 事件流/续传, T2.1), lease (TTL/续期/Revoke, T2.2), lock (跨 agent 互斥, M5a), election (唯一 leader, M5a), idgen (全局唯一 ID, M5a), registry (注册/发现/幽灵实例, M5a), cache (agent 本地缓存: TTL/重启持久化, M5b/AG-09), mq (agent 本地队列: at-least-once, M5b/AG-11), soakfull (组合浸泡, T6.1), or multi-register (N independent region registers, --regions N). cache/mq 的数据在 agent 进程本地，所以要求恰好 1 个 agent（--agents 1）"
    :default  :register
    :parse-fn keyword
    :validate [#{:register :cas-register :idempotency :map :txn :scan :mixture :watch :lease
                 :lock :election :idgen :registry :cache :mq :soakfull :multi-register}
               "Must be one of register, cas-register, idempotency, map, txn, scan, mixture, watch, lease, lock, election, idgen, registry, cache, mq, soakfull, multi-register"]]
   [nil "--soak-mix MIX"
    "T6.1 (--workload soakfull): the surfaces to interleave and their relative
    weights, e.g. 'map=40,txn=20,scan=5,watch=15,lease=10'. A surface that is
    not implemented yet (lock/election/registry need the M5 agent plugin
    surface) makes the test **fail at build time** -- a soak must never look
    green while quietly skipping a surface"
    :default  nil
    :parse-fn parse-mix]
   [nil "--mixture-ratio RATIO"
    "T1.5 (--workload mixture): relative weights of map,txn,scan as three
    positive integers, e.g. 4,2,2 (default = map 50% / txn 25% / scan 25%)"
    :default  nil
    :parse-fn (fn [s]
                (mapv (fn [x] (Long/parseLong (str/trim x)))
                      (str/split (str s) #",")))
    :validate [#(and (= 3 (count %)) (every? pos? %) (<= (reduce + %) 32))
               "must be three positive integers whose sum is <= 32, e.g. 4,2,2"]]
   [nil "--watch-semantics S"
    "T2.1 (--workload watch): event-stream semantics the checker assumes.
    overflow-marker (default; per the source: drop-oldest + synthesized
    BufferOverflow) / lossless (no gap is ever acceptable) / coalescing
    (only structural checks)"
    :default  nil
    :parse-fn keyword
    :validate [#{:overflow-marker :lossless :coalescing}
               "Must be one of overflow-marker, lossless, coalescing"]]
   [nil "--watch-window-ms MS"
    "T2.1: how long one watch session stays open before closing (default 2000;
    the source's buffer-overflow protection is about slow consumers, so a
    longer window is what makes BUFFER_OVERFLOW reachable)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--watch-max-events N"
    "T2.1: cap on events collected by one session (default 10000)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--watch-resumes N"
    "T2.1: maximum stream re-opens inside one session, each resuming at
    last-observed-revision + 1 (contract: at-least-once delivery; default 2)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--watch-min-events N"
    "T2.1: s5.1 sample gate -- below this many observed events the watch
    checker is invalid (not green); default 200"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--lease-ttl-seconds N"
    "T2.2 (--workload lease): requested lease TTL in seconds (default 2). All
    assertions use the **granted** ttl from the response: the server may clamp
    the requested value (lease.proto says so explicitly)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--lease-grace-ms MS"
    "T2.2: grace beyond ttl within which an expired/revoked key must have
    disappeared (default 4000; s5.4-5 'lease grace = 2xttl' is the reference
    shape). Also the window for the revoke cascade-delete assertion -- the
    contract does not promise an exact expiry instant"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--lease-keepalive-ms MS"
    "T2.2: how long the keepalive scenario renews the lease (default 2xttl).
    Must exceed one ttl, otherwise 'the key is still present' is not
    attributable to renewal"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--lease-revoke-ttl-seconds N"
    "T2.2: TTL for the revoke scenario (default 30). Deliberately long so that
    a disappearing key cannot be explained by natural expiry"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--lease-min-grants N"
    "T2.2: s5.1 sample gate -- leases granted (default 0 = do not gate; the
    plan asks for >= 100 in acceptance runs). Below it the lease checker is
    invalid, not green"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--lease-min-expiries N"
    "T2.2: s5.1 sample gate -- expiry scenarios actually observed (default 0;
    the plan asks for >= 30). Below it the lease checker is invalid, not green"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--lease-tolerance-ms MS"
    "T2.2: slack on the safety side (default 500). Only 'the key vanished
    early' is relaxed by this -- better to under-report than to emit a false
    red from client/server timing skew"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--map-keys N"
    "T1.1 (--workload map): number of distinct keys (default 8)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %) (<= % 1024)) "must be an integer in 1..1024"]]
   [nil "--value-size BYTES"
    "T1.1/T1.3: written value length in bytes (default 16; F3 sweeps 4KB/64KB).
    Values stay globally unique, so a size below the unique prefix length is
    ignored (uniqueness beats length)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--map-min-deletes N"
    "T1.1: §5.1 sample gate -- the map checker is invalid (:reason
    :insufficient-sample) unless at least N delete completions were seen
    (default 0 = do not gate; the matrix passes 1/10th of the required count)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--nemesis NEMESIS"
    "Nemesis: none, kill, kill-all, pause, partition, partition-halves, partition-ring, all, soak; M5a (needs --agents N): kill-agent, kill-agent-all, pause-agent, partition-agent-server, agent-all"
    :default  :none
    :parse-fn keyword
    :validate [#{:none :kill :kill-all :pause :partition :partition-halves
                 :partition-ring :all :soak
                 :kill-agent :kill-agent-all :pause-agent :partition-agent-server :agent-all}
               "Must be one of none, kill, kill-all, pause, partition, partition-halves, partition-ring, all, soak, kill-agent, kill-agent-all, pause-agent, partition-agent-server, agent-all"]]
   [nil "--rate RATE"
    "Client ops/sec at a fixed global rate (soak default 0.5); otherwise exponential stagger"
    :default  nil
    :parse-fn (fn [s] (Double/parseDouble s))
    :validate [#(and (number? %) (pos? %)) "rate must be a positive number"]]
   [nil "--regions N"
    "Multi-raft mode: split the keyspace into N regions (each its own register
    key / raft group, replicated to every node). Default (absent): single-Raft
    legacy mode."
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %) (<= % 999))
               "regions must be an integer in 1..999"]]
   [nil "--checker CHECKER"
    "linear (knossos, default) or soak (O(n), for long 72h runs)"
    :default  nil
    :parse-fn keyword
    :validate [#{:linear :soak} "Must be one of linear, soak"]]
   [nil "--soak-quiet SECONDS"
    "Soak nemesis: quiet seconds between disruptions (default 1800)"
    :default  1800
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(pos? %) "must be a positive integer"]]
   [nil "--soak-disrupt SECONDS"
    "Soak nemesis: seconds each disruption lasts (default 600)"
    :default  600
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(pos? %) "must be a positive integer"]]
   [nil "--seed SEED"
    "Random seed for the nemesis jitter schedule (T0.5). Default: generated
    and printed; record it with the run so a failing history can be replayed
    via scripts/replay.clj."
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= (Math/abs (long %)) 2147483647))
               "seed must fit in a 32-bit signed integer"]]
   [nil "--no-jitter"
    "Disable nemesis jitter (T0.6/G3): fixed 5s beat (short runs) and exact
    soak quiet/disrupt windows, matching pre-T0.6 behaviour"
    ;; 布尔旗标**不能**写 `:parse-fn`（见 --via-agent 的注释：tools.cli 会把默认值
    ;; `false` 喂给 parse-fn，抛 Boolean cannot be cast to String）。
    :default  false
    :assoc-fn (fn [m _ _] (assoc m :jitter false))]
   [nil "--soak-max-rto-seconds SECONDS"
    "T0.2: override the per-nemesis RTO budget with a single global bound
    (see jepsen/docs/1.md s5.4-2)"
    :default  nil
    :parse-fn (fn [s] (Double/parseDouble s))
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]
   [nil "--quiet-availability-min RATIO"
    "T0.2: quiet-window write :ok ratio threshold (default 0.95, s5.4-3)"
    :default  nil
    :parse-fn (fn [s] (Double/parseDouble s))
    :validate [#(and (number? %) (<= 0.0 %) (<= % 1.0)) "must be in [0,1]"]]
   [nil "--quiet-min-sample N"
    "T0.2: minimum ops in a quiet window before it is judged (default 100,
    s5.4-3); smaller windows are recorded in the summary but not judged"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--min-op-ok-ratio RATIO"
    "G6 (F-13): minimum :ok ratio per client op type (default 0.1). A type with
    >= --min-op-sample completions below the ratio means that path never really
    ran (e.g. the client throws, jepsen records :info, knossos says valid)"
    :default  nil
    :parse-fn (fn [s] (Double/parseDouble s))
    :validate [#(and (number? %) (<= 0.0 %) (<= % 1.0)) "must be in [0,1]"]]
   [nil "--min-op-sample N"
    "G6: minimum completions of an op type before its :ok ratio is judged
    (default 10); smaller samples are recorded but not judged"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]

   [nil "--idem-replay-delay-ms MS"
    "T1.4 (--workload idempotency): sleep MS before each replay of the same
    request_id (default 0). Non-zero pushes the replay past a kill/restart so
    it lands on a node with no cached entry (F-03); 0 is the control group"
    :default  0
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 % 60000)) "must be 0..60000"]]

   [nil "--idem-replay-node-offset N"
    "T1.4: start each *replay* attempt N nodes after the cached leader
    (default 0 = same node). NOTE: this only moves the *first* node tried --
    a write can only be served by the leader, so a non-leader attempt rotates
    back to the same leader and hits the same cache. Measured: offset=1 gave 0
    violations in 344 replays. To actually exercise cross-node replay, change
    the leader between attempts, e.g. --nemesis kill --idem-replay-delay-ms 4000"
    :default  0
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 % 5)) "must be 0..5"]]

   [nil "--idem-min-replay-attempts N"
    "T1.4: sample gate (default 50, s5.1). Below it the idempotency checker is
    invalid (:reason :insufficient-sample) rather than green"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]

   ;; ---- M5a：agent 层（jepsen.coord.agent）--------------------------------
   [nil "--agents N"
    "M5a: number of coord-agent instances (default 0 = no agent, behaviour is
    byte-identical to before). Agents run the *same* coord binary
    (coord agent --agent-config ...) on the nodes **outside** the 3-node
    cluster, bind loopback only, and are reached through an SSH tunnel from
    the control node. N >= 2 is required for the mutual-exclusion surfaces:
    with a single agent there is only one local cache to look at"
    :default  0
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 % 5)) "must be 0..5"]]

   [nil "--via-agent"
    "M5a: point the client at the agent tunnels instead of the coord nodes.
    The workload/checker code is unchanged (an agent is a transport path), and
    every such run is gated on a **routing proof**: the agent's
    coord_agent_grpc_requests_total must have advanced (a mistyped endpoint
    would otherwise be a green run that never touched an agent)"
    ;; 布尔旗标**不能**写 `:parse-fn`：tools.cli 一看到 `:parse-fn` 就把该选项
    ;; 当成「吃一个值」的选项，于是裸写 `--via-agent` 会把**默认值 `false`**
    ;; 喂给 parse-fn，抛 `Boolean cannot be cast to String` —— 而报错发生在
    ;; CLI 解析阶段，看起来跟 agent 毫无关系。
    ;; 实测（M5a 第二轮）：`make matrix-m5-diff` 的 4 个 via-agent cell 全部
    ;; 死在这一行上，而 T5.2 的差分基线因此从未真正建立。
    ;; 正确写法：`(没有 :parse-fn)` + `:default false` ⇒ 出现即 true。
    :default  false]

   [nil "--agent-verifying-key HEX"
    "M5a: Ed25519 public key (hex, 64 chars) the agents use to verify the server's\n    CCTs. Defaults to the value derived from the lab's auth_root_key (see\n    db.clj's default-agent-verifying-key and scripts/derive-cct-pubkey.py).\n    Without a matching key the agent rejects every CCT-bearing request with\n    'Ed25519 CCT presented but no public key configured' — there is no\n    automatic way to fetch it from the server today"
    :default  nil]

   [nil "--agent-idgen-node-ids IDS"
    "AG-08: force the snowflake node ids of the agents, comma-separated
    (e.g. '7,7' to make two agents collide on purpose -- the uniqueness
    checker must then catch duplicate ids; the agent's own /_idgen/nodes CAS
    registration may roll the second one over, which is a legitimate
    resolution and is what the run documents)"
    :default  nil
    :parse-fn (fn [s] (mapv (fn [x] (Long/parseLong (str/trim x)))
                            (str/split (str s) #",")))]

   [nil "--lock-ttl-seconds N"
    "M5a (--workload lock): requested lock TTL in seconds (default 5)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--lock-hold-ms MS"
    "M5a: how long one contender holds the lock before releasing (default 200).
    Longer holds make overlap much easier to reach"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--lock-deadline-ms MS"
    "M5a: how long a contender keeps retrying Acquire before giving up
    (:fail :lock-held -- a legitimate business outcome, whitelisted in s5.1)
    (default 5000)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--lock-grace-ms MS"
    "M5a: window after (an attempted) release within which GetLockInfo must
    show the lock gone (default 4000)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--lock-min-acquires N"
    "M5a: s5.1 sample gate -- locks acquired (default 0; the plan asks for
    >= 200). Below it the lock checker is invalid, not green"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]

   [nil "--election-hold-ms MS"
    "M5a (--workload election): how long a winner stays leader before resigning
    (default 200)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--election-ttl-seconds N"
    "M5a: requested leader TTL in seconds (default 5)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--election-min-campaigns N"
    "M5a: s5.1 sample gate -- successful campaigns (default 0; the plan asks
    for >= 50 rounds). Below it the election checker is invalid, not green"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]

   [nil "--idgen-min-ids N"
    "M5a (--workload idgen): sample gate -- ids observed (default 0). Below it
    the idgen checker is invalid, not green"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--idgen-min-regressions N"
    "M5a: how many id regressions (decreasing ids within one client process)
    are tolerated (default 0 = assume the clock-rollback guard is implemented,
    which is what STATUS.md's remediation item asks for). Loosening this is a
    *policy* decision: record the written confirmation and link it in the
    MANIFEST -- never adjust it to make a red run green"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]

   [nil "--registry-ttl-seconds N"
    "M5a (--workload registry): registration TTL in seconds (default 5)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--registry-grace-ms MS"
    "M5a: grace beyond the ttl within which a deregistered instance must
    disappear from discovery (default 4000)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--registry-min-cycles N"
    "M5a: s5.1 sample gate -- register/expire cycles (default 0; the plan asks
    for >= 30). Below it the registry checker is invalid, not green"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]

   ;; ---- M5b：agent 本地数据面（cache / mq）--------------------------------
   [nil "--cache-ttl-seconds SECONDS"
    "M5b (--workload cache): TTL for **half** of the string Sets (default 0 =
    all Sets are durable, which is what the 'write visible' and
    'survives restart' judgements need). With T>0 the TTL judgements become
    live too: a value must not disappear before T and must not survive T+grace"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--cache-ttl-grace-ms MS"
    "M5b: grace after TTL expiry before a still-present value counts as a
    ghost (default 3000; covers the server-side cleanup beat)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %)) "must be a positive integer"]]
   [nil "--cache-min-sets N"
    "M5b: s5.1 sample gate -- confirmed Sets (default 0). Below it the cache
    checker is invalid, not green"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--cache-min-gets N"
    "M5b: s5.1 sample gate -- completed Gets (default 0)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--cache-min-list-ops N"
    "M5b: sample gate for the list surface (LPush/LRange/LLen completions,
    default 0)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--cache-min-set-ops N"
    "M5b: sample gate for the set surface (SAdd/SMembers completions,
    default 0)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--mq-topic NAME"
    "M5b (--workload mq): topic name (default jepsen-mq). The client creates
    it in setup! (CreateTopic is not an idempotent contract)"
    :default  nil]
   [nil "--mq-consumer-group NAME"
    "M5b: consumer group (default is run-scoped, so a previous run's committed
    offsets cannot be mistaken for this run's)"
    :default  nil]
   [nil "--mq-min-publishes N"
    "M5b: s5.1 sample gate -- confirmed publishes (default 0)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--mq-min-polls N"
    "M5b: s5.1 sample gate -- completed Polls (default 0)"
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (<= 0 %)) "must be >= 0"]]
   [nil "--mq-expect-idem-dedupe"
    "M5b: turn the idempotency_key dedupe observation into a hard judgement.
    The proto declares the field but the publish path ignores it (measured
    2026-09-19), so the default is **record only** until the contract wording
    is confirmed (dev.md s5.4). Boolean flag: no :parse-fn"
    :default  false]])

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd
              {:test-fn  coord-test
               :opt-spec cli-opts})
            args))
