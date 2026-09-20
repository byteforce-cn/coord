(ns jepsen.coord.mixck
  "T1.5 —— `mixture` workload 的组合 checker（M1 收口）。

  ## 为什么需要它

  T1.1/T1.2/T1.3 各自有专用 checker（`mapck` / `txnck` / `scanck`），但它们都在
  **单一面**上跑。`mixture` 把三个面混在同一条历史里跑，于是有两类新风险：

  1. **路由漏掉某个面** —— 组合 checker 必须证明「三个面都真的被检查了」。
     F-13 的教训是「一条历史里某个 op 类从来没真的跑过，而报告是绿的」；在
     组合场景下它换了个形态：某个面的子历史是空的 / 该面的 op 没有被路由到
     任何 checker（例如生成器忘了打 `:sub` 标记）—— 组合结果依然全绿。
     本文件用 `routing-checker` 把这条钉死：**每个面必须达到最小样本数，且
     不允许存在没被路由的客户端 op**。
  2. **面的边界被混淆** —— 三个面的 key 空间必须互不相交（map 的 `:write`
     不能进 scan 的写索引，反之亦然），否则一个面的判据会去判另一个面的
     观察。路由是按 op 上的 `:sub` 标记做的（生成器打的，客户端 completion
     原样保留），每个子 checker 只看到自己那一份历史。

  ## 组合方式

  用 `jepsen.checker/compose`：`{:valid? ...}` 取所有子 checker 的**最差**值
  （`false` > `:unknown` > `true`，见 jepsen `checker/valid-priorities`）。
  compose 会把**完整历史**传给每个子 checker，所以每个子 checker 外面套一层
  `restrict`（按 `:sub` 过滤）。

  判据本身完全复用三个面的专属 checker —— 本文件**不新造判据**，因此它的
  负控制 fixture 只需要证明「任一面的违反都能穿透组合」+「路由/样本门槛真的
  生效」两类（见 `scripts/mixture-fixtures/`）。"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.coord.electck :as electck]
            [jepsen.coord.idgenck :as idgenck]
            [jepsen.coord.leaseck :as leaseck]
            [jepsen.coord.lockck :as lockck]
            [jepsen.coord.mapck :as mapck]
            [jepsen.coord.regck :as regck]
            [jepsen.coord.scanck :as scanck]
            [jepsen.coord.txnck :as txnck]
            [jepsen.coord.watchck :as watchck]))

(def surfaces
  "`mixture` workload 的三个面（= 生成器打在 op 上的 `:sub` 取值）。

  取名 `surfaces` 而不是 `subs`：后者会遮蔽 `clojure.core/subs`（编译告警）。

  T6.1 的 `soakfull` 用 `:surfaces` 显式声明更多面（watch / lease），此时
  本值是默认值。"
  [:map :txn :scan])

(def known-surfaces
  "`surface-checker` 认识的面（缺一个就不可能在组合里用它 —— 显式 `:checkers`
  可绕过，但那样测试侧要自己提供 checker，不会静默）。

  M5a 把 agent 本地面（lock / election / idgen / registry）也纳进来：它们的
  契约判据本来就是**跨 agent 的**（互斥、唯一 leader、全局唯一 ID），在组合
  浸泡里与数据面共用同一条历史正合适。"
  #{:map :txn :scan :watch :lease :lock :election :idgen :registry})

(def default-min-sample
  "每个面的完成数下界（默认与 G6 的 `--min-op-sample` 一致）。

  低于它 ⇒ 该面视为**未执行**（`§5.1`：样本不足的 cell 既不计绿也不计红，
  必须补跑）。这里判 invalid（而不是 `:unknown`）是刻意的：一个「只跑了一个面」
  的 run 不能拿来当 M1 收口证据。"
  10)

;; ---------------------------------------------------------------------------
;; 按面过滤
;; ---------------------------------------------------------------------------

(defn- restrict
  "把 `ck` 限制在 `:sub` = `sub` 的子历史上。

  过滤的是**原始历史**（invoke 与 completion 都带 `:sub`，所以配对不会被打断）——
  这一步很重要：`mapck` 的 knossos 路径要求 invoke op 在场。"
  [ck sub]
  (reify checker/Checker
    (check [_ test history opts]
      (checker/check ck test (filterv #(= sub (:sub %)) history) opts))))

;; ---------------------------------------------------------------------------
;; 路由 / 样本门槛
;; ---------------------------------------------------------------------------

(defn- routing-checker
  "证明「三个面都真的被检查了」。

  只统计客户端 op（`:process` 是数字的 `:ok`/`:info`/`:fail` completion；
  nemesis 与 generator op 没有数字 `:process`），断言：

    * 每个 `surfaces` 的完成数 ≥ `min-sample`；
    * 不存在**没被路由**的客户端 op（`:sub` 不在 `surfaces` 里）——这是「生成器
      忘了打标记 / 打了错标记」的直接检测，也是 F-13 类假绿在组合层的入口。

  报告里额外给出**实测 op 份额** `:share`：`gen/mix` 是按 `mix` 的槽位均匀
  抽取的，三个面的子生成器槽位数不等，所以「目标份额」（`--mixture-ratio`）
  与实际份额的偏差必须可见 —— 否则多面混合会以看不见的方式倾斜。"
  [min-sample surfaces]
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [ops (filterv #(and (number? (:process %))
                               (contains? #{:ok :info :fail} (:type %)))
                         history)
            by-sub (frequencies (map :sub ops))
            counts (into {} (map (fn [s] [s (long (or (get by-sub s) 0))]) surfaces))
            total (max 1 (long (count ops)))
            share (into {} (map (fn [[s n]]
                                  [s (Double/parseDouble
                                       (format "%.3f" (double (/ n total))))])
                                counts))
            unrouted (filterv #(not (contains? (set surfaces) (:sub %))) ops)
            thin (filterv #(< (long (get counts %)) (long min-sample)) surfaces)
            valid? (and (empty? unrouted) (empty? thin))
            report {:valid? valid?
                    :by-sub counts
                    :share share
                    :min-sample (long min-sample)
                    :client-ops (count ops)
                    :unrouted (count unrouted)
                    :unrouted-sample (vec (take 5 (map #(select-keys % [:f :sub :process])
                                                       unrouted)))
                    :insufficient (vec thin)}]
        (info "mixture routing:" (pr-str report))
        report))))

;; ---------------------------------------------------------------------------
;; 入口
;; ---------------------------------------------------------------------------

(defn- surface-checker
  "按面构造它的专属 checker。**不新造判据**：每个面都直接用该面自己的 checker，
  组合层只负责路由与门槛。

  T6.1 的 soakfull 因此可以声明任意已实现的面集合（`--soak-mix`），而组合
  checker 仍然是「每面各自判 + 路由门槛」同一套语义。"
  [surface opts]
  (case surface
    :map   (mapck/checker {:mode (or (:map-mode opts) :index)
                           :min-deletes (or (:min-deletes opts) 0)})
    :txn   (txnck/checker)
    :scan  (scanck/checker)
    :watch (watchck/checker {:semantics  (:watch-semantics opts)
                             :min-events (:watch-min-events opts)})
    :lease (leaseck/checker {:min-grants   (:min-grants opts)
                             :min-expiries (:min-expiries opts)
                             :tolerance-ms (:tolerance-ms opts)})
    ;; M5a agent 本地面
    ;; lock：生成器必然混地面真值探针 + 弃锁 op ⇒ **两条门禁都要开**（缺了判
    ;; 未执行，见 F-34 / AG-06）。
    ;;
    ;; 注意 `:agent-nodes` 必须与 `:abandon?` 同时传：弃锁判据要把 op 归因到具体
    ;; agent 才能对上 nemesis 的时间窗；只开 `:abandon?` 会把每一条弃锁都记成
    ;; `:no-agent-attribution` ⇒ 整个 soak 判 invalid（宁可红，不可假绿）。
    :lock      (lockck/checker {:min-acquires (:lock-min-acquires opts)
                                :probe?       true
                                :abandon?     true
                                :agent-nodes  (:agent-nodes opts)
                                :grace-ms     (:lock-grace-ms opts)})
    :election  (electck/checker {:min-campaigns (:election-min-campaigns opts)
                                 ;; 探针同样必然混入（1/3 槽位）
                                 :probe?       true})
    :idgen     (idgenck/checker {:min-ids (:idgen-min-ids opts)
                                 :min-regressions (:idgen-min-regressions opts)})
    :registry  (regck/checker {:min-cycles (:registry-min-cycles opts)
                               :grace-ms   (:registry-grace-ms opts)})
    (throw (ex-info (str "mixck: unknown surface " surface)
                    {:surface surface
                     :known (vec (sort known-surfaces))}))))

(defn checker
  "T1.5 / T6.1 组合 checker。opts：

    :map-mode     map 面的模式：`:index`（默认，O(n log n)，长跑用）
                  | `:linear`（knossos 短矩阵）
    :min-deletes  §5.1 的 map delete 样本门槛（默认 0 = 不判）
    :min-sample   每个面的完成数下界（默认 10，见 `default-min-sample`）
    :surfaces     要组合的面（关键字向量）。默认 = `surfaces`（map/txn/scan）；
                  T6.1 的 soakfull 用 `--soak-mix` 的键当它（可含 :watch/:lease）。
                  未实现的面在**这里**抛异常（不静默丢弃）。
    :checkers     显式给出每个面的子 checker（覆盖 `:surfaces` 的自动构造），
                  `{:map <ck> :watch <ck> ...}`。

  两种入口的判据完全一致：每个面套一层 `restrict` 后各自判，路由/样本门槛
  负责证明「每个面都真的跑了」。"
  ([] (checker {}))
  ([{:keys [map-mode min-deletes min-sample checkers] :as opts}]
   (let [map-mode   (or map-mode :index)
         min-sample (long (or min-sample default-min-sample))
         min-deletes (long (or min-deletes 0))
         explicit?  (some? checkers)
         ;; 注意：**不要**把 `:surfaces` 解构成局部名 `surfaces` —— 那会遮蔽本
         ;; ns 的同名默认值，`(or surfaces surfaces)` 于是恒为 nil（实测报
         ;; 「no surfaces」，而且只在走默认三面时才出现）。F-24。
         surfaces*  (if explicit?
                      (vec (sort-by name (keys checkers)))
                      (vec (sort-by name (or (:surfaces opts) surfaces))))
         checkers*  (if explicit?
                      checkers
                      (into {} (map (fn [s] [s (surface-checker s opts)])
                                    surfaces*)))]
     (when-not (contains? #{:linear :index} map-mode)
       (throw (ex-info (str "mixture checker: unknown map-mode " map-mode)
                       {:map-mode map-mode})))
     (when (empty? surfaces*)
       (throw (ex-info "mixture checker: no surfaces" {})))
     (checker/compose
       (into {:routing (routing-checker min-sample surfaces*)}
             (map (fn [[sub ck]] [sub (restrict ck sub)]) checkers*))))))
