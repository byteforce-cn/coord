;; #312 —— 离线复判（offline re-judge）：拿一次 run 的历史，用**当前**的
;; checker / gates 重判，不重跑。
;;
;; 动机：72h 是**取证**成本，不是**验证**成本。过去改一个判据只能再跑一轮
;; 72h 才能知道它对不对，一轮三天；而历史里已经有十几万条 op，用当前判据
;; 重判只要十几秒。于是：
;;
;;   * 判据/门槛的改动 —— 先拿既有 store 复判（本脚本），秒级；
;;   * 只有 server 行为变化才需要新 run，且先用 `make fast-soak`（1h，同
;;     workload + 加密扰动）验证，72h 只留给最终取证；
;;   * 长跑期间 `soak-watchdog.sh` 也调本脚本（喂 `--op-log` 的增量历史），
;;     判据一红就停跑，而不是等 72h 后的收尾 checker。
;;
;; 用法（控制机，coord-test 项目根目录）：
;;   LEIN_ROOT=true lein -o run -m clojure.main scripts/rejudge.clj TARGET [选项]
;;
;;   TARGET  store 目录（有 test.jepsen / history.edn[.gz]）
;;           或一个 history 文件（history.edn / history.edn.gz / 增量 op 日志）
;;
;; 选项：
;;   --workload W        TARGET 不是 store 时必须给（kvv2 / txnv2 / …）
;;   --checker C         缺省 :soak（v2 工作负载只有这一个能判）
;;   --nemesis N         传给 gates 的 RTO 预算来源（缺省 soak）
;;   --min-op-sample N   单类 op 的最小样本（缺省沿用 store 里的值，否则 10）
;;   --min-op-ok-ratio R 单类 op 的 :ok 率下界（缺省沿用 store，否则 0.9）
;;   --min-writes/--min-reads/--min-ts   mvcc 样本门槛（缺省沿用 store）
;;   --history-max-ratio R  历史读失败率上限（缺省 0.01，见设计 §5.4）
;;
;; 输出：`key: value` 行（与 summarize-results.clj 同构），末行是
;;   `rejudge-verdict: valid | invalid | insufficient-sample`
;;   `rejudge-reason: :kw`（非 valid 时）
;; 退出码：0 = 通过；非 0 = 不通过或输入不可读。**具体原因看 verdict/reason 行**
;; —— `lein run` 会把任意非 0 退出码折叠成 1（"Suppressed exit"），所以调用方
;; （如 soak-watchdog.sh）必须读 verdict 行，不要只看退出码。
;;
;; 注意：这是**判据**复判，不代替取证（哈希三元组见 collect-evidence.sh）；
;; 它回答「按当前判据，这段历史过没过」，不回答「跑的二进制对不对」。

(require '[clojure.edn :as edn]
         '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[jepsen.checker :as ck]
         '[jepsen.store :as store]
         '[jepsen.coord]
         '[jepsen.coord.mvcc :as mvcc])

;; ---------------------------------------------------------------------------
;; 参数
;; ---------------------------------------------------------------------------

(defn- usage! []
  (binding [*out* *err*]
    (println (str "usage: rejudge.clj <store-dir|history.edn[.gz]>"
                  " [--workload W] [--checker C] [--nemesis N]"
                  " [--min-op-sample N] [--min-op-ok-ratio R]"
                  " [--min-writes N] [--min-reads N] [--min-ts N]"
                  " [--history-max-ratio R]")))
  (System/exit 3))

(defn- parse-args
  "把 CLI 参数解析成 `{:target path :opts {...}}`；未给的判定参数留在 opts
  缺失状态，由 store 的 test map 兜底。"
  [args]
  (loop [args args, m {}]
    (if-let [a (first args)]
      (case a
        "--workload"            (recur (drop 2 args) (assoc-in m [:opts :workload]
                                                               (keyword (second args))))
        "--checker"             (recur (drop 2 args) (assoc-in m [:opts :checker]
                                                               (keyword (second args))))
        "--nemesis"             (recur (drop 2 args) (assoc-in m [:opts :nemesis]
                                                               (keyword (second args))))
        "--min-op-sample"       (recur (drop 2 args) (assoc-in m [:opts :min-op-sample]
                                                               (Long/parseLong (second args))))
        "--min-op-ok-ratio"     (recur (drop 2 args) (assoc-in m [:opts :min-op-ok-ratio]
                                                               (Double/parseDouble (second args))))
        "--min-writes"          (recur (drop 2 args) (assoc-in m [:opts :v2-min-writes]
                                                               (Long/parseLong (second args))))
        "--min-reads"           (recur (drop 2 args) (assoc-in m [:opts :v2-min-reads]
                                                               (Long/parseLong (second args))))
        "--min-ts"              (recur (drop 2 args) (assoc-in m [:opts :v2-min-ts]
                                                               (Long/parseLong (second args))))
        "--history-max-ratio"   (recur (drop 2 args) (assoc-in m [:opts :max-history-unavailable-ratio]
                                                               (Double/parseDouble (second args))))
        (if (str/starts-with? a "--")
          (usage!)
          (recur (rest args) (assoc m :target a))))
      m)))

;; ---------------------------------------------------------------------------
;; 读取
;; ---------------------------------------------------------------------------

(defn- read-stream
  "EDN op 流（`history.edn` 与增量 op 日志都是这个形状）。

  也接受单个 vector 字面量（fixture 手写历史就是这种形状）：两种形状都认，
  免得「fixture 能跑、复判读不到」——那会把历史读率的 0/0 当成 0%。"
  [r]
  (let [forms (loop [acc (transient [])]
                (let [x (edn/read {:eof ::eof} r)]
                  (if (= ::eof x) (persistent! acc) (recur (conj! acc x)))))]
    (if (and (= 1 (count forms)) (sequential? (first forms)))
      (vec (first forms))
      forms)))

(defn- history-file
  "store 目录里优先用 history.edn（完整历史）；>5MiB 归档策略会把 store 里的
  原始文件留着，所以两种都认。"
  [dir]
  (->> ["history.edn" "history.edn.gz"]
       (map #(io/file dir %))
       (filter #(.exists ^java.io.File %))
       first))

(defn- load-history-file [f]
  (if (str/ends-with? (.getName ^java.io.File f) ".gz")
    (with-open [in (java.util.zip.GZIPInputStream. (io/input-stream f))
                r  (java.io.PushbackReader. (io/reader in))]
      (read-stream r))
    (with-open [r (java.io.PushbackReader. (io/reader f))]
      (read-stream r))))

(defn- load-input
  "返回 `{:test test-map-or-nil :history [op …] :target path}`。"
  [target]
  (let [f (io/file target)]
    (cond
      (.isDirectory f)
      (let [test (let [tf (io/file f "test.jepsen")]
                   (when (.exists tf) (store/load-jepsen-file tf)))
            hist (or (when (seq (:history test)) (:history test))
                     (when-let [hf (history-file f)] (load-history-file hf)))]
        (when-not hist
          (binding [*out* *err*]
            (println "no history.edn / test.jepsen history under" target))
          (System/exit 2))
        {:test test :history (vec hist) :target target})

      (.isFile f)
      {:test nil :history (load-history-file f) :target target}

      :else
      (do (binding [*out* *err*] (println "not a store dir or history file:" target))
          (System/exit 2)))))

;; ---------------------------------------------------------------------------
;; 统计（报告面，不是判据）
;; ---------------------------------------------------------------------------

(defn- op-rows
  "按 `:f` 聚合（只算客户端 op：`:process` 是数字，nemesis 事件不参与）。"
  [history]
  (let [client (filter #(and (number? (:process %))
                             (contains? #{:invoke :ok :info :fail} (:type %)))
                       history)]
    (->> client
         (group-by :f)
         (map (fn [[f ops]]
                {:op f
                 :invoke (count (filter #(= :invoke (:type %)) ops))
                 :ok     (count (filter #(= :ok (:type %)) ops))
                 :info   (count (filter #(= :info (:type %)) ops))
                 :fail   (count (filter #(= :fail (:type %)) ops))}))
         (sort-by :invoke >))))

;; ---------------------------------------------------------------------------
;; 判定
;; ---------------------------------------------------------------------------

(def ^:private exit-codes {:valid 0})

(defn- judge
  "把 composed checker 的结果 + 历史读率折成 `{:verdict :reasons}`。

  `:insufficient-sample` 是「还没跑到门槛」，不是违规（watchdog 在长跑前段
  必须能区分这两件事，否则每次起跑都会假红）；但它**不**掩盖同时出现的真违规。
  多个 gate 同时红时全部列出（不要修一个就以为绿了）。"
  [{:keys [linear gates history-rate]}]
  (let [lin-reason (:reason linear)
        insufficient? (= :insufficient-sample lin-reason)
        hist-fail? (and (:judged history-rate)
                        (> (double (:ratio history-rate))
                           (double (:cap history-rate))))
        violations (cond-> []
                     (and (not (:valid? linear)) (not insufficient?))
                     (conj (or lin-reason :mvcc-violation))

                     (not (:valid? gates))
                     (into (map :type (:failures gates)))

                     hist-fail?
                     (conj :history-unavailable-rate))]
    (cond
      (seq violations) {:verdict :invalid :reasons (vec (distinct violations))}
      insufficient?    {:verdict :insufficient-sample :reasons [:mvcc-insufficient-sample]}
      :else            {:verdict :valid :reasons []})))

(defn- pct [x] (when (some? x) (format "%.2f%%" (* 100.0 (double x)))))

(let [{:keys [target opts]} (parse-args *command-line-args*)]
  (when-not target (usage!))
  (let [{:keys [test history]} (load-input target)
        test (or test {})
        opts (merge {:checker :soak :nemesis :soak}
                    ;; store 里记着这次 run 自己的门槛与 workload；CLI 覆盖它。
                    (select-keys test [:workload :checker :nemesis :min-op-sample
                                       :min-op-ok-ratio :quiet-min-sample
                                       :quiet-availability-min :soak-max-rto-seconds
                                       :v2-min-writes :v2-min-reads :v2-min-ts])
                    opts)
        _    (when-not (:workload opts)
               (binding [*out* *err*] (println "--workload is required for a bare history file"))
               (System/exit 3))
        rows (op-rows history)
        rate (mvcc/history-unavailable-stats history
                                             (or (:max-history-unavailable-ratio opts) 0.01))
        ;; `:perf` 的花图需要 `jepsen.history.IHistory`（真 run 里由 jepsen 传
        ;; 入）；离线复判拿的是 vector，跑它只会回 `:unknown` 并刷一屏 WARN。
        ;; 图不是判据，去掉它，其余判据原样复用生产组合。
        chk  (let [c ((ns-resolve 'jepsen.coord 'checker) opts)]
               (if (map? (:checkers c))
                 (assoc c :checkers (dissoc (:checkers c) :perf))
                 c))
        res  (ck/check chk test history test)
        linear (:linear res)
        gates  (:gates res)
        verdict (judge {:linear linear :gates gates :history-rate rate})
        show (fn [k v] (println (str k ": " (if (nil? v) "-" v))))]

    (show "rejudge-target" target)
    (show "rejudge-workload" (:workload opts))
    (show "rejudge-ops" (count history))
    (doseq [{:keys [op invoke ok info fail]} rows]
      (show (str "rejudge-op-" (name op))
            (str "invoke=" invoke " ok=" ok " info=" info " fail=" fail
                 " ok-ratio=" (pct (when (pos? invoke) (/ ok invoke))))))
    (show "rejudge-history"
          (str "historical-reads=" (:historical-reads rate)
               " unavailable=" (:unavailable rate)
               " ratio=" (pct (:ratio rate))
               " cap=" (pct (:cap rate))
               " judged=" (:judged rate)))
    (show "rejudge-linear"
          (str "valid=" (boolean (:valid? linear))
               " errors=" (or (get-in linear [:mvcc :errors-count]) 0)
               " sample=" (pr-str (get-in linear [:mvcc :sample]))
               " judged=" (pr-str (get-in linear [:mvcc :judged]))))
    (show "rejudge-gates"
          (str "valid=" (boolean (:valid? gates))
               " premise=" (if (get-in gates [:gates :premise :disabled?])
                             "disabled"
                             (str "writes=" (get-in gates [:gates :premise :writes])))
               " availability-judged=" (get-in gates [:gates :availability :judged])
               "/" (get-in gates [:gates :availability :windows])
               " rto-measured=" (get-in gates [:gates :rto :n])
               " rto-not-measured=" (get-in gates [:gates :rto :not-measured])
               " liveness-judged=" (get-in gates [:gates :liveness :judged])
               " liveness-violations=" (count (get-in gates [:gates :liveness :violations]))
               " violations=" (count (:failures gates))))

    ;; 明确报出「判据没判」，不让空转看起来像通过（#308 的教训）。
    (let [writes (reduce + (map #(+ (:ok %) (:info %) (:fail %))
                                (filter #(contains? #{:v2-write :v2-delete :v2-txn :write}
                                                    (:op %))
                                        rows)))
          avail-judged (long (or (get-in gates [:gates :availability :judged]) 0))
          rto-n (long (or (get-in gates [:gates :rto :n]) 0))
          premise-writes (long (or (get-in gates [:gates :premise :writes]) 0))
          premise-disabled? (boolean (get-in gates [:gates :premise :disabled?]))]
      (when (and (pos? writes) (zero? avail-judged))
        (show "rejudge-warning" "gates-availability-not-judged (writes>0, judged=0)"))
      (when (and (pos? writes) (zero? rto-n))
        (show "rejudge-warning" "gates-rto-not-measured (write invokes exist, n=0)"))
      (when (and (pos? writes) (zero? premise-writes) (not premise-disabled?))
        (show "rejudge-warning" "gates-premise-not-judged (writes>0, premise writes=0)")))

    (show "rejudge-verdict" (name (:verdict verdict)))
    (when (seq (:reasons verdict))
      (show "rejudge-reason" (str/join " " (map str (:reasons verdict)))))
    (System/exit (get exit-codes (:verdict verdict) 1))))