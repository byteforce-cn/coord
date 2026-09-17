;; T0.5 —— 随机种子与回放（§0-4 可复现）
;;
;; 用法（控制机，coord-test 项目根目录）：
;;   LEIN_ROOT=true lein run -m clojure.main scripts/replay.clj [STORE_DIR] [选项]
;;
;; 选项（不传 STORE_DIR 时必须给 --seed）：
;;   --seed N          run 的种子（记在 results/test 里，缺省从 STORE_DIR 的
;;                     jepsen.log 里抓，抓不到则报错——绝不猜）
;;   --workload W      register | cas-register | multi-register
;;   --nemesis N       soak / all / kill / …（决定按哪个节拍重建）
;;   --quiet S         与 run 时的 --soak-quiet 一致
;;   --disrupt S       与 run 时的 --soak-disrupt 一致
;;   --no-jitter       与 run 时的 --no-jitter 一致
;;   --steps N         重建多少个扰动窗口（默认 6）
;;
;; 做三件事：
;;   1. 由种子**重新推导** jittered nemesis 排期（coord/nemesis-schedule），
;;      并自校验「同一种子两次推导结果相同」（§0-4）；
;;   2. 若给了 STORE_DIR，读出 history.edn 里**实际发生**的 nemesis 排期
;;      （:f 序列 + 每个扰动窗口的实际秒数），与推导排期并排打印；
;;   3. 明确说出推导与实际无法逐项对齐的原因（扰动次数由 time-limit 截断，
;;      不受种子控制），避免制造"对不上就是 bug"的误解。
;;
;; 退出码：0 = 自校验通过；1 = 自校验失败（种子/抖动实现被改坏）。

(require '[clojure.edn :as edn]
         '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[jepsen.coord :as coord])

(defn- load-history [f]
  (with-open [r (java.io.PushbackReader. (io/reader f))]
    (loop [a []]
      (let [x (edn/read {:eof ::eof} r)]
        (if (= ::eof x) a (recur (conj a x)))))))

(defn- parse-opts [args]
  (loop [args args, m {}]
    (if-let [a (first args)]
      (case a
        "--seed"     (recur (drop 2 args) (assoc m :seed
                                                (Long/parseLong (second args))))
        "--workload" (recur (drop 2 args) (assoc m :workload
                                                (keyword (second args))))
        "--nemesis"  (recur (drop 2 args) (assoc m :nemesis
                                                (keyword (second args))))
        "--quiet"    (recur (drop 2 args) (assoc m :soak-quiet
                                                (Long/parseLong (second args))))
        "--disrupt"  (recur (drop 2 args) (assoc m :soak-disrupt
                                                (Long/parseLong (second args))))
        "--steps"    (recur (drop 2 args) (assoc m :steps
                                                (Long/parseLong (second args))))
        "--no-jitter" (recur (rest args) (assoc m :jitter false))
        (recur (rest args) (update m :store
                                  #(or % (when-not (str/starts-with? a "--") a)))))
      m)))

(defn- seed-from-log
  "从 jepsen.log 里抓 `:seed N`（test map 会被 jepsen 打印到日志里）。抓不到
  返回 nil——调用方必须显式给 --seed，不猜。"
  [store]
  (let [f (io/file store "jepsen.log")]
    (when (.exists f)
      (with-open [r (io/reader f)]
        (some (fn [line]
                (when-let [[_ n] (re-find #":seed (\d+)" line)]
                  (Long/parseLong n)))
              (line-seq r))))))

(defn- observed-schedule
  "history.edn 里实际发生的 nemesis 窗口：`[{:kind k :start t :end t2
  :seconds s} ...]`（纳秒 → 秒）。"
  [history]
  (let [events (->> history
                    (filter #(= :nemesis (:process %)))
                    (keep #(when-let [f (:f %)]
                             (when (contains? #{:info :ok} (:type %))
                               [(long (:time %)) f])))
                    (sort-by first))
        starts #{:start :kill :pause :partition}]
    (loop [evs events, open nil, acc []]
      (if-let [[t f] (first evs)]
        (if (contains? starts f)
          (recur (rest evs) {:kind f :start t} acc)
          (recur (rest evs) nil
                 (cond-> acc
                   open (conj (assoc open
                                     :end t
                                     :seconds (/ (- (double t)
                                                    (double (:start open)))
                                                 1.0e9))))))
        acc))))

(let [opts     (parse-opts *command-line-args*)
      store    (:store opts)
      seed     (or (:seed opts) (when store (seed-from-log store)))
      opts     (assoc opts :seed seed)
      _        (when-not seed
                 (binding [*out* *err*]
                   (println "no seed: pass --seed N or give a STORE_DIR whose"
                            "jepsen.log contains :seed"))
                 (System/exit 2))
      sched1   (coord/nemesis-schedule opts)
      sched2   (coord/nemesis-schedule opts)
      stable?  (= sched1 sched2)
      history  (when store
                 (let [f (io/file store "history.edn")]
                   (when (.exists f) (load-history f))))
      observed (when history (observed-schedule history))]

  (println "== replay ==")
  (println "store:      " (or store "(none)"))
  (println "seed:       " seed)
  (println "workload:   " (or (:workload opts) "(unknown)"))
  (println "nemesis:    " (or (:nemesis opts) "(unknown)"))
  (println "jitter:     " (if (false? (:jitter opts)) "off (fixed)" "on (±20%)"))
  (println)
  (println "-- derived schedule (from the seed) --")
  (doseq [[i s] (map-indexed vector sched1)]
    (println (format "  #%-2d %-9s quiet %8.1fs  disrupt %8.1fs"
                     i (name (:kind s))
                     (double (:quiet-seconds s))
                     (double (:disrupt-seconds s)))))
  (println)
  (println "-- observed schedule (from history.edn) --")
  (if (seq observed)
    (do
      (doseq [[i w] (map-indexed vector observed)]
        (println (format "  #%-2d %-9s %s"
                         i (name (:kind w))
                         (if (:seconds w)
                           (format "%.1fs" (double (:seconds w)))
                           "still open at end of history"))))
      (println)
      (println (str "NOTE: the number of disruption windows is bounded by"
                    " --time-limit, not by the seed, so the derived and"
                    " observed lists are NOT expected to have equal length."
                    " The seed pins the *durations/offsets*; a run whose"
                    " derived schedule differs from a previous run at the same"
                    " seed is the bug this script is here to catch.")))
    (when store
      (println "  (no history.edn in" store "— derived schedule only)")))
  (println)
  (println "determinism self-check (same seed, two derivations):"
           (if stable? "OK" "FAIL"))
  (System/exit (if stable? 0 1)))
