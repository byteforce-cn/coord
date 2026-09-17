;; Print the nemesis disruption timeline from a coord history.edn as *relative
;; seconds* (history `:time` is a monotonic NANOSECOND clock on the control
;; node — NOT epoch: a --time-limit 60 run spans ~1.2e11 ns), so it can be
;; correlated with per-region leader changes from coord-region-leaders.py.
;;
;; Usage (control, from coord-test root):
;;   LEIN_ROOT=true lein run -m clojure.main scripts/nemesis-timeline.clj
;;     [STORE_DIR]  (default: latest)
(require '[clojure.edn :as edn])
(require '[clojure.java.io :as io])

(defn- load-history [path]
  (with-open [r (java.io.PushbackReader. (io/reader path))]
    (loop [a []]
      (let [x (edn/read {:eof ::eof} r)]
        (if (= ::eof x) a (recur (conj a x)))))))

(defn- latest-store []
  (let [d (io/file "/root/coord-test/store/coord")]
    (->> (.listFiles d)
         (filter #(.isDirectory ^java.io.File %))
         (sort-by #(.getName ^java.io.File %))
         last)))

(let [store (if-let [p (first *command-line-args*)]
              (io/file p)
              (latest-store))
      h     (load-history (str (.getPath store) "/history.edn"))
      ;; history :time is monotonic nanoseconds; report seconds since the first op
      t0    (apply min (map :time h))]
  (println "store:" (.getPath store))
  (println "disruption timeline (relative s | type | node):")
  (doseq [op (filter #(= :info (:type %)) h)]
    (let [f (name (:f op))
          v (:value op)
          node (if (coll? v) (second v) v)]
      (when (or (re-find #"kill|pause|partition|stop" f)
                (re-find #"start" f))
        (println (format "%10.1f  %-14s %s"
                         (/ (double (- (long (:time op)) (long t0))) 1.0e9)
                         f
                         (pr-str node))))))
  ;; T0.6/F-11：抖动校验。只判「间隔落在 [3,8]s」是**不够的**：F-11 的
  ;; clojure.core/cycle 会先把向量求值一次再无限重复，节拍因此是常数
  ;; （实测 gap 恒为 6.42s / 6.64s），落在区间里却完全没有抖动。
  ;; 所以这里报「不同取值的个数」：周期 ≥ 4 时不同取值 < 3 基本就是抖动没生效。
  (let [invs (->> h
                  (filter #(and (= :nemesis (:process %)) (= :info (:type %))))
                  (map (comp long :time))
                  sort)
        gaps (->> (partition 2 1 invs)
                  (map (fn [[a b]] (/ (- b a) 1.0e9)))
                  ;; 两位小数量化后再取 distinct：避免浮点噪声把「常数」看成「抖动」
                  (map #(/ (Math/round (* 100.0 (double %))) 100.0)))
        n    (count gaps)
        dist (count (distinct gaps))]
    (println)
    (println "nemesis beat gaps (s):" (pr-str (vec (take 24 gaps))))
    (when (pos? n)
      (println (format "nemesis beat gaps: n=%d distinct=%d min=%.2f max=%.2f"
                       n dist (apply min gaps) (apply max gaps)))
      (when (and (>= n 6) (< dist 3))
        (println "  !!  抖动疑似未生效（F-11）：节拍只有" dist
                 "个不同取值。检查 nemesis 生成器是否用 clojure.core/cycle"
                 "预求值了 op 向量（应改为惰性 mapcat 构造）。"))))
  (System/exit 0))
