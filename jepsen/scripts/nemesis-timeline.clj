;; Print the nemesis disruption timeline from a coord history.edn with epoch
;; seconds (ms -> s) so it can be correlated with per-region leader changes
;; from coord-region-leaders.py.
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
      h     (load-history (str (.getPath store) "/history.edn"))]
  (println "store:" (.getPath store))
  (println "disruption timeline (epoch s | type | node):")
  (doseq [op (filter #(= :info (:type %)) h)]
    (let [f (name (:f op))
          v (:value op)
          node (if (coll? v) (second v) v)]
      (when (or (re-find #"kill|pause|partition|stop" f))
        (println (format "%10.1f  %-14s %s"
                         (/ (double (:time op)) 1000.0)
                         f
                         (pr-str node))))))
  (System/exit 0))
