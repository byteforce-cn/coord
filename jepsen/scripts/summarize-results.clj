;; T0.1 —— 把 results.edn 压成 MANIFEST 需要的几行 key: value。
;;
;; 用法：
;;   lein run -m clojure.main scripts/summarize-results.clj <results.edn|STORE_DIR>
;;
;; 输出（stdout，每行 `key: value`，缺失的 key 打印 `-`）：
;;   overall-valid / linear-valid / gates-valid / gates-failures
;;   rto-budget-seconds / rto-p95-seconds / rto-max-seconds
;;   rto-unrecovered / rto-not-measured
;;   quiet-windows / quiet-judged / quiet-skipped-small-sample
;;   quiet-worst-ratio / quiet-violations
;;   premise-valid / premise-duplicate-values
;;
;; 为什么单独一个脚本：MANIFEST 要能回答"这次 run 到底过没过门槛、RTO 多少"，
;; 而这些数字只在嵌套的 checker 结果里；用 grep 抓 EDN 太脆，所以走 reader。

(require '[clojure.edn :as edn]
         '[clojure.java.io :as io]
         '[clojure.string :as str])

(defn- results-file [arg]
  (let [f (io/file arg)]
    (cond
      (.isDirectory f) (io/file f "results.edn")
      :else f)))

(defn- get-in* [m path]
  (reduce (fn [m k] (when (map? m) (get m k))) m path))

(defn- read-results
  "读 results.edn。

  F-12：register / cas-register run 的 test map 里会内嵌 knossos 的
  **tagged literal**（`#knossos.model.Register{...}`），默认 reader 没有这个
  tag 的 reader function，会直接抛 `No reader function for tag ...`。后果很隐蔽：
  `collect-evidence.sh` 把 stderr 丢掉、把 stdout 写成 summary.txt，于是这些
  run 的 MANIFEST 里门槛结论永远是 \"summary unavailable\" —— 看起来像「没结论」，
  实际是「摘要器挂了」。这里给未知 tag 一个原样保留的默认 reader（T0.1 只需要
  读 checker 的结论，不关心 model 对象本身）。"
  [^String s]
  (edn/read-string {:default (fn [tag v] {:tag tag :value v})} s))

(let [arg  (first *command-line-args*)
      file (results-file (or arg "."))
      r    (when (.exists file) (read-results (slurp file)))
      g    (:gates r)                 ; composed checker's :gates entry
      gg   (:gates g)                 ; the gates checker's own report
      av   (:availability gg)
      rto  (:rto gg)
      prm  (:premise gg)
      show (fn [k v] (println (str k ": " (if (nil? v) "-" v))))]
  (when-not r
    (binding [*out* *err*] (println "no results.edn at" (.getPath file)))
    (System/exit 2))
  (show "results-file" (.getPath file))
  (show "overall-valid" (:valid? r))
  (show "linear-valid" (:valid? (:linear r)))
  (show "gates-valid" (:valid? g))
  (show "gates-failures" (when (seq (:failures g)) (pr-str (:failures g))))
  (show "rto-budget-seconds" (:budget-seconds rto))
  (show "rto-p95-seconds" (:p95 rto))
  (show "rto-max-seconds" (:max rto))
  (show "rto-samples" (when (seq (:samples rto)) (pr-str (:samples rto))))
  (show "rto-unrecovered" (:unrecovered rto))
  (show "rto-not-measured" (:not-measured rto))
  (show "quiet-windows" (:windows av))
  (show "quiet-judged" (:judged av))
  (show "quiet-skipped-small-sample" (:skipped-small-sample av))
  (show "quiet-worst-ratio" (get-in av [:worst-window :avail :ratio]))
  (show "quiet-violations" (count (:violations av)))
  (show "premise-valid" (:valid? prm))
  (show "premise-duplicate-values" (:duplicate-value-count prm))
  (show "soak-converged" (get-in r [:soak :final-converged]))
  (show "soak-regions" (when-let [ks (seq (keys (get-in r [:soak :regions])))]
                          (pr-str (vec ks))))
  (System/exit 0))
