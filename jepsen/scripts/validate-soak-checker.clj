;; One-off validation helper for the soak checker.
;;
;; Usage:
;;   LEIN_ROOT=true lein run -m clojure.main /tmp/validate.clj <history.edn>
;;
;; Loads the given jepsen history.edn (line-delimited EDN) and runs the O(n)
;; soak checker over it, printing the result map.

(require '[jepsen.coord.soak :as soak])
(require '[jepsen.checker :as ck])
(require '[clojure.edn :as edn])

(defn- load-history [path]
  (with-open [r (java.io.PushbackReader.
                  (clojure.java.io/reader path))]
    (loop [a []]
      (let [x (edn/read {:eof ::eof} r)]
        (if (= ::eof x) a (recur (conj a x)))))))

(let [path (or (first *command-line-args*)
               (throw (ex-info "usage: validate.clj <history.edn>" {})))
      h    (load-history path)]
  (prn :ops (count h))
  (prn :checker-result
       (select-keys (ck/check (soak/checker) nil h {})
                    [:valid? :soak :failures]))
  (System/exit 0))

