;; TDD harness for the soak checker (P0 fixes from
;; coord-test-rectification-2026-08-30.md).
;;
;; Usage (on the control node, from the coord-test project root):
;;   LEIN_ROOT=true lein run -m clojure.main scripts/run-soak-checker-tests.clj \
;;       scripts/soak-checker-fixtures
;;
;; For every *.edn fixture it runs the soak checker and compares :valid?
;; against the expectation encoded in the file name:
;;   expect-valid-<name>.edn    -> must be :valid? true
;;   expect-invalid-<name>.edn  -> must be :valid? false
;;
;; Exit code 0 when all fixtures match; 1 otherwise.

(require '[jepsen.coord.soak :as soak])
(require '[jepsen.checker :as ck])
(require '[clojure.edn :as edn])
(require '[clojure.java.io :as io])

(defn- load-history [f]
  (with-open [r (java.io.PushbackReader. (io/reader f))]
    (loop [a []]
      (let [x (edn/read {:eof ::eof} r)]
        (if (= ::eof x) a (recur (conj a x)))))))

(defn- expected-valid? [^String name]
  (cond
    (.startsWith name "expect-valid-") true
    (.startsWith name "expect-invalid-") false
    :else (throw (ex-info
                   (str "fixture name must start with expect-valid- or "
                        "expect-invalid-: " name)
                   {}))))

(let [dir    (first *command-line-args*)
      files  (sort (filter #(.endsWith (.getName %) ".edn")
                           (.listFiles (io/file dir))))
      results
      (for [f files]
        (let [h    (load-history f)
              r    (ck/check (soak/checker) nil h {})
              exp? (expected-valid? (.getName f))
              ok?  (= (:valid? r) exp?)]
          (println (if ok? "PASS" "FAIL")
                   (.getName f)
                   "=> valid?" (:valid? r)
                   "(expected" exp? ")")
          (when-not ok?
            (prn (select-keys r [:soak :failures])))
          ok?))]
  (println "----")
  (println (count (filter true? results)) "/" (count results) "fixtures passed")
  (System/exit (if (every? true? results) 0 1)))
