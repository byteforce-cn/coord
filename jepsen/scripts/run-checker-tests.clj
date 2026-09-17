;; T0.3 —— 统一 checker fixture 运行器（负控制先行）
;;
;; 用法（控制机，coord-test 项目根目录）：
;;   LEIN_ROOT=true lein run -m clojure.main scripts/run-checker-tests.clj \
;;       <checker-ns> <fixture-dir> [<checker-opts-edn>]
;;
;; 例：
;;   ... scripts/run-checker-tests.clj jepsen.coord.soak  scripts/soak-checker-fixtures
;;   ... scripts/run-checker-tests.clj jepsen.coord.gates scripts/gates-fixtures \
;;       '{:quiet-min-sample 10 :nemesis :kill}'
;;
;; 约定：`<fixture-dir>/expect-valid-*.edn` 必须判 valid，`expect-invalid-*.edn`
;; 必须判 invalid；文件名不符合约定直接报错（避免"改了名字就静默失效"）。
;; `<checker-ns>` 必须暴露无参可构造的 `checker` 函数；第三个参数是传给该函数的
;; 选项 map（EDN），用于把 fixture 的门槛调到 fixture 的规模（生产默认值见
;; jepsen/docs/dev.md §5.4）。
;;
;; 退出码 0 = 全部 fixture 符合预期；1 = 有 fixture 不符（拒绝起跑，见 §0-1）。

(require '[jepsen.checker :as ck]
         '[clojure.edn :as edn]
         '[clojure.java.io :as io])

(defn- load-history
  "Reads a fixture into a vector of ops. Both shapes are accepted: a stream of
  whitespace-separated op maps (what jepsen writes as `history.edn`, and what
  the soak fixtures use) and a single vector literal (handier to write by
  hand)."
  [f]
  (with-open [r (java.io.PushbackReader. (io/reader f))]
    (let [forms (loop [a []]
                  (let [x (edn/read {:eof ::eof} r)]
                    (if (= ::eof x) a (recur (conj a x)))))]
      (if (and (= 1 (count forms)) (sequential? (first forms)))
        (vec (first forms))
        forms))))

(defn- expected-valid? [^String name]
  (cond
    (.startsWith name "expect-valid-")   true
    (.startsWith name "expect-invalid-") false
    :else (throw (ex-info
                   (str "fixture name must start with expect-valid- or "
                        "expect-invalid-: " name)
                   {}))))

(let [[checker-ns dir opts-edn] *command-line-args*
      _  (when-not (and checker-ns dir)
           (binding [*out* *err*]
             (println "usage: run-checker-tests.clj <checker-ns> <fixture-dir>"
                      "[<checker-opts-edn>]"))
           (System/exit 2))
      ns-sym (symbol checker-ns)
      _      (require ns-sym)
      ctor   (or (ns-resolve ns-sym 'checker)
                 (throw (ex-info (str checker-ns " has no checker fn") {})))
      arities (->> (-> ctor meta :arglists) (map count) set)
      opts   (if opts-edn (edn/read-string opts-edn) {})
      ;; Some checkers take no options (jepsen.coord.soak); refuse to silently
      ;; ignore an options map passed for a 0-arity-only checker, so a fixture
      ;; can't "pass" because its thresholds never reached the checker.
      _      (when (and (seq opts) (not (contains? arities 1)))
               (binding [*out* *err*]
                 (println "ERROR:" checker-ns "checker takes no options;"
                          "cannot apply" (pr-str opts)))
               (System/exit 2))
      ck-inst (if (contains? arities 1) (ctor opts) (ctor))
      files  (->> (.listFiles (io/file dir))
                  (filter #(.endsWith (.getName %) ".edn"))
                  (sort-by #(.getName %)))
      _      (when (empty? files)
               (binding [*out* *err*]
                 (println "no .edn fixtures in" dir))
               (System/exit 2))
      results
      (for [f files]
        (let [fname (.getName f)
              h     (load-history f)
              r     (ck/check ck-inst nil h {})
              exp?  (expected-valid? fname)
              ok?   (= (:valid? r) exp?)]
          (println (if ok? "PASS" "FAIL")
                   fname
                   "=> valid?" (:valid? r)
                   "(expected" exp? ")")
          (when-not ok?
            (prn (select-keys r [:soak :gates :failures])))
          ok?))]
  (println "----")
  (println (count (filter true? results)) "/" (count results) "fixtures passed"
           "[" checker-ns "]")
  (System/exit (if (every? true? results) 0 1)))
