;; Clojure 源语法预检（fast-fail 阶梯的 L0 预检，issue #322）。
;;
;; 为什么需要它：CI 跑不了 jepsen 的 `lein test`（jepsen 0.3.14-SNAPSHOT 不在 Maven
;; Central），于是 `jepsen/src/**`、`jepsen/scripts/**` 的**语法错误**在 CI 里完全
;; 不可见：测试全绿，`make soak-kv-t` 起来才炸（`(<= 1_000 %)` —— Clojure 没有
;; 数字分隔符字面量，reader 直接拒绝）。语法检查只需要 reader，不需要依赖。
;;
;; 只做 `read`：不 require、不解析命名空间、不编译。因此它能在**没有** jepsen 依赖
;; 的环境里跑（CI 用 Clojure CLI；控制机用 `lein -o run -m clojure.main`）。
;;
;; 用法：
;;   clojure -M jepsen/scripts/check-clj-syntax.clj             # 从仓库根目录
;;   cd jepsen && clojure -M scripts/check-clj-syntax.clj       # 从 jepsen 目录
;;   ... --selftest     # 负控制：注入 `1_000`，必须报错；正控制：合法文件必须通过
;;   ... [dir-or-file ...]  # 显式指定扫描面（缺省 = 脚本所在 jepsen 树的 src + scripts）
;;
;; 退出码：0 = 全部文件可 read（或 --selftest 的两条控制都符合预期）；
;;         1 = 有 reader 错误 / 扫描面为空 / 缺目录 / 自检不符合预期。

(require '[clojure.java.io :as io]
         '[clojure.string :as str])

(defn- read-all-forms
  "把一个文件读到 EOF，返回读到的 form 数。只依赖 reader。"
  [^java.io.File f]
  (with-open [rdr (java.io.PushbackReader. (io/reader f))]
    ;; `*read-eval*` 关掉：我们不求值，也不允许 `#=` 求值读入器。
    ;; `*default-data-reader-fn*` 兜底未知 tag：未知 tag 是**数据**问题，
    ;; 不该被当成语法错误（更不该让 `#js` / `#inst` 之外的 tag 漏报）。
    (binding [*read-eval* false
              *default-data-reader-fn* (fn [_tag value] value)]
      (let [eof (Object.)]
        (loop [forms 0]
          (let [form (read {:eof eof :read-cond :allow :features #{:clj}} rdr)]
            (if (identical? form eof)
              forms
              (recur (inc forms)))))))))

(defn- check-file
  "读一个文件；返回 nil = 通过，否则返回可读的错误行。"
  [^java.io.File f]
  (try
    (read-all-forms f)
    nil
    (catch Throwable e
      (str (.getPath f) ": " (.getName (class e)) ": " (.getMessage e)))))

(defn- clj-files
  [root]
  (let [^java.io.File dir (io/file root)]
    (if (.isDirectory dir)
      (->> (file-seq dir)
           (filter #(.isFile ^java.io.File %))
           (filter #(str/ends-with? (.getName ^java.io.File %) ".clj"))
           (sort-by #(.getPath ^java.io.File %)))
      [dir])))

(defn- script-jepsen-root
  "脚本所在 jepsen 树的根（`<jepsen>/scripts/check-clj-syntax.clj`）。
  脚本是通过相对路径加载的，因此相对 *file* 解析即可，不依赖 cwd。"
  []
  (let [script (io/file (or *file* "scripts/check-clj-syntax.clj"))]
    (or (some-> script .getParentFile .getParentFile) (io/file "."))))

(defn- run-check
  "读一组路径；返回 {:checked N :failures [行 ...]}。"
  [paths]
  (let [files (mapcat clj-files paths)
        failures (keep check-file files)]
    {:checked (count files) :failures (vec failures)}))

(defn- selftest!
  "两条控制：坏文件必须被 reader 拒绝，好文件必须通过。
  没有这两条，一个永远报错（或永远通过）的检查器也能让 CI 变绿。"
  []
  (let [tmp (io/file (System/getProperty "java.io.tmpdir")
                     (str "coord-clj-syntax-selftest-" (System/nanoTime)))
        bad (io/file tmp "bad.clj")
        good (io/file tmp "good.clj")]
    (.mkdirs tmp)
    (try
      (spit bad "(def n 1_000)\n")
      (spit good "(def n 1000)\n(defn f [x] {:x x})\n")
      (let [bad-failure (check-file bad)
            good-failure (check-file good)]
        (cond
          (nil? bad-failure)
          (do (println "SELFTEST FAIL: injected `1_000` was accepted (reader check is a no-op)")
              (System/exit 1))

          (some? good-failure)
          (do (println "SELFTEST FAIL: valid file rejected:" good-failure)
              (System/exit 1))

          :else
          (do (println "SELFTEST ok: injected `1_000` rejected, valid file accepted")
              (System/exit 0))))
      (finally
        (doseq [f [bad good]] (io/delete-file f true))
        (io/delete-file tmp true)))))

(let [args *command-line-args*
      selftest? (some #{"--selftest"} args)
      explicit (remove #(str/starts-with? % "--") args)]
  (when selftest?
    (selftest!))

  ;; 缺省扫描面：脚本所在 jepsen 树的 src + scripts（与 CI/控制机的两种 cwd 都成立）。
  (let [root (script-jepsen-root)
        roots (if (seq explicit)
                explicit
                [(str (io/file root "src")) (str (io/file root "scripts"))])
        missing (remove #(.exists (io/file ^String %)) roots)
        {:keys [checked failures]} (run-check roots)]
    (doseq [m missing]
      (println "FAIL missing scan root:" m))
    (doseq [f failures]
      (println "FAIL" f))
    (cond
      (seq missing)
      (do (println "clj-syntax: FAILED (missing scan roots)")
          (System/exit 1))

      ;; 空扫描面 = 静默绿（路径写错/文件被移走都会变成"通过"）。
      (zero? checked)
      (do (println "clj-syntax: FAILED (no .clj files under" (pr-str (vec roots)) ")")
          (System/exit 1))

      (seq failures)
      (do (println "clj-syntax: FAILED (" (count failures) "of" checked "files)")
          (System/exit 1))

      :else
      (do (println "clj-syntax: OK (" checked "files, 0 reader errors)")
          (System/exit 0)))))
