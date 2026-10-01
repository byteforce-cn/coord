;; M5a —— lock 历史的分诊脚本（F-34 的证据工具）
;;
;; 用法（控制机内，coord-test 项目根目录）：
;;   LEIN_ROOT=true lein run -m clojure.main scripts/lock-diag.clj <history.edn>
;;
;; 也支持宿主机的 `clojure -M scripts/lock-diag.clj <history.edn>`（只用
;; clojure.edn，不依赖 jepsen 的 classpath）。
;;
;; 为什么需要它：checker 只回答「valid? / 违规计数」，而 F-34 那一类问题的
;; 分诊需要**同一份历史在三种区间口径下的对照**：
;;
;;   :legacy    区间闭合看 `:gone?`，结束取 `:released-at-ms`
;;              —— 这是 F-34 当时的口径（`released-at-ms` 记在观测循环之后，
;;              且「空了」= exists=false）
;;   :new       结束取 `:gone-at-ms`（客户端能举证的**最早**「锁已不在我名下」
;;              时刻）—— 这是修好之后 checker 用的口径
;;   :true-end  结束取「最后一次 renew + 一次 sleep」—— 一个**独立于客户端
;;              自述字段**的估计（持有者自己决定了 hold 期，正点停手）
;;
;; `:legacy` 非 0 而 `:new` / `:true-end` 为 0 ⇒ 那些「重叠」是测试自身的区间
;; 膨胀，不是系统的互斥缺陷。三条都非 0 才是真问题。探针那一段再做服务端交叉
;; 验证（`:f :lock-probe` 绕开 agent 读 `/_lock/{name}`）。
(require '[clojure.edn :as edn])

(def path (or (first *command-line-args*)
              (do (binding [*out* *err*] (println "usage: lock-diag.clj <history.edn>"))
                  (System/exit 2))))

(def hist
  (with-open [r (java.io.PushbackReader. (clojure.java.io/reader path))]
    (doall (take-while some? (repeatedly #(edn/read {:eof nil} r))))))

;; ── 复刻 windex/pair-invokes（按 [process f] 配对，:invoke = invoke op 的 :time） ──
(def pending (atom {}))
(def ops
  (reduce (fn [acc op]
            (let [k [(:process op) (:f op)]]
              (case (:type op)
                :invoke (do (swap! pending update k (fnil conj []) op) acc)
                (:ok :info :fail)
                (let [inv (peek (get @pending k))]
                  (when inv (swap! pending update k pop))
                  (conj acc (assoc op :invoke (long (or (:time inv) (:time op))))))
                acc)))
          [] hist))

(defn- abs-ms [op rel]
  ;; 锚点与 lockck/abs-ms 一致：优先用 op 自己的 :t0-ns（同一 JVM 的 nanoTime）。
  (+ (quot (long (or (:t0-ns op) (:invoke op) (:time op) 0)) 1000000) (long rel)))

(def lock-ops (filterv #(= :lock-contend (:f %)) ops))
(def done (filterv #(= :ok (:type %)) lock-ops))
(def acquired (filterv :acquired-at-ms done))

(defn interval
  [mode op]
  (when-let [rel-start (:acquired-at-ms op)]
    (let [start   (abs-ms op rel-start)
          ;; F-34 当时的闭合判据：GetLockInfo 的 exists=false
          f34-closed? (boolean (false? (:exists (:lock-info-after-release op))))
          closed? (boolean (:gone? op))
          fail    (+ start (* 1000 (long (or (:holder-ttl-seconds op) 0)))
                     (long (or (:grace-ms op) 4000)))
          end     (case mode
                    ;; 修前口径 = released-at + exists=false 双重根因
                    :f34      (if (and f34-closed? (:at-ms op))
                                (abs-ms op (:at-ms op)) fail)
                    :legacy   (if (and closed? (:released-at-ms op))
                                (abs-ms op (:released-at-ms op)) fail)
                    :new      (cond
                                (and closed? (:gone-at-ms op))
                                (abs-ms op (:gone-at-ms op))
                                (and closed? (:released-at-ms op))
                                (abs-ms op (:released-at-ms op))
                                :else fail)
                    :true-end (if-let [r (:at-ms (last (:renews op)))]
                                (+ (abs-ms op (long r)) 100) fail))]
      {:name (:name op) :holder (:holder-id op) :pid (:process op)
       :start start :end end :closed? closed?
       :hold (long (or (get-in op [:value :hold-ms]) 0))
       :released-at (abs-ms op (long (or (:released-at-ms op) 0)))
       :gone-at (when (:gone-at-ms op) (abs-ms op (long (:gone-at-ms op))))})))

(defn intervals [mode] (vec (keep #(interval mode %) lock-ops)))

(defn- ov [[a b]]
  (max 0 (- (min (:end a) (:end b)) (max (:start a) (:start b)))))

(defn overlap-pairs [ivs]
  (vec (for [[_ vs] (group-by :name ivs)
             a vs b vs
             :when (and (neg? (compare (:holder a) (:holder b)))
                        (pos? (ov [a b])))]
         [a b])))

(def infl (sort (map (fn [iv] (- (:released-at iv) (+ (:start iv) (long (:hold iv)))))
                     (intervals :legacy))))
(defn- pct [xs p] (when (seq xs) (nth xs (min (dec (count xs)) (quot (* p (count xs)) 100)))))

(def probes (filterv #(= :lock-probe (:f %)) ops))
(def probe-ok (filterv #(= :ok (:type %)) probes))

(defn contradictions [ivs margin]
  (let [by-name (group-by :name ivs)]
    (vec (for [p probe-ok
               :let [h (get-in p [:server :holder-id])
                     t (abs-ms p (:server-at-ms p))]
               :when (and (:exists? p) (seq h))
               iv (get by-name (:name p))
               :when (not= h (:holder iv))
               :let [d-in (- t (long (:start iv))) d-out (- (long (:end iv)) t)]
               :when (and (pos? d-in) (pos? d-out))]
           {:name (:name p) :at-ms t :server-holder h :claiming (:holder iv)
            :d-in d-in :d-out d-out :hard? (>= (min d-in d-out) (long margin))}))))

(let [ivs-f (intervals :f34)
      ivs-l (intervals :legacy)
      ivs-n (intervals :new)
      ivs-t (intervals :true-end)]
  (println "== lock-diag ==" path)
  (println "lock-contend ops:" (count lock-ops) "| ok:" (count done)
           "| acquired:" (count acquired)
           "| :lock-held:" (count (filter #(= :lock-held (:error %)) lock-ops)))
  (println "fencing-succeeded:" (count (filter #(get-in % [:foreign-release :released?]) acquired))
           "| released?=true:" (count (filter :released? acquired))
           "| no-gone-at-ms:" (count (remove :gone-at-ms acquired)))
  (println)
  (println "-- 区间膨胀（:released-at-ms - (start+hold)，毫秒）--")
  (println (pr-str {:min (first infl) :p50 (pct infl 50) :p90 (pct infl 90)
                    :max (last infl) :negative (count (filter neg? infl))}))
  (println)
  (println "-- 互斥重叠：四种口径对照 --")
  (println (format "%-10s %s   (F-34 当时的完整口径：released-at 记在观测循环之后 + exists=false 才算空)"
                   ":f34" (count (overlap-pairs ivs-f))))
  (println (format "%-10s %s   (只把锚点/字段留下，但用 released-at 闭合)" ":legacy"
                   (count (overlap-pairs ivs-l))))
  (println (format "%-10s %s   (现在的 checker 口径：:gone-at-ms)" ":new"
                   (count (overlap-pairs ivs-n))))
  (println (format "%-10s %s   (独立估计：最后一次 renew + 100ms)" ":true-end"
                   (count (overlap-pairs ivs-t))))
  (doseq [[a b] (take 8 (overlap-pairs ivs-f))]
    (println (format "  [f34] ov=%d %s pid%s [%d,%d) vs pid%s [%d,%d)"
                     (ov [a b]) (:name a) (:pid a) (:start a) (:end a)
                     (:pid b) (:start b) (:end b))))
  (println)
  (println "-- 服务端地面真值探针 --")
  (println "samples:" (count probes)
           "| read-failures:" (count (filter #(= :info (:type %)) probes))
           "| with-key:" (count (filter :exists? probe-ok))
           "| distinct-holders:" (count (distinct (keep #(get-in % [:server :holder-id]) probe-ok))))
  (doseq [[m ivs] [[:f34 ivs-f] [:legacy ivs-l] [:new ivs-n] [:true-end ivs-t]]]
    (let [c (contradictions ivs 100)]
      (println (format "%-10s 硬违约=%d 边界=%d" (str m)
                       (count (filter :hard? c)) (count (remove :hard? c))))))
  (doseq [c (take 8 (filter :hard? (contradictions ivs-n 100)))]
    (println "  " (pr-str c)))
  (println)
  (println "判定：`:new` 与 `:true-end` 都为 0、且探针硬违约 = 0 ⇒ 互斥成立（客户端/服务端两侧证据）；")
  (println "      `:f34`（或 `:legacy`）的数值与它们的差 = 测试自身区间度量贡献的假红。"))
