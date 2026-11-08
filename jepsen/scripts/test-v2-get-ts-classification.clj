;; #309 —— v2 客户端的时间戳分配不得把异常穿透到 generator。
;;
;; 背景：`v2-get-ts` 过去把所有错误都当可重试（轮换 16 次）然后 `(throw err)`。
;; `CCT expired`（UNAUTHENTICATED）是会话级状态，轮换 16 次必然全失败，而抛出的
;; io.grpc.StatusRuntimeException 穿出 `invoke!` ⇒ jepsen 记
;; `Process N crashed` + `indeterminate: …`，该 worker 从此不再发 op（样本被削）。
;;
;; 用法（控制机，coord-test 项目根目录）：
;;   LEIN_ROOT=true lein -o run -m clojure.main scripts/test-v2-get-ts-classification.clj
;;
;; 退出码：0 = 全部符合预期；1 = 有不符合（`make checkers` 里会拒绝起跑）。

(require '[jepsen.coord.client :as client]
         '[jepsen.client :as jc])
(import (quote [io.grpc Status Status$Code StatusRuntimeException]))

(def ^:private get-ts
  "v2-get-ts 是 private；测试走 ns-resolve，保证测的是生产实现本身。"
  (ns-resolve 'jepsen.coord.client 'v2-get-ts))

(def ^:private invoke-v2-ts
  (ns-resolve 'jepsen.coord.client 'invoke-v2-ts))

(def ^:private invoke-coord!
  (ns-resolve 'jepsen.coord.client 'invoke-coord!))

(def ^:private v2-call-on
  (ns-resolve 'jepsen.coord.client 'v2-call-on))

(defn- ex
  "gRPC 异常（不带 trailer：分类走 gRPC code 兜底）。"
  [^Status$Code code]
  (StatusRuntimeException. (Status/fromCode code)))

(defn- stub-client
  "`v2-get-ts` 只用到 `:leader-idx` 与 `:channels`（rotate 取模）。"
  []
  (-> (client/coord-client {:nodes ["n1" "n2" "n3"]})
      (assoc :channels (atom [:c1 :c2 :c3]))))

(defn- with-call
  "把 `v2-call-on` 换成 `f`（参数同原函数：[this idx method req tag]）。"
  [f thunk]
  (with-redefs-fn {v2-call-on f} thunk))

(def passes (atom 0))
(def failures (atom []))

(defn- check! [label ok? detail]
  (if ok?
    (do (println "PASS" label)
        (swap! passes inc))
    (do (println "FAIL" label "--" (pr-str detail))
        (swap! failures conj label))))

;; ---------------------------------------------------------------------------
;; 1. v2-get-ts 不抛异常，且分类正确
;; ---------------------------------------------------------------------------

(let [calls (atom 0)
      r (with-call (fn [_ _ _ _ _]
                     (swap! calls inc)
                     {:err (ex Status$Code/UNAUTHENTICATED)})
                   #(get-ts (stub-client)))]
  (check! "UNAUTHENTICATED returns :unauthenticated without rotating"
          (and (= :unauthenticated (:class r)) (= 1 @calls) (string? (:detail r)))
          {:result r :calls @calls}))

(let [calls (atom 0)
      r (try (with-call (fn [_ _ _ _ _]
                          (swap! calls inc)
                          {:err (ex Status$Code/UNAVAILABLE)})
                        #(get-ts (stub-client)))
             (catch Throwable t t))]
  (check! "UNAVAILABLE rotates to the budget, then returns :unavailable (no throw)"
          (and (map? r) (= :unavailable (:class r)) (= 18 @calls))
          {:result r :calls @calls}))

(let [calls (atom 0)
      r (try (with-call (fn [_ _ _ _ _]
                          (swap! calls inc)
                          {:err (ex Status$Code/INTERNAL)})
                        #(get-ts (stub-client)))
             (catch Throwable t t))]
  (check! "INTERNAL (no trailer) returns :other (no throw, no silent :ok)"
          (and (map? r) (= :other (:class r)) (= 18 @calls))
          {:result r :calls @calls}))

;; ---------------------------------------------------------------------------
;; 2. invoke-v2-ts 把分配结果落成 op 结果
;; ---------------------------------------------------------------------------

(let [c (stub-client)]
  (check! "invoke-v2-ts / UNAUTHENTICATED => :fail :unauthenticated (re-auth path)"
          (= {:type :fail :error :unauthenticated :f :v2-ts}
             (with-call (fn [_ _ _ _ _] {:err (ex Status$Code/UNAUTHENTICATED)})
                        #(invoke-v2-ts c {:type :invoke :f :v2-ts})))
          nil)
  (check! "invoke-v2-ts / INTERNAL => :info :other with :detail"
          (let [r (with-call (fn [_ _ _ _ _] {:err (ex Status$Code/INTERNAL)})
                             #(invoke-v2-ts c {:type :invoke :f :v2-ts}))]
            (and (= :info (:type r)) (= :other (:error r)) (string? (:detail r))))
          nil))

;; ---------------------------------------------------------------------------
;; 3. invoke! 顶层兜底：gRPC 异常不外泄，非 gRPC 异常照旧传播
;; ---------------------------------------------------------------------------

(let [c (client/coord-client {:nodes ["n1" "n2" "n3"]})]
  (check! "invoke! guard: UNAUTHENTICATED => :fail :unauthenticated"
          (let [r (with-redefs-fn {invoke-coord!
                                   (fn [_ _ _] (throw (ex Status$Code/UNAUTHENTICATED)))}
                        #(jc/invoke! c nil {:type :invoke :f :v2-ts}))]
            (= {:type :fail :error :unauthenticated :f :v2-ts} r))
          nil)
  (check! "invoke! guard: INTERNAL => :info :other (does not escape)"
          (let [r (with-redefs-fn {invoke-coord!
                                   (fn [_ _ _] (throw (ex Status$Code/INTERNAL)))}
                        #(jc/invoke! c nil {:type :invoke :f :v2-ts}))]
            (and (= :info (:type r)) (= :other (:error r)) (string? (:detail r))))
          nil)
  (check! "invoke! guard does not swallow non-gRPC exceptions"
          (let [r (try (with-redefs-fn {invoke-coord!
                                        (fn [_ _ _] (throw (RuntimeException. "boom")))}
                                       #(jc/invoke! c nil {:type :invoke :f :v2-ts}))
                       :no-throw
                       (catch RuntimeException e (.getMessage e)))]
            (= "boom" r))
          nil))

(println "----")
(println @passes "checks passed," (count @failures) "failed")
(System/exit (if (empty? @failures) 0 1))
