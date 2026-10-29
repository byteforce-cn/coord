;; #272 Task 4: the v2 failure classifier must read the structured trailer.
;;
;; The gRPC code alone cannot tell these apart (SAFE_TS_LAG and a dropped
;; connection are both UNAVAILABLE; TXN_CONFLICT and EPOCH_STALE are both
;; ABORTED), so a classifier that only looks at the status would retry a
;; conflict as if it were a network error -- and hide it from the checker.
;;
;; Usage (control node, from the coord-test project root):
;;   LEIN_ROOT=true lein -o run -m clojure.main scripts/test-v2-error-class.clj

(require '[jepsen.coord.client :as client])
(import (quote [io.grpc Status Status$Code StatusRuntimeException Metadata Metadata$Key]))

(defn- ex
  "StatusRuntimeException carrying `code` in the x-coord-error-code trailer."
  [^Status$Code status-code code]
  (let [md (Metadata.)]
    (when code
      (.put md (Metadata$Key/of "x-coord-error-code"
                                Metadata/ASCII_STRING_MARSHALLER)
            code))
    (StatusRuntimeException. (-> (Status/fromCode status-code)
                                 (.withDescription "test"))
                             md)))

(defn- classify [^StatusRuntimeException e]
  (@#'client/v2-error-class e))

(def passes (atom 0))
(def failures (atom []))

(defn- check! [label expected e]
  (let [got (classify e)]
    (if (= expected got)
      (do (println "PASS" label "->" got)
          (swap! passes inc))
      (do (println "FAIL" label "expected" expected "got" got)
          (swap! failures conj label)))))

;; one per trailer value the client branches on
(check! "COMMIT_REJECTED"            :commit-rejected
        (ex Status$Code/FAILED_PRECONDITION "COMMIT_REJECTED"))
(check! "SAFE_TS_LAG"                :safe-ts-lag
        (ex Status$Code/UNAVAILABLE "SAFE_TS_LAG"))
(check! "EPOCH_STALE"                :epoch-stale
        (ex Status$Code/ABORTED "EPOCH_STALE"))
(check! "TXN_CONFLICT"               :txn-conflict
        (ex Status$Code/ABORTED "TXN_CONFLICT"))
(check! "NOT_LEADER"                 :not-leader
        (ex Status$Code/FAILED_PRECONDITION "NOT_LEADER"))
(check! "HISTORY_UNAVAILABLE"        :history-unavailable
        (ex Status$Code/OUT_OF_RANGE "HISTORY_UNAVAILABLE"))
(check! "TXN_NOT_FOUND"              :txn-not-found
        (ex Status$Code/ABORTED "TXN_NOT_FOUND"))
;; no trailer: fall back to the gRPC code
(check! "UNAVAILABLE (no trailer)"   :unavailable
        (ex Status$Code/UNAVAILABLE nil))
(check! "UNAUTHENTICATED (no trailer)" :unauthenticated
        (ex Status$Code/UNAUTHENTICATED nil))

(println "----")
(println @passes "checks passed," (count @failures) "failed")
(System/exit (if (empty? @failures) 0 1))
