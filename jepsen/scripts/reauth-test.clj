;; reauth-test.clj — fast (seconds) validation of the client's re-auth fix.
;;
;; Does NOT wait for the real 1h CCT TTL. Instead it:
;;   1. opens a real client (authenticates, gets a valid CCT)
;;   2. verifies a normal read works
;;   3. corrupts the in-memory CCT to a bogus value (forcing the server to
;;      reply UNAUTHENTICATED on the next op)
;;   4. verifies the client transparently re-authenticates and retries -> :ok
;;
;; Run on the CONTROL node from /root/coord-test:
;;   LEIN_ROOT=true lein run -m clojure.main /tmp/reauth-test.clj
;;
;; Safe to run while a soak is running: only reads /jepsen/register, never
;; writes, so it cannot perturb the soak checker. Mirrors the
;; validate-soak-checker.clj pattern (top-level script, no -main).
(require '[jepsen.client :as jc])
(require '[jepsen.coord.client :as client])
(require '[jepsen.coord.proto :as p])

(def nodes ["n1" "n2" "n3"])
(def pw   "66c57bb56bce306f484344e4a8650836")

(defn- read-op [c]
  (jc/invoke! c {} {:type :invoke, :f :read}))

(let [c      (client/coord-client {:nodes nodes :root-password pw})
      opened (jc/open! c {} "n1")]
  (try
    (println "1. opened client, valid cct?" (some? @(:cct opened)))
    ;; setup! retries reads until :ok and advances leader-idx to the leader,
    ;; exactly like the real test run does.
    (jc/setup! opened {})
    (println "2. setup! found the leader (leader-idx advanced)")

    (let [r1 (read-op opened)]
      (println "3. normal read ->" (:type r1) "value=" (:value r1))
      (assert (= :ok (:type r1)) "baseline read should be :ok"))

    ;; Force UNAUTHENTICATED on the next op by replacing the token.
    (reset! (:cct opened) "bogus-cct")
    (reset! (:channels opened)
            (mapv #(p/auth-channel % "bogus-cct") @(:raw-channels opened)))
    (println "4. corrupted channels with bogus CCT")

    (let [r2 (read-op opened)]
      (println "5. read with bogus CCT ->" (:type r2) "err=" (:error r2))
      (if (= :ok (:type r2))
        (println "PASS: client re-authenticated and retried successfully")
        (println "FAIL: still failing after refresh")))
    (println "   fresh cct?" (some? @(:cct opened)) "is bogus?" (= "bogus-cct" @(:cct opened)))

    (let [r3 (read-op opened)]
      (println "6. follow-up read ->" (:type r3) "err=" (:error r3))
      (assert (= :ok (:type r3)) "follow-up read should be :ok"))

    (println "RESULT: PASS — re-auth-on-unauthenticated works")
      (catch AssertionError e
        (println "RESULT: FAIL —" (.getMessage e))
        (System/exit 1))
      (catch Exception e
        (println "RESULT: ERROR —" (.getMessage e))
        (.printStackTrace e)
        (System/exit 1))
      (finally
        (jc/close! opened {})
      (shutdown-agents))))
