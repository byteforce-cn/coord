;; P0 smoke for the KV-T v2 wire layer (#272 Task 2).
;;
;; Proves the hand-written coord.tso.v2 / coord.kv.v2 descriptors can actually
;; talk to a running v2 cluster *with auth enabled* -- the Rust fixtures run
;; with auth_enabled=false, so this path is new in Jepsen. Run it on the
;; control node after `make up` + `make upload` with a --kv-t cluster started
;; by a test run (or against an existing v2 cluster).
;;
;; Checks:
;;   1. authenticate as root and reach every node's TSO;
;;   2. 20 consecutive GetTs are strictly increasing (per node);
;;   3. Range(read_ts=0) on a fresh key returns no kvs and echoes a read_ts;
;;   4. any failure prints the endpoint plus the x-coord-error-code trailer.
;;
;; Usage (control node, from the coord-test project root):
;;   LEIN_ROOT=true lein -o run -m clojure.main scripts/smoke-kv-t.clj
;;   COORD_NODES="n1 n2 n3" ...   # override the node list (default: /root/nodes)

(require '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[jepsen.coord.proto :as p]
         '[jepsen.coord.db :as db])
(import '[io.grpc StatusRuntimeException])

(def nodes
  (or (some-> (System/getenv "COORD_NODES") (str/split #"[\s,]+") vec)
      (let [f (io/file "/root/nodes")]
        (when (.isFile f)
          (->> (slurp f) str/split-lines (remove str/blank?) (take 3) vec)))
      ["n1" "n2" "n3"]))

(defn- fail! [endpoint ^StatusRuntimeException e]
  (println "FAIL" endpoint "->" (str e)
           "| error-code:" (pr-str (p/error-code e)))
  (System/exit 1))

(defn- call! [ch endpoint method req]
  (try
    (p/call ch method req)
    (catch StatusRuntimeException e (fail! endpoint e))))

;; ── auth ───────────────────────────────────────────────────────────────────
(println "smoke: nodes" (pr-str nodes))
(let [raw (mapv p/channel nodes)
      cct (loop [i 0]
            (if (< i 20)
              (let [ch (nth raw (mod i (count raw)))
                    r  (try
                         {:ok (p/cct (p/call ch p/authenticate
                                             (p/auth-req "root" db/default-root-password)))}
                         (catch StatusRuntimeException e {:err e}))]
                (if (:ok r)
                  (:ok r)
                  (do (Thread/sleep 1000) (recur (inc i)))))
              (do (println "FAIL could not authenticate to any node")
                  (System/exit 1))))
      chans (mapv #(p/auth-channel % cct) raw)]

  ;; ── 1-2. TSO on every node, strictly increasing ─────────────────────────
  (doseq [[node ch] (map vector nodes chans)]
    (let [req (p/tso-get-ts-req)
          tss (mapv (fn [_] (p/tso-get-ts (call! ch node p/tso-v2-get-ts-method req)))
                    (range 20))]
      (when-not (every? pos? tss)
        (println "FAIL" node "GetTs returned a non-positive ts:" (pr-str tss))
        (System/exit 1))
      (when-not (apply < tss)
        (println "FAIL" node "GetTs was not strictly increasing:" (pr-str tss))
        (System/exit 1))
      (println "PASS" node "tso.v2 GetTs x20 strictly increasing, first" (first tss))))

  ;; ── 3. Range(read_ts=0) on a fresh key ──────────────────────────────────
  (let [key (str "/jepsen/v2/ready-" (System/currentTimeMillis) "-" (rand-int 1000000))
        node (first nodes)
        ch   (first chans)
        resp (call! ch node p/kv-v2-range-method
                    (p/kv2-range-req* {:key key :read-ts 0}))
        {:keys [kvs read-ts]} (p/kv2-range-resp resp)]
    (when (seq kvs)
      (println "FAIL" node "fresh key" key "returned kvs:" (pr-str kvs))
      (System/exit 1))
    (when-not (>= read-ts 0)
      (println "FAIL" node "Range echoed a negative read_ts:" read-ts)
      (System/exit 1))
    (println "PASS" node "kv.v2 Range(read_ts=0) on a fresh key is empty, read_ts"
             read-ts)))

  ;; ── 3b. explicit read_ts > 0 also works (the echo is the oracle) ────────
  (let [node (first nodes)
        ch   (first chans)
        ts   (p/tso-get-ts (call! ch node p/tso-v2-get-ts-method (p/tso-get-ts-req)))
        resp (call! ch node p/kv-v2-range-method
                    (p/kv2-range-req* {:key "/jepsen/v2/definitely-absent" :read-ts ts}))
        {:keys [kvs read-ts]} (p/kv2-range-resp resp)]
    (when-not (= ts read-ts)
      (println "FAIL" node "Range did not echo read_ts: asked" ts "got" read-ts)
      (System/exit 1))
    (when (seq kvs)
      (println "FAIL" node "absent key returned kvs at read_ts" ts)
      (System/exit 1))
    (println "PASS" node "kv.v2 Range(read_ts=" ts ") echoes the snapshot and finds nothing"))

  (doseq [ch raw] (.shutdownNow ^io.grpc.ManagedChannel ch))
  (println "smoke: OK")
  (System/exit 0))
