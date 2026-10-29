;; TDD harness for the KV-T v2 cluster config mode (--kv-t) in db.clj / coord.clj.
;;
;; Usage (on the CONTROL node, from the coord-test project root):
;;   LEIN_ROOT=true lein run -m clojure.main scripts/test-kv-t-config.clj
;;
;; Asserts (exit 0 when all pass, 1 otherwise):
;;   1. legacy (no :kv-t?) config carries no [multi_raft] / [kv_t] block;
;;   2. legacy output is byte-identical to the pre-change golden (embedded below) —
;;      the v1 lab must not drift because kv-t support was added;
;;   3. the v2 config carries [kv_t].enabled = true, [multi_raft] with two fixed
;;      regions (""->"m", "m"->""), PD target_replicas = 3 / scheduler_paused;
;;   4. every node's v2 config is identical except [node] id/name and root_password;
;;   5. no legacy_migration / allow_unmigrated (v2 is greenfield, ADR-0015 D13).
;;
;; The golden in (2) is the exact config-str output before the --kv-t change:
;; a change there means v1 cluster config drifted and the v1 matrices would be
;; testing a different deployment than the one GA was signed on.

(require '[jepsen.coord.db :as db])
(require '[clojure.string :as str])

(def node-ips ["192.168.56.11" "192.168.56.12" "192.168.56.13"])

(def legacy-golden
  "[node]
id = 1
name = \"coord-01\"

[network]
grpc_addr = \"0.0.0.0:50051\"
raft_addr = \"0.0.0.0:50052\"
http_addr = \"0.0.0.0:50061\"

[cluster]
cluster_name = \"jepsen-coord\"
bootstrap = true
initial_nodes = [
  { id = 1, grpc = \"192.168.56.11:50051\", raft = \"192.168.56.11:50052\" },
  { id = 2, grpc = \"192.168.56.12:50051\", raft = \"192.168.56.12:50052\" },
  { id = 3, grpc = \"192.168.56.13:50051\", raft = \"192.168.56.13:50052\" }
]

[storage]
data_dir = \"/var/lib/coord\"

[security]
auth_enabled = true
root_password = \"66c57bb56bce306f484344e4a8650836\"
auth_root_key = \"820662a9f85924333d6d466714bc51734b251e73b827a394e1978e36d2429bfc\"
raft_shared_secret = \"93edf94d2860fec54bc59278a804cb1bbd2d0a7243e4f39d667d66966d9844d1\"
allow_plaintext_remote = true
")

(def passes   (atom 0))
(def failures (atom []))

(defn check!
  "Records a check: PASS label when (pred) is truthy, else FAIL with detail."
  [label pred & [detail]]
  (if pred
    (do (println "PASS" label)
        (swap! passes inc))
    (do
      (println "FAIL" label (when detail (pr-str detail)))
      (swap! failures conj label))))

(defn- node-line?
  "True for the per-node lines that legitimately differ between node configs."
  [line]
  (or (str/starts-with? line "id = ")
      (str/starts-with? line "name = ")
      (str/starts-with? line "bootstrap = ")
      (str/starts-with? line "root_password = ")))

(defn- shared-lines
  "Config lines that must be identical on every node (drops per-node lines)."
  [cfg]
  (->> (str/split-lines cfg)
       (remove node-line?)
       vec))

;; ---------------------------------------------------------------------------
;; 1. Legacy: no v2 blocks at all
;; ---------------------------------------------------------------------------
(let [legacy (db/coord {:nodes node-ips})
      cfgs   (mapv #(db/config-str legacy {:nodes node-ips} %) node-ips)]
  (check! "legacy config carries no [multi_raft] block"
          (every? #(not (str/includes? % "[multi_raft]")) cfgs))
  (check! "legacy config carries no [kv_t] block"
          (every? #(not (str/includes? % "[kv_t]")) cfgs))

  ;; -------------------------------------------------------------------------
  ;; 2. Legacy output is byte-identical to the pre-change golden
  ;; -------------------------------------------------------------------------
  (check! "legacy config-str is byte-identical to the pre-change golden"
          (= legacy-golden (db/config-str legacy {:nodes node-ips} (first node-ips)))
          (db/config-str legacy {:nodes node-ips} (first node-ips))))

;; ---------------------------------------------------------------------------
;; 3. v2 config: the three sections, verbatim
;; ---------------------------------------------------------------------------
(let [v2   (db/coord {:nodes node-ips :kv-t true})
      cfgs (mapv #(db/config-str v2 {:nodes node-ips} %) node-ips)
      cfg  (first cfgs)]
  (check! "v2 config enables [kv_t]"
          (str/includes? cfg "[kv_t]\nenabled = true\n"))
  (check! "v2 config enables [multi_raft]"
          (str/includes? cfg "[multi_raft]\nenabled = true\n"))
  (check! "v2 config nests [multi_raft.pd] with target_replicas = 3"
          (and (str/includes? cfg "[multi_raft.pd]")
               (str/includes? cfg "target_replicas = 3")))
  (check! "v2 config pauses the PD scheduler (split must not mix into data-plane checks)"
          (str/includes? cfg "scheduler_paused = true"))
  (check! "v2 config has exactly two fixed regions"
          (= 2 (count (re-seq #"\[\[multi_raft\.initial_regions\]\]" cfg))))
  (check! "v2 region 1 is [\"\", \"m\")"
          (str/includes? cfg (str "[[multi_raft.initial_regions]]\n"
                                  "id = 1\n"
                                  "start_key = \"\"\n"
                                  "end_key = \"m\"\n")))
  (check! "v2 region 2 is [\"m\", \"\")"
          (str/includes? cfg (str "[[multi_raft.initial_regions]]\n"
                                  "id = 2\n"
                                  "start_key = \"m\"\n"
                                  "end_key = \"\"\n")))

  ;; -------------------------------------------------------------------------
  ;; 4. Every node carries the same v2 deployment config
  ;; -------------------------------------------------------------------------
  (check! "v2 config is identical on all nodes except [node] id/name and root_password"
          (apply = (mapv shared-lines cfgs))
          (mapv shared-lines cfgs))

  ;; ---------------------------------------------------------------------------
  ;; 5. Greenfield only: no v1 migration switches
  ;; ---------------------------------------------------------------------------
  (check! "v2 config carries no legacy_migration"
          (not (str/includes? cfg "legacy_migration")))
  (check! "v2 config carries no allow_unmigrated"
          (not (str/includes? cfg "allow_unmigrated"))))

;; ---------------------------------------------------------------------------
(let [n-fail (count @failures)]
  (println "----")
  (println @passes "checks passed," n-fail "failed")
  (System/exit (if (zero? n-fail) 0 1)))
