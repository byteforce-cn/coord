;; TDD harness for Phase 4 T4.1: db.clj / regions.clj [multi_raft] config
;; generation (coord multi-raft mode, M4).
;;
;; Usage (on the CONTROL node, from the coord-test project root):
;;   LEIN_ROOT=true lein run -m clojure.main scripts/test-multi-raft-config.clj
;;
;; Asserts (exit 0 when all pass, 1 otherwise):
;;   1. region register keys are strictly byte-increasing in region id.
;;   2. region-configs N tiles the whole keyspace for N in {1,2,3,5,8}:
;;      first start-key empty, adjacent ranges contiguous (left-closed /
;;      right-open), last end-key empty; ids are 1..N.
;;   3. Routing invariant: for every N, region r's own register key routes to
;;      exactly region r and no other region (simulated byte routing).
;;   4. multi-raft-toml emits a well-formed [multi_raft] block: enabled = true,
;;      one [[multi_raft.initial_regions]] per region with correct id/start/end.
;;   5. Full per-node config: legacy (no regions) stays single-Raft (no
;;      [multi_raft] anywhere); multi-region configs carry enabled=true and the
;;      *identical* region table on every node.
;;
;; RED note: before the implementation this file must FAIL (the referenced
;; region-configs / config-str symbols do not exist yet).

(require '[jepsen.coord.regions :as r])
(require '[jepsen.coord.db :as db])
(require '[clojure.string :as str])

(defn- key<  [a b] (neg? (compare a b)))
(defn- key<= [a b] (not (key< b a)))

(defn- in-range?
  "True when key k falls in a region's [start-key, end-key) band (empty start =
  keyspace start, empty end = unbounded; coord compares bytes, ASCII == byte
  order for our keys)."
  [k {:keys [start-key end-key]}]
  (and (or (empty? start-key) (key<= start-key k))
       (or (empty? end-key)   (key< k end-key))))

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

;; ---------------------------------------------------------------------------
;; 1. Register keys are byte-increasing in region id
;; ---------------------------------------------------------------------------
(let [ks (mapv r/region-key (range 1 10))]
  (check! "region keys strictly byte-increasing for ids 1..9"
          (every? (fn [[a b]] (key< a b)) (partition 2 1 ks))
          ks))

;; ---------------------------------------------------------------------------
;; 2. region-configs tiles the keyspace
;; ---------------------------------------------------------------------------
(doseq [n [1 2 3 5 8]]
  (let [cfgs (r/region-configs n)]
    (check! (str "region-configs " n " -> " (count cfgs) " regions")
            (= n (count cfgs)))
    (check! (str "region-configs " n " ids are 1..N")
            (= (mapv :id cfgs) (vec (range 1 (inc n)))))
    (check! (str "region-configs " n " first start-key empty")
            (= "" (:start-key (first cfgs))))
    (check! (str "region-configs " n " last end-key empty (unbounded)")
            (= "" (:end-key (last cfgs))))
    (check! (str "region-configs " n " adjacent ranges contiguous")
            (every? (fn [[a b]] (= (:end-key a) (:start-key b)))
                    (partition 2 1 cfgs))
            cfgs)))

(check! "region-configs nil -> nil (single-Raft legacy)"
        (nil? (r/region-configs nil)))
(check! "region-configs 0 -> nil"
        (nil? (r/region-configs 0)))

;; ---------------------------------------------------------------------------
;; 3. Routing invariant: key r routes to exactly region r
;; ---------------------------------------------------------------------------
(doseq [n [1 2 3 5 8]]
  (let [cfgs (r/region-configs n)
        bad  (for [reg-id (range 1 (inc n))
                   :let [k (r/region-key reg-id)
                         owners (filter #(in-range? k %) cfgs)
                         own    (filter #(= reg-id (:id %)) cfgs)]
                   :when (or (not= 1 (count owners))
                             (not= reg-id (:id (first owners)))
                             (not (in-range? k (first own))))]
               {:region reg-id :key k :owners (mapv :id owners)})]
    (check! (str "routing invariant n=" n ": each register key in exactly its own region")
            (empty? bad)
            bad)))

;; ---------------------------------------------------------------------------
;; 4. multi-raft-toml block shape
;; ---------------------------------------------------------------------------
(check! "multi-raft-toml nil for empty table"
        (nil? (r/multi-raft-toml nil))
        (r/multi-raft-toml nil))

(let [n    3
      toml (r/multi-raft-toml (r/region-configs n))]
  (check! (str "multi-raft-toml n=" n " contains [multi_raft]")
          (str/includes? toml "[multi_raft]"))
  (check! (str "multi-raft-toml n=" n " enabled = true")
          (str/includes? toml "enabled = true"))
  (check! (str "multi-raft-toml n=" n " has " n " initial_regions blocks")
          (= n (count (re-seq #"\[\[multi_raft\.initial_regions\]\]" toml))))
  (doseq [reg-id (range 1 (inc n))]
    (check! (str "multi-raft-toml n=" n " declares region " reg-id " with its boundaries")
            (let [cfg (first (filter #(= reg-id (:id %)) (r/region-configs n)))]
              (and (str/includes? toml (str "id = " reg-id))
                   (str/includes? toml (str "start_key = \"" (:start-key cfg) "\""))
                   (str/includes? toml (str "end_key = \"" (:end-key cfg) "\"")))))))

(let [toml (r/multi-raft-toml (r/region-configs 1))]
  (check! "multi-raft-toml n=1: single full-keyspace region"
          (and (str/includes? toml "id = 1")
               (str/includes? toml "start_key = \"\"")
               (str/includes? toml "end_key = \"\""))))

;; ---------------------------------------------------------------------------
;; 5. Full per-node config integration
;; ---------------------------------------------------------------------------
(let [node-ips ["192.168.56.11" "192.168.56.12" "192.168.56.13"]
      test     {:nodes node-ips}
      legacy   (db/coord {:nodes node-ips})
      cfgs     (mapv #(db/config-str legacy test %) node-ips)]
  (check! "legacy per-node config carries no [multi_raft] anywhere"
          (every? #(not (str/includes? % "multi_raft")) cfgs))
  (check! "legacy per-node config keeps the legacy anchors"
          (every? (fn [cfg]
                    (and (str/includes? cfg "[node]")
                         (str/includes? cfg "[cluster]")
                         (str/includes? cfg "[security]")))
                  cfgs)))

(doseq [n [1 3]]
  (let [node-ips ["192.168.56.11" "192.168.56.12" "192.168.56.13"]
        test     {:nodes node-ips}
        mdb      (db/coord {:nodes node-ips :regions n})
        cfgs     (mapv #(db/config-str mdb test %) node-ips)
        section  (fn [cfg]
                   (when-let [i (str/index-of cfg "[multi_raft]")]
                     (subs cfg i)))]
    (check! (str "multi-region n=" n ": every node config has [multi_raft] enabled = true")
            (every? #(and (str/includes? % "[multi_raft]")
                          (str/includes? % "enabled = true"))
                    cfgs))
    (check! (str "multi-region n=" n ": region table identical across all nodes")
            (apply = (map section cfgs)))
    (check! (str "multi-region n=" n ": table has exactly " n " initial_regions")
            (every? #(= n (count (re-seq #"\[\[multi_raft\.initial_regions\]\]" %)))
                    cfgs))))

;; ---------------------------------------------------------------------------
(let [n-fail (count @failures)]
  (println "----")
  (println @passes "checks passed," n-fail "failed")
  (System/exit (if (zero? n-fail) 0 1)))
