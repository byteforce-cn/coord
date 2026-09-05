(ns jepsen.coord.regions
  "Multi-Raft region layout for the coord Jepsen tests (Phase 4 T4.1+).

  Multi-region mode (--regions N) splits the keyspace into N regions; region r
  is an independent openraft group replicated to every cluster member (v1
  static replication: coord assembles each region over cluster.initial_nodes,
  so a 3-node cluster gives every region 3 replicas across n1-n3). The layout
  is derived from the per-region register keys so routing is exact:

    region 1  = [\"\",            K2)     (R-MR-01: first start_key empty)
    region r  = [Kr,             K{r+1})  (1 < r < N)
    region N  = [KN,             \"\"]     (R-MR-01: last end_key unbounded)

  K r is the register key of region r; keys are byte-increasing in r
  (zero-padded to 3 digits), so region r's own register key falls in exactly
  region r's [start, end) band. coord matches keys byte-wise with left-closed /
  right-open ranges, so \"\" means keyspace start (no lower bound) for the first
  region and no upper bound for the last.

  All functions here are pure and run on the control node (no network/DB).
  "
  (:require [clojure.string :as str]))

(defn region-key
  "Register key for region r (1-indexed). Zero-padded so keys are strictly
  byte-increasing in r; valid for r <= 999."
  [r]
  (str "/jepsen/register-" (format "%03d" r)))

(defn region-keys
  "Register keys for regions 1..n (each routes to exactly its region; see
  region-configs). Empty for n <= 0."
  [n]
  (when (and n (pos? n))
    (mapv region-key (range 1 (inc n)))))

(defn region-configs
  "Region table for n regions derived from the region register keys: a vector
  of {:id r :start-key s :end-key e} that tiles the whole keyspace (first
  start-key empty, adjacent ranges contiguous, last end-key empty). Region r's
  register key routes to exactly region r. Returns nil when n is nil or 0
  (single-Raft legacy mode)."
  [n]
  (when (and n (pos? n))
    (when (> n 999)
      (throw (IllegalArgumentException.
              (str "region-configs supports at most 999 regions (got " n ")"))))
    (mapv (fn [r]
            {:id        r
             :start-key (if (= r 1) "" (region-key r))
             :end-key   (if (= r n) "" (region-key (inc r)))})
          (range 1 (inc n)))))

(defn multi-raft-toml
  "The `[multi_raft]` TOML block for a region table (from region-configs), or
  nil when the table is empty (single-Raft legacy mode). Keys contain only
  ASCII printable characters (no quotes/backslashes), so no TOML escaping is
  needed. The block is identical on every node."
  [region-configs]
  (when (seq region-configs)
    (str "\n"
         "[multi_raft]\n"
         "enabled = true\n"
         (apply str
                (for [{:keys [id start-key end-key]} region-configs]
                  (str "[[multi_raft.initial_regions]]\n"
                       "id = " id "\n"
                       "start_key = \"" start-key "\"\n"
                       "end_key = \"" end-key "\"\n"))))))
