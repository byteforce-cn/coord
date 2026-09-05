(ns jepsen.coord
  "Jepsen tests for coord, a strongly-consistent (linearizable) KV store.

  Workloads:
    :register       linearizable read/write register on a single key
    :cas-register   atomic compare-and-set register on a single key
    :multi-register N independent registers, one per region (--regions N):
                    ops carry :key and route to that key's region; each
                    region is its own raft group (multi_raft mode, T4.1).

  Nemeses (one per run for clean attribution, or :all):
    :none, :kill, :kill-all, :pause, :partition, :partition-halves,
    :partition-ring, :all, :soak

  :soak mode drives a slow, rotating fault cycle (kill / pause / partition
  with long quiet windows between disruptions) at a fixed low rate and runs a
  dedicated O(n log n) soak checker that stays tractable on 72h histories
  (see jepsen.coord.soak). Run it with --workload register --nemesis soak
  --time-limit 259200. Short runs: --checker linear keeps the knossos check.
  Multi-raft soak: --workload multi-register --regions N --nemesis soak
  (per-region soak checker grouping; T4.3)."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [os :as os]
                    [random :as rand]
                    [tests :as tests]]
            [jepsen.coord [client :as client]
                          [db :as db]
                          [nemesis :as n]
                          [regions :as regions]
                          [soak :as soak]]
            [knossos.model :as model])
  (:import [jepsen.coord CoordRpc]))

;; --------------------------------------------------------------------------
;; Workload generators
;; --------------------------------------------------------------------------

(defn- r [_ _]
  {:type :invoke, :f :read})

(defn- w [_ _]
  {:type :invoke, :f :write, :value (rand-int 1000000)})

(defn- cas [_ _]
  (let [old (rand-nth (vec @client/seen))
        new (inc (long (rand 1000000)))]
    {:type :invoke, :f :cas, :value [old new]}))

;; Soak mode writes unique, monotonically increasing values so the O(n) soak
;; checker (jepsen.coord.soak) is exact: a read must never return a value
;; older than the newest confirmed write that completed before it began.
;; For :multi-register the counter is per register key (:key on the op), so
;; each region's values are unique within that region.
(defonce ^:private soak-write-counter (atom 0))
(defonce ^:private multi-write-counter (atom {}))

(defn- soak-w
  [_ _]
  {:type :invoke, :f :write, :value (swap! soak-write-counter inc)})

(defn- soak?
  [opts]
  (= :soak (:nemesis opts)))

(defn- client-gen
  "Client generator for the given opts. For :multi-register, ops target one
  of the N region register keys (--regions N), each routing to its own region."
  [opts]
  (let [workload (:workload opts)
        soak?    (soak? opts)]
    (gen/mix
      (case workload
        :register     [r (if soak? soak-w w)]
        :cas-register [r cas cas]
        :multi-register
        (let [keys (vec (regions/region-keys (or (:regions opts) 1)))
              next-val (fn [k]
                         ;; Per-region monotonic counter: swap! returns the new
                         ;; atom state (the whole map) -- return the per-key
                         ;; value instead.
                         (let [v (inc (get @multi-write-counter k 0))]
                           (swap! multi-write-counter assoc k v)
                           v))
              w    (fn [_ _]
                     (let [k (rand-nth keys)]
                       {:type :invoke, :f :write, :key k
                        :value (if soak?
                                 (next-val k)
                                 (rand-int 1000000))}))
              rd   (fn [_ _]
                     {:type :invoke, :f :read, :key (rand-nth keys)})]
          [rd w])))))

;; --------------------------------------------------------------------------
;; Nemesis generators
;; --------------------------------------------------------------------------

(defn- single-nemesis-gen
  "Start/stop disruption every ~5s."
  []
  (cycle [(gen/sleep 5) {:type :info, :f :start}
          (gen/sleep 5) {:type :info, :f :stop}]))

(defn- combined-nemesis-gen
  "Drives the composed :all nemesis: kill, pause, partition, in rotation."
  []
  (cycle [(gen/sleep 5) {:type :info, :f :kill}
          (gen/sleep 5) {:type :info, :f :kill-stop}
          (gen/sleep 5) {:type :info, :f :pause}
          (gen/sleep 5) {:type :info, :f :pause-stop}
          (gen/sleep 5) {:type :info, :f :partition}
          (gen/sleep 5) {:type :info, :f :partition-stop}]))

(defn- soak-nemesis-gen
  "Slow rotating fault cycle for long soak runs: a quiet window, then a single
  disruption (kill / pause / single-node partition), then a recovery window;
  repeats forever (bounded by the surrounding gen/time-limit)."
  [opts]
  (let [quiet   (get opts :soak-quiet 1800)
        disrupt (get opts :soak-disrupt 600)]
    (cycle
      [(gen/sleep quiet)
       {:type :info, :f :kill}
       (gen/sleep disrupt)
       {:type :info, :f :kill-stop}
       (gen/sleep quiet)
       {:type :info, :f :pause}
       (gen/sleep disrupt)
       {:type :info, :f :pause-stop}
       (gen/sleep quiet)
       {:type :info, :f :partition}
       (gen/sleep disrupt)
       {:type :info, :f :partition-stop}])))

(defn- client-part
  "Client generator for the given opts. With --rate N, ops are issued at a
  fixed *global* rate of N ops/sec (gen/delay -- soak-friendly); otherwise
  the default exponential stagger (mean 0.3s) is used."
  [opts]
  (let [base (gen/clients (client-gen opts))
        rate (:rate opts)]
    (if (and rate (pos? rate))
      (gen/delay (/ 1.0 (double rate)) base)
      (gen/stagger 0.3 base))))

(defn- workload-gen
  [opts]
  (let [nemesis-key (:nemesis opts)
        soak?       (soak? opts)
        nemesis-gen (case nemesis-key
                      :none nil
                      :soak (soak-nemesis-gen opts)
                      :all  (combined-nemesis-gen)
                      (single-nemesis-gen))
        clients     (client-part opts)
        recovery    (if soak? 60 30)]
    (gen/phases
      (gen/time-limit (:time-limit opts)
        (if nemesis-gen
          (gen/nemesis nemesis-gen clients)
          clients))
      (when nemesis-gen
        (gen/nemesis {:type :info, :f :stop}))
      (gen/log "recovery: letting cluster converge")
      (gen/sleep recovery)
      (when soak?
        ;; Final verification reads: every client reads every register key
        ;; (single register: the one legacy key) until it observes an :ok
        ;; (bounded), which lets the soak checker confirm the cluster
        ;; recovered and converged on the latest committed value per region.
        (let [keys (if (= :multi-register (:workload opts))
                     (regions/region-keys (or (:regions opts) 1))
                     [nil])]
          (gen/log "soak: final verification reads")
          (->> (gen/mix
                 (for [k keys]
                   (gen/until-ok
                     (gen/limit 20
                       (gen/repeat {:f :read, :key k})))))
               gen/clients))))))

;; --------------------------------------------------------------------------
;; Checker
;; --------------------------------------------------------------------------

(defn- per-key-linear
  "Independent linearizable register check per key for :multi-register: ops
  carry :key, and each key is its own register (region). Valid iff every
  key's sub-history is linearizable."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [by-key (group-by (fn [op] (or (:key op) :register)) history)
            checks (for [[k h] by-key]
                     [k (checker/check (checker/linearizable
                                        {:model (model/register)
                                         :algorithm :wgl})
                                       test h opts)])]
        (into {:valid? (every? (fn [[_ r]] (:valid? r)) checks)}
              (map (fn [[k r]]
                     [(str "key-" k) {:valid? (:valid? r)}])
                   checks))))))

(defn- checker
  [opts]
  (let [workload (:workload opts)
        ck       (:checker opts)
        linear   (cond
                   ;; Soak mode: O(n) checker groups by :key (per region).
                   (= :soak ck)
                   (if (= :register workload)
                     (soak/checker)
                     (if (= :multi-register workload)
                       (soak/checker)
                       (do (warn "soak checker requires --workload register or "
                                 "multi-register; falling back to knossos linearizable")
                           (checker/linearizable
                             {:model (model/register), :algorithm :wgl}))))

                   ;; Multi-register, non-soak: each :key is its own register.
                   (= :multi-register workload)
                   (per-key-linear)

                   :else
                   (checker/linearizable
                     {:model (case workload
                               :cas-register (model/cas-register)
                               (model/register))
                      ;; WGL is more memory-efficient than the competition
                      ;; checker for histories with many :info ops.
                      :algorithm :wgl}))]
    (checker/compose
      {:linear linear
       :perf   (checker/perf)})))

;; --------------------------------------------------------------------------
;; Nemesis selection
;; --------------------------------------------------------------------------

(defn- build-nemesis
  [nemesis-key db]
  (case nemesis-key
    :none             nemesis/noop
    :kill            (n/kill-one db)
    :kill-all        (n/kill-all db)
    :pause           (n/pause-one)
    :partition       (nemesis/partition-random-node)
    :partition-halves (nemesis/partition-random-halves)
    :partition-ring  (nemesis/partition-majorities-ring)
    :all             (n/compose-all db)
    :soak            (n/compose-all db)
    nemesis/noop))

;; --------------------------------------------------------------------------
;; Test assembly
;; --------------------------------------------------------------------------

(defn coord-test
  "Constructs a coord test from CLI options."
  [opts]
  (let [soak?  (soak? opts)
        ;; Soak defaults: fixed low rate + the O(n) soak checker. Everything
        ;; is still overridable via --rate / --checker.
        opts   (cond-> opts
                 soak? (update :rate #(or % 0.5))
                 true  (update :checker #(or % (if soak? :soak :linear))))
        ;; :multi-register needs a region count; default to 1 (multi_raft
        ;; enabled, single full-keyspace region -- the T5.1 greyscale shape).
        opts   (cond-> opts
                 (and (= :multi-register (:workload opts))
                      (nil? (:regions opts)))
                 (assoc :regions 1))
        nodes  (take 3 (:nodes opts))
        opts   (assoc opts :nodes nodes)
        db     (db/coord opts)
        nemesis (build-nemesis (:nemesis opts) db)]
    (when soak?
      (reset! soak-write-counter 0)
      (reset! multi-write-counter {}))
    ;; Merge order matters: opts carries the *selection* keywords (:nemesis
    ;; :workload) which must not clobber the computed values below.
    (merge tests/noop-test
           opts
           {:name      "coord"
            :os        os/noop
            :db        db
            :client    (client/coord-client opts)
            :nemesis   nemesis
            :generator (workload-gen opts)
            :checker   (checker opts)})))

;; --------------------------------------------------------------------------
;; CLI
;; --------------------------------------------------------------------------

(def cli-opts
  [[nil "--workload WORKLOAD" "Workload: register, cas-register, or multi-register (N independent region registers, --regions N)"
    :default  :register
    :parse-fn keyword
    :validate [#{:register :cas-register :multi-register}
               "Must be one of register, cas-register, multi-register"]]
   [nil "--nemesis NEMESIS"
    "Nemesis: none, kill, kill-all, pause, partition, partition-halves, partition-ring, all, soak"
    :default  :none
    :parse-fn keyword
    :validate [#{:none :kill :kill-all :pause :partition :partition-halves
                 :partition-ring :all :soak}
               "Must be one of none, kill, kill-all, pause, partition, partition-halves, partition-ring, all, soak"]]
   [nil "--rate RATE"
    "Client ops/sec at a fixed global rate (soak default 0.5); otherwise exponential stagger"
    :default  nil
    :parse-fn (fn [s] (Double/parseDouble s))
    :validate [#(and (number? %) (pos? %)) "rate must be a positive number"]]
   [nil "--regions N"
    "Multi-raft mode: split the keyspace into N regions (each its own register
    key / raft group, replicated to every node). Default (absent): single-Raft
    legacy mode."
    :default  nil
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(and (integer? %) (pos? %) (<= % 999))
               "regions must be an integer in 1..999"]]
   [nil "--checker CHECKER"
    "linear (knossos, default) or soak (O(n), for long 72h runs)"
    :default  nil
    :parse-fn keyword
    :validate [#{:linear :soak} "Must be one of linear, soak"]]
   [nil "--soak-quiet SECONDS"
    "Soak nemesis: quiet seconds between disruptions (default 1800)"
    :default  1800
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(pos? %) "must be a positive integer"]]
   [nil "--soak-disrupt SECONDS"
    "Soak nemesis: seconds each disruption lasts (default 600)"
    :default  600
    :parse-fn (fn [s] (Long/parseLong s))
    :validate [#(pos? %) "must be a positive integer"]]])

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd
              {:test-fn  coord-test
               :opt-spec cli-opts})
            args))
