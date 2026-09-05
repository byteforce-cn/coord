(ns jepsen.coord.soak
  "Soak (72h) verification for coord.

  Knossos's linearizability search is intractable on multi-hundred-thousand-op
  histories, so long soak runs use this O(n log n) checker instead. For the
  :register workload -- a single key with *unique, monotonic* write values
  (jepsen.coord uses a monotonically increasing counter in soak mode) -- a
  register is linearizable iff every :ok read satisfies:

    * no fabricated value -- the returned value was actually written by some
      write (:ok *or* :info; an :info write may have applied even though the
      response was lost, so its value is legal to observe),
    * no future value     -- a read must not return a value whose write had
      not even been *invoked* before the read completed (an :info write whose
      response was slow is a legal concurrent write),
    * no stale value      -- a read must not return a value older than the
      newest :ok write that completed before the read began.

  The third property is exactly the one that caught coord's stale-read bug: once
  a write is confirmed (:ok), no later read may observe an older value. The
  The checker also reports a soak summary (op counts, max committed value,
  final value, verification-phase read availability). The run is only valid
  when (a) no read violates the three properties, (b) the last completed read
  is :ok (the soak finale drives per-client until-ok verification reads, so a
  non-:ok tail read means the cluster did not recover -- this also covers the
  case where the final applied value came from an :info write), and (c) at
  least half of the reads in the final verification phase (after the last
  nemesis stop) succeeded.

  Multi-Register (Phase 4 T4.3): :multi-register ops carry a :key (one
  register per region). The checker groups data ops by :key and runs all three
  predicates plus the convergence/availability gates independently per
  region -- any region failing makes the run invalid (:regions in the summary
  carries the per-region detail). Nemesis stop times and disruption counts are
  global. Ops without a :key (legacy :register workload) form a single default
  group, so the output is byte-compatible with the previous single-register
  checker."
  (:require [jepsen.checker :as checker]
            [clojure.tools.logging :refer [info]])
  (:import (java.util Arrays)))

;; ---------------------------------------------------------------------------
;; History bookkeeping
;; ---------------------------------------------------------------------------

(defn- write-index
  "Indexes every write completion in the history, pairing each completion
  with its invocation (per-process stack, like reads) so the :future check
  can compare against write *invoke* time.

  Returns {:values    {value {:time t, :invoke ti, :ok? bool}}
                       ; all writes (ok + info)
           :confirmed [[t value] ...]}               ; only :ok writes"
  [history]
  (let [pending (atom {})]  ; process -> stack of write invoke times
    (reduce
      (fn [acc op]
        (cond
          (and (= :invoke (:type op)) (= :write (:f op)))
          (do (swap! pending update (:process op) (fnil conj []) (:time op))
              acc)

          (and (= :write (:f op))
               (contains? #{:ok :info :fail} (:type op)))
          (let [stack  (get @pending (:process op))
                invoke (peek stack)]
            (when (seq stack)
              (swap! pending update (:process op) pop))
            (let [ok? (= :ok (:type op))]
              (-> acc
                  (assoc-in [:values (:value op)]
                            {:time   (:time op)
                             :invoke invoke
                             :ok?    ok?})
                  (cond-> ok?
                    (update :confirmed conj [(:time op) (:value op)])))))

          :else acc))
      {:values {} :confirmed []}
      history)))

(defn- confirmed-prefix
  "Given the :confirmed writes (completion-ordered, possibly unsorted),
  returns {:times long-array of completion times (sorted)
           :vals  long-array where vals[i] is the value of the write that
                  completed at times[i] — i.e. the value of the *latest*
                  write completed as of times[i]}."
  [confirmed]
  (let [sorted (sort-by first confirmed)
        [ts vals]
        (reduce (fn [[ts vals] [t v]]
                  [(conj ts t)
                   (conj vals v)])
                [[] []]
                sorted)]
    {:times (long-array ts)
     :vals  (long-array vals)}))

(defn- latest-before
  "Value of the latest :ok write completed at or before time t (0 if none).

  NOTE (P0-4): the staleness floor is the *latest-completed* write's value,
  NOT the max value among completed writes. coord writes normally commit in
  completion order, but a write whose RPC stalls on a paused/partitioned node
  (the leader the client was pinned to) can complete — and commit — *after*
  later-issued higher-value writes once the node heals and the client rotates
  (at-least-once re-issue to the new leader). A lower-value write may then
  legitimately be the last committed value, so reads of it are correct.
  Comparing a read against the max completed value falsely flags such legal
  histories as stale (observed in the 2026-09-04 150s smoke and the
  2026-08-31 72h soak, both bursts right after a pause heal); comparing
  against the latest completed value still catches the genuine stale-read
  class (a lagging node serves values below the latest confirmed write)."
  [{:keys [times vals]} t]
  (if (zero? (alength times))
    0
    (let [i (Arrays/binarySearch times (long t))]
      (if (neg? i)
        (let [ins (- -1 i)]       ; insertion point = -i - 1
          (if (zero? ins) 0 (aget vals (dec ins))))
        (aget vals i)))))

(defn- check-reads
  "Walks the history, pairing each read completion with its invocation, and
  verifies the fabricated / future / stale properties above. Returns a vector
  of failure maps (empty when the history is linearizable)."
  [history {:keys [values] :as idx} prefix]
  (let [pending   (atom {})      ; process -> stack of read invoke times
        failures  (atom [])]
    (doseq [op history]
      (case (:type op)
        :invoke
        (when (= :read (:f op))
          (swap! pending update (:process op) (fnil conj []) (:time op)))

        (:ok :info :fail)
        (when (= :read (:f op))
          (let [stack  (get @pending (:process op))
                invoke (peek stack)]
            (when (seq stack)
              (swap! pending update (:process op) pop))
            (when (and (= :ok (:type op)) (some? invoke))
              (let [v    (:value op)
                    done (:time op)
                    m    (latest-before prefix invoke)
                    w    (get values v)]
                (cond
                  ;; Initial (empty) register -- nil is only legal while no
                  ;; write has completed.
                  (nil? v)
                  (when (pos? m)
                    (swap! failures conj
                           {:type :stale, :op op, :expected m, :got nil}))

                  ;; A value nobody ever wrote (even ambiguously).
                  (nil? w)
                  (swap! failures conj {:type :fabricated, :op op})

                  ;; A read returned a value whose write had not even been
                  ;; *invoked* when the read completed -- impossible for any
                  ;; applied write. (Comparing against write completion would
                  ;; wrongly flag a legal concurrent write + slow-response
                  ;; case; see rectification P0-1.) Falls back to completion
                  ;; time on histories missing the invoke.
                  (< done (or (:invoke w) (:time w)))
                  (swap! failures conj
                         {:type :future, :op op
                          :write-invoke   (or (:invoke w) (:time w))
                          :write-complete (:time w)})

                  ;; A read returned a value older than a confirmed write that
                  ;; completed before the read began -- the stale-read bug.
                  (< v m)
                  (swap! failures conj
                         {:type :stale, :op op, :expected m, :got v}))))))
        nil))
    @failures))

(defn- completed-reads
  "All reads with a completion (:ok / :info / :fail), in history order."
  [ops]
  (filter #(and (= :read (:f %))
                (contains? #{:ok :info :fail} (:type %)))
          ops))

(defn- last-stop-time
  "Completion time of the final nemesis stop op (either the soak finale
  {:f :stop} or a {:f :kill-stop / :pause-stop / :partition-stop}), nil when
  the history has no stop op. Reads completing after this time are the final
  verification phase."
  [ops]
  (let [ts (->> ops
                (filter (fn [op]
                          (and (= :info (:type op))
                               (or (= :stop (:f op))
                                   (re-find #"-stop$" (str (:f op)))))))
                (map :time))]
    (when (seq ts) (apply max ts))))

(def ^:private default-key ::single)

(defn- key-of
  "Group key of a data op: its :key (multi-register ops) or the legacy single
  register group."
  [op]
  (or (:key op) default-key))

(defn- key-label
  "Summary label for a group key (:register for the legacy group)."
  [k]
  (if (= k default-key) :register k))

(defn- key-group-summary
  "Human-friendly soak statistics for ONE register key's data ops (read/write
  only). `stop-t` is the global last-nemesis-stop completion time; reads
  completing after it form this key's final verification phase.

  `converged` is true iff the last completed read for this key is :ok (the
  soak finale drives per-client until-ok verification reads per key, so a
  non-:ok tail read means that region did not recover). This fixes both
  rectification P0-2A (the final applied value may come from an :info write,
  so comparing against max(:ok write) was over-strict) and P0-2B (a dead
  cluster whose verification reads are all :info/:fail now fails).

  Verification-phase availability (:verification-ok-ratio) covers the reads
  after the last nemesis stop; the checker gates :valid? on it (P0-3)."
  [ops stop-t]
  (let [by-type  (frequencies (map :type ops))
        ok-writes (filter #(and (= :write (:f %)) (= :ok (:type %))) ops)
        ok-reads  (filter #(and (= :read (:f %)) (= :ok (:type %))) ops)
        reads     (vec (completed-reads ops))
        vmax      (when (seq ok-writes)
                     (apply max (map :value ok-writes)))
        vreads    (if stop-t
                    (filterv #(> (:time %) stop-t) reads)
                    reads)
        vreads-ok (count (filter #(= :ok (:type %)) vreads))
        last-read (peek reads)
        converged (if (seq reads)
                    (= :ok (:type last-read))
                    (nil? vmax))]
    {:writes-ok             (count ok-writes)
     :reads-ok              (count ok-reads)
     :info-ops              (:info by-type 0)
     :fail-ops              (:fail by-type 0)
     :max-committed         vmax
     :final-value           (:value last-read)
     :final-converged       converged
     :verification-reads    (count vreads)
     :verification-reads-ok vreads-ok
     :verification-ok-ratio (if (seq vreads)
                              (double (/ vreads-ok (count vreads)))
                              1.0)}))

(defn checker
  "An O(n log n) linearizability check for register workloads with unique,
  monotonic write values -- tractable on 72h (~100k+ op) histories where the
  knossos search would OOM. Data ops are grouped by :key (multi-register:
  one register per region); the legacy single-register workload (no :key)
  forms one default group, byte-compatible with the previous output.

  Returns {:valid? ... :soak {...}} where :soak carries global counts,
  aggregate convergence/availability flags, and :regions {key summary}."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [history (seq history)
            ops     (vec history)
            by-type (frequencies (map :type ops))
            stop-t  (last-stop-time ops)
            data    (filter #(contains? #{:read :write} (:f %)) ops)
            by-key  (group-by key-of data)
            results (for [[k kops] by-key
                          :let [idx    (write-index kops)
                                prefix (confirmed-prefix (:confirmed idx))
                                fails  (check-reads kops idx prefix)
                                gs     (key-group-summary kops stop-t)]]
                      {:key (key-label k) :fails fails :summary gs})
            all-fails (vec (mapcat :fails results))
            per-region (into {}
                             (map (fn [{:keys [key summary]}] [key summary]))
                             results)
            group-ok? (fn [{:keys [fails summary]}]
                        (and (empty? fails)
                             (:final-converged summary)
                             (>= (:verification-ok-ratio summary) 0.5)))
            ratios    (map (comp :verification-ok-ratio :summary) results)
            converged (if (seq results)
                        (every? (comp :final-converged :summary) results)
                        true)
            min-ratio (if (seq ratios) (apply min ratios) 1.0)
            max-comm  (when (seq results)
                        (apply max (keep (comp :max-committed :summary) results)))
            single?   (= 1 (count results))
            ok-writes (filter #(and (= :write (:f %)) (= :ok (:type %))) data)
            ok-reads  (filter #(and (= :read (:f %)) (= :ok (:type %))) data)
            valid?    (if (seq results)
                        (and (empty? all-fails) converged (>= min-ratio 0.5))
                        true)
            ;; Global + aggregate summary. For the legacy single-register
            ;; workload the single group's fields are merged to the top level
            ;; (previous output shape preserved).
            stats     (merge {:ops          (count ops)
                              :ok           (:ok by-type 0)
                              :info         (:info by-type 0)
                              :fail         (:fail by-type 0)
                              :writes-ok    (count ok-writes)
                              :reads-ok     (count ok-reads)
                              :disruptions  (count (filter #(contains?
                                                              #{:kill :pause
                                                                :partition}
                                                              (:f %))
                                                           ops))
                              :regions      per-region}
                             (when single? (first (vals per-region)))
                             {:final-converged       converged
                              :verification-ok-ratio min-ratio
                              :max-committed         max-comm})]
        (info "soak checker:"
              (pr-str (assoc stats :failures all-fails)))
        ;; P0-3: gate on verification-phase read availability (>= 0.5) per
        ;; region; healthy soaks read until :ok, so the ratio is normally 1.0.
        (cond-> {:valid? valid?
                 :soak   stats}
          (seq all-fails) (assoc :failures (take 10 all-fails)))))))
