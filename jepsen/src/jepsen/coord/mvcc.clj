(ns jepsen.coord.mvcc
  "Deterministic MVCC checker for the KV-T v2 data plane (#272).

  Why not knossos: v2 writes carry their `commit_ts` and reads echo the
  `read_ts` they used, so \"what should this read have seen\" is computable --
  the last committed write with `commit_ts <= read_ts`. That makes the oracle
  O(n log n) and exact; a single-register model cannot express multi-version
  expectations (and would have to search).

  Op shape (shared with the kvv2 / txnv2 workloads; fixtures use the same):

    {:type :invoke|:ok|:info|:fail
     :f    :v2-write|:v2-delete|:v2-read|:v2-txn|:v2-ts|:nemesis
     :time <monotonic ns> :process p :node n
     ;; direct write/delete:
     :key :value :commit-ts :request-id :retried [code…]
     ;; :info direct write: :attempted-ts [t1 t2 …] (every ts it tried)
     ;; read (point): :key :read-ts :value :mod-revision :create-revision
     ;; read (range):  :kvs [{:key :value :mod-revision …}] :read-ts
     ;; txn: :txn-id :start-ts :commit-ts(optional) :writes {k v} :reads {k v}
     ;;      :info txn may carry :ts-unknown true and :attempted-ts
     ;; final convergence reads: :final true
     ;; ledger reads: :ledger-total n}

  What it judges (and what it does not):
    1. snapshot reads: the value must be the last :ok write with
       commit_ts <= read_ts; an :info write only widens the allowed set to its
       candidate values (it may or may not have applied);
    2. reads never see the future (:mod-revision > read_ts is invalid);
    3. fabricated values / revisions are invalid;
    4. safe_ts monotonicity for read_ts=0 reads, per (process, node), reset on
       nemesis :start (restart) -- explicit historical reads only have to echo
       the requested ts;
    5. TSO: strictly increasing per process; distinct commit_ts across :ok
       writes (a duplicate means the premise does not hold -> invalid);
    6. txn atomicity: all-or-none of the write set, for any read_ts;
       an :info txn with unknown commit_ts is only judged all-or-none;
    7. final reads (:final true) must see the latest :ok write -- safe_ts that
       never catches up is a liveness failure;
    8. sample thresholds, fail-closed (insufficient sample is invalid, never
       green). The ts threshold is lower on purpose: TSO ops are ~10% of the
       mix, snapshot reads are 25%, and a txn is four RPCs (so txnv2 does
       ~2.3 txns/s/worker): a 45s matrix cell clears 100 writes/reads/ts.

  Not judged: watch/lease/storage v2, split/merge windows (the lab runs with
  the PD scheduler paused), cross-partition ordering."
  (:require [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; History indexing
;; ---------------------------------------------------------------------------

(defn- parse-long* [v]
  (cond
    (nil? v) nil
    (number? v) (long v)
    (string? v) (try (Long/parseLong (str/trim v)) (catch NumberFormatException _ nil))
    :else nil))

(defn- absolute-ts
  "All timestamps this write op may have applied under. `:ok` => [commit-ts];
  `:info` => every attempted ts (COMMIT_REJECTED retries leave several)."
  [op]
  (cond
    (= :ok (:type op)) (when-let [ts (:commit-ts op)] [ts])
    (= :info (:type op)) (->> (or (seq (:attempted-ts op))
                                  (when (:commit-ts op) [(:commit-ts op)]))
                              (remove nil?)
                              vec)
    :else []))

(defn- txn-writes
  "Flattens a txn op's write set into per-key records (one commit_ts)."
  [op]
  (let [ts  (:commit-ts op)
        w   (:writes op)]
    (if (and ts (seq w))
      (for [[k v] w]
        {:key k :value v :ts ts :kind :write :txn-id (:txn-id op)})
      [])))

(defn- index-writes
  "key -> vector of {:key :value :ts :kind :ok? :txn-id}, sorted by ts."
  [history]
  (let [recs (for [op history
                   :let [kind (:f op)]
                   :when (contains? #{:v2-write :v2-delete :v2-txn} kind)
                   :when (not= :invoke (:type op))]
               (cond
                 (= :v2-txn kind)
                 (for [r (txn-writes op)]
                   (assoc r :ok? (= :ok (:type op))))

                 :else
                 (let [delete? (= :v2-delete kind)
                       v (if delete? "" (:value op))]
                   (for [ts (absolute-ts op)]
                     {:key (:key op) :value v :ts ts
                      :kind (if delete? :delete :write)
                      :ok? (= :ok (:type op)) :request-id (:request-id op)}))))]
    (->> (apply concat recs)
         (sort-by :ts)
         (group-by :key)
         (into {} (map (fn [[k rs]] [k (vec rs)]))))))

;; ---------------------------------------------------------------------------
;; Checks
;; ---------------------------------------------------------------------------

(defn- invalid [errors msg] (swap! errors conj msg))

(defn- unknown-ts-txn-values
  "key -> set of values written by an :info txn whose commit_ts could not be
  recovered. Those keys have no placeable timestamp, so a read may legally show
  their value (or the previous one) and its mod_revision cannot be matched to a
  known write."
  [history]
  (->> history
       (filter #(and (= :v2-txn (:f %)) (= :info (:type %)) (:ts-unknown %)))
       (mapcat :writes)
       (reduce (fn [m [k v]] (update m k (fnil conj #{}) v)) {})))

(defn- check-read
  "Checks one read observation (point or range entry) at read_ts R."
  [errors writes-by-key unknown-values
   {:keys [key value read-ts mod-revision final] :as obs}]
  (let [rs        (get writes-by-key key [])
        ok-writes (filter :ok? rs)
        candidates (remove :ok? rs)
        before    (filter #(<= (:ts %) read-ts) ok-writes)
        last-ok   (last before)
        cand-ts   (filter #(<= (:ts %) read-ts) candidates)
        ;; An :info write may or may not have applied; seeing either its value
        ;; or the last :ok value is legal. Final reads come after everything
        ;; settled, so they must be exact.
        unplaced  (get unknown-values key #{})
        ;; A committed delete makes the key ABSENT: kv.v2 Range only returns
        ;; live keys (coord-server/src/kvt/services.rs read_kv_range_segment),
        ;; while a returned tombstone would carry value "". Both are legal.
        value-of  (fn [w] (if (= :delete (:kind w)) #{nil ""} #{(:value w)}))
        needed    (cond-> (if final
                            (if last-ok (value-of last-ok) #{nil})
                            (into (if last-ok (value-of last-ok) #{nil})
                                  (mapcat value-of cand-ts)))
                    (seq unplaced) (into unplaced))
        known-ts  (set (map :ts rs))]
    ;; 2. no read from the future
    (when (and mod-revision (> (long mod-revision) (long read-ts)))
      (invalid errors (format "read-from-future: %s mod_revision %s > read_ts %s"
                              key mod-revision read-ts)))
    ;; 3. fabricated revision
    (when (and mod-revision (empty? unplaced)
               (not (contains? known-ts (long mod-revision))))
      (invalid errors (format "fabricated-revision: %s mod_revision %s matches no write"
                              key mod-revision)))
    ;; 1. value must be one of the legal snapshots
    (when-not (contains? needed (or value nil))
      (invalid errors
               (format "stale-or-fabricated: %s at read_ts %s saw %s, legal %s%s"
                       key read-ts (pr-str value) (pr-str needed)
                       (if final " (final read)" ""))))
    nil))

(defn- check-reads
  [errors writes-by-key unknown-values history]
  (doseq [op history
          :when (and (= :v2-read (:f op)) (= :ok (:type op)))]
    (let [R (long (:read-ts op))]
      (if-let [kvs (seq (:kvs op))]
        (doseq [obs kvs]
          (check-read errors writes-by-key unknown-values
                      (assoc obs :read-ts R :final (:final op))))
        (check-read errors writes-by-key unknown-values
                    {:key (:key op) :value (:value op) :read-ts R
                     :mod-revision (:mod-revision op) :final (:final op)})))
    ;; 6b. ledger sum conservation (txnv2): the read set covers every account
    (when-let [total (parse-long* (:ledger-total op))]
      (let [vals (keep (comp parse-long* :value) (:kvs op))]
        (when (and (seq vals) (not= (reduce + vals) total))
          (invalid errors (format "ledger-sum: read %s, expected %s"
                                  (reduce + vals) total)))))))

(defn- check-safe-ts
  "read_ts=0 reads must not regress per (process, node); the baseline resets on
  a nemesis :start (restart / partition start both restart the local view)."
  [errors history]
  (let [base (atom {})]
    (doseq [op history]
      (cond
        (and (= :nemesis (:f op))
             (= :start (:nemesis op)))
        (reset! base {})

        (and (= :v2-read (:f op))
             (= :ok (:type op))
             (or (nil? (:requested-read-ts op)) (zero? (long (:requested-read-ts op)))))
        (let [k [(str (:process op)) (str (:node op))]
              R (long (:read-ts op))
              prev (get @base k)]
          (when (and prev (< R (long prev)))
            (invalid errors (format "safe-ts-regression: %s on %s echoed %s after %s"
                                    (:process op) (:node op) R prev)))
          (swap! base update k (fnil max 0) R))))))

(defn- check-tso
  [errors history]
  (let [by-proc (->> history
                     (filter #(and (= :v2-ts (:f %)) (= :ok (:type %))))
                     (group-by :process))]
    (doseq [[p ops] by-proc]
      (let [tss (map :ts ops)]
        (when-not (apply <= (cons Long/MIN_VALUE tss))
          (invalid errors (format "tso-regression: process %s %s" p (pr-str tss))))))
    ;; premise: an :ok write's commit_ts is unique (two writes sharing one ts
    ;; means the history itself is broken, not just a stale read)
    (let [ok-writes (for [op history
                          :when (and (= :v2-write (:f op)) (= :ok (:type op)))
                          :let [ts (:commit-ts op)]]
                      [ts (:request-id op)])
          dups (->> ok-writes
                    (group-by first)
                                          (filter (fn [[_ v]] (> (count (distinct (map second v))) 1))))]
      (doseq [[ts rs] dups]
        (invalid errors (format "shared-commit-ts: %s used by request-ids %s"
                                ts (pr-str (map second rs))))))))

(defn- check-historical-echo
  "Explicit historical reads must echo the requested read_ts."
  [errors history]
  (doseq [op history
          :when (and (= :v2-read (:f op)) (= :ok (:type op))
                     (some? (:requested-read-ts op))
                     (pos? (long (:requested-read-ts op))))]
    (when (not= (long (:requested-read-ts op)) (long (:read-ts op)))
      (invalid errors (format "historical-echo: asked read_ts %s, echoed %s"
                              (:requested-read-ts op) (:read-ts op))))))

(defn- check-txn-atomicity
  "A committed txn's write set is visible as a block: for a range read that
  covers every key of the write set, `read_ts >= commit_ts` must show all of
  them and `read_ts < commit_ts` must show none."
  [errors history]
  (let [txns  (filter #(and (= :v2-txn (:f %)) (= :ok (:type %))
                            (:commit-ts %) (seq (:writes %)))
                      history)
        reads (filter #(and (= :v2-read (:f %)) (seq (:kvs %)) (seq (:covers %)))
                      history)]
    (doseq [t txns
            r reads
            :let [C     (long (:commit-ts t))
                  R     (long (:read-ts r))
                  seen  (into {} (map (juxt :key :value) (:kvs r)))
                  cover (:covers r)]
            :when (every? (set cover) (keys (:writes t)))]
      (doseq [[k v] (:writes t)]
        (cond
          (and (>= R C) (not= v (get seen k)))
          (invalid errors (format "txn-half-visible: txn %s key %s missing/stale at read_ts %s (commit_ts %s)"
                                  (:txn-id t) k R C))

          (and (< R C) (= v (get seen k)))
          (invalid errors (format "txn-visible-before-commit: txn %s key %s visible at read_ts %s < commit_ts %s"
                                  (:txn-id t) k R C)))))))

(defn- check-unknown-ts-txn
  "An :info txn whose commit_ts could not be recovered must still be
  all-or-none within one range observation."
  [errors history]
  (let [unknown (->> history
                     (filter #(and (= :v2-txn (:f %)) (= :info (:type %)) (:ts-unknown %)))
                     (filter #(seq (:writes %))))
        reads   (filter #(and (= :v2-read (:f %)) (seq (:kvs %))) history)]
    (doseq [t unknown
            r reads
            :let [w (:writes t)
                  seen (into {} (map (juxt :key :value) (:kvs r)))
                  hits (for [[k v] w
                             :when (contains? seen k)]
                         (= v (get seen k)))
                  total (count w)]
            :when (>= (count seen) total)]
      (when (and (some true? hits) (some false? hits))
        (invalid errors (format "txn-half-visible: txn %s partially visible at read_ts %s"
                                (:txn-id t) (:read-ts r)))))))

;; ---------------------------------------------------------------------------
;; Checker
;; ---------------------------------------------------------------------------

(defn checker
  "Builds the MVCC checker. Thresholds are fail-closed: below any of them the
  history is invalid (`:insufficient-sample`), because a run that barely
  happened must never look green."
  [{:keys [min-writes min-reads min-ts]
    :or {min-writes 100 min-reads 100 min-ts 50}}]
  (reify jepsen.checker/Checker
    (check [_ _test history _opts]
      (let [writes-by-key (index-writes history)
            writes (count (filter #(and (= :v2-write (:f %)) (= :ok (:type %))) history))
            deletes (count (filter #(and (= :v2-delete (:f %)) (= :ok (:type %))) history))
            txns (count (filter #(and (= :v2-txn (:f %)) (= :ok (:type %))) history))
            reads (count (filter #(and (= :v2-read (:f %)) (= :ok (:type %))) history))
            ;; txnv2 reads inside the transaction (point reads of the accounts);
            ;; counting them keeps the read threshold meaningful for that workload.
            txn-reads (reduce + 0 (for [op history
                                        :when (and (= :v2-txn (:f op)) (= :ok (:type op)))]
                                    (count (:reads op))))
            tss (count (filter #(and (= :v2-ts (:f %)) (= :ok (:type %))) history))
            unknown (unknown-ts-txn-values history)
            errors (atom [])]
        (check-reads errors writes-by-key unknown history)
        (check-safe-ts errors history)
        (check-tso errors history)
        (check-historical-echo errors history)
        (check-txn-atomicity errors history)
        (check-unknown-ts-txn errors history)
        (let [sample {:writes (+ writes deletes txns) :reads (+ reads txn-reads) :ts tss}
              enough? (and (>= (:writes sample) min-writes)
                           (>= (:reads sample) min-reads)
                           (>= (:ts sample) min-ts))
              errs @errors]
          (cond
            (seq errs)
            {:valid? false :reason :mvcc-violation
             :mvcc {:sample sample :errors (take 20 errs) :errors-count (count errs)}}

            (not enough?)
            {:valid? false :reason :insufficient-sample
             :mvcc {:sample sample :need {:writes min-writes :reads min-reads :ts min-ts}}}

            :else
            {:valid? true
             :mvcc {:sample sample :errors [] :errors-count 0}}))))))
