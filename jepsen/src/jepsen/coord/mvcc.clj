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
    6. txn atomicity: for a read that covers every key of an :ok txn, the
       write set is all-or-none *at that read_ts*: keys the txn still owns
       (its commit_ts is the latest write <= read_ts) must show the txn's
       value. A key superseded by a later write is not evidence either way;
       a half-applied txn shows up as: one key still ours and correct, another
       key still ours but missing/stale. An :info txn with unknown commit_ts
       cannot be placed -- the ledger-sum check is its backstop (a half
       transfer changes the sum);
    7. convergence (:final true reads, emitted after the last nemesis stop):
       the echoed safe_ts must cover every committed write, and the value must
       be the latest :ok write. Without the read_ts bar a safe_ts that never
       catches up is invisible -- read_ts=0 reads of a stalled clock echo the
       stale ts and are judged self-consistently;
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
;; Historical-read sampling premise
;; ---------------------------------------------------------------------------

(def default-history-unavailable-cap
  "设计 §5.4：历史读上的 `HISTORY_UNAVAILABLE` 是合法结果（fail-closed、不会
  给错值），但必须计入失败率上限：超过它说明「历史读真的在采样」这个前提不
  成立，整轮 run 不能当证据（不是正确性问题，是取证问题）。

  分母只算**显式历史读**（`read_ts > 0`）：`read_ts = 0` 是 safe_ts 快照读，
  而压缩水位恒 `< safe_ts`，它碰不到保留窗口，混进分母只会稀释比率。"
  0.01)

(defn history-unavailable-stats
  "历史读失败率：`{:historical-reads n :unavailable m :ratio r :cap c
  :judged bool}`。

  `judged` 为 false 表示这段历史里没有显式历史读 —— 调用方必须显式说不判
  （`:not-judged`），不能把 0/0 当 0%。这就是 #311 的核心：数字要进证据面，
  而不是安静地缺席。"
  ([history] (history-unavailable-stats history default-history-unavailable-cap))
  ([history cap]
   (let [read-ts-of (fn [op] (long (or (:requested-read-ts op) (:read-ts op) 0)))
         historical (count (filter #(and (= :v2-read (:f %))
                                         (= :invoke (:type %))
                                         (pos? (read-ts-of %)))
                                   history))
         unavailable (count (filter #(and (= :v2-read (:f %))
                                          (= :info (:type %))
                                          (= :history-unavailable (:error %))
                                          (pos? (read-ts-of %)))
                                    history))]
     {:historical-reads historical
      :unavailable unavailable
      :ratio (when (pos? historical) (/ (double unavailable) historical))
      :cap (double cap)
      :judged (pos? historical)})))

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
        ;; or the last :ok value is legal -- including on a `:final` read,
        ;; which may legitimately observe an `:info` write that did apply.
        ;; Final reads are tightened by `check-final-convergence` (the echoed
        ;; safe_ts must cover every committed write), not by narrowing this
        ;; set.
        unplaced  (get unknown-values key #{})
        ;; A committed delete makes the key ABSENT: kv.v2 Range only returns
        ;; live keys (coord-server/src/kvt/services.rs read_kv_range_segment),
        ;; while a returned tombstone would carry value "". Both are legal.
        value-of  (fn [w] (if (= :delete (:kind w)) #{nil ""} #{(:value w)}))
        needed    (cond-> (into (if last-ok (value-of last-ok) #{nil})
                                (mapcat value-of cand-ts))
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
  (let [sums (atom 0)]
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
        (swap! sums inc)
        (let [vals (keep (comp parse-long* :value) (:kvs op))]
          ;; Fail-closed: the read is whole-ledger by construction, so an
          ;; empty/partial result sums to 0 instead of `total` and is a
          ;; violation, not "no data to check".
          (when (not= (reduce + vals) total)
            (invalid errors (format "ledger-sum: read %s (%s keys), expected %s"
                                    (reduce + vals) (count vals) total))))))
    {:ledger-sums @sums}))

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

(defn- check-final-convergence
  "The soak's convergence gate: after the last nemesis stop and the recovery
  window, every `:final` read must have echoed a safe_ts that covers every
  committed write, and at least one final read must have succeeded.

  Why the read_ts bar and not just the value: a `read_ts=0` read of a stalled
  safe_ts returns an old-but-legal snapshot (the checker judges it against the
  ts it echoed), so a cluster whose safe_ts never catches up would stay green
  forever. The fence advances safe_ts to the TSO high-water every second, so
  on a healthy cluster the bar is met within the recovery window; not meeting
  it is a liveness failure, not a slow test.

  `:info` writes do not set the bar -- their commit_ts may not exist."
  [errors history]
  (let [max-ok-ts (->> history
                       (filter #(and (= :ok (:type %))
                                     (contains? #{:v2-write :v2-txn} (:f %))))
                       (keep :commit-ts)
                       (map long)
                       (reduce max 0))
        finals    (filter #(and (= :v2-read (:f %)) (:final %)) history)]
    (when (and (seq finals) (not (some #(= :ok (:type %)) finals)))
      (invalid errors "final-reads-never-succeeded"))
    (doseq [op finals
            :when (= :ok (:type op))]
      (when (< (long (:read-ts op)) max-ok-ts)
        (invalid errors
                 (format "final-read-lagging: %s echoed safe_ts %s < last commit_ts %s"
                         (:key op) (:read-ts op) max-ok-ts))))
    {:final-reads (count (filter #(= :ok (:type %)) finals))}))

(defn- last-ok-write-at
  "Latest :ok write for `key` with ts <= R, or nil."
  [writes-by-key key R]
  (->> (get writes-by-key key [])
       (filter :ok?)
       (filter #(<= (long (:ts %)) (long R)))
       last))

(defn- check-txn-atomicity
  "A committed txn's write set must be visible as a block *at a given read_ts*.

  Only keys the txn still owns count: a key overwritten by a later txn is
  superseded and says nothing about atomicity (a half-applied txn whose keys
  were later overwritten would otherwise look green). A violation is: some
  owned key shows the txn's value while another owned key is missing/stale.

  This deliberately does NOT judge a read at read_ts < commit_ts by matching
  values: ledger balances repeat, so `read value == txn value` is not evidence
  of early visibility. Per-key snapshot expectations (`check-read`) cover
  early visibility on keys the read actually returns."
  [errors writes-by-key history]
  (let [judged (atom 0)
        txns  (filter #(and (= :v2-txn (:f %)) (= :ok (:type %))
                            (:commit-ts %) (seq (:writes %)))
                      history)
        ;; Only :ok reads have an echoed read_ts; a failed range read still
        ;; carries :covers on the invoke/completion op, so filtering by
        ;; :covers alone would NPE on (:read-ts r) during a disruption.
        reads (filter #(and (= :v2-read (:f %)) (= :ok (:type %))
                            (seq (:covers %)))
                      history)]
    (doseq [t txns
            r reads
            :let [C     (long (:commit-ts t))
                  R     (long (:read-ts r))
                  seen  (into {} (map (juxt :key :value) (:kvs r)))
                  cover (set (:covers r))]
            :when (and (>= R C) (every? cover (keys (:writes t))))]
      (swap! judged inc)
      (let [states (into {}
                         (for [[k v] (:writes t)
                               :let [w (last-ok-write-at writes-by-key k R)]]
                           [k (cond
                                (nil? w)                :unindexed
                                (not= (long (:ts w)) C) :superseded
                                (= v (get seen k))      :visible
                                :else                   :half)]))]
        (when (and (some #{:visible} (vals states))
                   (some #{:half} (vals states)))
          (invalid errors
                   (format "txn-half-visible: txn %s at read_ts %s (commit_ts %s) %s"
                           (:txn-id t) R C (pr-str states))))))
    {:txn-atomic-pairs @judged}))

;; ---------------------------------------------------------------------------
;; Checker
;; ---------------------------------------------------------------------------

(defn checker
  "Builds the MVCC checker. Thresholds are fail-closed: below any of them the
  history is invalid (`:insufficient-sample`), because a run that barely
  happened must never look green.

  历史读失败率（设计 §5.4）也是判据：显式历史读到 `HISTORY_UNAVAILABLE` 是
  合法结果，但超过 `:max-history-unavailable-ratio`（默认 1%）说明「历史读
  真的在采样」这个前提不成立 ⇒ `:reason :history-unavailable-rate`。这条
  指标过去只存在于设计文档里，72h run 上 10.08% 无人判红（#311）；现在它
  进 `results.edn`（`:history`）并由 checker 判，不再靠人工算。"
  [{:keys [min-writes min-reads min-ts max-history-unavailable-ratio]
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
            hist (history-unavailable-stats
                  history
                  (or max-history-unavailable-ratio default-history-unavailable-cap))
            errors (atom [])]
        (let [judged (merge (check-reads errors writes-by-key unknown history)
                            (check-final-convergence errors history)
                            (check-txn-atomicity errors writes-by-key history))
              _      (check-safe-ts errors history)
              _      (check-tso errors history)
              _      (check-historical-echo errors history)
              sample {:writes (+ writes deletes txns) :reads (+ reads txn-reads) :ts tss}
              enough? (and (>= (:writes sample) min-writes)
                           (>= (:reads sample) min-reads)
                           (>= (:ts sample) min-ts))
              hist-fail? (and (:judged hist)
                              (> (double (:ratio hist)) (double (:cap hist))))
              errs @errors]
          (cond
            (seq errs)
            {:valid? false :reason :mvcc-violation :history hist
             :mvcc {:sample sample :judged judged
                    :errors (take 20 errs) :errors-count (count errs)}}

            (not enough?)
            {:valid? false :reason :insufficient-sample :history hist
             :mvcc {:sample sample :judged judged
                    :need {:writes min-writes :reads min-reads :ts min-ts}}}

            ;; 采样前提不成立时，历史读本身仍然一条没错（HISTORY_UNAVAILABLE
            ;; 是 fail-closed 的合法结果）；判红的是「这一轮没有在真正采历史读」。
            hist-fail?
            {:valid? false :reason :history-unavailable-rate :history hist
             :mvcc {:sample sample :judged judged
                    :errors [] :errors-count 0}}

            :else
            {:valid? true :history hist
             :mvcc {:sample sample :judged judged
                    :errors [] :errors-count 0}}))))))
