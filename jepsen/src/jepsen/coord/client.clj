(ns jepsen.coord.client
  "gRPC client for coord.

  Implements the register / cas-register operations with leader discovery:
  on UNAVAILABLE (not leader / election in progress) it rotates to the next
  node; on DEADLINE_EXCEEDED it records :info (the write may or may not have
  been applied); on UNAUTHENTICATED it refreshes the session (coord CCTs
  expire after ~1h) and retries, recording :fail only if the fresh token is
  rejected too."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]]
            [jepsen.coord.proto :as p])
  (:import [io.grpc ManagedChannel Status Status$Code StatusRuntimeException]
           [jepsen.coord CoordRpc]))

(def ^:private ^:const register-key "/jepsen/register")
(def ^:private ^:const rpc-timeout-ms 5000)
(def ^:private ^:const auth-timeout-ms 60000)

;; Values the clients have recently observed; the cas-register generator draws
;; `old` from this pool so CASes frequently target the current register value.
(def seen
  "Atom of recently-observed register values (bounded window)."
  (atom #{nil}))

(defn- reset-seen!
  []
  (reset! seen #{nil}))

(defn- observe!
  "Records a value as recently observed (bounded to the last 200)."
  [v]
  (when (some? v)
    (swap! seen (fn [s]
                  (-> (conj s v)
                      (->> (take-last 200))
                      (set))))))

(defn- status-code
  "gRPC status code of a StatusRuntimeException. (StatusRuntimeException has
  no getCode() in grpc 1.68 -- go through getStatus.)"
  ^Status$Code [^StatusRuntimeException e]
  (.. e getStatus getCode))

(defn- timeout?
  [^StatusRuntimeException e]
  (= Status$Code/DEADLINE_EXCEEDED (status-code e)))

(defn- unauthenticated?
  [^StatusRuntimeException e]
  (= Status$Code/UNAUTHENTICATED (status-code e)))

(defn- unavailable?
  [^StatusRuntimeException e]
  (= Status$Code/UNAVAILABLE (status-code e)))

(defn- resource-exhausted?
  "Login rate limiter (per-IP token bucket) rejection -- retryable with
  backoff, NOT a final failure."
  [^StatusRuntimeException e]
  (= Status$Code/RESOURCE_EXHAUSTED (status-code e)))

(defn- request-id
  []
  (str (System/currentTimeMillis) "-" (rand-int Integer/MAX_VALUE)))

(defn- parse-value
  "Parses a value string read from the register into an integer (nil if empty)."
  [^String s]
  (when (and s (not (.isEmpty s)))
    (Long/parseLong s)))

(defn- authenticate!
  "Authenticates as root against any node, rotating through all channels until
  one succeeds. Throws after auth-timeout-ms. RESOURCE_EXHAUSTED (login rate
  limiter) is retried with a backoff instead of being treated as final:
  the per-IP token bucket refills at ~0.5 tokens/s, so a 2s sleep between
  attempts lets legitimate login bursts through."
  [channels password]
  (let [req      (p/auth-req "root" password)
        n        (count channels)
        deadline (+ (System/currentTimeMillis) auth-timeout-ms)]
    (loop [attempt 0]
      (if (< (System/currentTimeMillis) deadline)
        (let [ch  (nth channels (mod attempt n))
              res (try
                    {:ok (p/cct (p/call ch p/authenticate req))}
                    (catch StatusRuntimeException e
                      {:err e}))]
          (if-let [cct (:ok res)]
            cct
            (if (or (unavailable? (:err res))
                    (timeout? (:err res))
                    (resource-exhausted? (:err res)))
              (do
                (when (resource-exhausted? (:err res))
                  ;; 登录限流：退避 2s 等令牌回补，避免把服务端防爆破
                  ;; 限流当最终失败抛给调用方。
                  (Thread/sleep 2000))
                (recur (inc attempt)))
              (throw (:err res)))))
        (throw (ex-info "Failed to authenticate to coord" {:channels channels}))))))

(defn- reauthenticate!
  "Fetches a fresh CCT (coord tokens expire after ~1h) and swaps in new auth
  channels for every node. Returns the new CCT."
  [this]
  (let [raw-channels @(:raw-channels this)
        cct          (authenticate! raw-channels (:root-password this))
        channels     (mapv #(p/auth-channel % cct) raw-channels)]
    (reset! (:cct this) cct)
    (reset! (:channels this) channels)
    cct))

(defn- not-leader?
  "True if a StatusRuntimeException is a definitive 'not leader' rejection
  (the server rejected the write before applying it)."
  [^StatusRuntimeException e]
  (boolean (re-find #"(?i)not[ _-]?leader" (str (.getMessage e)))))

(defn- forward-request?
  "True if a StatusRuntimeException is the linearizable-read path's 'not
  leader, forward to current leader' rejection. The server surfaces this as
  INTERNAL ('linearizable read failed: has to forward request to ...') rather
  than UNAVAILABLE, so it must be detected by message."
  [^StatusRuntimeException e]
  (boolean (re-find #"(?i)has to forward request|forward request to"
                    (str (.getMessage e)))))

(defn- op-key
  "The register key an op targets: an explicit :key (multi-register ops) or
  the single legacy register key."
  [op]
  (or (:key op) register-key))

(defn- leader-start
  "Cached leader node index for `key` (0 when unknown)."
  [this key]
  (get @(:leader-idx this) key 0))

(defn- leader-seen!
  "Records that node index `idx` served `key` successfully (per-region/per-key
  leader cache: in multi-region mode each register key may be led by a
  different node)."
  [this key idx]
  (swap! (:leader-idx this) assoc key idx))

(defn- try-nodes
  "Attempts (f channel) against nodes starting from the cached leader index
  for `key`. Rotates through UNAVAILABLE responses (and the read path's
  INTERNAL 'forward request' rejection). Returns {:ok response :node node} on
  success (and advances the per-key leader index), or {:err StatusRuntimeException
  :all-not-leader? bool} when every node failed."
  [this key f]
  (let [n     (count @(:channels this))
        start (leader-start this key)]
    (loop [i 0, last-err nil, all-not-leader? true]
      (if (< i n)
        (let [idx (mod (+ start i) n)
              ch  (nth @(:channels this) idx)
              res (try
                    (let [resp (f ch)]
                      (leader-seen! this key idx)
                      {:ok resp, :node (nth (:nodes this) idx)})
                    (catch StatusRuntimeException e
                      {:err e}))]
          (if-let [resp (:ok res)]
            res
            (if (or (unavailable? (:err res)) (forward-request? (:err res)))
              (recur (inc i) (:err res)
                     (and all-not-leader?
                          (or (not-leader? (:err res))
                              (forward-request? (:err res)))))
              res)))
        {:err last-err, :all-not-leader? all-not-leader?}))))

(defn- result-op
  "Maps an invocation op + either {:ok resp :node n} or {:err e} to the
  completion op, using the doc's :ok/:fail/:info mapping rules.

  For writes/CAS the critical rule is: a failure that *may* have taken effect
  (timeout, connection-level UNAVAILABLE) is recorded as :info, never :fail --
  otherwise a committed-but-misreported write makes later reads look like
  anomalies."
  [op res ok-fn]
  (if-let [resp (:ok res)]
    (ok-fn resp)
    (let [e (:err res)]
      (cond
        (timeout? e)         (assoc op :type :info :error :timeout)
        (unauthenticated? e) (assoc op :type :fail :error :unauthenticated)
        (:all-not-leader? res)
                             (assoc op :type :fail :error :not-leader)
        :else                (assoc op :type :info
                                         :error :unavailable-maybe-applied)))))

(defn- invoke-read [this op]
  (let [key (op-key op)
        res (try-nodes this key #(p/call % p/kv-range (p/range-req key)))]
    (result-op op res
               (fn [resp]
                 (let [kvs   (p/range-kvs resp)
                       value (if (seq kvs)
                               (parse-value (p/kv-value (first kvs)))
                               nil)]
                   (observe! value)
                   (assoc op :type :ok :value value
                              :node (:node res)))))))

(defn- invoke-write [this op]
  (let [key (op-key op)
        v   (:value op)
        rid (request-id)
        req (p/put-req key (str v) rid)]
    (result-op op
               (try-nodes this key #(p/call % p/put req))
               (fn [_]
                 (observe! v)
                 (assoc op :type :ok :value v)))))

(defn- invoke-cas [this op]
  (let [[old new] (:value op)
        old-str   (if (nil? old) "" (str old))
        rid       (request-id)
        req       (p/txn-req
                   [(p/compare-value-equal register-key old-str)]
                   [(p/request-put-op (p/put-req register-key (str new) rid))]
                   []
                   rid)]
    (result-op op
               (try-nodes this register-key #(p/call % p/txn req))
               (fn [resp]
                 (if (p/txn-succeeded resp)
                   (do (observe! new)
                       (assoc op :type :ok :value [old new]))
                   ;; CAS miss: the register value was not `old`. Legitimate
                   ;; failure, not a bug -- knossos drops :fail ops.
                   (assoc op :type :fail :value [old new] :error :cas-miss))))))

(defn- invoke-coord!
  "Runs op, transparently re-authenticating and retrying when the cluster
  rejects our CCT as UNAUTHENTICATED (coord CCTs expire after ~1h; a rejected
  request is never applied, so retrying after a fresh login is safe)."
  [this test op]
  (let [attempt (fn []
                  (case (:f op)
                    :read  (invoke-read this op)
                    :write (invoke-write this op)
                    :cas   (invoke-cas this op)
                    (assoc op :type :fail :error (str "unknown op " (:f op)))))]
    (loop [re-auths-left 2]
      (let [op' (attempt)]
        (if (and (pos? re-auths-left)
                 (= :fail (:type op'))
                 (= :unauthenticated (:error op')))
          (let [refreshed? (try
                             (info "CCT rejected as unauthenticated — refreshing session")
                             (reauthenticate! this)
                             true
                             (catch Exception e
                               (warn e "Session refresh failed; keeping op result")
                               false))]
            (if refreshed?
              (recur (dec re-auths-left))
              op'))
          op')))))

;; --------------------------------------------------------------------------

(defrecord CoordClient [nodes root-password cct channels raw-channels leader-idx]
  client/Client
  (open! [this test node]
    (info "Opening coord client on" node)
    (let [raw-channels (mapv p/channel nodes)]
      (try
        (let [cct      (authenticate! raw-channels root-password)
              channels (mapv #(p/auth-channel % cct) raw-channels)]
          (assoc this :cct (atom cct)
                      :channels (atom channels)
                      :raw-channels (atom raw-channels)
                      :leader-idx (atom {})))
        (catch Exception e
          ;; Don't leak gRPC channels if auth fails partway through.
          (doseq [^ManagedChannel ch raw-channels]
            (.shutdownNow ch))
          (throw e)))))

  (setup! [this test]
    (info "coord client setup: waiting for a successful read (quorum + auth)")
    (let [res (loop [i 0]
                (if (< i 60)
                  (let [res (try
                              (try-nodes this register-key
                                         #(p/call % p/kv-range (p/range-req register-key)))
                              (catch Exception e {:err e}))]
                    (if (:ok res)
                      res
                      (do (Thread/sleep 1000)
                          (recur (inc i)))))
                  nil))]
      (when-not res
        (throw (ex-info "coord cluster did not become ready (no successful read)" {})))
      (info "coord cluster is ready"))
    this)

  (invoke! [this test op]
    (invoke-coord! this test op))

  (close! [this test]
    (doseq [^ManagedChannel ch @(:raw-channels this)]
      (.shutdownNow ch)))

  (teardown! [this test] this))

(defn coord-client
  "Builds the (template) coord client. Channels are opened per-worker in
  open!."
  [opts]
  (reset-seen!)
  (->CoordClient (vec (:nodes opts))
                 (or (:root-password opts) "66c57bb56bce306f484344e4a8650836")
                 nil nil nil (atom {})))
