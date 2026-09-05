(ns jepsen.coord.proto
  "Clojure-friendly helpers over the CoordRpc gRPC layer: request builders,
  response readers, and call helpers. All gRPC errors surface as
  io.grpc.StatusRuntimeException."
  (:import [com.google.protobuf ByteString DynamicMessage]
           [io.grpc ManagedChannel Channel MethodDescriptor Status Status$Code StatusRuntimeException]
           [jepsen.coord CoordRpc]))

(def grpc-port 50051)

;; Method descriptors ------------------------------------------------------

(def put CoordRpc/PUT)
(def kv-range CoordRpc/RANGE)
(def delete CoordRpc/DELETE)
(def txn CoordRpc/TXN)
(def status CoordRpc/STATUS)
(def authenticate CoordRpc/AUTHENTICATE)

(def ^:private ^:const default-timeout-ms 5000)

;; Channels ----------------------------------------------------------------

(defn channel
  "Opens a plaintext gRPC channel to host:grpc-port."
  [host]
  (CoordRpc/channel host grpc-port))

(defn auth-channel
  "Wraps a channel so every request carries authorization: Bearer <cct>."
  ^Channel [^ManagedChannel ch cct]
  (CoordRpc/withAuth ch cct))

(defn call
  "Blocking unary call. Returns the DynamicMessage response, or throws
  StatusRuntimeException. timeout-ms defaults to 5000."
  ([^Channel ch method req]
   (call ch method req default-timeout-ms))
  ([^Channel ch method req timeout-ms]
   (CoordRpc/call ch method req timeout-ms)))

;; Request builders ---------------------------------------------------------

(defn- bytes-field
  "Returns a DynamicMessage with a single bytes field set."
  [desc field-name ^String s]
  (-> (DynamicMessage/newBuilder desc)
      (.setField (.findFieldByName desc field-name)
                 (ByteString/copyFromUtf8 s))
      (.build)))

(defn put-req
  "PutRequest for key/value/request-id (strings)."
  [^String key ^String value ^String request-id]
  (let [b (DynamicMessage/newBuilder CoordRpc/KV_PUT_REQUEST)
        d CoordRpc/KV_PUT_REQUEST]
    (-> b
        (.setField (.findFieldByName d "key") (ByteString/copyFromUtf8 key))
        (.setField (.findFieldByName d "value") (ByteString/copyFromUtf8 value))
        (.setField (.findFieldByName d "request_id") (ByteString/copyFromUtf8 request-id))
        (.build))))

(defn range-req
  "RangeRequest for an exact key lookup at the latest revision."
  [^String key]
  (bytes-field CoordRpc/KV_RANGE_REQUEST "key" key))

(defn delete-req
  "DeleteRequest for an exact key."
  [^String key request-id]
  (let [b (DynamicMessage/newBuilder CoordRpc/KV_DELETE_REQUEST)
        d CoordRpc/KV_DELETE_REQUEST]
    (-> b
        (.setField (.findFieldByName d "key") (ByteString/copyFromUtf8 key))
        (.setField (.findFieldByName d "request_id") (ByteString/copyFromUtf8 request-id))
        (.build))))

(defn status-req
  "Empty Maintenance/Status request."
  []
  (CoordRpc/message CoordRpc/MAINT_STATUS_REQUEST))

(defn auth-req
  "AuthenticateRequest."
  [^String name ^String password]
  (let [b (DynamicMessage/newBuilder CoordRpc/AUTH_AUTHENTICATE_REQUEST)
        d CoordRpc/AUTH_AUTHENTICATE_REQUEST]
    (-> b
        (.setField (.findFieldByName d "name") name)
        (.setField (.findFieldByName d "password") password)
        (.build))))

(defn- enum-value
  "Looks up an enum value by name from a DynamicMessage's field descriptor."
  [field-desc ^String name]
  (let [enum (.getEnumType field-desc)]
    (.findValueByName enum name)))

(defn compare-value-equal
  "A Compare that checks VALUE == old-value (bytes)."
  [^String key ^String old-value]
  (let [b (DynamicMessage/newBuilder CoordRpc/TXN_COMPARE)
        d CoordRpc/TXN_COMPARE
        result-field (.findFieldByName d "result")
        target-field (.findFieldByName d "target")
        value-field  (.findFieldByName d "value")]
    (-> b
        (.setField result-field (enum-value result-field "EQUAL"))
        (.setField target-field (enum-value target-field "VALUE"))
        (.setField (.findFieldByName d "key") (ByteString/copyFromUtf8 key))
        ;; oneof target_value: setting "value" clears version/mod_revision
        (.setField value-field (ByteString/copyFromUtf8 old-value))
        (.build))))

(defn request-put-op
  "A RequestOp wrapping a PutRequest."
  [put-msg]
  (let [b (DynamicMessage/newBuilder CoordRpc/TXN_REQUEST_OP)
        d CoordRpc/TXN_REQUEST_OP]
    (-> b
        (.setField (.findFieldByName d "request_put") put-msg)
        (.build))))

(defn txn-req
  "TxnRequest: compare (seq of Compare messages), success ops (seq of RequestOp),
  failure ops, request-id."
  [compares success-ops failure-ops ^String request-id]
  (let [b (DynamicMessage/newBuilder CoordRpc/TXN_REQUEST)
        d CoordRpc/TXN_REQUEST]
    (-> b
        (.addAllField (.findFieldByName d "compare") (vec compares))
        (.addAllField (.findFieldByName d "success") (vec success-ops))
        (.addAllField (.findFieldByName d "failure") (vec failure-ops))
        (.setField (.findFieldByName d "request_id") (ByteString/copyFromUtf8 request-id))
        (.build))))

;; Response readers ---------------------------------------------------------

(defn- field
  "Reads a field value from a DynamicMessage by name."
  [^DynamicMessage msg ^String field-name]
  (.getField msg (.findFieldByName (.getDescriptorForType msg) field-name)))

(defn revision
  "Reads the int64 :revision field (put/range/delete/txn responses)."
  [^DynamicMessage msg]
  (field msg "revision"))

(defn status-revision
  "Reads revision from a StatusResponse."
  [^DynamicMessage msg]
  (field msg "revision"))

(defn range-kvs
  "Returns the seq of KeyValue messages in a RangeResponse."
  [^DynamicMessage msg]
  (field msg "kvs"))

(defn kv-value
  "Reads a KeyValue's value as a String (nil if empty)."
  [^DynamicMessage kv]
  (let [bs ^ByteString (field kv "value")]
    (when (and bs (pos? (.size bs)))
      (String. (.toByteArray bs) "UTF-8"))))

(defn kv-version
  "Reads a KeyValue's version field."
  [^DynamicMessage kv]
  (field kv "version"))

(defn txn-succeeded
  "Reads the bool succeeded from a TxnResponse."
  [^DynamicMessage msg]
  (field msg "succeeded"))

(defn cct
  "Reads the cct (session token) from an AuthenticateResponse."
  [^DynamicMessage msg]
  (field msg "cct"))

;; Status helpers -----------------------------------------------------------

(defn status-up?
  "True if the gRPC endpoint responds at all. Any gRPC status code (including
  UNAUTHENTICATED -- Status needs a CCT token in this coord build) means the
  server is up and serving; only connection-level failures (UNAVAILABLE,
  DEADLINE_EXCEEDED) count as not-ready."
  ^Boolean [^Channel ch]
  (try
    (call ch status (status-req) 2000)
    true
    (catch StatusRuntimeException e
      (let [^Status s (.getStatus ^StatusRuntimeException e)
            code      (.getCode s)]
        (not (or (= Status$Code/UNAVAILABLE code)
                 (= Status$Code/DEADLINE_EXCEEDED code)))))
    (catch Exception _
      false)))

(defn status-ok?
  "True if Maintenance/Status succeeds (requires auth)."
  ^Boolean [^Channel ch]
  (try
    (let [resp (call ch status (status-req) 2000)]
      (<= 0 (status-revision resp)))
    (catch StatusRuntimeException _
      false)
    (catch Exception _
      false)))
