(ns jepsen.coord.proto
  "Clojure-friendly helpers over the CoordRpc gRPC layer: request builders,
  response readers, and call helpers. All gRPC errors surface as
  io.grpc.StatusRuntimeException."
  (:require [clojure.string :as str])
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
  "Opens a plaintext gRPC channel.

  `host` 可以是裸主机名（补默认 grpc-port），也可以是显式 `host:port` ——
  `--via-agent` 的客户端连的是**控制机上的隧道端口**（见 jepsen.coord.agent），
  而不是节点的 50051。IPv6 的 `[::1]:port` 也按最后一个冒号切分（括号内的
  冒号不会误切，因为只切最后一个）。"
  [host]
  (let [s (str host)
        i (str/last-index-of s ":")
        port? (and i (not (str/includes? (subs s (inc i)) ":"))
                   (re-matches #"[0-9]+" (subs s (inc i))))]
    (if port?
      (CoordRpc/channel (subs s 0 i) (int (Long/parseLong (subs s (inc i)))))
      (CoordRpc/channel s grpc-port))))

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
  "PutRequest for key/value/request-id (strings). With `prev-kv?` the response
  carries the overwritten value (kv.proto PutRequest.prev_kv)."
  ([^String key ^String value ^String request-id]
   (put-req key value request-id false))
  ([^String key ^String value ^String request-id prev-kv?]
   (let [b (DynamicMessage/newBuilder CoordRpc/KV_PUT_REQUEST)
         d CoordRpc/KV_PUT_REQUEST]
     (-> b
         (.setField (.findFieldByName d "key") (ByteString/copyFromUtf8 key))
         (.setField (.findFieldByName d "value") (ByteString/copyFromUtf8 value))
         (.setField (.findFieldByName d "request_id") (ByteString/copyFromUtf8 request-id))
         (.setField (.findFieldByName d "prev_kv") (boolean prev-kv?))
         (.build)))))

(defn put-req*
  "PutRequest 的通用形式（kv.proto PutRequest 全字段）：

    {:key k :value v :lease-id n :prev-kv true :request-id s}

  `lease_id` 是 T2.2 的绑定原语：绑定 Lease 的 Key 在 Lease 过期/被 Revoke 时
  被**级联删除**（kv.proto:42 的承诺）。`put-req` 保持旧签名不变。"
  [{:keys [key value lease-id prev-kv request-id]}]
  (let [b (DynamicMessage/newBuilder CoordRpc/KV_PUT_REQUEST)
        d CoordRpc/KV_PUT_REQUEST]
    (-> b
        (.setField (.findFieldByName d "key") (ByteString/copyFromUtf8 (str key)))
        (.setField (.findFieldByName d "value") (ByteString/copyFromUtf8 (str value)))
        (cond-> lease-id
          (.setField (.findFieldByName d "lease_id") (long lease-id)))
        (.setField (.findFieldByName d "prev_kv") (boolean prev-kv))
        (cond-> request-id
          (.setField (.findFieldByName d "request_id")
                     (ByteString/copyFromUtf8 (str request-id))))
        (.build))))

(defn range-req
  "RangeRequest for an exact key lookup at the latest revision."
  [^String key]
  (bytes-field CoordRpc/KV_RANGE_REQUEST "key" key))

(defn range-req*
  "RangeRequest, general form (T1.3 / D1 / C3 覆盖)：

    {:key key :range-end end :limit n :revision r :keys-only true :count-only true}

  见 kv.proto RangeRequest：`range_end` 左闭右开（空 = 仅精确查询 key）；
  `revision` 指定历史 Revision（0 = 最新）；被压缩清理的历史读返回
  OUT_OF_RANGE。"
  [{:keys [key range-end limit revision keys-only count-only]}]
  (let [b (DynamicMessage/newBuilder CoordRpc/KV_RANGE_REQUEST)
        d CoordRpc/KV_RANGE_REQUEST
        set (fn [n v] (.setField b (.findFieldByName d n) v))]
    (set "key" (ByteString/copyFromUtf8 (or key "")))
    (when range-end (set "range_end" (ByteString/copyFromUtf8 range-end)))
    (when limit (set "limit" (long limit)))
    (when revision (set "revision" (long revision)))
    (when keys-only (set "keys_only" true))
    (when count-only (set "count_only" true))
    (.build b)))

(defn delete-req
  "DeleteRequest. Map form supports `:range-end` (left-closed right-open range
  delete) and `:prev-kv` (response carries the deleted values) -- kv.proto
  DeleteRequest.range_end / .prev_kv."
  ([^String key request-id]
   (delete-req key request-id {}))
  ([^String key request-id {:keys [range-end prev-kv]}]
   (let [b (DynamicMessage/newBuilder CoordRpc/KV_DELETE_REQUEST)
         d CoordRpc/KV_DELETE_REQUEST]
     (-> b
         (.setField (.findFieldByName d "key") (ByteString/copyFromUtf8 key))
         (cond-> range-end
           (.setField (.findFieldByName d "range_end")
                      (ByteString/copyFromUtf8 range-end)))
         (.setField (.findFieldByName d "prev_kv") (boolean prev-kv))
         (.setField (.findFieldByName d "request_id") (ByteString/copyFromUtf8 request-id))
         (.build)))))

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

(def ^:private compare-ops
  "tables/txn Compare.result 名称（contract: EQUAL | GREATER | LESS | NOT_EQUAL）"
  {:equal "EQUAL" :greater "GREATER" :less "LESS" :not-equal "NOT_EQUAL"})

(def ^:private compare-targets
  "tables/txn Compare.target 名称（contract: VERSION | VALUE | MOD_REV）"
  {:value "VALUE" :version "VERSION" :mod-revision "MOD_REV"})

(defn compare
  "A txn Compare message (T1.2 全形态：4 种比较符 × 3 种比较目标).

  m: {:op      :equal|:greater|:less|:not-equal
      :target  :value|:version|:mod-revision
      :key     key
      :value   bytes (target=:value)
      :version n     (target=:version)
      :mod-revision n (target=:mod-revision)}"
  [{:keys [op target key value version mod-revision]}]
  (let [b (DynamicMessage/newBuilder CoordRpc/TXN_COMPARE)
        d CoordRpc/TXN_COMPARE
        result-field (.findFieldByName d "result")
        target-field (.findFieldByName d "target")]
    (.setField b result-field
               (enum-value result-field
                           (or (get compare-ops op)
                               (throw (ex-info (str "unknown compare op " op) {:op op})))))
    (.setField b target-field
               (enum-value target-field
                           (or (get compare-targets target)
                               (throw (ex-info (str "unknown compare target " target)
                                               {:target target})))))
    (.setField b (.findFieldByName d "key") (ByteString/copyFromUtf8 key))
    ;; oneof target_value: exactly one of version/value/mod_revision
    (case target
      :value (when (some? value)
               (.setField b (.findFieldByName d "value") (ByteString/copyFromUtf8 value)))
      :version (.setField b (.findFieldByName d "version") (long version))
      :mod-revision (.setField b (.findFieldByName d "mod_revision") (long mod-revision)))
    (.build b)))

(defn compare-value-equal
  "A Compare that checks VALUE == old-value (bytes)."
  [^String key ^String old-value]
  (compare {:op :equal, :target :value, :key key, :value old-value}))

(defn request-put-op
  "A RequestOp wrapping a PutRequest."
  [put-msg]
  (let [b (DynamicMessage/newBuilder CoordRpc/TXN_REQUEST_OP)
        d CoordRpc/TXN_REQUEST_OP]
    (-> b
        (.setField (.findFieldByName d "request_put") put-msg)
        (.build))))

(defn request-delete-op
  "A RequestOp wrapping a DeleteRequest (T1.2 失败分支副作用探针)."
  [delete-msg]
  (let [b (DynamicMessage/newBuilder CoordRpc/TXN_REQUEST_OP)
        d CoordRpc/TXN_REQUEST_OP]
    (-> b
        (.setField (.findFieldByName d "request_delete") delete-msg)
        (.build))))

(defn request-range-op
  "A RequestOp wrapping a RangeRequest (T1.2 txn 内读，覆盖 txn 内 ReadOp 路径)."
  [range-msg]
  (let [b (DynamicMessage/newBuilder CoordRpc/TXN_REQUEST_OP)
        d CoordRpc/TXN_REQUEST_OP]
    (-> b
        (.setField (.findFieldByName d "request_range") range-msg)
        (.build))))

(defn txn-req
  "TxnRequest: compare (seq of Compare messages), success ops (seq of RequestOp),
  failure ops, request-id.

  F-13（测试自身，2026-09-16）：这里原来用的是 `(.addAllField b field v)` ——
  `DynamicMessage$Builder` 上**没有**这个方法（只有 `addRepeatedField` /
  `setField`）。异常在客户端里被 jepsen 记成 `:info`（`:error` 是那段
  reflection 报错），于是**所有 Txn 形态（cas-register / exists / txn-*）
  从未真正发出过请求**，而 knossos 对一份全 `:info` 的历史判「valid」——
  典型的假绿。repeated 字段用 `setField` + List 才是对 protobuf-java 的用法。"
  [compares success-ops failure-ops ^String request-id]
  (let [b (DynamicMessage/newBuilder CoordRpc/TXN_REQUEST)
        d CoordRpc/TXN_REQUEST
        ;; 注意：`->` 会把 builder 作为**第一个**参数穿过，所以这里必须接 3 个
        ;; 参数（builder + 字段名 + 值）。写成 2 参数会报
        ;; 「Wrong number of args (3) passed」——G6 门槛一次就抓到了（F-13 的
        ;; 同一条门槛先是抓到 addAllField，再抓到这里的 arity）。
        set-rep (fn [b n vs]
                  (.setField ^com.google.protobuf.DynamicMessage$Builder b
                             (.findFieldByName ^com.google.protobuf.Descriptors$Descriptor d n)
                             (vec vs)))]
    (-> b
        (set-rep "compare" compares)
        (set-rep "success" success-ops)
        (set-rep "failure" failure-ops)
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

(defn range-count
  "RangeResponse.count（命中总数；count_only=true 时仅本字段有意义）。"
  [^DynamicMessage msg]
  (field msg "count"))

(defn kv-key
  "Reads a KeyValue's key as a String."
  [^DynamicMessage kv]
  (let [bs ^ByteString (field kv "key")]
    (when (and bs (pos? (.size bs)))
      (String. (.toByteArray bs) "UTF-8"))))

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

(defn kv-create-revision
  [^DynamicMessage kv]
  (field kv "create_revision"))

(defn kv-mod-revision
  [^DynamicMessage kv]
  (field kv "mod_revision"))

(defn kv->edn
  "KeyValue → 纯 EDN（历史里绝不能出现 byte[]：既不可读，也会让 history.edn
  体积爆炸）。

  形状与 `jepsen.coord.client/kv-edn` **必须一致**（那边是私有的，所以这里独立
  实现）——checker 按这些字段名取值。`kv-edn` 的契约字段名：
  `create_revision` / `mod_revision` 用连字符，值语义见 kv.proto。"
  [^DynamicMessage kv]
  (when kv
    {:key             (kv-key kv)
     :value           (kv-value kv)
     :version         (kv-version kv)
     :create-revision (kv-create-revision kv)
     :mod-revision    (kv-mod-revision kv)}))

(defn kv-lease-id
  [^DynamicMessage kv]
  (field kv "lease_id"))

(defn- set-msg?
  "True when the message-typed (or oneof) field `field-name` is present on
  `msg`. Scalar proto3 fields have no presence, so this is only used on
  message / oneof fields (prev_kv, response_put, ...)."
  [^DynamicMessage msg ^String field-name]
  (.hasField msg (.findFieldByName (.getDescriptorForType msg) field-name)))

(defn put-prev-kv
  "PutResponse.prev_kv (nil when unset)."
  [^DynamicMessage msg]
  (when (set-msg? msg "prev_kv")
    (field msg "prev_kv")))

;; --------------------------------------------------------------------------
;; T2.0 —— Watch（双向流）
;; --------------------------------------------------------------------------

(def watch-method
  "coord.watch.Watch/Watch 的 BIDI_STREAMING MethodDescriptor。"
  CoordRpc/WATCH)

(defn watch-create-req
  "WatchCreateRequest{key, range_end, start_revision, prev_kv}（watch.proto）。

  `start-revision` 0 = 从当前最新开始；契约承诺「断线重连后以
  start_revision = 已确认最大 revision + 1 重建 Watch 即可续传（至少一次投递）」。"
  [{:keys [key range-end start-revision prev-kv?]}]
  (let [d CoordRpc/WATCH_CREATE_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "key") (ByteString/copyFromUtf8 (str key)))
        (cond-> range-end
          (.setField (.findFieldByName d "range_end")
                     (ByteString/copyFromUtf8 (str range-end))))
        (.setField (.findFieldByName d "start_revision") (long (or start-revision 0)))
        (.setField (.findFieldByName d "prev_kv") (boolean prev-kv?))
        (.build))))

(defn watch-req
  "WatchRequest 的 oneof `create` 包装。"
  [create-msg]
  (let [d CoordRpc/WATCH_REQUEST]
    (-> (DynamicMessage/newBuilder d)
        (.setField (.findFieldByName d "create") create-msg)
        (.build))))

(def ^:private event-types
  {0 :put, 1 :delete, 2 :buffer-overflow, 3 :history-unavailable})

(defn watch-id
  "WatchResponse.watch_id（同一条流内不变）。"
  [^DynamicMessage resp]
  (field resp "watch_id"))

(defn event-type
  "WatchEvent.type → `:put` / `:delete` / `:buffer-overflow` / `:history-unavailable`。

  proto3 的 enum 标量字段没有 presence，读到的是 EnumValueDescriptor；用
  `.getNumber` 归一化（**不能**用 `hasField`，那对 enum 会抛异常）。"
  [^DynamicMessage ev]
  (let [v (field ev "type")]
    (get event-types (long (if (instance? com.google.protobuf.Descriptors$EnumValueDescriptor v)
                             (.getNumber ^com.google.protobuf.Descriptors$EnumValueDescriptor v)
                             v))
         :unknown)))

(defn event-revision
  [^DynamicMessage ev]
  (field ev "revision"))

(defn event-kvs
  "WatchEvent.kvs 的 KeyValue 序列。"
  [^DynamicMessage ev]
  (field ev "kvs"))

(defn event-prev-kv
  "WatchEvent.prev_kv（创建时 prev_kv=true 且存在旧值时有值）。"
  [^DynamicMessage ev]
  (when (set-msg? ev "prev_kv")
    (field ev "prev_kv")))

(defn event->edn
  "WatchEvent → 纯 EDN（进 history 的东西绝不能是不可序列化的 DynamicMessage）。

  `:kvs` 用 `client/kv-edn` 的形状（`:key` / `:value` / `:version` /
  `:create-revision` / `:mod-revision`）—— 那个函数在 client 里是私有的，所以
  形状在这里独立实现（**两边必须一致**，checker 是按这些字段名取值的）。
  `:prev-kv` 同理。`type` 为符号，便于 checker 直接判缺口标记。"
  [^DynamicMessage ev]
  {:type        (event-type ev)
   :revision    (long (event-revision ev))
   :kvs         (mapv kv->edn (event-kvs ev))
   :prev-kv     (when-let [p (event-prev-kv ev)] (kv->edn p))})

(defn response-events
  "WatchResponse.events → 纯 EDN 向量。"
  [^DynamicMessage resp]
  (mapv event->edn (field resp "events")))

(defn open-watch
  "打开一条 watch 流（T2.1 的客户端原语）。

  `create` 可以是**普通 map**（由本函数转成 `WatchCreateRequest`）或者已经建好的
  `watch-create-req` 产物。**这个宽容是刻意的**（F-19）：oneof 字段必须塞
  DynamicMessage，塞 map 会得到一句很难定位的运行期报错
  （`Wrong object type used with protocol message reflection. Field number: 1,
  field java type: MESSAGE, value type: clojure.lang.PersistentArrayMap`），
  而它发生在**每个 op 的调用路径上** —— 表现为「整类 op 变 `:info`、checker
  说样本不足」，正是 F-13 那一类假绿的形态。让 builder 自己接受 map，就不会
  再有人忘了转。

  `queue-size` 是客户端接收缓冲。返回 `jepsen.coord.CoordRpc$Watcher`：
  `.poll ms`（阻塞到超时，返回 `WatchMsg` 或 nil）、`.tryPoll`、`.buffered`、
  `.close`。`WatchMsg` 有 `.isError` / `.response` / `.error` / `.nanoTime`。"
  ([^Channel ch create] (open-watch ch create 1024))
  ([^Channel ch create queue-size]
   (CoordRpc/watch ch
                   (watch-req (if (map? create) (watch-create-req create) create))
                   (int queue-size))))

(defn delete-deleted
  "DeleteResponse.deleted (实际删除的 Key 数量)。"
  [^DynamicMessage msg]
  (field msg "deleted"))

(defn delete-prev-kvs
  "DeleteResponse.prev_kvs (seq of KeyValue)."
  [^DynamicMessage msg]
  (field msg "prev_kvs"))

(defn txn-succeeded
  "Reads the bool succeeded from a TxnResponse."
  [^DynamicMessage msg]
  (field msg "succeeded"))

(defn txn-responses
  "TxnResponse.responses (seq of ResponseOp)。"
  [^DynamicMessage msg]
  (field msg "responses"))

(defn response-put
  "ResponseOp.response_put (nil when the op is not a put)."
  [^DynamicMessage resp-op]
  (when (set-msg? resp-op "response_put")
    (field resp-op "response_put")))

(defn response-range
  "ResponseOp.response_range (nil when the op is not a range)."
  [^DynamicMessage resp-op]
  (when (set-msg? resp-op "response_range")
    (field resp-op "response_range")))

(defn response-delete
  "ResponseOp.response_delete (nil when the op is not a delete)."
  [^DynamicMessage resp-op]
  (when (set-msg? resp-op "response_delete")
    (field resp-op "response_delete")))

(defn cct
  "Reads the cct (session token) from an AuthenticateResponse."
  [^DynamicMessage msg]
  (field msg "cct"))

;; --------------------------------------------------------------------------
;; T2.2 —— Lease（Grant / Revoke / KeepAlive 双向流）
;; --------------------------------------------------------------------------

(def lease-grant
  "coord.lease.Lease/LeaseGrant（unary）。"
  CoordRpc/LEASE_GRANT)

(def lease-revoke
  "coord.lease.Lease/LeaseRevoke（unary）。"
  CoordRpc/LEASE_REVOKE)

(defn lease-grant-req
  "LeaseGrantRequest{ttl, id}。id = 0 表示服务端自动分配。

  契约要点（coord/lease/lease.proto）：实际授予的 TTL 以响应里的 ttl 为准
  （服务端可按配置上下限调整），所以断言一律用**响应里的** ttl，不用请求值。"
  [{:keys [ttl id]}]
  (let [d CoordRpc/LEASE_GRANT_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "ttl") (long (or ttl 0)))
        (.setField (.findFieldByName d "id") (long (or id 0)))
        (.build))))

(defn lease-revoke-req
  "LeaseRevokeRequest{id}。契约：Revoke 时绑定该 Lease 的 Key 被级联删除。"
  [id]
  (let [d CoordRpc/LEASE_REVOKE_REQUEST]
    (-> (DynamicMessage/newBuilder d)
        (.setField (.findFieldByName d "id") (long id))
        (.build))))

(defn lease-id
  "LeaseGrantResponse.id / LeaseKeepAliveResponse.id。"
  [^DynamicMessage msg]
  (field msg "id"))

(defn lease-ttl
  "LeaseGrantResponse.ttl（实际授予）或 LeaseKeepAliveResponse.ttl（续期后的
  剩余 TTL；**0 = 租约已不存在**，契约要求客户端重新 Grant）。"
  [^DynamicMessage msg]
  (field msg "ttl"))

(defn open-keepalive
  "打开一条 LeaseKeepAlive 双向流（T2.2）。

  返回 `jepsen.coord.CoordRpc$KeepAliver`：`.send id`（发一次续期）、
  `.poll ms` / `.tryPoll`（取 `KeepAliveMsg`）、`.close`。
  `KeepAliveMsg` 有 `.isError` / `.response` / `.error` / `.nanoTime`。

  为什么需要流而不是 unary：契约里 KeepAlive 是双向流，服务端**逐条回应**；
  流断开（kill / partition / 重启）就是「续期停了」，正是 T2.2 要观察的形态。"
  ([^Channel ch] (open-keepalive ch 64))
  ([^Channel ch queue-size]
   (CoordRpc/keepAlive ch (int queue-size))))

(defn keepalive->edn
  "`KeepAliveMsg` → 纯 EDN（历史里绝不能出现 DynamicMessage）。"
  [^jepsen.coord.CoordRpc$KeepAliveMsg m]
  (if (.isError m)
    {:error (.error m)}
    {:id (lease-id (.response m)) :ttl (lease-ttl (.response m))}))

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

;; --------------------------------------------------------------------------
;; M5 —— agent 本地面（coord.agent.*）
;;
;; 这三段（lock / election / idgen / registry）是 coord-agent **独有**的服务：
;; server 的 router 里没有 coord.agent.* —— 也就是说，一次成功的调用本身
;; 就证明请求真的落到了某个 agent 上（见 jepsen.coord.agent 的「路由证明」）。
;;
;; 字段号/服务名与 `coord-proto/src/proto/agent_api.proto`（package
;; coord.agent）逐字一致；差异由 `scripts/check-agent-proto-sync.clj` 兜住
;; （手写 descriptor 会漂移，而漂移的表现是 UNIMPLEMENTED 或**静默少读一个
;; 字段**，后者比前者危险得多）。
;; --------------------------------------------------------------------------

;; Lock ---------------------------------------------------------------------

(def lock-acquire CoordRpc/LOCK_ACQUIRE)
(def lock-release CoordRpc/LOCK_RELEASE)
(def lock-renew CoordRpc/LOCK_RENEW)
(def lock-get-info CoordRpc/LOCK_GET_INFO)

(defn lock-acquire-req
  "LockAcquireRequest{name, holder_id, ttl_seconds}。

  `ttl_seconds` 是**请求**值；服务端可调整，判定一律用
  `lock-info->edn` 里 `acquired_at + ttl_seconds` 的实际值。"
  [{:keys [name holder-id ttl-seconds]}]
  (let [d CoordRpc/LOCK_ACQUIRE_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "name") (str name))
        (.setField (.findFieldByName d "holder_id") (str holder-id))
        (.setField (.findFieldByName d "ttl_seconds") (long (or ttl-seconds 30)))
        (.build))))

(defn lock-release-req
  "LockReleaseRequest{name, holder_id, lease_id}。契约：仅 (holder_id, lease_id)
  匹配者可释放/续期，否则 PERMISSION_DENIED（§9-⑥ 的倒逼点）。"
  [{:keys [name holder-id lease-id]}]
  (let [d CoordRpc/LOCK_RELEASE_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "name") (str name))
        (.setField (.findFieldByName d "holder_id") (str holder-id))
        (.setField (.findFieldByName d "lease_id") (long (or lease-id 0)))
        (.build))))

(defn lock-renew-req
  "LockRenewRequest{name, holder_id, lease_id}。"
  [{:keys [name holder-id lease-id]}]
  (let [d CoordRpc/LOCK_RENEW_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "name") (str name))
        (.setField (.findFieldByName d "holder_id") (str holder-id))
        (.setField (.findFieldByName d "lease_id") (long (or lease-id 0)))
        (.build))))

(defn lock-get-info-req
  "LockGetInfoRequest{name}。"
  [name]
  (let [d CoordRpc/LOCK_GET_INFO_REQUEST]
    (-> (DynamicMessage/newBuilder d)
        (.setField (.findFieldByName d "name") (str name))
        (.build))))

(defn lock-acquired?
  "LockAcquireResponse.acquired。`false` 是**业务结果**（别人持有），不是错误。"
  [^DynamicMessage msg]
  (field msg "acquired"))

(defn lock-lease-id
  "LockAcquireResponse.lease_id（= 该锁绑定的 Lease）。字段名/号与
  LockGetInfoResponse.lease_id 相同，所以同一个 reader 两个响应都能用。"
  [^DynamicMessage msg]
  (field msg "lease_id"))

(defn lock-holder-id [^DynamicMessage msg] (field msg "holder_id"))
(defn lock-released? [^DynamicMessage msg] (field msg "released"))
(defn lock-new-ttl [^DynamicMessage msg] (field msg "new_ttl"))

(defn lock-info->edn
  "LockGetInfoResponse → 纯 EDN（`exists = false` 时其余字段无意义）。

  `acquired_at` 是**服务端墙钟秒**（LockInfo::new 用 unix_ts），所以发布测的
  时间差一律取客户端自己的单调量（与 T2.2 lease 同一取向，见 F-08）。"
  [^DynamicMessage msg]
  (when msg
    {:name         (field msg "name")
     :holder-id    (field msg "holder_id")
     :lease-id     (field msg "lease_id")
     :acquired-at  (field msg "acquired_at")
     :ttl-seconds  (field msg "ttl_seconds")
     :exists       (field msg "exists")}))

;; IdGen --------------------------------------------------------------------

(def idgen-next-id CoordRpc/IDGEN_NEXT_ID)
(def idgen-next-batch CoordRpc/IDGEN_NEXT_BATCH)

(defn idgen-next-id-req
  "IdGenNextIdRequest{name, step}。step 只对 segment 模式有意义（号段步长）。"
  [{:keys [name step]}]
  (let [d CoordRpc/IDGEN_NEXT_ID_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "name") (str name))
        (cond-> step (.setField (.findFieldByName d "step") (long step)))
        (.build))))

(defn idgen-next-batch-req
  "IdGenNextBatchRequest{name, count, step}。契约：返回 count 个**互异** ID。"
  [{:keys [name count step]}]
  (let [d CoordRpc/IDGEN_NEXT_BATCH_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "name") (str name))
        (.setField (.findFieldByName d "count") (int (or count 10)))
        (cond-> step (.setField (.findFieldByName d "step") (long step)))
        (.build))))

(defn idgen-id [^DynamicMessage msg] (field msg "id"))
(defn idgen-ids [^DynamicMessage msg] (vec (field msg "ids")))

;; LeaderElection -----------------------------------------------------------

(def election-campaign CoordRpc/ELECTION_CAMPAIGN)
(def election-resign CoordRpc/ELECTION_RESIGN)
(def election-get-leader CoordRpc/ELECTION_GET_LEADER)

(defn election-campaign-req
  "LeaderCampaignRequest{group_name, candidate_id, ttl_seconds}。

  契约：同一 group 同一时刻至多一个 leader；`elected=false` 是业务结果
  （别组已在位），不是错误。"
  [{:keys [group-name candidate-id ttl-seconds]}]
  (let [d CoordRpc/ELECTION_CAMPAIGN_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "group_name") (str group-name))
        (.setField (.findFieldByName d "candidate_id") (str candidate-id))
        (.setField (.findFieldByName d "ttl_seconds") (long (or ttl-seconds 30)))
        (.build))))

(defn election-resign-req
  "LeaderResignRequest{group_name, candidate_id, lease_id}。"
  [{:keys [group-name candidate-id lease-id]}]
  (let [d CoordRpc/ELECTION_RESIGN_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "group_name") (str group-name))
        (.setField (.findFieldByName d "candidate_id") (str candidate-id))
        (.setField (.findFieldByName d "lease_id") (long (or lease-id 0)))
        (.build))))

(defn election-get-leader-req
  "LeaderGetLeaderRequest{group_name}。"
  [group-name]
  (let [d CoordRpc/ELECTION_GET_LEADER_REQUEST]
    (-> (DynamicMessage/newBuilder d)
        (.setField (.findFieldByName d "group_name") (str group-name))
        (.build))))

(defn election-elected? [^DynamicMessage msg] (field msg "elected"))
(defn election-resigned? [^DynamicMessage msg] (field msg "resigned"))
(defn election-lease-id [^DynamicMessage msg] (field msg "lease_id"))

(defn leader->edn
  "LeaderGetLeaderResponse → EDN（`exists=false` = 该 group 当前无 leader，
  这在「Resign 之后」「TTL 过期之后」都是**期望**结果，不是错误）。"
  [^DynamicMessage msg]
  (when msg
    {:leader-id (field msg "leader_id")
     :lease-id  (field msg "lease_id")
     :elected-at (field msg "elected_at")
     :exists    (field msg "exists")}))

;; Registry -----------------------------------------------------------------

(def registry-register CoordRpc/REGISTRY_REGISTER)
(def registry-deregister CoordRpc/REGISTRY_DEREGISTER)
(def registry-heartbeat CoordRpc/REGISTRY_HEARTBEAT)
(def registry-discover CoordRpc/REGISTRY_DISCOVER)

(def ^:private filter-modes
  "Registry FilterMode（agent_api.proto）。默认 0 与 EXACT 等价。"
  {:unspecified "FILTER_MODE_UNSPECIFIED"
   :exact       "FILTER_MODE_EXACT"
   :prefix      "FILTER_MODE_PREFIX"
   :all         "FILTER_MODE_ALL"})

(defn registry-register-req
  "RegisterRequest{service_name, instance_id, metadata, ttl_seconds}。

  契约要点：`ttl_seconds` 绑定 Lease，实例随之自动过期；重复注册**幂等**
  （台账整改要点），所以 checker 会把「同 (service, instance) 连续注册两次」
  当作合法且必须只有一个实例来判。"
  [{:keys [service-name instance-id metadata ttl-seconds]}]
  (let [d CoordRpc/REGISTRY_REGISTER_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "service_name") (str service-name))
        (.setField (.findFieldByName d "instance_id") (str instance-id))
        (.setField (.findFieldByName d "metadata") (str (or metadata "")))
        (.setField (.findFieldByName d "ttl_seconds") (int (or ttl-seconds 30)))
        (.build))))

(defn registry-deregister-req
  "DeregisterRequest{service_name, instance_id, lease_id}。"
  [{:keys [service-name instance-id lease-id]}]
  (let [d CoordRpc/REGISTRY_DEREGISTER_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "service_name") (str service-name))
        (.setField (.findFieldByName d "instance_id") (str instance-id))
        (.setField (.findFieldByName d "lease_id") (long (or lease-id 0)))
        (.build))))

(defn registry-heartbeat-req
  "HeartbeatRequest{service_name, instance_id, lease_id} → HeartbeatResponse.ttl。"
  [{:keys [service-name instance-id lease-id]}]
  (let [d CoordRpc/REGISTRY_HEARTBEAT_REQUEST
        b (DynamicMessage/newBuilder d)]
    (-> b
        (.setField (.findFieldByName d "service_name") (str service-name))
        (.setField (.findFieldByName d "instance_id") (str instance-id))
        (.setField (.findFieldByName d "lease_id") (long (or lease-id 0)))
        (.build))))

(defn registry-discover-req
  "DiscoverRequest{service_name, filter_mode}。"
  [{:keys [service-name filter-mode]}]
  (let [d CoordRpc/REGISTRY_DISCOVER_REQUEST
        b (DynamicMessage/newBuilder d)
        mode-field (.findFieldByName d "filter_mode")]
    (-> b
        (.setField (.findFieldByName d "service_name") (str service-name))
        (.setField mode-field
                   (enum-value mode-field
                               (or (get filter-modes filter-mode)
                                   "FILTER_MODE_UNSPECIFIED")))
        (.build))))

(defn registry-lease-id [^DynamicMessage msg] (field msg "lease_id"))
(defn registry-ttl [^DynamicMessage msg] (field msg "ttl"))
(defn registry-revision [^DynamicMessage msg] (field msg "revision"))

(defn registry-instances
  "DiscoverResponse.instances → 纯 EDN 的 vector（按 :instance-id 排序）。

  刻意**不**返回 DynamicMessage：history 里放 Java 对象既不可读，也会让
  `checker`/`replay` 在反序列化时炸（T1.1 起就守住的纪律）。"
  [^DynamicMessage msg]
  (->> (field msg "instances")
       (mapv (fn [^DynamicMessage inst]
               {:instance-id  (field inst "instance_id")
                :service-name (field inst "service_name")
                :metadata     (field inst "metadata")}))
       (sort-by :instance-id)
       vec))

;; --------------------------------------------------------------------------
;; M5b —— agent 本地数据面（coord.cache.v1.Cache / coord.mq.v1.MQ）
;;
;; 与上面四个面同一条纪律：字段号/服务名与 `agent_api.proto` 逐字一致，
;; 由 `scripts/check-agent-wire.clj` 自检（手写 descriptor 的漂移表现是
;; UNIMPLEMENTED 或**静默少读一个字段**）。
;;
;; 两者的数据都在 **agent 本地**（cache 是 redb；MQ 是 agent 本地日志 +
;; 可选 ISR 复制），不是 server 上的共享状态 —— 所以判定「读到自己刚写的值」
;; 只在 `--agents 1`（或没有跨 agent 路由）时成立。checker 的文档里写明了
;; 这条前提，`coord.clj` 的 workload 校验会在 --agents > 1 时**拒绝**
;; `--workload cache`/`mq`（见 `local-consistency-workloads`）。
;; --------------------------------------------------------------------------

(defn- utf8
  "proto bytes → String（空 ByteString 也返回 \"\"，不返回 nil：缓存里的空值
  与「没读到」是两件事，checker 靠 `:found` 区分）。"
  [^ByteString bs]
  (if (nil? bs) nil (String. (.toByteArray bs) "UTF-8")))

(defn- field-name
  "Clojure 关键字 → proto 字段名（`-` ⇒ `_`）。请求构造里一律用关键字，避免
  手写字符串时把 `idempotency_key` 拼成 `idempotencyKey` 而**静默少传一个
  字段**（proto3 标量没有 presence，少传字段不报错，只是值为默认值 —— 这类
  错误只有靠行为判据才抓得到）。"
  [k]
  (str/replace (name k) "-" "_"))

;; Cache --------------------------------------------------------------------

(def cache-get CoordRpc/CACHE_GET)
(def cache-set CoordRpc/CACHE_SET)
(def cache-delete CoordRpc/CACHE_DELETE)
(def cache-lpush CoordRpc/CACHE_LPUSH)
(def cache-lrange CoordRpc/CACHE_LRANGE)
(def cache-llen CoordRpc/CACHE_LLEN)
(def cache-sadd CoordRpc/CACHE_SADD)
(def cache-smembers CoordRpc/CACHE_SMEMBERS)

(defn- cache-request
  "Cache 请求的通用构造：`fields` 是 {字段名 值}，字符串字段走 UTF-8 bytes。

  `ttl-seconds` 的语义（`cache.rs`）：0 = **不过期**（持久条目），>0 = 绝对
  到期时间戳随数据一起持久化/复制。

  字段类型分派用 `cond` + `=`（**不要**用 `case`）：`case` 的测试常量必须是
  编译期字面量，Java 枚举常量放进去**编译能过、运行期匹配不上**，于是每个请求
  都落到「unsupported field type」分支 —— 实测（M5b 第八轮）六个 lab cell 全部
  以 `cache-request: unsupported field type STRING for :key` 记成 `:info`，
  而 checker 只看到「0 条完成」，看起来像「被测系统一条 op 都没成功」。"
  [^com.google.protobuf.Descriptors$Descriptor desc fields]
  (let [b (DynamicMessage/newBuilder desc)]
    (doseq [[k v] fields]
      (let [fd (.findFieldByName desc (field-name k))
            t  (.getType fd)]
        (.setField b fd
                   (cond
                     (= t com.google.protobuf.Descriptors$FieldDescriptor$Type/STRING)
                     (str v)
                     (= t com.google.protobuf.Descriptors$FieldDescriptor$Type/BYTES)
                     (ByteString/copyFromUtf8 (str v))
                     (= t com.google.protobuf.Descriptors$FieldDescriptor$Type/INT64)
                     (long v)
                     (= t com.google.protobuf.Descriptors$FieldDescriptor$Type/INT32)
                     (int v)
                     (= t com.google.protobuf.Descriptors$FieldDescriptor$Type/BOOL)
                     (boolean v)
                     :else (throw (ex-info (str "cache-request: unsupported field type "
                                                t " for " k)
                                           {:field k :type (str t)}))))))
    (.build b)))

(defn cache-get-req [k]
  (cache-request CoordRpc/CACHE_GET_REQUEST {:key k}))
(defn cache-set-req
  "CacheSetRequest{key, value, ttl_seconds}。"
  [{:keys [key value ttl-seconds]}]
  (cache-request CoordRpc/CACHE_SET_REQUEST
                 {:key (str key) :value (str value)
                  :ttl_seconds (long (or ttl-seconds 0))}))
(defn cache-delete-req [k]
  (cache-request CoordRpc/CACHE_DELETE_REQUEST {:key (str k)}))
(defn cache-lpush-req [{:keys [key value]}]
  (cache-request CoordRpc/CACHE_LPUSH_REQUEST {:key (str key) :value (str value)}))
(defn cache-lrange-req [{:keys [key start stop]}]
  (cache-request CoordRpc/CACHE_LRANGE_REQUEST
                 {:key (str key) :start (long (or start 0)) :stop (long (or stop -1))}))
(defn cache-llen-req [k]
  (cache-request CoordRpc/CACHE_LLEN_REQUEST {:key (str k)}))
(defn cache-sadd-req [{:keys [key member]}]
  (cache-request CoordRpc/CACHE_SADD_REQUEST {:key (str key) :member (str member)}))
(defn cache-smembers-req [k]
  (cache-request CoordRpc/CACHE_SMEMBERS_REQUEST {:key (str k)}))

(defn cache-value-found? [^DynamicMessage msg] (boolean (field msg "found")))
(defn cache-value
  "CacheGetResponse.value：**空串也是一个值**（与 found=false 区分）。
  `found=false` 时返回 nil。"
  [^DynamicMessage msg]
  (when (cache-value-found? msg)
    (str (utf8 (field msg "value")))))
(defn cache-deleted? [^DynamicMessage msg] (field msg "deleted"))
(defn cache-length [^DynamicMessage msg] (field msg "length"))
(defn cache-values
  "LRangeResponse.values / SMembersResponse.members（同一形状的 repeated bytes）。"
  [^DynamicMessage msg]
  (mapv utf8 (field msg (if (.findFieldByName (.getDescriptorForType msg) "values")
                          "values" "members"))))

;; MQ -----------------------------------------------------------------------

(def mq-create-topic CoordRpc/MQ_CREATE_TOPIC)
(def mq-publish CoordRpc/MQ_PUBLISH)
(def mq-poll CoordRpc/MQ_POLL)
(def mq-ack CoordRpc/MQ_ACK)

(defn- mq-request [^com.google.protobuf.Descriptors$Descriptor desc fields]
  (cache-request desc fields))

(defn mq-create-topic-req [{:keys [topic partitions]}]
  (mq-request CoordRpc/MQ_CREATE_TOPIC_REQUEST
              {:topic (str topic) :partitions (int (or partitions 1))}))
(defn mq-publish-req
  "MqPublishRequest{topic, partition, key, payload, idempotency_key}。

  `idempotency_key` 是 broker 侧的**去重键**（m5b 的登台判据之一）：同一 key
  重复发布必须只落一条。"
  [{:keys [topic partition key payload idempotency-key]}]
  (mq-request CoordRpc/MQ_PUBLISH_REQUEST
              (cond-> {:topic (str topic) :partition (int (or partition 0))
                       :payload (str payload)}
                key (assoc :key (str key))
                idempotency-key (assoc :idempotency-key (str idempotency-key)))))
(defn mq-poll-req [{:keys [topic partition consumer-group start-offset max-count]}]
  (mq-request CoordRpc/MQ_POLL_REQUEST
              {:topic (str topic) :partition (int (or partition 0))
               :consumer_group (str (or consumer-group "g1"))
               :start_offset (long (or start-offset 0))
               :max_count (int (or max-count 100))}))
(defn mq-ack-req [{:keys [topic partition consumer-group offset]}]
  (mq-request CoordRpc/MQ_ACK_REQUEST
              {:topic (str topic) :partition (int (or partition 0))
               :consumer_group (str (or consumer-group "g1"))
               :offset (long offset)}))

(defn mq-offset [^DynamicMessage msg] (field msg "offset"))
(defn mq-messages
  "MqPollResponse.messages → 纯 EDN（:topic/:partition/:offset/:key/:payload）。"
  [^DynamicMessage msg]
  (mapv (fn [^DynamicMessage m]
          {:topic     (field m "topic")
           :partition (field m "partition")
           :offset    (field m "offset")
           :key       (utf8 (field m "key"))
           :payload   (utf8 (field m "payload"))
           :timestamp (field m "timestamp")})
        (field msg "messages")))
