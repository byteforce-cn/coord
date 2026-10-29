(ns jepsen.coord.protoparse
  "A deliberately small proto-text reader for the wire self-checks.

  The checks compare hand-written `DescriptorProtos` (CoordRpc.java) against
  the contract `.proto` files. Parsing the contract text is the only way to
  catch drift without protoc (the lab is offline and has no protoc plugin).

  Scope: what coord's contracts actually use -- flat messages, scalar /
  message / enum fields, `repeated`, nested enums. A message that still
  contains a `{` after its nested enums are stripped is reported as
  `:nested` so the caller fails loudly instead of silently comparing half a
  field set (the failure mode that let `MqAckRequest` ship with fields 2/3
  swapped)."
  (:require [clojure.string :as str]))

(defn blocks
  "Splits `keyword name { … }` blocks with brace matching, returning
  [[name body start end] …] where start/end are body offsets.

  Regex matching (`\\{([^}]*)\\}`) stops at the first `}`: a nested enum /
  message would silently truncate the body, so brace matching is the point."
  [text kw]
  (let [re (re-pattern (str kw "\\s+([A-Za-z0-9_]+)\\s*\\{"))]
    (loop [m (re-matcher re text) acc []]
      (if (.find m)
        (let [name       (.group m 1)
              body-start (.end m)
              body-end   (loop [i body-start depth 1]
                           (cond
                             (>= i (count text)) (count text)
                             (= \{ (.charAt text i)) (recur (inc i) (inc depth))
                             (= \} (.charAt text i)) (if (= depth 1)
                                                       i
                                                       (recur (inc i) (dec depth)))
                             :else (recur (inc i) depth)))]
          (recur m (conj acc [name (subs text body-start body-end)
                              body-start body-end])))
        acc))))

(defn find-blocks
  "Like `blocks` but only [name body]."
  [text kw]
  (mapv (fn [[n b _ _]] [n b]) (blocks text kw)))

(defn strip-blocks
  "Removes every `kw name { … }` block from `body`; returns [body' [names…]].

  Nested enums are legal inside a message (`txn.v2.Mutation.Op`) and are not
  fields, so they must not make the message look nested."
  [body kw]
  (let [found (blocks body kw)]
    (if (empty? found)
      [body []]
      ;; Remove back-to-front so earlier offsets stay valid.
      [(reduce (fn [s [_ _ start end]]
                 ;; start-1 is the '{'; re-find the keyword before it.
                 (let [prefix (subs s 0 start)
                       kw-at  (str/last-index-of prefix kw)]
                   (if kw-at
                     (str (subs s 0 kw-at) (subs s (inc end)))
                     s)))
               body
               (sort-by #(nth % 2) > found))
       (mapv first found)])))

(defn wire-class
  "proto type name → protobuf wire class (:varint / :length / :fixed32 / :fixed64).

  `map<…>`, `string`, `bytes`, messages are length-delimited; enum names
  resolve through `enums` (nested enums are collected by `parse-proto`)."
  [type-name enums]
  (cond
    (#{"string" "bytes"} type-name)                                   :length
    (str/starts-with? type-name "map<")                                :length
    (contains? enums (last (str/split type-name #"\.")))               :varint
    (#{"int32" "int64" "uint32" "uint64" "sint32" "sint64" "bool"} type-name) :varint
    (#{"fixed32" "sfixed32" "float"} type-name)                        :fixed32
    (#{"fixed64" "sfixed64" "double"} type-name)                       :fixed64
    :else                                                              :length))

(defn parse-proto
  "Parses one proto file into

    {:messages {name {field-name {:number n :wire kw :repeated bool}}}
     :services #{name}
     :service-methods {service #{method…}}
     :enums #{name}}

  Comments are stripped first (a `//` mentioning `message X {` would
  otherwise create a phantom block)."
  [path]
  (let [text           (-> (slurp path)
                           (str/replace #"(?m)//.*$" "")
                           ;; block comments: coord's contracts use them for
                           ;; file headers; without this the header braces
                           ;; would be parsed as fields.
                           (str/replace #"(?s)/\*.*?\*/" ""))
        service-blocks (find-blocks text "service")
        services       (->> service-blocks (mapv first) set)
        service-methods (into {}
                              (map (fn [[sname body]]
                                     [sname (->> (re-seq #"rpc\s+([A-Za-z0-9_]+)\s*\(" body)
                                                 (mapv second)
                                                 set)]))
                              service-blocks)
        enum-names     (->> (blocks text "enum") (mapv first) set)
        field-of       (fn [body]
                         (into {}
                               (map (fn [[_ rep type-name fname num]]
                                      [fname {:number   (Long/parseLong num)
                                              :wire     (wire-class type-name enum-names)
                                              :repeated (some? rep)}]))
                               (re-seq #"(?m)^\s*(repeated\s+)?(map<[^>]*>|[A-Za-z0-9_.]+)\s+([A-Za-z0-9_]+)\s*=\s*([0-9]+)\s*;"
                                       body)))
        messages       (into {}
                             (map (fn [[mname body]]
                                    (let [[stripped _nested-enums] (strip-blocks body "enum")
                                          nested-msgs? (str/includes? stripped "{")]
                                      [mname (if nested-msgs? :nested (field-of stripped))])))
                             (find-blocks text "message"))]
    {:package         (second (re-find #"(?m)^\s*package\s+([A-Za-z0-9_.]+)\s*;" text))
     :messages        messages
     :services        services
     :service-methods service-methods
     :enums           enum-names}))
