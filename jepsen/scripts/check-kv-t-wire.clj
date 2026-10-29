;; KV-T v2 wire drift check (#272).
;;
;; The v2 descriptors in CoordRpc.java are hand-written (the lab has no
;; protoc). Hand-written drifts. This compares them field-by-field against
;; apis/contracts/proto/coord/{tso,kv,txn}/v2/*.proto:
;;
;;   * message names, both directions (a phantom type / a missing type);
;;   * every field: name, number, wire class, repeated-ness, both directions;
;;   * service names and their rpc names, both directions.
;;
;; Usage (control node, from the coord-test project root):
;;   LEIN_ROOT=true lein -o run -m clojure.main scripts/check-kv-t-wire.clj
;;   ... --selftest     # mutates an expected field number; MUST report it
;;
;; Exit 0 = no drift (or, with --selftest, the injected drift was reported).
;; Exit 1 = drift / missing proto tree / selftest that did not catch anything.

(require '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[jepsen.coord.protoparse :as pp])
(import '[jepsen.coord CoordRpc])

(def selftest? (contains? (set *command-line-args*) "--selftest"))

;; ── proto tree ─────────────────────────────────────────────────────────────
(def proto-root
  "Directory holding `coord/{tso,kv,txn}/v2/*.proto`.

  Every candidate is tried; none existing is fail-closed (skipping the check
  would turn a missing checkout into a silent green)."
  (let [candidates [(some-> (System/getenv "COORD_PROTO_DIR"))
                    "protos/coord"
                    "../apis/contracts/proto/coord"
                    "/opt/coord/apis/contracts/proto/coord"
                    "apis/contracts/proto/coord"]]
    (or (first (filter #(and % (.isDirectory (io/file %))) candidates))
        (do (println "FAIL no proto tree found; tried:" (pr-str candidates))
            (System/exit 1)))))

(def targets
  [{:pkg "coord.tso.v2" :proto "tso/v2/tso.proto" :desc CoordRpc/TSO_V2_FILE}
   {:pkg "coord.kv.v2"  :proto "kv/v2/kv.proto"   :desc CoordRpc/KV_V2_FILE}
   {:pkg "coord.txn.v2" :proto "txn/v2/txn.proto" :desc CoordRpc/TXN_V2_FILE}])

;; ── descriptor → comparable table ──────────────────────────────────────────
(defn- wire-class
  [^com.google.protobuf.Descriptors$FieldDescriptor f]
  (case (.name (.getType f))
    ("STRING" "BYTES" "MESSAGE" "GROUP")                                    :length
    ("INT32" "INT64" "UINT32" "UINT64" "SINT32" "SINT64" "BOOL" "ENUM")     :varint
    ("FIXED32" "SFIXED32" "FLOAT")                                          :fixed32
    ("FIXED64" "SFIXED64" "DOUBLE")                                         :fixed64
    (do (println "FAIL unknown descriptor field type" (.name (.getType f)))
        :unknown)))

(defn- descriptor-table
  [^com.google.protobuf.Descriptors$FileDescriptor f]
  {:package (.getPackage f)
   :messages
   (into {}
         (map (fn [^com.google.protobuf.Descriptors$Descriptor d]
                [(.getName d)
                 (into {}
                       (map (fn [^com.google.protobuf.Descriptors$FieldDescriptor fd]
                              [(.getName fd)
                               {:number   (.getNumber fd)
                                :wire     (wire-class fd)
                                :repeated (.isRepeated fd)}]))
                       (.getFields d))]))
         (.getMessageTypes f))
   :service-methods
   (into {}
         (map (fn [^com.google.protobuf.Descriptors$ServiceDescriptor s]
                [(.getName s)
                 (set (map #(.getName ^com.google.protobuf.Descriptors$MethodDescriptor %)
                           (.getMethods s)))]))
         (.getServices f))})

;; ── comparison ─────────────────────────────────────────────────────────────
(defn- compare-file
  "Returns a vector of human-readable mismatches (empty = no drift)."
  [expected actual label]
  (let [out (atom [])
        bad! (fn [fmt & args] (swap! out conj (apply format fmt args)))]
    (when (not= (:package expected) (:package actual))
      (bad! "%s: package %s in the .proto, %s in the descriptor"
            label (:package expected) (:package actual)))

    ;; messages, both directions
    (doseq [mname (sort (into (set (keys (:messages expected)))
                              (keys (:messages actual))))]
      (let [e (get (:messages expected) mname)
            a (get (:messages actual) mname)]
        (cond
          (nil? e) (bad! "%s: descriptor declares message %s, absent from the .proto" label mname)
          (nil? a) (bad! "%s: .proto declares message %s, missing from the descriptor" label mname)
          :else
          (doseq [fname (sort (into (set (keys e)) (keys a)))]
            (let [ef (get e fname)
                  af (get a fname)]
              (cond
                (nil? ef) (bad! "%s.%s.%s: descriptor-only field (ghost field)"
                                label mname fname)
                (nil? af) (bad! "%s.%s.%s: missing from the descriptor (coverage gap)"
                                label mname fname)
                :else
                (do
                  (when (not= (:number ef) (:number af))
                    (bad! "%s.%s.%s: 字段号不符 proto #%s / descriptor #%s"
                          label mname fname (:number ef) (:number af)))
                  (when (not= (:wire ef) (:wire af))
                    (bad! "%s.%s.%s: wire type 不符 proto %s / descriptor %s"
                          label mname fname (:wire ef) (:wire af)))
                  (when (not= (boolean (:repeated ef)) (boolean (:repeated af)))
                    (bad! "%s.%s.%s: repeated 不符 proto %s / descriptor %s"
                          label mname fname (:repeated ef) (:repeated af))))))))))

    ;; services / methods, both directions
    (doseq [sname (sort (into (set (keys (:service-methods expected)))
                              (keys (:service-methods actual))))]
      (let [e (get (:service-methods expected) sname)
            a (get (:service-methods actual) sname)]
        (cond
          (nil? e) (bad! "%s: descriptor declares service %s, absent from the .proto"
                         label sname)
          (nil? a) (bad! "%s: .proto declares service %s, missing from the descriptor"
                         label sname)
          :else
          (do
            (doseq [m (sort (remove a e))]
              (bad! "%s.%s: rpc %s missing from the descriptor" label sname m))
            (doseq [m (sort (remove e a))]
              (bad! "%s.%s: rpc %s not in the .proto (ghost method)" label sname m))))))
    @out))

(defn- run-checks
  "Compares every target; returns {:failures [...] :count n}."
  [expected-tables]
  (let [failures (atom [])
        fields   (atom 0)]
    (doseq [{:keys [pkg proto desc]} targets]
      (let [expected (get expected-tables pkg)
            actual   (descriptor-table desc)
            label    pkg]
        (doseq [[_ fs] (:messages expected)]
          (swap! fields + (count fs)))
        (doseq [m (compare-file expected actual label)]
          (swap! failures conj m))))
    {:failures @failures :count @fields}))

;; ── expected tables from the contracts ────────────────────────────────────
(def expected-tables
  (into {}
        (map (fn [{:keys [pkg proto]}]
               (let [path (str (io/file proto-root proto))]
                 (when-not (.isFile (io/file path))
                   (println "FAIL missing contract file" path)
                   (System/exit 1))
                 [pkg (pp/parse-proto path)])))
        targets))

(when-not (some-> expected-tables keys count (= 3))
  (println "FAIL expected three parsed contract files")
  (System/exit 1))

(doseq [[pkg t] expected-tables
        [mname body] (:messages t)
        :when (= :nested body)]
  (println "FAIL" pkg mname "has a nested block the parser does not model")
  (System/exit 1))

(println "parsed" (count targets) "v2 contracts under" proto-root)

;; ── selftest: the comparator must not be a no-op ──────────────────────────
(if selftest?
  (let [injured (update-in expected-tables ["coord.kv.v2" :messages "PutRequest" "value" :number]
                           (constantly 99))
        {:keys [failures]} (run-checks injured)]
    (if (some #(and (str/includes? % "PutRequest.value")
                    (str/includes? % "字段号不符"))
              failures)
      (do (println "SELFTEST ok: injected PutRequest.value=99 was reported")
          (System/exit 0))
      (do (println "SELFTEST FAIL: the comparator did not report the injected number mismatch")
          (println (str/join "\n" failures))
          (System/exit 1)))))

;; ── the real check ────────────────────────────────────────────────────────
(let [{:keys [failures] n :count} (run-checks expected-tables)]
  (doseq [f failures] (println "FAIL" f))
  (if (seq failures)
    (do (println "\n" (count failures) "FAILURE(S)")
        (System/exit 1))
    (do (println "kv-t wire self-check: OK"
                 (str "(" n " fields across " (count targets) " files, 0 failures)"))
        (System/exit 0))))
