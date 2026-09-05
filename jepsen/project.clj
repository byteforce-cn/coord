(defproject jepsen.coord "0.1.0-SNAPSHOT"
  :description "Jepsen tests for coord, a strongly-consistent KV store"
  :url "https://jepsen.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.coord
  ;; the gRPC helper (CoordRpc.java) lives under src/java — declare it so
  ;; `lein run`/`lein javac` compile it (some lein versions have no default)
  :java-source-paths ["src/java"]
  :javac-options ["--release" "21"]
  ;; checker heap: control VM now has 16GB; 12g keeps heavy partition-ring
  ;; histories (knossos) tractable without OOMing (was 6g -> OOM)
  :jvm-opts ["-Xmx12g"]
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [jepsen "0.3.14-SNAPSHOT"]
                 [io.grpc/grpc-netty-shaded "1.68.1"]
                 [io.grpc/grpc-stub "1.68.1"]
                 [com.google.protobuf/protobuf-java "3.25.5"]])
