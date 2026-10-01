(defproject jepsen.noop "0.1.0-SNAPSHOT"
  :description "Minimal Jepsen smoke test: noop DB, noop nemesis"
  :url "https://jepsen.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.noop
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [jepsen "0.3.14-SNAPSHOT"]])
