(ns jepsen.noop
  "A do-nothing Jepsen test used to smoke-test the lab: it exercises SSH into
  all DB nodes, the os/db/noop lifecycle, and the checker, without installing
  any real database."
  (:require [jepsen.cli :as cli]
            [jepsen.tests :as tests]))

(defn test-fn
  "Given an options map from the command line runner, constructs a test map."
  [opts]
  (merge tests/noop-test
         {:pure-generators true}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn test-fn})
            args))
