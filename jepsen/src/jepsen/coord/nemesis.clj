(ns jepsen.coord.nemesis
  "Nemeses for coord. kill/restart/pause target individual coord daemons via
  pkill (killall is not installed on the Debian lab nodes), and partitions
  reuse Jepsen's built-in iptables nemeses."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [nemesis :as nemesis]
                    [db :as db]
                    [random :as rand]
                    [control :as c]
                    [util :as util :refer [meh]]]))

(def coord-pattern
  "pkill -f pattern matching coord daemons without matching the pkill
  invocation itself."
  "/opt/[c]oord/coord")

(defn- restart-coord!
  "Runs db/start! for a killed node, but never throws: if the node cannot
  come back (e.g. data corruption), jepsen's node-start-stopper would keep
  its internal state if the stopper threw -- every later :kill would then
  report \"already disrupting\" and silently stop disrupting, which invalidates
  a long soak without any visible failure. Report the failure instead."
  [db test node]
  (try
    (db/start! db test node)
    [:restarted node]
    (catch Exception e
      (warn e "Failed to restart coord node" node
            "(continuing on remaining nodes)")
      [:restart-failed node])))

(defn kill-one
  "Kills a random coord node (kill -9), then restarts it on :stop (same data
  directory, so this doubles as a crash+restart durability check)."
  [db]
  (nemesis/node-start-stopper
    rand/nth
    (fn [test node]
      (c/su (meh (c/exec :pkill :-9 :-f coord-pattern)))
      [:killed node])
    (fn [test node]
      (restart-coord! db test node))))

(defn kill-all
  "Kills every coord node, then restarts all of them on :stop."
  [db]
  (nemesis/node-start-stopper
    (fn [test nodes] nodes)
    (fn [test node]
      (c/su (meh (c/exec :pkill :-9 :-f coord-pattern)))
      [:killed node])
    (fn [test node]
      (restart-coord! db test node))))

(defn pause-one
  "SIGSTOPs a random coord node (simulating a long GC pause / clock stall),
  resumes it with SIGCONT on :stop."
  []
  (nemesis/node-start-stopper
    rand/nth
    (fn [test node]
      (c/su (meh (c/exec :pkill :-STOP :-f coord-pattern)))
      [:paused node])
    (fn [test node]
      (c/su (meh (c/exec :pkill :-CONT :-f coord-pattern)))
      [:resumed node])))

(defn compose-all
  "Combines kill, pause, and a single-node partition. The generator drives it
  with :kill/:kill-stop, :pause/:pause-stop, :partition/:partition-stop.

  A bare :stop (sent at the end of every test run to let the cluster
  recover) stops *all* sub-nemeses. jepsen's MapCompose cannot do that -- it
  routes each :f to a single sub-nemesis and would throw \"no nemesis can
  handle :stop\", leaving any mid-disruption active through the recovery
  window. Every sub-nemesis's :stop is idempotent (safe when not started)."
  [db]
  (let [kill      (kill-one db)
        pause     (pause-one)
        partition (nemesis/partition-random-node)]
    (reify
      nemesis/Nemesis
      (setup! [this test]
        (nemesis/setup! kill test)
        (nemesis/setup! pause test)
        (nemesis/setup! partition test)
        this)

      (invoke! [this test op]
        (case (:f op)
          :kill           (assoc (nemesis/invoke! kill test (assoc op :f :start))
                                 :f :kill)
          :kill-stop      (assoc (nemesis/invoke! kill test (assoc op :f :stop))
                                 :f :kill-stop)
          :pause          (assoc (nemesis/invoke! pause test (assoc op :f :start))
                                 :f :pause)
          :pause-stop     (assoc (nemesis/invoke! pause test (assoc op :f :stop))
                                 :f :pause-stop)
          :partition      (assoc (nemesis/invoke! partition test (assoc op :f :start))
                                 :f :partition)
          :partition-stop (assoc (nemesis/invoke! partition test (assoc op :f :stop))
                                 :f :partition-stop)
          :stop           (do (nemesis/invoke! kill test (assoc op :f :stop))
                              (nemesis/invoke! pause test (assoc op :f :stop))
                              (nemesis/invoke! partition test (assoc op :f :stop))
                              op)
          (throw (IllegalArgumentException.
                   (str "no nemesis can handle " (:f op))))))

      (teardown! [this test]
        (nemesis/teardown! kill test)
        (nemesis/teardown! pause test)
        (nemesis/teardown! partition test)
        this)

      nemesis/Reflection
      (fs [_]
        #{:kill :kill-stop :pause :pause-stop :partition :partition-stop
          :stop}))))
