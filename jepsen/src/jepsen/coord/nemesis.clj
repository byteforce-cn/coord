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

(def env-reset-script
  "T0.4: 幂等环境清理脚本，在**节点侧**的路径。jepsen/ 只上传到控制机
  （lab/Makefile 的 `upload` 目标），所以 `db/setup!` 会把它再传到每个节点；
  这也是脚本必须用 `--network-only` 的原因：它真的跑在被测节点上。"
  "/opt/coord/env-reset.sh")

(defn cleanup-network!
  "T0.4 —— 每个 nemesis 的 `:stop` 路径统一调用的幂等清理（脚本侧幂等）。

  用 `--network-only`：只清 iptables DROP/REJECT 规则、netem qdisc、磁盘填充
  文件。**绝不**在这里 pkill coord —— 本函数跑在 `:stop` 里，此时 kill 的
  `stop!` 刚把节点重启、pause 的 `stop!` 刚 SIGCONT，一个 pkill 就能把刚恢复的
  进程杀掉，让「扰动→恢复」变成「扰动→永久宕机」（那会让整个 soak 的历史无法
  归因）。进程级清理在 run 边界做（db.clj 的 setup! 已经做了）。

  失败只告警不抛：清理脚本缺失或单节点失败不应让 run 崩掉。jepsen 的
  node-start-stopper 一旦在 stop! 里抛异常，后续所有扰动会永久停止且只在
  日志里留一行（见 restart-coord! 的注释）。"
  []
  (c/su (meh (c/exec :bash env-reset-script "--network-only" "--quiet"))))

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
      ;; T0.4: 幂等网络清理（放在返回值之前，否则 op 的 :value 会变成 nil）
      (cleanup-network!)
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
      (cleanup-network!)
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
      (cleanup-network!)
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
                              ;; T0.4: 分区规则/日志捕获的 :stop 路径各调了一次，
                              ;; 这里再幂等跑一次作为兼底（脚本重复执行无害）。
                              (cleanup-network!)
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
