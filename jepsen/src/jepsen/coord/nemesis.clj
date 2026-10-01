(ns jepsen.coord.nemesis
  "Nemeses for coord. kill/restart/pause target individual coord daemons via
  pkill (killall is not installed on the Debian lab nodes), and partitions
  reuse Jepsen's built-in iptables nemeses.

  M5a 起还包括 **agent 侧**故障（kill-agent / pause-agent /
  partition-agent-server）：agent 跑在集群**之外**的节点上，所以它的目标不是从
  `(:nodes test)` 里抽的，而是由 `jepsen.coord.agent` 的 plan 直接给出。"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen [nemesis :as nemesis]
                    [db :as db]
                    [random :as rand]
                    [control :as c]
                    [util :as util :refer [meh]]]
            [jepsen.coord.agent :as agent]))

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

;; --------------------------------------------------------------------------
;; M5a —— agent 侧故障注入
;;
;; 与 coord 的 kill/pause 不同，目标不是从 `(:nodes test)` 抽的（agent 在集群
;; 之外的节点上），而是从 plan 里取 —— 所以不用 `node-start-stopper`，自己实现
;; 同样的 `:start`/`:stop` 契约。
;;
;; 硬约束：`stop` 分支**绝不抛**。jepsen 的 node-start-stopper 一旦在 stop 里抛，
;; 后续所有扰动静默停止（长跑会在没有可见失败的情况下作废）—— 见
;; `restart-coord!` 的注释。agent 的 restart-one! 已经遵守了这条。
;; --------------------------------------------------------------------------

(def ^:private agent-nemesis-keys
  #{:kill-agent :kill-agent-all :pause-agent :partition-agent-server :agent-all})

(defn agent-nemesis?
  "该 nemesis 关键字是否作用于 agent（用于起跑前的参数校验）。"
  [k]
  (contains? agent-nemesis-keys k))

(defn- require-plan!
  [plan]
  (when-not plan
    (throw (ex-info (str "agent nemesis 需要 --agents N>0（lock/election/idgen/"
                         "registry 与 agent 故障注入都是 agent 层面的事）")
                    {:nemesis :agent})))
  plan)

(defn- pick-agent
  "随机选一个 agent 序号（1-based）。"
  [plan]
  (inc (long (rand-int (int (:count plan))))))

(defn- some-agents
  [plan]
  (range 1 (inc (long (:count plan)))))

(defn- partition-agent-server!
  "把 agent 与**整个**集群隔开（双向 iptables DROP）。

  只动 agent 那个主机上的规则，而且是对**集群节点 IP** 逐条加/删：控制机与
  agent 之间的 SSH 隧道（客户端路径）不受影响 —— 这正是 AG-03 要的形态：
  「客户端→agent 通，agent→server 断」。

  用显式 `-D` 删除，不依赖 env-reset.sh（那脚本只上传到了集群节点，agent 节点
  可能根本没跑过 coord server）。"
  [plan i on?]
  (let [ips (mapv #(first (str/split % #":")) (:peers plan))]
    (c/on (agent/agent-host plan i)
      (c/su
        (doseq [ip ips]
          (meh (c/exec :iptables (if on? :-A :-D) :OUTPUT :-d ip :-j :DROP))
          (meh (c/exec :iptables (if on? :-A :-D) :INPUT :-s ip :-j :DROP)))))
    (if on?
      [:partitioned-agent (agent/agent-host plan i)]
      [:healed-agent (agent/agent-host plan i)])))

(defn- completion
  "把 nemesis 动作的产物包成 jepsen 要求的 **completion op**。

  jepsen 的 nemesis worker 对 `invoke!` 的返回值有硬契约：必须是 op map，且
  `:type/:process/:f` 要跟原 op 对得上（否则抛 `:jepsen.nemesis/invalid-completion`）。
  实测（M5a 第二轮矩阵）：四个 agent nemesis 都直接 return 了
  `[:killed-agent \"n4\"]` 这样的**向量** ⇒ 每次扰动都在日志里抛一条
  `invalid-completion`（一轮 9 个 cell 里 86 条），而且扰动状态机自己也拿不到
  动作结果。事件的语义信息放在 `:value` 里，不丢。"
  [op v]
  (assoc op :type :info :value v))

(defn kill-agent
  "kill -9 一个随机 agent（:stop 时重启它，保留 data_dir ⇒ 顺带验凭据的持久化）。"
  [db plan]
  (let [plan (require-plan! plan)]
    (reify
      nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [_ _test op]
        (let [i (pick-agent plan)]
          (case (:f op)
            :start (completion op (agent/kill-one! plan i))
            :stop  (completion op (agent/restart-one! plan i))
            (throw (IllegalArgumentException. (str "kill-agent: bad op " (:f op)))))))
      (teardown! [this test] this)
      nemesis/Reflection
      (fs [_] #{:start :stop}))))

(defn kill-agent-all
  "kill -9 所有 agent（:stop 时全部重启）。"
  [db plan]
  (let [plan (require-plan! plan)]
    (reify
      nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [_ _test op]
        (case (:f op)
          :start (completion op (vec (map #(agent/kill-one! plan %) (some-agents plan))))
          :stop  (completion op (vec (map #(agent/restart-one! plan %) (some-agents plan))))
          (throw (IllegalArgumentException. (str "kill-agent-all: bad op " (:f op))))))
      (teardown! [this test] this)
      nemesis/Reflection
      (fs [_] #{:start :stop}))))

(defn pause-agent
  "SIGSTOP 一个随机 agent（本地不推进、连接还在 —— 观察「假死」形态）。"
  [db plan]
  (let [plan (require-plan! plan)]
    (reify
      nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [_ _test op]
        (let [i (pick-agent plan)]
          (case (:f op)
            :start (completion op (agent/pause-one! plan i))
            :stop  (completion op (agent/resume-one! plan i))
            (throw (IllegalArgumentException. (str "pause-agent: bad op " (:f op)))))))
      (teardown! [this test] this)
      nemesis/Reflection
      (fs [_] #{:start :stop}))))

(defn partition-agent-server
  "把**随机一个** agent 与整个集群隔开（AG-03 的核心故障）。"
  [db plan]
  (let [plan (require-plan! plan)]
    (reify
      nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [_ _test op]
        (let [i (pick-agent plan)]
          (case (:f op)
            :start (completion op (partition-agent-server! plan i true))
            :stop  (completion op (partition-agent-server! plan i false))
            (throw (IllegalArgumentException.
                     (str "partition-agent-server: bad op " (:f op)))))))
      (teardown! [this test] this)
      nemesis/Reflection
      (fs [_] #{:start :stop}))))

(defn compose-agent-all
  "轮换：kill → pause → partition-agent-server。

  `:stop` 必须把**全部**子 nemesis 收尾（jepsen 的 MapCompose 只会把 :stop
  路由给一个子 nemesis，其余的会带着未收尾的故障穿过恢复窗口）。"
  [db plan]
  (let [plan (require-plan! plan)
        k    (kill-agent db plan)
        p    (pause-agent db plan)
        pa   (partition-agent-server db plan)]
    (reify
      nemesis/Nemesis
      (setup! [this test]
        (nemesis/setup! k test) (nemesis/setup! p test) (nemesis/setup! pa test)
        this)
      (invoke! [_ test op]
        (case (:f op)
          :kill-agent      (completion op (nemesis/invoke! k test (assoc op :f :start)))
          :kill-agent-stop (completion op (nemesis/invoke! k test (assoc op :f :stop)))
          :pause-agent     (completion op (nemesis/invoke! p test (assoc op :f :start)))
          :pause-agent-stop (completion op (nemesis/invoke! p test (assoc op :f :stop)))
          :partition-agent (completion op (nemesis/invoke! pa test (assoc op :f :start)))
          :partition-agent-stop (completion op (nemesis/invoke! pa test (assoc op :f :stop)))
          :stop (do (nemesis/invoke! k test (assoc op :f :stop))
                    (nemesis/invoke! p test (assoc op :f :stop))
                    (nemesis/invoke! pa test (assoc op :f :stop))
                    (completion op [:agent-all-stopped]))
          (throw (IllegalArgumentException. (str "no nemesis can handle " (:f op))))))
      (teardown! [this test]
        (nemesis/teardown! k test)
        (nemesis/teardown! p test)
        (nemesis/teardown! pa test)
        this)
      nemesis/Reflection
      (fs [_] #{:kill-agent :kill-agent-stop :pause-agent :pause-agent-stop
                :partition-agent :partition-agent-stop :stop}))))
