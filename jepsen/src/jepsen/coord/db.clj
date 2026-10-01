(ns jepsen.coord.db
  "Deploys, starts, and tears down a 3-node coord cluster across DB nodes.

  Each coord node runs as a start-stop-daemon background process with a
  pidfile, so the kill/restart nemesis can target individual nodes. All
  configuration (shared secrets, root password) is generated once and used
  identically on every node."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [control :as c]
                    [util :as util :refer [meh]]]
            [jepsen.control [util :as cu]]
            [jepsen.coord [proto :as p]
                          [regions :as regions]])
  (:import [io.grpc ManagedChannel StatusRuntimeException]))

;; Default secrets for the lab cluster (shared across all 3 nodes).
(def default-root-password "66c57bb56bce306f484344e4a8650836")
(def default-auth-root-key "820662a9f85924333d6d466714bc51734b251e73b827a394e1978e36d2429bfc")
(def default-raft-secret "93edf94d2860fec54bc59278a804cb1bbd2d0a7243e4f39d667d66966d9844d1")

(def default-agent-verifying-key
  "agent 验签 CCT 用的 Ed25519 **公钥**（hex），由 `default-auth-root-key` 按 server
  同一路径派生：

      seed = HKDF-SHA256(ikm = auth_root_key, salt = None(全 0), info =
                         \"coord-cct-ed25519-v1\", L = 32)
      verifying_key = Ed25519(seed).public_key()

  （`coord-server/src/auth/token_signing.rs:196-208` 的算法；重新派生用
  `jepsen/scripts/derive-cct-pubkey.py`，`make checkers` 会把它与常量对比。）

  为什么必须显式配：server 用 Ed25519 签发 CCT（HMAC 对称方案已弃用，因为任何
  持密钥的 agent 都能自签 root token），而 agent **只验签**。没有它，agent 会以
  「Ed25519 CCT presented but no public key configured」拒绝**所有**带 CCT 的
  请求 —— 实测过，表现为客户端 setup 阶段反复读失败。

  注：当前**没有**任何自动获取路径（CLI/RPC/日志都没有），部署时必须手工派生
  并同步 —— 这是 M5a 落地实测到的可运维性缺口，已记入覆盖方案 §7 待确认清单。"
  "d0c6d1ff5fd200db97c05ffeb873340adb89a7a6de7f421f6d27db58050eedd4")

(def server-count
  "集群固定 3 节点（`initial-nodes-toml` 取前 3 个 `:nodes`）。这个数字是
  agent 拓扑选择的依据：agent 默认跑在第 4 个节点之后，这样
  `:partition-agent-server` 才能把 agent 与**整个**集群隔开（见
  `jepsen.coord.agent` 的 ns 注释 3）。"
  3)

(def agent-pattern
  "pkill -f 模式：agent 二进制。**与 coord 的路径必须不同**
  （`/opt/[c]oord/coord` 不匹配 `/opt/coord-agent/coord`）—— 否则
  「杀 server」会顺带杀掉 agent，两条故障注入纠缠在一起。

  定义在 db 侧是为了避开循环依赖：`agent.clj` 依赖 db，db 不依赖 agent。"
  "/opt/[c]oord-agent/coord")

(def grpc-port p/grpc-port)

;; --------------------------------------------------------------------------
;; Configuration
;; --------------------------------------------------------------------------

(defn node-id
  "1-based index of `node` within the test's node list."
  [test node]
  (inc (.indexOf (vec (:nodes test)) node)))

(defn- node-ip
  "Resolves a node hostname to an IP address (from the control node's
  /etc/hosts, which maps n1..n5 in this lab). coord requires IP literals in
  initial_nodes -- hostnames are rejected."
  [node]
  (.getHostAddress (java.net.InetAddress/getByName node)))

(defn- initial-nodes-toml
  "The initial_nodes array for a 3-node cluster, using resolved node IPs."
  [nodes]
  (->> (take server-count nodes)
       (map-indexed (fn [i n]
                      (let [ip (node-ip n)]
                        (str "  { id = " (inc i)
                             ", grpc = \"" ip ":" grpc-port "\""
                             ", raft = \"" ip ":" (+ grpc-port 1) "\" }"))))
       (str/join ",\n")))

(defn agent-peers
  "集群成员的 `ip:grpc-port` 列表（agent 的 `static_peers` 与
  `coord security bootstrap-role --addr` 都用它）。

  用 **IP 字面量**而不是主机名：节点侧不一定有 n1..n5 的 /etc/hosts 条目
  （db.clj 一直在控制机上解析 IP，coord 的 initial_nodes 也要求 IP 字面量）。"
  [nodes]
  (mapv (fn [n] (str (node-ip n) ":" grpc-port))
        (take server-count nodes)))

(defn config-str
  "The full TOML config for a given node.

  Multi-raft mode (Phase 4 T4.1): when the db record carries a :regions table
  (from --regions N, see jepsen.coord.regions), appends an identical
  `[multi_raft]` block on every node -- each region replicated to all
  cluster.initial_nodes members (v1 static replication). Without :regions the
  output is byte-identical to the single-Raft legacy config (T2.6)."
  [db test node]
  (let [id          (node-id test node)
        bootstrap?  (= id 1)
        nodes       (:nodes test)
        multi-raft  (regions/multi-raft-toml (:regions db))]
    (str
     "[node]\n"
     "id = " id "\n"
     "name = \"coord-" (format "%02d" id) "\"\n"
     "\n"
     "[network]\n"
     "grpc_addr = \"0.0.0.0:" grpc-port "\"\n"
     "raft_addr = \"0.0.0.0:" (+ grpc-port 1) "\"\n"
     "http_addr = \"0.0.0.0:" (+ grpc-port 10) "\"\n"
     "\n"
     "[cluster]\n"
     "cluster_name = \"" (:cluster-name db) "\"\n"
     "bootstrap = " bootstrap? "\n"
     "initial_nodes = [\n"
     (initial-nodes-toml nodes)
     "\n]\n"
     "\n"
     "[storage]\n"
     "data_dir = \"" (:data-dir db) "\"\n"
     "\n"
     "[security]\n"
     "auth_enabled = true\n"
     (when bootstrap?
       (str "root_password = \"" (:root-password db) "\"\n"))
     (when (:agent-bootstrap-token db)
       ;; M5：agent 用一次性 bootstrap token 换短期 agent-bootstrap CCT，
       ;; 再用它开通插件/自身账户。token 必须与 agent 侧 [auth].bootstrap_token
       ;; 一致，否则 agent 起得来但所有角色门控 RPC 全 403（lib.rs 的 A3 校验）。
       (str "agent_bootstrap_tokens = [\"" (:agent-bootstrap-token db) "\"]\n"))
     "auth_root_key = \"" (:auth-root-key db) "\"\n"
     "raft_shared_secret = \"" (:raft-secret db) "\"\n"
     ;; W4-1：lab 是 docker 内网测试环境（非生产），gRPC 未启用 TLS；
     ;; coord 默认 fail-closed（鉴权开启 + 非 loopback gRPC + 无 TLS ⇒ 拒绝启动，
     ;; R-SEC-04），此处为显式逃生阀。生产严禁开启。
     "allow_plaintext_remote = true\n"
     (when multi-raft multi-raft))))

(defn- write-config!
  "Writes this node's config file on the node."
  [db test node]
  (let [id     (node-id test node)
        local  (str "/tmp/coord-node" id ".toml")
        remote (str (:config-dir db) "/node" id ".toml")]
    (spit local (config-str db test node))
    (try
      (c/su
        (c/exec :mkdir :-p (:config-dir db))
        (c/upload local remote))
      (finally
        (io/delete-file local true)))))

;; --------------------------------------------------------------------------
;; Process management
;; --------------------------------------------------------------------------

(defn- pidfile
  [db test node]
  (str (:pidfile-prefix db) (node-id test node) ".pid"))

(defn- pkill-coord!
  "Sends a signal to every coord daemon on the current node via pkill.
  The bracketed pattern avoids pkill matching its own command line.
  (killall is not installed on these Debian nodes, so jepsen's stop-daemon!
  is unusable here.)"
  [signal]
  (c/su
    (meh (c/exec :pkill signal :-f "/opt/[c]oord/coord"))))

(defn- start-coord!
  "Starts the coord daemon for this node (start-stop-daemon, backgrounded).

  NOTE: coord's CLI --addr/--cluster-name have defaults that always override
  the config file, so we pass them explicitly here (grpc must bind 0.0.0.0
  for cross-host access)."
  [db test node]
  (let [id (node-id test node)]
    (c/su
      (cu/start-daemon!
        {:logfile (:logfile db)
         :pidfile (pidfile db test node)
         :chdir   "/"
         :env     {"RUST_LOG" (or (System/getenv "COORD_RUST_LOG") "coord=info")}}
        (str (:coord-dir db) "/coord")
        "server"
        "--id" (str id)
        "--addr" (str "0.0.0.0:" grpc-port)
        "--cluster-name" (:cluster-name db)
        "--config" (str (:config-dir db) "/node" id ".toml")
        "--log-format" "pretty"))))

(defn- stop-coord!
  "Kills every coord daemon on the node. SIGTERM first (graceful), then
  SIGKILL as a fallback (kill -9 is safe: coord fsyncs all writes before
  returning)."
  [db test node]
  (c/su
    (meh (c/exec :pkill :-TERM :-f "/opt/[c]oord/coord"))
    (Thread/sleep 1500)
    (pkill-coord! :-9)))

;; --------------------------------------------------------------------------
;; Readiness
;; --------------------------------------------------------------------------

(defn node-ready?
  "True if this node's gRPC endpoint responds at all (Status needs a token in
  this coord build, so any gRPC response counts as up)."
  [node]
  (let [ch (p/channel node)]
    (try
      (p/status-up? ch)
      (finally
        (.shutdownNow ^ManagedChannel ch)))))

(defn wait-for-node-ready!
  "Blocks until the given node's gRPC responds, or throws after timeout-ms."
  [node timeout-ms]
  (info "Waiting for coord on" node "to be ready")
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (node-ready? node)
        (info "coord" node "is ready")
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 1000)
              (recur))
          (throw (ex-info (str "coord on " node " did not become ready within "
                               timeout-ms "ms")
                          {:node node})))))))

;; --------------------------------------------------------------------------
;; DB protocol
;; --------------------------------------------------------------------------

(defrecord CoordDB [coord-bin nodes data-dir config-dir coord-dir logfile
                    pidfile-prefix root-password auth-root-key raft-secret
                    cluster-name regions agents agent-peers
                    agent-bootstrap-token]
  db/DB
  (setup! [db test node]
    (info "Setting up coord on" node)
    (c/su
      ;; Clean slate: no leftover coord processes, fresh data directory
      (pkill-coord! :-9)
      (c/exec :rm :-rf (:data-dir db))
      (c/exec :mkdir :-p (:data-dir db) (:coord-dir db) (:config-dir db))
      ;; T0.4: 节点侧没有 coord-test（jepsen/ 只传到控制机），把环境清理脚本
      ;; 传到节点，nemesis 的 :stop 路径依赖它。缺脚本只告警不中断——一次缺
      ;; helper 不应让整个套件跑不起来，但会在这里留痕方便归因。
      (try
        (c/upload "scripts/env-reset.sh" (str (:coord-dir db) "/env-reset.sh"))
        (c/exec :chmod :+x (str (:coord-dir db) "/env-reset.sh"))
        (catch Exception e
          (warn e "T0.4: could not deploy env-reset.sh to" node
                "— nemesis :stop network cleanup will be a no-op")))
      ;; Upload the binary (idempotent)
      (c/upload (:coord-bin db) (str (:coord-dir db) "/coord"))
      (c/exec :chmod :+x (str (:coord-dir db) "/coord"))
      ;; Config
      (write-config! db test node)
      ;; Log file
      (c/exec :rm :-f (:logfile db))
      ;; Start
      (start-coord! db test node))
    ;; Wait for this node's gRPC endpoint (from the control node)
    (wait-for-node-ready! node 60000))

  (teardown! [db test node]
    (info "Tearing down coord on" node)
    (stop-coord! db test node)
    (c/su
      ;; M5：防御性清 agent。正常收尾由 client/teardown! → agent/teardown! 做，
      ;; 但那一步在异常路径上可能没跑到；**留一个跑着的 agent 会污染下一次 run
      ;; 的路由证明**（计数器不是从 0 起），所以这里再清一次（幂等）。
      (meh (c/exec :pkill :-9 :-f agent-pattern))
      (c/exec :rm :-rf (:data-dir db))))

  db/Kill
  (kill! [db test node]
    (info "Killing coord on" node)
    (pkill-coord! :-9))

  (start! [db test node]
    (info "Restarting coord on" node)
    (start-coord! db test node)
    (wait-for-node-ready! node 60000))

  db/LogFiles
  (log-files [db test node]
    {(:logfile db) "coord.log"}))

(defn coord
  "Constructs the coord DB from test options.

  opts :regions (int, from --regions) enables multi-raft mode: the db record
  carries the derived region table (nil = single-Raft legacy).

  opts :agents (int, from --agents) > 0 additionally carries the agent plan
  inputs: the resolved cluster peers (for the agent's `static_peers`) and a
  shared one-shot bootstrap token. The agent processes themselves are managed
  by `jepsen.coord.agent` (they are not coord servers, so they do not fit the
  per-node DB protocol -- they must be started *after* the whole cluster is up)."
  [opts]
  (let [agents (long (or (:agents opts) 0))]
    (map->CoordDB
      {:coord-bin      (or (:coord-bin opts) "/root/coord-test/coord")
       :nodes          (:nodes opts)
       :data-dir       (or (:data-dir opts) "/var/lib/coord")
       :config-dir     (or (:config-dir opts) "/etc/coord")
       :coord-dir      (or (:coord-dir opts) "/opt/coord")
       :logfile        (or (:logfile opts) "/var/log/coord.log")
       :pidfile-prefix (or (:pidfile-prefix opts) "/var/run/coord-")
       :root-password  (or (:root-password opts) default-root-password)
       :auth-root-key  (or (:auth-root-key opts) default-auth-root-key)
       :raft-secret    (or (:raft-secret opts) default-raft-secret)
       :cluster-name   (or (:cluster-name opts) "jepsen-coord")
       :regions        (regions/region-configs (:regions opts))
       :agents         agents
       :agent-peers    (when (pos? agents) (agent-peers (:nodes opts)))
       :agent-bootstrap-token (when (pos? agents)
                                (or (:agent-bootstrap-token opts)
                                    "jepsen-agent-bootstrap-token-0123456789abcdef"))})))
