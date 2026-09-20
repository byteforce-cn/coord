(ns jepsen.coord.agent
  "TA1 —— coord-agent 部署与「路由证明」（M5: agent 层）。

  ## 为什么是这套做法

  1. **agent 就是同一个 `coord` 二进制**（`coord agent --agent-config …`，
     `coord/src/main.rs:1656`）。所以不需要新产物、不需要新镜像：把节点上
     `db.clj` 已经传好的 `/opt/coord/coord` **复制**到 `/opt/coord-agent/coord`。

     为什么必须换路径：coord 的 kill nemesis 用
     `pkill -f /opt/[c]oord/coord`。若 agent 从同一路径运行，「杀 server」会顺带
     杀 agent、「杀 agent」也会杀 server —— 两条故障注入从此纠缠，任何红 run
     都无法归因。

  2. **agent 只绑 loopback，客户端经 SSH 隧道访问**。非 loopback 绑定被
     `coord-agent/src/lib.rs:908-920` 硬性要求 auth+TLS（fail-closed，与 server
     同口径），而生产形态本来就是「应用与本机 agent 讲 loopback」（daemonset）。
     所以控制机为每个 agent 建一条
     `ssh -N -L 127.0.0.1:<local>:127.0.0.1:19527 root@<node>`，
     jepsen 客户端连 `127.0.0.1:<local>`：既不必给 agent 配 TLS，语义上也与生产
     一致（同机客户端 → 本机 agent）。

     隧道是持久的：agent 被 kill/restart 不影响隧道进程，后续连接由 ssh 重新
     拨号到重启后的 agent —— 这正是 `:kill-agent` 想要的（客户端看到的是 agent
     不可用，而不是隧道坏了）。

  3. **agent 跑在集群之外的节点上**（默认取 `:nodes` 里第 4 个起）。这样
     `:partition-agent-server` 才可能把 agent 与**整个**集群隔开；若 agent 与
     server 同机，本机那条链路永远通，「断连降级」永远测不到（AG-03）。

  4. **控制机侧的动作不走 jepsen session**。实测（2026-09-18）：jepsen-control
     容器里**没有 sshd**，而 `jepsen.control/ssh*` 又要求 `*session*`，所以控制机
     自己**无法**成为 `c/on` 的目标。但测试进程本来就跑在控制机上 ⇒ 控制机侧的
     命令（建隧道、`coord auth login` / `security bootstrap-role`）一律用
     `clojure.java.shell` 直接本地执行（见 `shell!`）；只有节点侧的动作用 `c/on`。

  5. **鉴权**：server 侧 `[security].agent_bootstrap_tokens`，agent 侧
     `[auth].bootstrap_token`，启动前幂等跑 `coord security bootstrap-role`
     （否则 agent 的 RoleCache 永远同步不上来，一切角色门控 RPC 全 403 ——
     `lib.rs` 的 A3 校验专门拦这种「启动了但全 403」的形态，它比拒绝启动更糟）。

  ## 路由证明（AG-01，防「直连假绿」）

  `--via-agent` 只是把客户端 endpoint 换到隧道端口；若端点写错（写到 server 上），
  代理面的 workload **照样全绿**，而 agent 一行代码都没执行。

  所以每个 `--via-agent` 的 run 都要回答「这次真的经过 agent 了吗」：

  - **代理面**（KV/Txn/Lease/Watch 经 agent）：抓
    `coord_agent_grpc_requests_total{method=…}`（`metrics.rs` 的
    `AgentGrpcMetricsLayer` 按 Put/Range/Delete/Txn/… 路径计数）。agent 每次 run
    都是**新进程 + 新 data_dir**，计数器从 0 起，所以 run 结束后抓到 > 0 就等于
    「确有请求落到 agent」。这就是 `routing-proof!`。
  - **本地面**（`coord.agent.Lock` 等）：这些服务 **server 上根本不存在**
    （UNIMPLEMENTED），能成功本身就是证明 —— 指标不覆盖它们，所以不能拿
    `grpc_requests_total` 当那类 workload 的判据（否则会把「真的跑过了」误判成
    「没经过 agent」）。

  抓取路径：在 agent 所在节点上用 bash 内建 `/dev/tcp` 直接 HTTP GET
  `127.0.0.1:19528/metrics`（不依赖 curl/nc，也不经过隧道）；隧道自身的健康检查
  则用同一条 helper 打到控制机的隧道端口（端到端证明）。"
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [util :as util :refer [meh]]]
            [jepsen.control [util :as cu]]
            [jepsen.coord.db :as db]))

;; --------------------------------------------------------------------------
;; 常量
;; --------------------------------------------------------------------------

(def agent-port
  "agent 的 gRPC 端口（`AgentConfig::default`）。各 agent 在不同主机上，端口可相同。"
  19527)

(def agent-http-port
  "agent 的可观测性（/metrics、/health）端口。"
  19528)

(def tunnel-port-base
  "控制机上隧道端口的基址：第 i 个 agent 用 19600+i（同一台控制机上必须错开）。"
  19600)

(def tunnel-key
  "控制机连节点用的私钥（lab 生成的 id_ed25519；jepsen 自己也用它）。"
  "/root/.ssh/id_ed25519")

(def agent-dir
  "节点上 agent 二进制目录（**必须**与 db.clj 的 /opt/coord 分开，见 ns 注释 1）。"
  "/opt/coord-agent")

(def agent-config-dir "/etc/coord-agent")

(def ^:private metrics-script-local
  "控制机侧的脚本路径（测试进程的 cwd = /root/coord-test）。"
  "scripts/agent-metrics.sh")

(def ^:private port-probe-script-local
  "控制机侧的「端口是否在监听」探针（上传到节点后用于 gRPC 就绪判定）。"
  "scripts/agent-port-open.sh")

(def default-services
  "默认启用的 agent 本地面。cache/mq/event 不在 M5a 范围：它们的语义（ISR 复制、
  at-least-once）属 M5b，开了就得配判据，否则只是多一份没人看的绿灯。"
  #{:registry :lock :idgen :leader-election})

;; --------------------------------------------------------------------------
;; 控制机侧本地执行（不走 jepsen session，见 ns 注释 4）
;; --------------------------------------------------------------------------

(defn shell!
  "在**控制机**上跑一段 bash（返回 stdout；非 0 退出则抛）。"
  [cmd]
  (let [{:keys [exit out err]} (sh/sh "bash" "-c" cmd)]
    (when-not (zero? exit)
      (throw (ex-info (str "local command failed (exit " exit "): " cmd "\n" err)
                      {:cmd cmd :exit exit :err err})))
    out))

(defn shell
  "控制机侧 bash，返回 `{:exit :out :err}`（不抛）。"
  [cmd]
  (sh/sh "bash" "-c" cmd))

;; --------------------------------------------------------------------------
;; 计划（plan）
;; --------------------------------------------------------------------------

(defn enabled?
  "本次 run 是否启用 agent（`:agents N`，N>0）。缺省 0 = 完全不改变现有行为。"
  [opts]
  (pos? (long (or (:agents opts) 0))))

(defn agent-nodes
  "选 agent 所在节点：优先用**集群之外**的节点（`:nodes` 的第 4 个起），不够时
  退回集群节点并告警（此时 `:partition-agent-server` 无法整体隔离）。"
  [nodes n]
  (let [nodes (vec nodes)
        tail  (vec (drop db/server-count nodes))]
    (cond
      (zero? (long n)) []
      (>= (count tail) n) (vec (take n tail))
      :else (do (warn "coord-agent: only" (count tail) "node(s) outside the"
                      db/server-count "-node cluster, but" n "agent(s) requested;"
                      "agents will share nodes with coord servers —"
                      " :partition-agent-server cannot isolate the agent")
                (vec (take n nodes))))))

(defn plan
  "派生出跑 `n` 个 agent 需要的一切。未启用 agent 时返回 nil。

  opts：:agents · :agent-peers（db.clj 解析出的 ip:50051 列表）· :coord-dir
        :coord-bin · :root-password · :auth-root-key · :agent-bootstrap-token
        :agent-services · :agent-idgen-node-ids · :agent-provisioner-prefix"
  [opts test]
  (let [n (long (or (:agents opts) 0))]
    (when (pos? n)
      (let [hosts (agent-nodes (:nodes test) n)
            peers (vec (:agent-peers opts))]
        (when-not (seq peers)
          (throw (ex-info (str "coord-agent: :agent-peers 为空 —— db/coord 必须在 "
                               ":agents > 0 时解析出集群 IP（见 db.clj 的 :agent-peers）")
                          {})))
        {:count          n
         :hosts          hosts
         :peers          peers
         :coord-dir      (or (:coord-dir opts) "/opt/coord")
         :coord-bin      (or (:coord-bin opts) "/root/coord-test/coord")
         :root-password  (or (:root-password opts) db/default-root-password)
         :auth-root-key  (or (:auth-root-key opts) db/default-auth-root-key)
         ;; agent 验签 CCT 的 Ed25519 公钥（HMAC 对称密钥已弃用；agent 只验签，
         ;; 所以必须显式给公钥 —— 否则所有带 CCT 的请求都会被拒，见 db.clj 常量注释）
         :agent-verifying-key (or (:agent-verifying-key opts)
                                  db/default-agent-verifying-key)
         :bootstrap-token (or (:agent-bootstrap-token opts)
                              "jepsen-agent-bootstrap-token-0123456789abcdef")
         :services       (or (:agent-services opts) default-services)
         :idgen-node-ids (vec (or (:agent-idgen-node-ids opts) []))
         :provisioner-prefix (or (:agent-provisioner-prefix opts)
                                 "coord-agent-jepsen")
         ;; 隧道端口基址**每个 run 随机**：固定端口在连续 run 之间会撞上
         ;; TIME_WAIT（客户端上一次 run 连过 19601，socket 关闭后本地端口进入
         ;; TIME_WAIT，而下一次 run 的 `ssh -L` 不加 SO_REUSEADDR，绑不上；
         ;; `ExitOnForwardFailure=yes` 又让 ssh 直接退出）⇒ 客户端连不上 ⇒
         ;; 「coord cluster did not become ready」这种**与 agent 无关的假红**。
         ;; 随机基址把碰撞概率压到可忽略，且不需要任何重试逻辑。
         :tunnel-base    (+ 20000 (long (rand-int 40000)))}))))

(defn agent-host [{:keys [hosts]} i] (nth hosts (dec (long i))))
(defn data-dir    [i] (str "/var/lib/coord-agent-a" i))
(defn pidfile     [i] (str "/var/run/coord-agent-a" i ".pid"))
(defn logfile     [i] (str "/var/log/coord-agent-a" i ".log"))
(defn config-path [i] (str agent-config-dir "/a" i ".toml"))
(defn tunnel-pidfile [i] (str "/var/run/coord-tunnel-a" i ".pid"))
(defn tunnel-logfile [i] (str "/var/log/coord-tunnel-a" i ".log"))
(defn local-port
  "控制机上的隧道端口：run 级随机基址 + agent 序号（见 plan 的 :tunnel-base）。"
  [{:keys [tunnel-base] :as plan} i]
  (+ (long (or tunnel-base tunnel-port-base)) (long i)))

(defn endpoint [plan i] (str "127.0.0.1:" (local-port plan i)))

(defn endpoints
  "客户端应当连接的地址（控制机上的隧道端口）。"
  [plan]
  (when plan
    (mapv #(endpoint plan %) (range 1 (inc (long (:count plan)))))))

;; --------------------------------------------------------------------------
;; agent TOML
;; --------------------------------------------------------------------------

(defn- toml-bool [b] (if b "true" "false"))

(defn config-str
  "AgentConfig TOML（字段名与 `coord-agent/src/lib.rs` 的 serde 定义一致）。

  - 刻意**不写** `discovery_mode`：走 serde 默认（Static），并由 CLI 显式传
    `--discovery static` —— 少一个「枚举字符串拼错 → 启动失败」的面。
  - `cache_kv_ttl_secs = 0` 与 B1 的实现默认一致（**默认关闭**本地 KV 读缓存）；
    显式写出来是让「缓存开着」成为**有意为之**的实验变量（M5b 用），而不是
    默认行为的副产品。
  - `auth.signing_key_hex` 用集群共享的 auth_root_key：与 server 同口径验 CCT
    （HMAC 路径，>= 32 字节，满足 A3 校验）。"
  [{:keys [peers services bootstrap-token provisioner-prefix idgen-node-ids]
    :as plan} i]
  (let [n     (long i)
        svc   (fn [k] (toml-bool (contains? services k)))]
    (str
     "# coord-agent —— 由 jepsen.coord.agent 生成（每个 run 重新生成）\n"
     "agent_addr = \"127.0.0.1:" agent-port "\"\n"
     "http_addr = \"127.0.0.1:" agent-http-port "\"\n"
     "data_dir = \"" (data-dir n) "\"\n"
     "static_peers = [" (str/join ", " (map #(str "\"" % "\"") peers)) "]\n"
     "\n"
     "cache_kv_ttl_secs = 0\n"
     "proxy_max_retries = 3\n"
     "proxy_request_timeout_secs = 5\n"
     "\n[auth]\n"
     "enabled = true\n"
     ;; Ed25519 公钥：server 持私钥签发、agent 仅验签（HMAC 对称方案已弃用）。
     ;; 两者都写上是刻意的：signing_key_hex 只用于「存量 HMAC token 的宽限期验
     ;; 证」，而新签发的 token 是 Ed25519 —— 只配 HMAC 会让所有请求被拒。
     "signing_key_hex = \"" (:auth-root-key plan) "\"\n"
     "verifying_key_hex = \"" (:agent-verifying-key plan) "\"\n"
     "clock_drift_secs = 300\n"
     "bootstrap_token = \"" bootstrap-token "\"\n"
     ;; 多个 agent 必须各有唯一名字（identity.rs 的 provisioner_user 说明）
     "provisioner_user = \"" provisioner-prefix "-a" n "\"\n"
     "\n[services]\n"
     "registry = " (svc :registry) "\n"
     "lock = " (svc :lock) "\n"
     "idgen = " (svc :idgen) "\n"
     "leader_election = " (svc :leader-election) "\n"
     "event_notification = " (svc :event) "\n"
     "cache = " (svc :cache) "\n"
     "mq = " (svc :mq) "\n"
     ;; AG-08：显式 nodeid 才能构造「两个 agent 抢同一 nodeid」的形态。
     ;; 不给时 agent 按主机名派生（不同主机 → 不同 nodeid）。
     (when-let [nid (get (vec idgen-node-ids) (dec n))]
       (str "idgen_node_id = " (long nid) "\n")))))

(defn- write-config!
  [plan i]
  (let [node (agent-host plan i)
        tmp  (str "/tmp/coord-agent-a" i ".toml")]
    (spit tmp (config-str plan i))
    (try
      (c/on node
        (c/su
          (c/exec :mkdir :-p agent-config-dir)
          (c/upload tmp (config-path i))))
      (finally
        (io/delete-file tmp true)))))

;; --------------------------------------------------------------------------
;; 进程生命周期（节点侧）
;; --------------------------------------------------------------------------

(defn- deploy-metrics-script!
  "把 `scripts/agent-metrics.sh` 与 `scripts/agent-port-open.sh` 上传到 agent 节点。

  前者：就绪探测（HTTP /metrics）与路由证明共用；
  后者：**gRPC 端口**探针（见 `agent-port-open.sh` 的头注释与 `wait-for-agent!`）。"
  [plan i]
  (c/on (agent-host plan i)
    (c/su
      (c/exec :mkdir :-p agent-dir)
      (c/upload metrics-script-local (str agent-dir "/agent-metrics.sh"))
      (c/exec :chmod :+x (str agent-dir "/agent-metrics.sh"))
      (c/upload port-probe-script-local (str agent-dir "/agent-port-open.sh"))
      (c/exec :chmod :+x (str agent-dir "/agent-port-open.sh")))))

(defn- install-binary!
  "把控制机上的 coord 二进制上传到 agent 目录。

  为什么是**上传**而不是复制节点上的 `/opt/coord/coord`：agent 默认跑在集群
  **之外**的节点（第 4 个起），那些节点 jepsen 根本没跑过 coord server，
  所以 `/opt/coord/coord` 不存在。控制机侧一定有这份二进制（lab 把仓库
  bind-mount 进来，`jepsen/coord` 指向 release 产物）。"
  [{:keys [coord-bin] :as plan} i]
  (c/on (agent-host plan i)
    (c/su
      (c/exec :mkdir :-p agent-dir)
      (c/upload coord-bin (str agent-dir "/coord"))
      (c/exec :chmod :+x (str agent-dir "/coord")))))

(defn stop!
  "SIGTERM → SIGKILL（按 pattern；pidfile 可能已不在）。幂等。"
  [plan i]
  (c/on (agent-host plan i)
    (c/su
      (meh (c/exec :pkill :-TERM :-f db/agent-pattern))
      (Thread/sleep 300)
      (meh (c/exec :pkill :-9 :-f db/agent-pattern))
      (meh (c/exec :rm :-f (pidfile i))))))

(defn- start!
  "start-stop-daemon 起 agent（pidfile-backed，nemesis 才能定向 kill）。"
  [plan i]
  (c/on (agent-host plan i)
    (c/su
      (cu/start-daemon!
        {:logfile (logfile i)
         :pidfile (pidfile i)
         :chdir   "/"
         :env     {"RUST_LOG" (or (System/getenv "COORD_RUST_LOG") "coord=info")}}
        (str agent-dir "/coord")
        "agent"
        "--agent-addr" (str "127.0.0.1:" agent-port)
        "--http-addr"  (str "127.0.0.1:" agent-http-port)
        "--discovery"  "static"
        "--static-peers" (str/join "," (:peers plan))
        "--agent-config" (config-path i)))))

;; --------------------------------------------------------------------------
;; 隧道（控制机侧，本地进程）
;; --------------------------------------------------------------------------

(defn parse-metrics
  "把 /metrics 文本解析成 `{:grpc-requests {method n} :total-requests n
  :connected bool :uptime-seconds x}`。**纯函数**（fixture 用，不碰网络）。"
  [text]
  (if (str/blank? (str text))
    nil
    (let [lines (str/split-lines (str text))
          grpc  (into {}
                      (keep (fn [l]
                              (when-let [[_ m v]
                                         (re-find #"^coord_agent_grpc_requests_total\{method=\"([^\"]+)\"\}\s+(\d+)"
                                                  l)]
                                [(keyword m) (Long/parseLong v)]))
                            lines))]
      {:grpc-requests  grpc
       :total-requests (reduce + 0 (vals grpc))
       :connected (some (fn [l]
                          (when-let [[_ v] (re-find #"^coord_agent_connected\s+(\d+)" l)]
                            (pos? (Long/parseLong v))))
                        lines)
       :uptime-seconds (some (fn [l]
                               (when-let [[_ v]
                                          (re-find #"^coord_agent_uptime_seconds\s+([0-9.]+)" l)]
                                 (Double/parseDouble v)))
                             lines)})))

(defn tunnel-metrics!
  "从控制机**经隧道**抓一次指标：端到端证明「控制机 → 隧道 → agent」。"
  [plan i]
  (let [{:keys [exit out]} (shell (str "bash " metrics-script-local " "
                                       (local-port plan i)))]
    (when (zero? exit)
      (parse-metrics out))))

(defn- kill-tunnel-by-port!
  "按端口模式兜底杀隧道。

  pidfile 可能已被后一次 start 覆盖（或上一次 run 崩掉留下孤儿 ssh），那种孤儿
  会一直占着端口，让后续 run 的 `ExitOnForwardFailure` 直接失败。"
  [plan i]
  (shell (str "pkill -f '127.0.0.1:" (local-port plan i) ":' 2>/dev/null; true")))

(defn- wait-tunnel-up!
  "功能验证：**经隧道**抓一次指标。它同时证明四件事：ssh 起了、端口绑上了、
  节点的 loopback 能到、agent 在服务。

  为什么不用 `ss`/`netstat` 探监听：jepsen-control 镜像里**没有 ss**
  （`ss -ltn` 返回空，看起来像「没监听」），那会造出「隧道明明通了却被判失败」
  的假红 —— 实测踩过。"
  [plan i timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (if (tunnel-metrics! plan i)
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 300) (recur))
          false)))))

(def ^:private tunnel-attempts
  "建隧道的重试次数（每次重试前先清掉上一次的 ssh）。

  为什么需要重试：这一段的失败模式几乎都是**瞬时**的 ——
    * agent 的 gRPC listener 比 HTTP 晚起 ~9s（见 `wait-for-agent!`；那一层已经拦住
      大部分，重试是第二道防线）；
    * 上一次 run 的 ssh 孤儿/端口 TIME_WAIT 让 `ExitOnForwardFailure` 立刻退出。
  实测（M5a 第二轮矩阵）：不重试时一次瞬时失败会**整 cell 崩在 open!**，而
  `open!` 崩掉意味着 teardown 也不跑 ⇒ 残留 agent 影响下一个 cell（9 个 cell
  全灭）。"
  5)

(def ^:private tunnel-attempt-delay-ms
  "重试间隔。总预算 ≈ 5 × (1s 建 + 最多 12s 验证 + 3s 间隔) ≈ 80s，与就绪超时同量级。"
  3000)

(defn- start-tunnel!
  "控制机 → agent 节点 loopback 的 SSH 转发（本地后台进程 + pidfile）。

  启动后**功能验证**（见 `wait-tunnel-up!`）：`ExitOnForwardFailure=yes` 会让
  绑定失败的 ssh 立即退出，于是 `kill -0` 就能先抓到那种情形；剩下的（能绑上
  但转发不通）由功能验证扣住。两者都失败时把隧道日志尾部带进异常，
  避免再走一遍「假红 → 找日志」的老路。

  失败**重试** `tunnel-attempts` 次（见该常量的注释：失败模式几乎都是瞬时的，
  而不重试的代价是整 cell 崩在 `open!` ⇒ teardown 不跑 ⇒ 残留影响下一个 cell）。"
  [plan i]
  (let [node (agent-host plan i)
        port (local-port plan i)
        pidfile (tunnel-pidfile i)
        logfile (tunnel-logfile i)
        attempt (fn []
                  (kill-tunnel-by-port! plan i)
                  (let [cmd (str "mkdir -p /var/run /var/log; rm -f " pidfile "; "
                                 "setsid nohup ssh -N"
                                 " -o StrictHostKeyChecking=no"
                                 " -o UserKnownHostsFile=/dev/null"
                                 " -o ExitOnForwardFailure=yes"
                                 " -o ServerAliveInterval=15"
                                 " -o ServerAliveCountMax=3"
                                 " -i " tunnel-key
                                 " -L 127.0.0.1:" port ":127.0.0.1:" agent-port
                                 " root@" node
                                 " </dev/null >> " logfile " 2>&1 & echo $! > " pidfile
                                 "; sleep 1; cat " pidfile)
                        pid (str/trim (shell! cmd))
                        alive? (and (seq pid)
                                    (zero? (:exit (shell (str "kill -0 " pid
                                                              " 2>/dev/null")))))]
                    {:pid pid
                     :alive? (boolean alive?)
                     :up? (boolean (when alive? (wait-tunnel-up! plan i 12000)))}))]
    (loop [n 1]
      (let [{:keys [pid alive? up?]} (attempt)]
        (if up?
          (do (info "coord-agent: tunnel a" i "127.0.0.1:" port "->" node
                    ":" agent-port "(pid" pid ") verified end-to-end"
                    (if (> n 1) (str "after " n " attempts") ""))
              pid)
          (if (< n (long tunnel-attempts))
            (do
              ;; 重试前给 agent 一点时间：它的 gRPC listener 可能刚起
              ;; （实测 HTTP 与 gRPC 之间差 ~9s，见 wait-for-agent!）。
              (warn "coord-agent: tunnel a" i "->" node "attempt" n
                    "failed (alive=" alive? "); retrying")
              (Thread/sleep (long tunnel-attempt-delay-ms))
              (recur (inc n)))
            (let [{:keys [out err]} (shell (str "tail -5 " logfile " 2>/dev/null"))]
              (throw (ex-info (str "coord-agent: tunnel a" i " to " node
                                   " did not come up after " n " attempts (pid=" pid
                                   " alive=" alive? ")"
                                   "; log tail: " (str/trim (str out err)))
                              {:agent i :node node :port port :attempts n})))))))))

(defn- stop-tunnel!
  [plan i]
  (let [pidfile (tunnel-pidfile i)]
    (when (.exists (io/file pidfile))
      (let [pid (str/trim (slurp pidfile))]
        (shell (str "kill -9 " pid " 2>/dev/null; rm -f " pidfile)))))
  (kill-tunnel-by-port! plan i))

;; --------------------------------------------------------------------------
;; 指标抓取与解析
;; --------------------------------------------------------------------------

(defn- metrics-cmd
  "抓 `/metrics` 的**节点侧**命令。

  用部署到 agent 目录的 `agent-metrics.sh` 而不是内联一行：内联要过
  jepsen.control 转义 + sudo + 外层 `bash -c` 三层引号，实测 `/dev/tcp` 在这种
  嵌套下会失败（`bash: line 1: /dev/tcp/...: No such file or directory`），
  而同一行在节点上交互执行完全正常 —— 上传一个文件就彻底消掉这类脆弱性。

  本地（控制机侧）的同一个脚本用 `scripts/agent-metrics.sh`，见
  `tunnel-metrics!`。"
  [port]
  (str "/bin/bash " agent-dir "/agent-metrics.sh " port))

(defn node-metrics!
  "在 agent 节点自身的 loopback 上抓一次指标（不走隧道）。失败返回 nil。"
  [plan i]
  (try
    (-> (c/on (agent-host plan i)
              (c/su (c/exec :bash :-c (metrics-cmd agent-http-port))))
        (parse-metrics))
    (catch Exception e
      (warn e "coord-agent: metrics scrape failed on" (agent-host plan i))
      nil)))

(def ^:private scrape-log
  "run 期间的抓取记录：`{host {:scrapes n :up n :max-total n :last <metrics>}}`。

  为什么需要**多次**抓取而不是 run 结束抓一次：agent 的代理计数是**进程内**的，
  重启就归零；而 M5a 的 nemesis 会故意 kill agent（`:kill-agent` 杀完再重启）。
  run 结束时的单次抓取因此有两个假红来源：
    1. 被杀的 agent 还没起回来 ⇒ 端点抓不到 ⇒ 「路由证明失败」（实测 idgen/kill-agent）；
    2. 起回来了但计数从 0 重新开始 ⇒ total = 0 ⇒ 「路由证明失败」（数据面 workload
       在 kill 之后确实又走了一波请求，但那一波被算在新进程头上）。
  —— 实测一轮 9 个 cell 里 1 个 cell 因为这两条被判红，而它其实是时序问题。

  记录口径：`:up` = **曾经**抓到过；`:max-total` = 各次抓取里 `total_requests` 的
  最大值（同一次进程生命内计数单调递增，所以最大值就是「曾经有多少请求落到过
  agent」的下界证据）。"
  (atom {}))

(defn- record-scrape!
  [host m]
  (swap! scrape-log update host
         (fn [acc]
           (let [acc (or acc {:scrapes 0 :up 0 :max-total 0 :last nil})]
             {:scrapes   (inc (long (:scrapes acc)))
              :up        (+ (long (:up acc)) (if m 1 0))
              :max-total (max (long (:max-total acc))
                              (long (or (:total-requests m) 0)))
              :last      (or m (:last acc))}))))

(defn sample-routing!
  "在 run **期间**抓一次所有 agent 的指标并累积（`with-agent` 的 `setup!` /
  `teardown!` 调用）。返回 nil（副作用是 `scrape-log`）。"
  [plan]
  (doseq [i (range 1 (inc (long (:count plan))))]
    (record-scrape! (agent-host plan i) (node-metrics! plan i)))
  nil)

(defn routing-proof!
  "AG-01 路由证明：抓**所有** agent 的代理面计数，并入 run 期间累积的采样。

  返回 `{:total n :by-method {...} :agents [{:host :up? :up-now? :total :by-method} ...]}`。

  口径（见 `scrape-log` 的注释）：
    * `:up?`      = run 期间**曾经**抓到过该 agent 的指标端点（证明进程真的存在过）；
    * `:up-now?`  = 本次（run 结束后）能不能抓到 —— kill 类 nemesis 下为 false 是正常的；
    * `:total`    = 各次采样里 `total_requests` 的**最大值**（防「重启后归零」把
                   已经发生过的代理流量抹掉）。
  判据用 `:up?`/`:total`，不要用 `:up-now?`（见 gates.clj 的 agent 门槛）。"
  [plan]
  (let [fresh (mapv (fn [i]
                      (let [host (agent-host plan i)
                            m    (node-metrics! plan i)]
                        (record-scrape! host m)
                        {:host host :up-now? (some? m)
                         :total (long (or (:total-requests m) 0))
                         :by-method (:grpc-requests m)}))
                    (range 1 (inc (long (:count plan)))))]
    (let [agents (mapv (fn [{:keys [host]}]
                         (let [a (get @scrape-log host)]
                           {:host       host
                            :up?        (pos? (long (or (:up a) 0)))
                            :up-now?    (:up-now? (first (filter #(= host (:host %)) fresh)))
                            :total      (long (or (:max-total a) 0))
                            :by-method  (:grpc-requests (:last a))}))
                       fresh)]
      {:total     (reduce + 0 (map :total agents))
       :by-method (apply merge-with + {} (keep :by-method agents))
       :agents    agents})))

;; --------------------------------------------------------------------------
;; 就绪 / 引导
;; --------------------------------------------------------------------------

(defn- port-open?
  "在 agent 节点上探一次 TCP 端口是否在监听（`agent-port-open.sh` 的退出码）。

  单独抽出来是因为**要探的是 gRPC 端口，不是 HTTP 端口** —— 见 `wait-for-agent!`。

  注意 `jepsen.control` 的 `c/exec` 在**非零退出码时抛异常**（不像
  `clojure.java.shell` 那样返回 `{:exit n}`）—— 所以这里的语义是
  「不抛 = 退出码 0 = 端口在监听」。第一版写成 `(:exit (c/on …))`，
  于是每次探测都 NPE、就绪永远不成立（矩阵里的表现是「几十条 port probe failed
  + 就绪超时」，而那看起来又像 agent 起不来）。"
  [plan i port]
  (try
    (c/on (agent-host plan i)
          (c/su (c/exec :bash (str agent-dir "/agent-port-open.sh") (str port))))
    true
    (catch Exception _ false)))

(defn- wait-for-agent!
  "等一个 agent **真的能服务**为止。

  判据**两条都要**：
    1. 节点侧 HTTP `/metrics` 抓得到（进程在、可观测面起来了）；
    2. 节点侧 **gRPC 端口** TCP 连得上。

  为什么必须加第 2 条（M5a 第二轮矩阵的实测教训）：agent 的启动顺序是
  「HTTP 先起 → 连 server 集群 → 连上之后才起 gRPC listener」，实测 gRPC 比
  HTTP 晚 ~9s（`coord-agent connected to server cluster (attempt 1)`）。而隧道与
  客户端要的都是 **gRPC 端口** ⇒ 只等 HTTP 会得到一个「看起来就绪、实际连不上」
  的窗口，表现是 `tunnel a2 to n5 did not come up ... Connection refused` ——
  一轮 9 个 cell 全部在这个窗口里挂掉，而且**看起来像 agent 的缺陷**（本仓反复
  出现的「测试自身缺陷伪装成被测系统缺陷」那一类）。

  超时抛异常（不静默继续：以前返回 false 被调用方忽略，于是失败点被推到了后面的
  隧道/客户端，日志上看不出真正原因）。"
  [plan i timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (if (and (node-metrics! plan i) (port-open? plan i agent-port))
        (do (info "coord-agent: a" i "on" (agent-host plan i)
                  "is ready (http + grpc" agent-port ")")
            true)
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 500) (recur))
          (throw (ex-info (str "coord-agent a" i " on " (agent-host plan i)
                               " did not become ready within " timeout-ms
                               "ms (http=" (boolean (node-metrics! plan i))
                               " grpc-" agent-port "=" (port-open? plan i agent-port)
                               ")")
                          {:agent i :node (agent-host plan i)
                           :grpc-port agent-port})))))))

(def client-capabilities
  "jepsen 客户端需要的 capability 全集（经 agent 的 run 必须显式授予，见
  `grant-client-capabilities!` 的注释）。

  分两组：代理面（agent 转发到 server，server 也按同一能力点鉴权）与 agent
  本地面（M5a 的四个服务）。"
  [;; 代理面（coord.* 数据面）
   ;;
   ;; 这份清单必须与 coord-core 的权威表逐条对齐（`coord-core/src/grpc_auth.rs`
   ;; 的 `rpc_capability`）。漏一条的后果**不是**「少测一条」而是「整类 op 经 agent
   ;; 全失败」：server 侧对 root 有全能力旁路（F-32），所以直连永远绿；而 agent 侧
   ;; 只按显式能力集判定 —— 于是「直连绿 + 经 agent 全红」这种形态只能靠**差分跑**
   ;; 抓（AG-01 的初衷）。
   ;;
   ;; 实测（M5a 第二轮，第一批差分跑）：漏了 `data:kv:delete` ⇒ map 的 `:delete`
   ;; 一个都没成功（13/13 `:unauthenticated`），而 Put 67 / Range 55 全部正常 ——
   ;; 症状精确地指向「这个方法少了一个能力点」。
   "data:kv:read" "data:kv:write" "data:kv:delete" "data:txn:execute"
   "data:lease:grant" "data:lease:revoke" "data:lease:keepalive"
   "data:watch:subscribe"
   "admin:maintenance:status"
   ;; agent 本地面（M5a）
   "coord:lock:acquire" "coord:lock:release" "coord:lock:renew" "coord:lock:info"
   "coord:election:campaign" "coord:election:resign" "coord:election:read"
   "coord:idgen:next"
   "coord:registry:register" "coord:registry:deregister"
   "coord:registry:heartbeat" "coord:registry:discover"
   ;; agent 本地数据面（M5b：cache / mq）。能力字符串取自权威表
   ;; `coord-core/src/grpc_auth.rs` 的 `rpc_capability`：cache 只有 read/write
   ;; 两个；MQ 分 manage（CreateTopic/PollDlq）/ publish / subscribe / consume
   ;; （Poll 与 Ack 共用 consume）。
   "coord:cache:read" "coord:cache:write"
   "coord:mq:manage" "coord:mq:publish" "coord:mq:consume"
   "coord:health:check"])

(defn- root-token!
  "控制机上登录取管理员 CCT（两步 CLI 的第一步）。"
  [{:keys [coord-bin root-password peers]}]
  (let [addr  (first peers)
        out   (str/trim (shell! (str "COORD_PASSWORD=" root-password " " coord-bin
                                    " auth login root --addr " addr
                                    " --token-only --no-save 2>&1")))
        token (last (str/split-lines out))]
    (when (str/blank? (str token))
      (throw (ex-info (str "coord-agent: root login failed: " (pr-str out))
                      {:addr addr})))
    [addr token]))

(defn grant-client-capabilities!
  "给 `root` 角色**显式**授上 jepsen 客户端用到的能力点（幂等）。

  ## 为什么必须这样做（这是一个实测到的实质缺陷，不是 lab 的权宜之计）

  server 侧对 root 有「全能力放行」旁路（`coord-server/src/auth/manager.rs:563`
  的 `if role_name == ROOT_ROLE`），但 **agent 侧没有同样的旁路** —— agent 只
  按 RoleCache 里的显式能力集判定。于是：

      UNAUTHENTICATED: role(s) 「root」 do not have capability 'data:kv:read'

  ⇒ **任何依赖 root 全能力语义的客户端，在 via-agent 路径上 100% 不可用**
  （2026-09-18 首次跑通 agent 链路时实测到，见 coord-findings.md F-28）。
  对生产的影响面比看上去大：Java SDK 客户端、运维脚本、任何用 root 的集成都
  走同一条路。

  lab 的处置：把能力**显式**授给 root（幂等；server 侧本来就会放行 root，因此
  这不改变任何 server 行为，只是让 CCT 在 agent 侧也能通过）。
  **不**把这两侧的语义差异“修”在测试侧，而是记成缺陷单交给 coord 团队选方案
  （agent 补 root 旁路 / server 把 root 的全能力物化进角色记录 / 明确要求部署
  时逐项授权）。

  幂等性：重复授予同一个 (role, capability) 应返回「已存在」，CLI 会打印错误但
  不影响后续 —— 这里对单条失败只看是否 AlreadyExists 类消息，其余抛。"
  [{:keys [coord-bin peers] :as plan}]
  (let [[addr token] (root-token! plan)]
    (doseq [cap client-capabilities]
      (let [{:keys [exit out err]}
            (shell (str coord-bin " auth role grant-capability root " cap
                        " --addr " addr " --token " token " 2>&1"))]
        (when (and (not (zero? exit))
                   (not (re-find #"(?i)already|exists|已存在" (str out err))))
          (throw (ex-info (str "coord-agent: grant-capability " cap " failed: "
                               (str/trim (str out err)))
                          {:capability cap :addr addr})))))
    (info "coord-agent: granted" (count client-capabilities)
          "capabilities to role root against" addr)
    nil))

(defn bootstrap-role!
  "幂等授予 server 侧 `agent-bootstrap` 角色的引导最小能力集（在控制机上跑
  CLI；控制机有 coord 二进制 —— lab 把仓库 bind-mount 进控制机）。

  两步：`coord auth login root --token-only --no-save` 拿管理员 CCT，
  再用 `--token` 跑 `coord security bootstrap-role`。

  失败必须**抛**：不做这一步的后果不是「跑不起来」，而是「agent 起来了但所有
  角色门控 RPC 全 403」。"
  [{:keys [coord-bin root-password peers]}]
  (let [addr  (first peers)
        login (str "COORD_PASSWORD=" root-password " " coord-bin
                   " auth login root --addr " addr " --token-only --no-save 2>&1")
        out   (str/trim (shell! login))
        token (last (str/split-lines out))]
    (when (str/blank? (str token))
      (throw (ex-info (str "coord-agent: root login failed: " (pr-str out))
                      {:addr addr})))
    (shell! (str coord-bin " security bootstrap-role --addr " addr
                 " --token " token " 2>&1"))
    (info "coord-agent: agent-bootstrap role granted against" addr)
    nil))

;; --------------------------------------------------------------------------
;; 集群级 setup / teardown（由 client 的 setup!/teardown! 调用）
;; --------------------------------------------------------------------------

(defn- clear-leftover-partitions!
  "把 agent↔集群 的 iptables DROP 规则清干净（幂等）。

  为什么必须在 `setup!` 里做：`:partition-agent-server` 的收尾（`:stop` → `-D`）
  只在 run 正常走到 teardown 时才执行。**被中断/崩溃的 run 会把 DROP 规则留在
  节点上**，而 `scripts/env-reset.sh` 只覆盖集群节点（agent 节点从没跑过 coord
  server，不在那个脚本的节点集合里）—— 于是后续每一次 run 的 agent 都连不上集群，
  症状是每一步都报 `cluster unavailable: no leader found; all endpoints
  unreachable`、启动被拖到 90s+（每个重试都等超时才走下一步），**看起来像 agent
  起不来**。

  实测（M5a 第二轮）：一次被中断的 partition cell 让实验室连续两轮矩阵全红，
  而排查方向一度全落在「就绪探测」和「agent 启动」上。⇒ 清残留要做全：
  进程、data_dir、**网络规则**。

  第七轮补充（F-55）：**清理之后必须验证**。原实现只用 `meh` 逐条 `-D`，而
  `-D` 在规则不存在时会非零退出、被 `meh` 吃掉 —— 于是「清没清干净」这件事
  在日志里完全看不见。实测：一次 `:partition-agent-server` cell **正常结束**之后
  n5 上仍留着 6 条 DROP，紧接着的三个 cell 全部以
  `coord-agent a2 on n5 did not become ready within 60000ms (http=true grpc-19527=false)`
  失败 —— 症状与 F-38/F-39 一模一样。所以这里改成：删完**读回**
  `iptables -S | grep -c DROP`，非 0 就抛异常（宁可当场红，不要留一屋子假症状）。"
  ([plan] (clear-leftover-partitions! plan true))
  ([plan verify?]
   (let [ips (mapv #(first (str/split % #":")) (:peers plan))]
     (doseq [i (range 1 (inc (long (:count plan))))]
       (let [host (agent-host plan i)]
         (c/on host
           (c/su
             ;; 每个 IP/方向删**多次**：规则可能有重复份（每次 `:start` 都 `-A`
             ;; 一条，而 `-D` 一次只删一条）。多删的几次会失败，被 `meh` 吃掉 ——
             ;; 这正是「删除可能本来就没删干净」的地方，所以下面还要读回验证。
             (doseq [ip ips]
               (dotimes [_ 8]
                 (meh (c/exec :iptables :-w :-D :OUTPUT :-d ip :-j :DROP))
                 (meh (c/exec :iptables :-w :-D :INPUT :-s ip :-j :DROP))))))
         (when verify?
           ;; `c/exec` 成功时返回 stdout（字符串），失败时抛异常 —— 所以这里把
           ;; 返回值统一成字符串再抽数字（不假设它一定是 String 还是 map）。
           (let [r    (try
                        (c/on host (c/su (c/exec :bash :-lc
                                                 "iptables -S | grep -c DROP || true")))
                        (catch Exception e e))
                 s    (if (map? r) (str (:out r) (:err r)) (str r))
                 left (some-> (re-find #"\d+" s) Long/parseLong)]
             (when (and left (pos? (long left)))
               (throw (ex-info (str "agent " i " (" host ") 清残留后仍有 " left
                                    " 条 iptables DROP 规则：后续 run 的 agent 会"
                                    "连不上集群（症状是就绪超时，看起来像 agent 崩了）")
                               {:host host :left left}))))))))))
(defn setup!
  "启动全部 agent 并建好隧道。

  幂等：**先 pkill 清残留** —— 上一次 run 崩掉留下的 agent 会污染指标，甚至让
  「路由证明」在本次**没跑**的情况下也是绿的（这正是 AG-01 要防的假绿）。

  顺序：清残留（进程 + **网络分区规则**）→ 复制二进制 → 写配置 → 起进程 →
  等就绪 → 建隧道 → 隧道端到端确认。"
  [plan]
  (when plan
    (let [n (long (:count plan))]
      (info "coord-agent: starting" n "agent(s) on" (vec (:hosts plan)))
      (clear-leftover-partitions! plan)
      (doseq [i (range 1 (inc n))]
        (stop! plan i))
      (doseq [i (range 1 (inc n))]
        (install-binary! plan i)
        (deploy-metrics-script! plan i)
        (write-config! plan i)
        (start! plan i))
      (doseq [i (range 1 (inc n))]
        (wait-for-agent! plan i 60000))
      (doseq [i (range 1 (inc n))]
        (start-tunnel! plan i))
      (info "coord-agent: all" n "tunnel(s) verified end-to-end")
      plan)))

(defn teardown!
  "停隧道 + 停 agent + 删 data_dir。任一步失败只告警（收尾不该让 run 崩；
  下一次 run 的 setup! 会再清一遍）。"
  [plan]
  (when plan
    (let [n (long (:count plan))]
      (doseq [i (range 1 (inc n))] (meh (stop-tunnel! plan i)))
      (doseq [i (range 1 (inc n))] (meh (stop! plan i)))
      (doseq [i (range 1 (inc n))]
        (meh (c/on (agent-host plan i)
                   (c/su (c/exec :rm :-rf (data-dir i))))))
      (info "coord-agent: teardown complete")))
  plan)

;; --------------------------------------------------------------------------
;; 供 nemesis 使用
;; --------------------------------------------------------------------------

(defn kill-one!
  "kill -9 一个 agent（**不**重启；重启由 restart nemesis 的 stop 分支做）。"
  [plan i]
  (c/on (agent-host plan i) (c/su (meh (c/exec :pkill :-9 :-f db/agent-pattern))))
  [:killed-agent (agent-host plan i)])

(defn pause-one!
  "SIGSTOP 一个 agent（本地不推进、但客户端连接还在 —— 观察「假死」形态）。"
  [plan i]
  (c/on (agent-host plan i) (c/su (meh (c/exec :pkill :-STOP :-f db/agent-pattern))))
  [:paused-agent (agent-host plan i)])

(defn resume-one!
  [plan i]
  (c/on (agent-host plan i) (c/su (meh (c/exec :pkill :-CONT :-f db/agent-pattern))))
  [:resumed-agent (agent-host plan i)])

(defn restart-one!
  "重启一个 agent（**保留 data_dir**：凭据/本地状态的持久化语义，AG-06/AG-13）。

  与 `nemesis/restart-coord!` 同一纪律：**绝不抛** —— node-start-stopper 一旦在
  stop! 里抛，jepsen 内部状态会卡住，之后所有扰动静默停止（长跑会在没有任何
  可见失败的情况下作废）。"
  [plan i]
  (try
    (start! plan i)
    (wait-for-agent! plan i 60000)
    [:restarted-agent (agent-host plan i)]
    (catch Exception e
      (warn e "coord-agent: failed to restart agent" i "on" (agent-host plan i))
      [:restart-failed (agent-host plan i)])))
