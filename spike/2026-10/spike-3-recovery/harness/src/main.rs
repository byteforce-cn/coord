//! SPIKE 2026-10 · Spike 3 —— Outbox + 崩溃恢复的进程级 kill -9 工装。
//!
//! 用真实引擎装配（`WorkflowEngineService::new_with_kv_store_durable`）连真实
//! `coord server`，把「写状态 + 写 outbox + 写持久化定时器」全部落 Raft KV；
//! 进程被 `kill -9` 后由 `--mode recover` 重启，按 KV 恢复挂起实例并补投 outbox。
//!
//! 两个模式都打印带时间戳（相对进程启动的毫秒数）的状态行，供
//! `spike/2026-10/spike-3-recovery/scripts/repro.sh` 落成完整时间线证据：
//!
//! ```text
//! <ts_ms> BOOT          mode=start scenario=wait
//! <ts_ms> READY         instance=<id> status=waiting timer=<key|none>
//! <ts_ms> RECOVER_SCAN  ...
//! <ts_ms> SUBSCRIBED    instance=<id>          # listen 模式：重订阅完成
//! <ts_ms> STATUS        status=waiting
//! <ts_ms> STATUS        status=completed
//! <ts_ms> SUMMARY       instance=.. final=.. outbox=.. delivered_unique=.. ...
//! ```
//!
//! 所有输出即时 flush：进程会被 SIGKILL，缓冲里的时间线就是丢掉的证据。

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use coord_agent::cache::AgentCache;
use coord_agent::service::BaseService;
use coord_agent::services::mq::MessageQueueService;
use coord_agent::services::mq_event_provider::{CursorStore, MqEventProvider};
use coord_agent::services::outbox_kv::{KvCursorStore, KvOutboxStore};
use coord_agent::services::outbox_relay::drain_topic;
use coord_agent::services::workflow::phase4::{DurableEngineOptions, WorkflowEngineService};
use coord_agent::{AgentInner, WorkflowDispatchConfig};
use coord_client::config::Config;
use coord_client::Client;
use coord_core::workflow::durable::{DurableTimerStore, OutboxStore};
use coord_core::workflow::model::InstanceStatus;
use coord_core::workflow::ports::EventProvider;

const TRIGGER_EVENT_TYPE: &str = "com.china-workflow.spike3.go-ahead";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

struct Clock(Instant);

impl Clock {
    fn ms(&self) -> u128 {
        self.0.elapsed().as_millis()
    }
}

fn line(clock: &Clock, tag: &str, body: impl std::fmt::Display) {
    println!("{:>8} {:<14} {}", clock.ms(), tag, body);
    let _ = std::io::stdout().flush();
}

#[derive(Clone, Debug)]
struct Args {
    mode: String,
    scenario: String,
    server: String,
    mq_dir: PathBuf,
    topic: String,
    namespace: String,
    instance: Option<String>,
    relay: bool,
    report_dir: PathBuf,
    trigger_file: Option<PathBuf>,
    deadline_secs: u64,
    wait_secs: u64,
}

impl Args {
    fn parse() -> Args {
        let mut map = BTreeMap::new();
        let mut it = std::env::args().skip(1);
        while let Some(k) = it.next() {
            if let Some(name) = k.strip_prefix("--") {
                let v = it.next().unwrap_or_default();
                map.insert(name.to_string(), v);
            }
        }
        let get = |k: &str, d: &str| map.get(k).cloned().unwrap_or_else(|| d.to_string());
        Args {
            mode: get("mode", "start"),
            scenario: get("scenario", "wait"),
            server: get("server", "127.0.0.1:12379"),
            mq_dir: PathBuf::from(get("mq-dir", "/tmp/coord-spike-spike3/mq")),
            topic: get("topic", "com.china-workflow.spike3.events"),
            namespace: get("namespace", "spike3"),
            instance: map.get("instance").cloned(),
            relay: get("relay", "on") != "off",
            report_dir: PathBuf::from(get("report-dir", "/tmp/coord-spike-spike3/report")),
            trigger_file: map.get("trigger-file").map(PathBuf::from),
            deadline_secs: get("deadline-secs", "90").parse().unwrap_or(90),
            wait_secs: get("wait-secs", "6").parse().unwrap_or(6),
        }
    }
}

fn wait_dsl(scenario: &str, wait_secs: u64) -> String {
    match scenario {
        "wait" => format!(
            r#"
document:
  dsl: "1.0.3"
  namespace: spike3
  name: wait-recover
  version: "1.0.0"
id: wait-recover
version: "1.0.0"
start: pause
states:
  - name: pause
    type: delay
    duration: PT{wait_secs}S
    transition: done
  - name: done
    type: inject
    data:
      completed: true
    end: true
"#
        ),
        "listen" => r#"
document:
  dsl: "1.0.3"
  namespace: spike3
  name: listen-recover
  version: "1.0.0"
id: listen-recover
version: "1.0.0"
start: await
states:
  - name: await
    type: event
    onEvents:
      - eventRefs:
          - eventRef: goAhead
    transition: done
  - name: done
    type: inject
    data:
      completed: true
    end: true
events:
  - name: goAhead
    type: com.china-workflow.spike3.go-ahead
    source: /spike3
"#
        .to_string(),
        "retry" => format!(
            // 旧 coord DSL：任务级 `retry` 策略是唯一能让 `definition.task_meta`
            // 拿到 retry 的写法（CNCF `retries[]` 目前未接到 task_meta）。
            // 端点故意指向不可达端口 ⇒ 可重试失败（connect error）⇒ 进入
            // Waiting(reason="retry") 并写持久化定时器。
            r#"
document:
  dsl: "1.0.0"
  namespace: spike3
  name: retry-recover
  version: "1.0.0"
do:
  - call_it:
      call: http
      retry:
        delay: PT{wait_secs}S
        limit: 2
      with:
        method: GET
        endpoint: "http://127.0.0.1:1/never"
  - done:
      call: http
      with:
        method: GET
        endpoint: "http://127.0.0.1:1/never"
"#
        ),
        other => panic!("unknown scenario: {other}"),
    }
}

async fn connect(server: &str) -> Arc<AgentInner> {
    let client = Client::connect_direct(Config::new(vec![server.to_string()]))
        .await
        .expect("connect to coord server");
    Arc::new(AgentInner {
        client,
        cache: AgentCache::new(256, 0, 32, 0),
    })
}

async fn start_mq(dir: &std::path::Path) -> Arc<MessageQueueService> {
    std::fs::create_dir_all(dir).expect("create mq dir");
    let mq = Arc::new(MessageQueueService::new(
        dir.to_path_buf(),
        1024 * 1024 * 1024,
    ));
    mq.start().await.expect("start mq service");
    mq
}

fn build_service(
    inner: &Arc<AgentInner>,
    mq: Arc<MessageQueueService>,
    args: &Args,
) -> WorkflowEngineService {
    WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(inner),
        &WorkflowDispatchConfig::default(),
        DurableEngineOptions {
            mq,
            topic: args.topic.clone(),
            relay_enabled: args.relay,
            relay_interval_ms: 50,
            approval_roles: std::collections::HashMap::new(),
            on_behalf_services: Vec::new(),
        },
    )
}

async fn read_status(inner: &Arc<AgentInner>, id: &str) -> Option<InstanceStatus> {
    let pairs = inner
        .client
        .kv()
        .range(
            format!("/_workflow/v3/instances/{id}").as_bytes(),
            &[],
            0,
            0,
        )
        .await
        .ok()?;
    let raw = pairs.into_iter().next().map(|(_, v)| v)?;
    let inst: coord_core::workflow::model::WorkflowInstance = serde_json::from_slice(&raw).ok()?;
    Some(inst.status)
}

async fn read_instance_json(
    inner: &Arc<AgentInner>,
    id: &str,
) -> Option<coord_core::workflow::model::WorkflowInstance> {
    let pairs = inner
        .client
        .kv()
        .range(
            format!("/_workflow/v3/instances/{id}").as_bytes(),
            &[],
            0,
            0,
        )
        .await
        .ok()?;
    let raw = pairs.into_iter().next().map(|(_, v)| v)?;
    serde_json::from_slice(&raw).ok()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let clock = Clock(Instant::now());
    let args = Args::parse();
    std::fs::create_dir_all(&args.report_dir).expect("create report dir");

    line(
        &clock,
        "BOOT",
        format!(
            "mode={} scenario={} server={} relay={}",
            args.mode, args.scenario, args.server, args.relay
        ),
    );

    let inner = connect(&args.server).await;
    let mq = start_mq(&args.mq_dir).await;
    let svc = build_service(&inner, Arc::clone(&mq), &args);

    match args.mode.as_str() {
        "start" => run_start(&clock, &args, &inner, &svc).await,
        "recover" => run_recover(&clock, &args, &inner, &svc, &mq).await,
        other => panic!("unknown mode: {other}"),
    }
}

async fn run_start(
    clock: &Clock,
    args: &Args,
    inner: &Arc<AgentInner>,
    svc: &WorkflowEngineService,
) {
    svc.start().await.expect("engine start");
    let def_id = svc
        .deploy_definition(&args.namespace, &wait_dsl(&args.scenario, args.wait_secs))
        .await
        .expect("deploy definition");
    line(clock, "DEPLOYED", format!("definition_id={def_id}"));

    let inst = svc
        .start_instance(&def_id, serde_json::json!({}))
        .await
        .expect("start instance");
    line(clock, "STARTED", format!("instance={}", inst.id));

    // 等实例真的挂起，并且（wait 场景）持久化定时器已落 KV —— 这是「杀进程前
    // 状态已经 durable」的前提，否则 kill -9 之后无可恢复。
    let outbox = KvOutboxStore::new(Arc::clone(inner));
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut timer_key = "none".to_string();
    loop {
        let status = read_status(inner, &inst.id).await;
        if matches!(status, Some(InstanceStatus::Waiting)) {
            let timers = DurableTimerStore::list_all(&outbox)
                .await
                .unwrap_or_default();
            if let Some(t) = timers.iter().find(|t| t.instance_id == inst.id) {
                timer_key = t.id.clone();
                break;
            }
            if args.scenario == "listen" {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "instance {} did not reach durable waiting state",
            inst.id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let status = read_status(inner, &inst.id).await;
    line(
        clock,
        "READY",
        format!(
            "instance={} status={:?} timer={} kv_state_written=true",
            inst.id, status, timer_key
        ),
    );

    // 保持进程存活等待 SIGKILL（不优雅退出：kill -9 才是被测场景）。
    std::future::pending::<()>().await;
}

async fn run_recover(
    clock: &Clock,
    args: &Args,
    inner: &Arc<AgentInner>,
    svc: &WorkflowEngineService,
    mq: &Arc<MessageQueueService>,
) {
    let instance = args
        .instance
        .clone()
        .expect("--instance is required for recover");
    line(clock, "RECOVER_START", format!("instance={instance}"));

    svc.start().await.expect("engine recovery start");
    line(clock, "RECOVER_SCAN", "durable recovery completed");
    {
        let snap = KvOutboxStore::new(Arc::clone(inner));
        let mut ids: Vec<String> = DurableTimerStore::list_all(&snap)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|t| t.instance_id == instance)
            .map(|t| format!("{}@{}", t.id, t.fire_at_ms))
            .collect();
        ids.sort();
        line(clock, "TIMERS_AT_RECOVER", ids.join(","));
    }

    if args.scenario == "listen" {
        // 恢复扫描已经重订阅；等事件源触发再发事件（事件必须发生在重启之后）。
        line(clock, "SUBSCRIBED", format!("instance={instance}"));
        if let Some(trigger) = &args.trigger_file {
            let deadline = Instant::now() + Duration::from_secs(args.deadline_secs);
            while !trigger.exists() {
                assert!(Instant::now() < deadline, "trigger file never appeared");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        let emitter = MqEventProvider::new_with_cursor_store(
            Arc::clone(mq),
            args.topic.clone(),
            Arc::new(KvCursorStore::new(Arc::clone(inner))) as Arc<dyn CursorStore>,
        );
        emitter
            .emit(
                TRIGGER_EVENT_TYPE,
                Some("/spike3"),
                &serde_json::json!({"approved": true}),
            )
            .await;
        line(clock, "EVENT_EMITTED", format!("type={TRIGGER_EVENT_TYPE}"));
    }

    // 观察实例直到终态
    let deadline = Instant::now() + Duration::from_secs(args.deadline_secs);
    let mut last: Option<InstanceStatus> = None;
    loop {
        let snapshot = read_instance_json(inner, &instance).await;
        let status = snapshot.as_ref().map(|i| i.status);
        if status != last {
            let meta = snapshot
                .as_ref()
                .and_then(|i| i.suspension_meta.as_ref())
                .map(|m| {
                    format!(
                        "reason={} retry_count={} until_ms={}",
                        m.reason,
                        m.retry_count.unwrap_or(0),
                        m.until_ms.unwrap_or(0)
                    )
                })
                .unwrap_or_else(|| "susp=null".to_string());
            line(clock, "STATUS", format!("status={:?} {meta}", status));
            last = status;
        }
        if let Some(s) = status {
            if s.is_terminal() {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "instance {instance} did not reach a terminal state within {:?}",
            Duration::from_secs(args.deadline_secs)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 终态后等 outbox 排空：relay 是异步的，「状态终态」与「事件全部投递」之间
    // 有一个 relay 轮询窗口；把窗口收进恢复流程，避免把「还在投」误报成「丢了」。
    //
    // **稳定性要求**：驱动的状态写与生命周期事件的 outbox 提交是两次写（见
    // OUTBOX.md「与 CAS/fencing 写路径的集成」），外部观测者可能先看到终态、
    // 后看到事件记录。单点 `pending==0` 观测会把这个窗口误报成「事件丢失」，
    // 因此要求连续多轮（间隔 500ms）都为 0 才判定排空。
    const STABLE_ROUNDS: u32 = 4;
    const STABLE_INTERVAL: Duration = Duration::from_millis(500);
    let outbox_store = KvOutboxStore::new(Arc::clone(inner));
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    let mut zero_rounds = 0u32;
    loop {
        let pending = OutboxStore::pending(&outbox_store, now_ms(), 256)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|r| r.aggregate_id == instance)
            .count();
        if pending == 0 {
            zero_rounds += 1;
            if zero_rounds >= STABLE_ROUNDS {
                break;
            }
        } else {
            zero_rounds = 0;
        }
        if Instant::now() >= drain_deadline {
            line(clock, "DRAIN_TIMEOUT", format!("pending={pending}"));
            break;
        }
        tokio::time::sleep(STABLE_INTERVAL).await;
    }
    line(clock, "OUTBOX_DRAINED", "pending=0");

    // ─── 证据落盘：outbox / 定时器 / MQ 消费者（按 event_id 去重）───
    // 只统计**本实例**的记录：KV 是同一份（同 server/data-dir），别的场景实例的
    // 记录也在里面；跨实例统计会把「别的场景的事件」误算成本场景的丢失。
    let all_outbox = OutboxStore::list_all(&outbox_store)
        .await
        .unwrap_or_default();
    let outbox: Vec<_> = all_outbox
        .iter()
        .filter(|r| r.aggregate_id == instance)
        .cloned()
        .collect();
    let pending = outbox
        .iter()
        .filter(|r| r.status == coord_core::workflow::durable::OutboxStatus::Pending)
        .count();
    let delivered_in_outbox: BTreeSet<String> = outbox
        .iter()
        .filter(|r| r.status == coord_core::workflow::durable::OutboxStatus::Delivered)
        .map(|r| r.id.clone())
        .collect();

    let mut outbox_tsv = String::from("id\taggregate_id\tsequence\tevent_type\tstatus\tattempts\tcreated_at_ms\tdelivered_at_ms\n");
    let mut outbox_jsonl = String::new();
    for r in &outbox {
        if let Ok(j) = serde_json::to_string(r) {
            outbox_jsonl.push_str(&j);
            outbox_jsonl.push('\n');
        }
        outbox_tsv.push_str(&format!(
            "{}\t{}\t{}\t{}\t{:?}\t{}\t{}\t{}\n",
            r.id,
            r.aggregate_id,
            r.sequence,
            r.event_type,
            r.status,
            r.attempts,
            r.created_at_ms,
            r.delivered_at_ms.map(|v| v.to_string()).unwrap_or_default()
        ));
    }
    write_report(&args.report_dir, "outbox.tsv", &outbox_tsv);
    write_report(&args.report_dir, "outbox.jsonl", &outbox_jsonl);

    let timers = DurableTimerStore::list_all(&outbox_store)
        .await
        .unwrap_or_default();
    let mut timer_tsv = String::from("id\tinstance_id\tfire_at_ms\tkind\tcreated_at_ms\n");
    for t in &timers {
        timer_tsv.push_str(&format!(
            "{}\t{}\t{}\t{:?}\t{}\n",
            t.id, t.instance_id, t.fire_at_ms, t.kind, t.created_at_ms
        ));
    }
    write_report(&args.report_dir, "timers.tsv", &timer_tsv);

    let messages = drain_topic(mq, &args.topic, 0).expect("drain topic");
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut duplicates = 0usize;
    let mut events_tsv =
        String::from("offset\tevent_id\taggregate_id\tsequence\tevent_type\tduplicate\n");
    let mut events_jsonl = String::new();
    for rec in &messages {
        let v: serde_json::Value = match serde_json::from_slice(&rec.payload) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Ok(raw) = std::str::from_utf8(&rec.payload) {
            events_jsonl.push_str(raw);
            events_jsonl.push('\n');
        }
        let id = v["id"].as_str().unwrap_or("").to_string();
        let dup = !seen.insert(id.clone());
        if dup {
            duplicates += 1;
        }
        events_tsv.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            rec.offset,
            id,
            v["aggregate_id"].as_str().unwrap_or(""),
            v["sequence"].as_u64().unwrap_or(0),
            v["event_type"].as_str().unwrap_or(""),
            dup
        ));
    }
    write_report(&args.report_dir, "events.tsv", &events_tsv);
    write_report(&args.report_dir, "events.jsonl", &events_jsonl);

    // 不丢：outbox 里已标记投递的记录，必须全部在消费者侧出现（按 event_id 匹配）
    let missing: Vec<&String> = delivered_in_outbox.difference(&seen).collect();
    // 不重：同一 event_id 只应产生一次**业务处理**（消费端按 event_id 去重后的集合）
    let final_status = read_instance_json(inner, &instance).await.map(|i| i.status);

    let summary = serde_json::json!({
        "scenario": args.scenario,
        "instance": instance,
        "final_status": final_status
            .map(|s| format!("{s:?}").to_lowercase())
            .unwrap_or_else(|| "unknown".to_string()),
        "outbox_records": outbox.len(),
        "outbox_delivered": delivered_in_outbox.len(),
        "outbox_pending": pending,
        "mq_messages_total": messages.len(),
        "delivered_unique": seen.len(),
        "duplicate_event_ids": duplicates,
        "missing_event_ids": missing.len(),
        "timers_total": timers.len(),
        "remaining_timers_for_instance": timers.iter().filter(|t| t.instance_id == instance).count(),
        "generated_at_ms": now_ms(),
    });
    write_report(
        &args.report_dir,
        "summary.json",
        &serde_json::to_string_pretty(&summary).unwrap(),
    );
    line(clock, "SUMMARY", serde_json::to_string(&summary).unwrap());
}

fn write_report(dir: &std::path::Path, name: &str, body: &str) {
    let path = dir.join(name);
    if let Err(e) = std::fs::write(&path, body) {
        eprintln!("failed to write {}: {e}", path.display());
    }
}
