//! MQ v2 Rust consumer helper（M3-4）。
//!
//! 把「Join → Poll → 处理 → Ack → Heartbeat」的手工循环收成一个句柄，并处理三件容易
//! 出错的事：
//!
//! 1. **代际**：心跳发现 `GROUP_EPOCH_STALE`/`REBALANCE_IN_PROGRESS` 时置脏；
//!    下一次 `poll`/`ack` 自动重新 Join（保留仍属于自己的分区的位点）；
//! 2. **越界位点**：`OFFSET_OUT_OF_RANGE` 按 [`ResetPolicy`] 处理（默认 `Fail`，
//!    不静默丢历史）；
//! 3. **topic_epoch**：Join 后按 topic 拉一次 `GetTopicLeader` 取代际，写入数据面
//!    请求（契约要求 SDK 带，防删后重建时旧客户端写/读进新 topic）。
//!
//! 语义边界：`Poll` + `Ack` 是至少一次路径；重复投递是正常形态，调用方必须按
//! `(topic, partition, offset)` 幂等；**新分配**（该分区没有已提交位点）从当前最早可用
//! offset 开始（保留期内的历史都可见）。`Subscribe` 不在这里（best-effort，M3-5）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coord_core::error::{Error, Result};
use coord_core::error_code::CoordErrorCode;
use parking_lot::Mutex;

use crate::mq_v2::{MqAck, MqJoinGroup, MqMessage, MqPoll, MqV2Client};

/// `OFFSET_OUT_OF_RANGE` 的处理策略（窗口 = topic 保留期）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetPolicy {
    /// 从当前最早可用开始（接受丢掉已回收的历史）。
    Earliest,
    /// 跳到最新（接受丢掉积压）。
    Latest,
    /// 把错误抛给调用方（默认：不静默丢数据）。
    Fail,
}

/// consumer 的装配参数。
#[derive(Debug, Clone)]
pub struct MqConsumerOptions {
    pub group: String,
    /// 空 = 让服务端分配 `member_id`。
    pub member_id: String,
    pub topics: Vec<String>,
    /// 0 = 服务端 `[mq_v2].group_session_timeout_ms`。
    pub session_timeout_ms: u32,
    pub reset_policy: ResetPolicy,
    /// 0 = 服务端 `[mq_v2].poll_max_messages`。
    pub max_messages: i32,
    /// 0 = 服务端 `[mq_v2].poll_max_bytes`。
    pub max_bytes: i32,
    /// 空批时的长轮询上限（0 = 立即返回）。
    pub wait_ms: i32,
}

impl Default for MqConsumerOptions {
    fn default() -> Self {
        Self {
            group: String::new(),
            member_id: String::new(),
            topics: Vec::new(),
            session_timeout_ms: 0,
            reset_policy: ResetPolicy::Fail,
            max_messages: 0,
            max_bytes: 0,
            wait_ms: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct Cursor {
    topic: String,
    partition: i32,
    topic_epoch: u64,
    /// `None` = 从最早可用开始（尚无已提交位点）。
    next_offset: Option<u64>,
}

#[derive(Debug, Default)]
struct ConsumerState {
    member_id: String,
    generation: u64,
    /// 服务端 `heartbeat_interval_ms`（= session_timeout/3，至少 1ms）。
    heartbeat_interval_ms: u64,
    cursors: Vec<Cursor>,
    needs_rejoin: bool,
}

/// 一个消费组成员（`Drop` 会停掉心跳；主动离组用 [`MqConsumer::close`]）。
pub struct MqConsumer {
    mq: MqV2Client,
    options: MqConsumerOptions,
    state: Arc<Mutex<ConsumerState>>,
    stop: Arc<AtomicBool>,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for MqConsumer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.abort();
        }
    }
}

impl MqConsumer {
    /// 加入组并启动心跳。
    pub async fn join(mq: MqV2Client, options: MqConsumerOptions) -> Result<Self> {
        if options.group.is_empty() {
            return Err(Error::InvalidArgument(
                "consumer group must not be empty".to_string(),
            ));
        }
        if options.topics.is_empty() {
            return Err(Error::InvalidArgument(
                "consumer topics must not be empty".to_string(),
            ));
        }
        let mut consumer = Self {
            mq,
            options,
            state: Arc::new(Mutex::new(ConsumerState::default())),
            stop: Arc::new(AtomicBool::new(false)),
            heartbeat: None,
        };
        consumer.join_once(false).await?;
        consumer.start_heartbeat();
        Ok(consumer)
    }

    pub fn member_id(&self) -> String {
        self.state.lock().member_id.clone()
    }

    pub fn generation(&self) -> u64 {
        self.state.lock().generation
    }

    /// 当前分配（`(topic, partition)`）。
    pub fn assignments(&self) -> Vec<(String, i32)> {
        self.state
            .lock()
            .cursors
            .iter()
            .map(|cursor| (cursor.topic.clone(), cursor.partition))
            .collect()
    }

    /// 拉一批消息（每个分配最多一批；无数据时只对第一个分配做长轮询）。
    pub async fn poll(&mut self) -> Result<Vec<MqMessage>> {
        self.ensure_joined().await?;
        let cursors = self.state.lock().cursors.clone();
        let mut messages = Vec::new();
        for cursor in &cursors {
            messages.extend(self.poll_partition(cursor, 0).await?);
        }
        if messages.is_empty() && self.options.wait_ms > 0 {
            if let Some(cursor) = cursors.first() {
                messages.extend(self.poll_partition(cursor, self.options.wait_ms).await?);
            }
        }
        Ok(messages)
    }

    /// 提交一条消息的位点；代际过期时自动重新 Join 并重试一次。
    pub async fn ack(&mut self, message: &MqMessage) -> Result<u64> {
        let ack = MqAck {
            topic: message.topic.clone(),
            partition: message.partition,
            topic_epoch: self.epoch_of(&message.topic),
            offset: message.offset,
        };
        let (group, member_id, generation) = self.identity();
        match self.mq.ack(&group, &member_id, generation, &ack).await {
            Ok(committed) => Ok(committed),
            Err(err) if is_rebalance(&err) => {
                self.rejoin().await?;
                let (group, member_id, generation) = self.identity();
                self.mq.ack(&group, &member_id, generation, &ack).await
            }
            Err(err) => Err(err),
        }
    }

    /// 批量提交（1–256 条，同一组；服务端整批原子）。
    pub async fn ack_batch(&mut self, messages: &[MqMessage]) -> Result<Vec<u64>> {
        let acks: Vec<MqAck> = messages
            .iter()
            .map(|message| MqAck {
                topic: message.topic.clone(),
                partition: message.partition,
                topic_epoch: self.epoch_of(&message.topic),
                offset: message.offset,
            })
            .collect();
        let (group, member_id, generation) = self.identity();
        match self
            .mq
            .ack_batch(&group, &member_id, generation, &acks)
            .await
        {
            Ok(results) => Ok(results
                .into_iter()
                .map(|result| result.committed_offset)
                .collect()),
            Err(err) if is_rebalance(&err) => {
                self.rejoin().await?;
                let (group, member_id, generation) = self.identity();
                let results = self
                    .mq
                    .ack_batch(&group, &member_id, generation, &acks)
                    .await?;
                Ok(results
                    .into_iter()
                    .map(|result| result.committed_offset)
                    .collect())
            }
            Err(err) => Err(err),
        }
    }

    /// 主动离组（分区立即转移给其他成员；位点保留）。
    pub async fn close(mut self) -> Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.abort();
        }
        let (group, member_id, generation) = self.identity();
        if member_id.is_empty() {
            return Ok(());
        }
        self.mq.leave_group(&group, &member_id, generation).await
    }

    /// 处理脏代际：下一次 poll/ack 前重新 Join。
    async fn ensure_joined(&mut self) -> Result<()> {
        if self.state.lock().needs_rejoin {
            self.rejoin().await?;
        }
        Ok(())
    }

    async fn rejoin(&mut self) -> Result<()> {
        self.join_once(true).await
    }

    /// Join（保留仍属于自己的分区的 next_offset）。
    async fn join_once(&mut self, preserve_cursors: bool) -> Result<()> {
        let previous: HashMap<(String, i32), Cursor> = if preserve_cursors {
            self.state
                .lock()
                .cursors
                .iter()
                .map(|cursor| ((cursor.topic.clone(), cursor.partition), cursor.clone()))
                .collect()
        } else {
            HashMap::new()
        };
        let member_id = self.state.lock().member_id.clone();
        let joined = self
            .mq
            .join_group(&MqJoinGroup {
                group: self.options.group.clone(),
                member_id: if member_id.is_empty() {
                    self.options.member_id.clone()
                } else {
                    member_id
                },
                topics: self.options.topics.clone(),
                session_timeout_ms: self.options.session_timeout_ms,
            })
            .await?;
        let mut epochs: HashMap<String, u64> = HashMap::new();
        for assignment in &joined.assignments {
            if !epochs.contains_key(&assignment.topic) {
                if let Ok(leaders) = self.mq.get_topic_leader(&assignment.topic).await {
                    epochs.insert(assignment.topic.clone(), leaders.topic_epoch);
                }
            }
        }
        let cursors = joined
            .assignments
            .iter()
            .map(|assignment| {
                let topic_epoch = epochs.get(&assignment.topic).copied().unwrap_or(0);
                let next_offset = previous
                    .get(&(assignment.topic.clone(), assignment.partition))
                    .and_then(|cursor| cursor.next_offset)
                    .or_else(|| {
                        (assignment.committed_offset > 0).then_some(assignment.committed_offset)
                    });
                Cursor {
                    topic: assignment.topic.clone(),
                    partition: assignment.partition,
                    topic_epoch,
                    next_offset,
                }
            })
            .collect();
        let mut state = self.state.lock();
        state.member_id = joined.member_id;
        state.generation = joined.generation;
        state.heartbeat_interval_ms = u64::from(joined.heartbeat_interval_ms);
        state.cursors = cursors;
        state.needs_rejoin = false;
        drop(state);
        if joined.heartbeat_interval_ms == 0 {
            return Ok(());
        }
        // 心跳间隔只随 Join 结果变化；已有任务时由下一次 Join 重启。
        if self.heartbeat.is_none() {
            self.start_heartbeat();
        }
        Ok(())
    }

    fn start_heartbeat(&mut self) {
        if self.heartbeat.is_some() {
            return;
        }
        let mq = self.mq.clone();
        let state = Arc::clone(&self.state);
        let stop = Arc::clone(&self.stop);
        let group = self.options.group.clone();
        self.heartbeat = Some(tokio::spawn(async move {
            // 每个周期重新读取 generation 与间隔（重新 Join 后不需要重建任务）；
            // 间隔 = 服务端 `session_timeout/3`（0 = 兜底 10s）。
            loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let interval_ms = {
                    let state = state.lock();
                    if state.heartbeat_interval_ms == 0 {
                        10_000
                    } else {
                        state.heartbeat_interval_ms
                    }
                };
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let (member_id, generation) = {
                    let state = state.lock();
                    (state.member_id.clone(), state.generation)
                };
                if member_id.is_empty() {
                    continue;
                }
                match mq.heartbeat(&group, &member_id, generation).await {
                    Ok(reply) if !reply.rebalance_required => {}
                    Ok(_) => state.lock().needs_rejoin = true,
                    Err(err) if is_rebalance(&err) => state.lock().needs_rejoin = true,
                    // 传输/无 quorum：下一个周期重试（不把瞬时故障当成再平衡）。
                    Err(_) => {}
                }
            }
        }));
    }

    fn identity(&self) -> (String, String, u64) {
        let state = self.state.lock();
        (
            self.options.group.clone(),
            state.member_id.clone(),
            state.generation,
        )
    }

    fn epoch_of(&self, topic: &str) -> u64 {
        self.state
            .lock()
            .cursors
            .iter()
            .find(|cursor| cursor.topic == topic)
            .map(|cursor| cursor.topic_epoch)
            .unwrap_or(0)
    }

    async fn poll_partition(&mut self, cursor: &Cursor, wait_ms: i32) -> Result<Vec<MqMessage>> {
        let mut request = MqPoll {
            topic: cursor.topic.clone(),
            partition: cursor.partition,
            topic_epoch: cursor.topic_epoch,
            start_offset: cursor.next_offset,
            max_messages: self.options.max_messages,
            max_bytes: self.options.max_bytes,
            wait_ms,
        };
        let mut reset_applied = false;
        loop {
            match self.mq.poll(&request).await {
                Ok(result) => {
                    self.update_cursor(&cursor.topic, cursor.partition, result.next_offset);
                    return Ok(result.messages);
                }
                Err(err) if is_offset_out_of_range(&err) && !reset_applied => {
                    match self.options.reset_policy {
                        ResetPolicy::Fail => return Err(err),
                        ResetPolicy::Earliest => {
                            // `None` = 服务端从当前最早可用开始（不 clamp 显式值）。
                            request.start_offset = None;
                        }
                        ResetPolicy::Latest => {
                            request.start_offset =
                                Some(self.latest_offset(&cursor.topic, cursor.partition).await?);
                        }
                    }
                    reset_applied = true;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn latest_offset(&self, topic: &str, partition: i32) -> Result<u64> {
        let leaders = self.mq.get_topic_leader(topic).await?;
        leaders
            .partitions
            .into_iter()
            .find(|entry| entry.partition == partition)
            .map(|entry| entry.next_offset)
            .ok_or_else(|| Error::NotFound {
                resource: "partition",
                key: format!("{topic}/{partition}"),
            })
    }

    fn update_cursor(&self, topic: &str, partition: i32, next_offset: u64) {
        let mut state = self.state.lock();
        if let Some(cursor) = state
            .cursors
            .iter_mut()
            .find(|cursor| cursor.topic == topic && cursor.partition == partition)
        {
            // 服务端空批时返回请求起点：单调取大，避免把 cursor 拉回去。
            cursor.next_offset = Some(
                cursor
                    .next_offset
                    .map_or(next_offset, |current| current.max(next_offset)),
            );
        }
    }
}

/// 需要重新 Join 的错误（代际/成员资格/组消失）。
fn is_rebalance(err: &Error) -> bool {
    matches!(
        err,
        Error::V2 { code, .. }
            if matches!(
                code,
                CoordErrorCode::GroupEpochStale
                    | CoordErrorCode::RebalanceInProgress
                    | CoordErrorCode::NotGroupMember
                    | CoordErrorCode::GroupNotFound
            )
    )
}

fn is_offset_out_of_range(err: &Error) -> bool {
    matches!(
        err,
        Error::V2 {
            code: CoordErrorCode::OffsetOutOfRange,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_do_not_silently_reset_offsets() {
        let options = MqConsumerOptions::default();
        assert_eq!(options.reset_policy, ResetPolicy::Fail);
        assert!(options.group.is_empty());
        assert_eq!(options.wait_ms, 0);
    }

    #[test]
    fn rebalance_and_out_of_range_codes_are_recognised() {
        let rebalance = [
            CoordErrorCode::GroupEpochStale,
            CoordErrorCode::RebalanceInProgress,
            CoordErrorCode::NotGroupMember,
            CoordErrorCode::GroupNotFound,
        ];
        for code in rebalance {
            let err = Error::V2 {
                code,
                message: "x".to_string(),
            };
            assert!(is_rebalance(&err), "{code:?}");
        }
        let unrelated = Error::V2 {
            code: CoordErrorCode::TopicNotFound,
            message: "x".to_string(),
        };
        assert!(!is_rebalance(&unrelated));
        assert!(!is_offset_out_of_range(&unrelated));
        assert!(is_offset_out_of_range(&Error::V2 {
            code: CoordErrorCode::OffsetOutOfRange,
            message: "x".to_string(),
        }));
    }
}
