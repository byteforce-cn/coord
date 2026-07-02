// write_batcher_test.rs — Phase 1: WriteBatcher 测试
//
// TDD: 测试共享写入批处理器（Group Commit）
// 验证多 Region 批量 Raft Log append 的正确性

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// 模拟 WriteBatcher
// ============================================================================

/// 模拟写入条目
#[derive(Debug, Clone, PartialEq, Eq)]
struct MockEntry {
    region_id: u64,
    index: u64,
    data: Vec<u8>,
}

/// 模拟存储后端（用于验证批量写入正确性）
struct MockStorage {
    /// region_id → (index → data) 映射
    logs: parking_lot::RwLock<HashMap<u64, HashMap<u64, Vec<u8>>>>,
    /// 写入调用计数
    write_count: AtomicU64,
}

impl MockStorage {
    fn new() -> Self {
        Self {
            logs: parking_lot::RwLock::new(HashMap::new()),
            write_count: AtomicU64::new(0),
        }
    }

    /// 模拟批量写入
    fn batch_append(&self, entries: &[MockEntry]) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        let mut logs = self.logs.write();
        for entry in entries {
            logs.entry(entry.region_id)
                .or_default()
                .insert(entry.index, entry.data.clone());
        }
    }

    /// 读取单条日志
    fn read(&self, region_id: u64, index: u64) -> Option<Vec<u8>> {
        self.logs
            .read()
            .get(&region_id)
            .and_then(|r| r.get(&index).cloned())
    }

    fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Relaxed)
    }
}

/// 简化版 WriteBatcher（非异步，适合单元测试）
struct WriteBatcher {
    storage: Arc<MockStorage>,
    pending: parking_lot::Mutex<Vec<MockEntry>>,
}

impl WriteBatcher {
    fn new(storage: Arc<MockStorage>) -> Self {
        Self {
            storage,
            pending: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// 追加条目到待处理队列
    fn append(&self, region_id: u64, index: u64, data: Vec<u8>) {
        let mut pending = self.pending.lock();
        pending.push(MockEntry {
            region_id,
            index,
            data,
        });
    }

    /// 批量提交所有待处理条目到存储
    fn flush(&self) -> usize {
        let batch: Vec<MockEntry> = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };

        let count = batch.len();
        if !batch.is_empty() {
            self.storage.batch_append(&batch);
        }
        count
    }

    fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }
}

// ============================================================================
// 测试用例
// ============================================================================

#[test]
fn test_batcher_empty_flush() {
    let storage = Arc::new(MockStorage::new());
    let batcher = WriteBatcher::new(storage.clone());

    let count = batcher.flush();
    assert_eq!(count, 0);
    assert_eq!(storage.write_count(), 0);
}

#[test]
fn test_batcher_single_entry() {
    let storage = Arc::new(MockStorage::new());
    let batcher = WriteBatcher::new(storage.clone());

    batcher.append(1, 1, b"hello".to_vec());
    assert_eq!(batcher.pending_count(), 1);

    let count = batcher.flush();
    assert_eq!(count, 1);
    assert_eq!(storage.write_count(), 1);
    assert_eq!(storage.read(1, 1), Some(b"hello".to_vec()));
}

#[test]
fn test_batcher_multiple_entries_same_region() {
    let storage = Arc::new(MockStorage::new());
    let batcher = WriteBatcher::new(storage.clone());

    for i in 0..10 {
        batcher.append(1, i, format!("data_{}", i).into_bytes());
    }

    let count = batcher.flush();
    assert_eq!(count, 10);
    assert_eq!(storage.write_count(), 1); // 一次批量写入

    for i in 0..10 {
        assert_eq!(
            storage.read(1, i),
            Some(format!("data_{}", i).into_bytes())
        );
    }
}

#[test]
fn test_batcher_multiple_regions_single_flush() {
    let storage = Arc::new(MockStorage::new());
    let batcher = WriteBatcher::new(storage.clone());

    batcher.append(1, 1, b"r1_data".to_vec());
    batcher.append(2, 1, b"r2_data".to_vec());
    batcher.append(3, 1, b"r3_data".to_vec());
    batcher.append(1, 2, b"r1_data_2".to_vec());

    let count = batcher.flush();
    assert_eq!(count, 4);
    assert_eq!(storage.write_count(), 1); // 仅一次批量写入（单次 fsync）

    // 验证各 Region 数据正确
    assert_eq!(storage.read(1, 1), Some(b"r1_data".to_vec()));
    assert_eq!(storage.read(1, 2), Some(b"r1_data_2".to_vec()));
    assert_eq!(storage.read(2, 1), Some(b"r2_data".to_vec()));
    assert_eq!(storage.read(3, 1), Some(b"r3_data".to_vec()));
}

#[test]
fn test_batcher_multiple_flushes() {
    let storage = Arc::new(MockStorage::new());
    let batcher = WriteBatcher::new(storage.clone());

    batcher.append(1, 1, b"batch1".to_vec());
    let count1 = batcher.flush();
    assert_eq!(count1, 1);

    batcher.append(2, 1, b"batch2".to_vec());
    let count2 = batcher.flush();
    assert_eq!(count2, 1);

    assert_eq!(storage.write_count(), 2); // 两次独立写入
    assert_eq!(storage.read(1, 1), Some(b"batch1".to_vec()));
    assert_eq!(storage.read(2, 1), Some(b"batch2".to_vec()));
}

#[test]
fn test_batcher_pending_clear_after_flush() {
    let storage = Arc::new(MockStorage::new());
    let batcher = WriteBatcher::new(storage.clone());

    batcher.append(1, 1, b"data".to_vec());
    assert_eq!(batcher.pending_count(), 1);

    batcher.flush();
    assert_eq!(batcher.pending_count(), 0);
}

#[test]
fn test_batcher_large_batch() {
    let storage = Arc::new(MockStorage::new());
    let batcher = WriteBatcher::new(storage.clone());

    // 模拟大量 Region 同时写入
    for region_id in 0..100 {
        for idx in 0..10 {
            batcher.append(region_id, idx, vec![region_id as u8; 64]);
        }
    }

    let count = batcher.flush();
    assert_eq!(count, 1000); // 100 regions * 10 entries
    assert_eq!(storage.write_count(), 1); // 仅一次批量提交（单次 fsync）

    // 抽样验证
    assert_eq!(storage.read(0, 0), Some(vec![0u8; 64]));
    assert_eq!(storage.read(42, 7), Some(vec![42u8; 64]));
    assert_eq!(storage.read(99, 9), Some(vec![99u8; 64]));
}

// ============================================================================
// 并发安全测试（多线程 append + 单线程 flush）
// ============================================================================

#[test]
fn test_batcher_concurrent_appends() {
    let storage = Arc::new(MockStorage::new());
    let batcher = Arc::new(WriteBatcher::new(storage.clone()));

    // 多个线程并发追加
    let mut handles = vec![];
    for t in 0..4 {
        let b = batcher.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..50 {
                b.append(t, i, vec![t as u8; 16]);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // 所有追加后一次性 flush
    let count = batcher.flush();
    assert_eq!(count, 200); // 4 threads * 50 entries
    assert_eq!(storage.write_count(), 1);

    // 验证数据完整性
    for t in 0..4u64 {
        for i in 0..50u64 {
            assert_eq!(
                storage.read(t, i),
                Some(vec![t as u8; 16]),
                "missing: region={} index={}",
                t,
                i
            );
        }
    }
}

// ============================================================================
// 批量写入顺序保证测试
// ============================================================================

#[test]
fn test_batcher_preserves_order_per_region() {
    let storage = Arc::new(MockStorage::new());
    let batcher = WriteBatcher::new(storage.clone());

    // 同一 Region 的条目按 index 追加
    batcher.append(1, 1, b"first".to_vec());
    batcher.append(1, 2, b"second".to_vec());
    batcher.append(1, 3, b"third".to_vec());

    // 交叉其他 Region 的条目
    batcher.append(2, 1, b"other".to_vec());

    batcher.flush();

    // 验证同 Region 内顺序正确
    assert_eq!(storage.read(1, 1), Some(b"first".to_vec()));
    assert_eq!(storage.read(1, 2), Some(b"second".to_vec()));
    assert_eq!(storage.read(1, 3), Some(b"third".to_vec()));
    assert_eq!(storage.read(2, 1), Some(b"other".to_vec()));
}
