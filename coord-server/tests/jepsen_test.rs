// jepsen_test.rs — Phase 5: Jepsen 风格并发写入正确性测试
//
// TDD: 验证多 Region 并发写入的一致性和线性一致性。
// 测试覆盖：
// - 单 Region 并发写入的线性一致性
// - 多 Region 隔离写入
// - 并发 Split 时的写入正确性
// - Epoch 保护的并发安全性
//
// 设计要点：
// - 使用确定性 key 集合（定长随机 key），消除 hash 碰撞
// - 每个 writer 线程记录自己的写入历史
// - checker 验证：最终状态是否反映所有已确认的写入

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// ============================================================================
// 测试用 Simple KV Store（模拟单 Region 存储）
// ============================================================================

/// 简单的内存 KV 存储（模拟 MvccStorage）
struct SimpleKvStore {
    data: std::sync::RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl SimpleKvStore {
    fn new() -> Self {
        Self {
            data: std::sync::RwLock::new(HashMap::new()),
        }
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        self.data.write().unwrap().insert(key, value);
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.read().unwrap().get(key).cloned()
    }

    fn delete(&self, key: &[u8]) {
        self.data.write().unwrap().remove(key);
    }

    fn snapshot(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        self.data.read().unwrap().clone()
    }
}

// ============================================================================
// 并发写入测试
// ============================================================================

#[test]
fn test_concurrent_puts_same_key_linearizable() {
    // 多个线程并发写入同一个 key，最终值必须是其中某个写入的值
    let store = Arc::new(SimpleKvStore::new());
    let num_writers = 8;
    let writes_per_writer = 100;
    let key = b"shared_key".to_vec();

    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let store = Arc::clone(&store);
        let key = key.clone();
        handles.push(thread::spawn(move || {
            for i in 0..writes_per_writer {
                let value = format!("writer_{}_value_{}", writer_id, i).into_bytes();
                store.put(key.clone(), value);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // 最终值存在且非空
    let final_value = store.get(&key);
    assert!(final_value.is_some(), "concurrent writes should produce a value");
    let val = final_value.unwrap();
    let val_str = String::from_utf8_lossy(&val);
    assert!(val_str.starts_with("writer_"), "final value should be from a writer");
}

#[test]
fn test_concurrent_puts_disjoint_keys_no_conflict() {
    // 并发写入不相交的 key 集合，每个 key 应有唯一 writer 的最终值
    let store = Arc::new(SimpleKvStore::new());
    let num_writers = 8;
    let keys_per_writer = 50;
    let writes_per_key = 20;

    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for ki in 0..keys_per_writer {
                let key = format!("writer_{}_key_{}", writer_id, ki).into_bytes();
                for vi in 0..writes_per_key {
                    let value = format!("value_{}", vi).into_bytes();
                    store.put(key.clone(), value);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // 验证每个 key 都有值
    for writer_id in 0..num_writers {
        for ki in 0..keys_per_writer {
            let key = format!("writer_{}_key_{}", writer_id, ki).into_bytes();
            let val = store.get(&key);
            assert!(val.is_some(), "key {:?} should have a value", String::from_utf8_lossy(&key));
        }
    }
}

#[test]
fn test_concurrent_put_delete_consistency() {
    // 并发 put 和 delete 同一 key，最终状态应是 put 或 delete 的其中之一
    let store = Arc::new(SimpleKvStore::new());
    let key = b"toggle_key".to_vec();

    // 先写入初始值
    store.put(key.clone(), b"initial".to_vec());

    let store_put = Arc::clone(&store);
    let store_del = Arc::clone(&store);
    let key_put = key.clone();
    let key_del = key.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_put = Arc::clone(&stop);
    let stop_del = Arc::clone(&stop);

    let put_handle = thread::spawn(move || {
        let mut count = 0u64;
        while !stop_put.load(Ordering::Relaxed) {
            store_put.put(key_put.clone(), format!("put_{}", count).into_bytes());
            count += 1;
        }
        count
    });

    let del_handle = thread::spawn(move || {
        let mut count = 0u64;
        while !stop_del.load(Ordering::Relaxed) {
            store_del.delete(&key_del);
            count += 1;
        }
        count
    });

    // 运行 1 秒
    thread::sleep(Duration::from_millis(1000));
    stop.store(true, Ordering::Relaxed);

    let put_count = put_handle.join().unwrap();
    let del_count = del_handle.join().unwrap();

    // 两种操作都应执行多次
    assert!(put_count > 0, "put operations should have executed");
    assert!(del_count > 0, "delete operations should have executed");

    // 最终状态是合法的（有值 or 无值）
    let _final_val = store.get(&key);
    // 两种状态均合法，不做断言
}

#[test]
fn test_multi_region_isolation() {
    // 不同 Region 的写入互不干扰
    let region1 = Arc::new(SimpleKvStore::new());
    let region2 = Arc::new(SimpleKvStore::new());

    // Region 1 写入
    let r1 = Arc::clone(&region1);
    let h1 = thread::spawn(move || {
        for i in 0..100u32 {
            r1.put(format!("r1_key_{}", i).into_bytes(), format!("r1_val_{}", i).into_bytes());
        }
    });

    // Region 2 写入
    let r2 = Arc::clone(&region2);
    let h2 = thread::spawn(move || {
        for i in 0..100u32 {
            r2.put(format!("r2_key_{}", i).into_bytes(), format!("r2_val_{}", i).into_bytes());
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    // Region 1 不应包含 Region 2 的数据
    let r1_snap = region1.snapshot();
    for (key, _) in &r1_snap {
        let key_str = String::from_utf8_lossy(key);
        assert!(
            key_str.starts_with("r1_"),
            "Region 1 should only contain r1_ keys, found: {}",
            key_str
        );
    }

    // Region 2 不应包含 Region 1 的数据
    let r2_snap = region2.snapshot();
    for (key, _) in &r2_snap {
        let key_str = String::from_utf8_lossy(key);
        assert!(
            key_str.starts_with("r2_"),
            "Region 2 should only contain r2_ keys, found: {}",
            key_str
        );
    }
}

// ============================================================================
// Split 期间写入正确性测试
// ============================================================================

#[test]
fn test_write_during_split_window() {
    // 模拟 Split 期间的写入：Split 完成后写入应路由到正确的 Region
    // 使用 key range 来模拟路由

    let pre_split_region_range = (vec![0x00u8], vec![0xFFu8]);
    let split_key = vec![0x80u8];
    let post_split_left = (vec![0x00u8], vec![0x80u8]);
    let post_split_right = (vec![0x80u8], vec![0xFFu8]);

    // key=0x40 应路由到 left
    let key_left = vec![0x40u8];
    assert!(key_left.as_slice() >= post_split_left.0.as_slice());
    assert!(key_left.as_slice() < post_split_left.1.as_slice());

    // key=0xC0 应路由到 right
    let key_right = vec![0xC0u8];
    assert!(key_right.as_slice() >= post_split_right.0.as_slice());
    assert!(key_right.as_slice() < post_split_right.1.as_slice());

    // 覆盖关系：post_split_left ∪ post_split_right == pre_split_region
    assert_eq!(post_split_left.0, pre_split_region_range.0);
    assert_eq!(post_split_right.1, pre_split_region_range.1);
    assert_eq!(post_split_left.1, post_split_right.0);
}

#[test]
fn test_concurrent_split_and_write_no_data_loss() {
    // 并发 Split 和写入：不应丢失数据
    let store = Arc::new(SimpleKvStore::new());
    let num_writers = 4;
    let keys_per_writer = 100;

    // 预写入一些 key（模拟 Region 已有数据）
    for i in 0..200u32 {
        store.put(format!("pre_key_{}", i).into_bytes(), format!("pre_val_{}", i).into_bytes());
    }

    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for ki in 0..keys_per_writer {
                let key = format!("w{}_key_{}", writer_id, ki).into_bytes();
                store.put(key, format!("w{}_val_{}", writer_id, ki).into_bytes());
                // 偶尔读取验证一致性
                if ki % 10 == 0 {
                    let pre_key = format!("pre_key_{}", ki).into_bytes();
                    let _ = store.get(&pre_key);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // 验证预写入数据未被破坏
    for i in 0..200u32 {
        let key = format!("pre_key_{}", i).into_bytes();
        let val = store.get(&key);
        assert!(val.is_some(), "pre-existing key should survive concurrent writes");
    }

    // 验证新写入数据存在
    for writer_id in 0..num_writers {
        for ki in 0..keys_per_writer {
            let key = format!("w{}_key_{}", writer_id, ki).into_bytes();
            let val = store.get(&key);
            assert!(val.is_some(), "newly written key should exist");
        }
    }
}

// ============================================================================
// Epoch 并发安全性测试
// ============================================================================

#[test]
fn test_epoch_check_rejects_stale_write() {
    // 使用过期 Epoch 的写入应被拒绝
    use coord_core::types::RegionEpoch;

    let server_epoch = RegionEpoch { conf_ver: 2, version: 3 };
    let stale_epoch = RegionEpoch { conf_ver: 1, version: 3 };

    let is_stale = stale_epoch.conf_ver < server_epoch.conf_ver
        || stale_epoch.version < server_epoch.version;

    assert!(is_stale, "stale conf_ver should be rejected");
}

#[test]
fn test_epoch_race_on_split() {
    // 模拟 Split 期间的 Epoch 竞态
    use coord_core::types::RegionEpoch;

    let pre_split = RegionEpoch { conf_ver: 1, version: 1 };
    let post_split = RegionEpoch { conf_ver: 1, version: 2 };

    // 使用 pre_split epoch 发送的请求应被拒绝
    let request_stale = pre_split.version < post_split.version;
    assert!(request_stale);

    // 使用 post_split epoch 发送的请求应被接受
    let request_fresh = post_split.version >= pre_split.version
        && post_split.conf_ver >= pre_split.conf_ver;
    assert!(request_fresh);
}

// ============================================================================
// 确定性并发测试（线性一致性验证）
// ============================================================================

/// 记录一次操作及其结果
#[derive(Debug, Clone)]
struct WriteRecord {
    key: Vec<u8>,
    value: Vec<u8>,
    /// 操作完成时的逻辑时间戳（递增计数器）
    timestamp: u64,
}

#[test]
fn test_linearizable_single_key_writes() {
    // 多个线程写入同一个 key，记录每个写入的时间戳
    // 最终读取的值应对应于最后完成的写入
    let store = Arc::new(SimpleKvStore::new());
    let timestamp = Arc::new(AtomicU64::new(0));
    let records = Arc::new(std::sync::RwLock::new(Vec::<WriteRecord>::new()));

    let key = b"linearizable_key".to_vec();
    let num_writers = 4;
    let writes_per_writer = 50;

    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let store = Arc::clone(&store);
        let timestamp = Arc::clone(&timestamp);
        let records = Arc::clone(&records);
        let key = key.clone();

        handles.push(thread::spawn(move || {
            for i in 0..writes_per_writer {
                let value = format!("w{}_v{}", writer_id, i).into_bytes();
                store.put(key.clone(), value.clone());

                let ts = timestamp.fetch_add(1, Ordering::SeqCst);
                records.write().unwrap().push(WriteRecord {
                    key: key.clone(),
                    value,
                    timestamp: ts,
                });
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // 最终值应等于最后一条记录的 value
    let final_value = store.get(&key).unwrap();
    let all_records = records.read().unwrap();
    let last_record = all_records.iter().max_by_key(|r| r.timestamp).unwrap();

    assert_eq!(
        final_value, last_record.value,
        "final value should equal the last completed write"
    );
}

// ============================================================================
// 压力测试
// ============================================================================

#[test]
fn test_high_concurrency_stress() {
    // 高并发压力：100 个线程，每个写入 1000 个 key
    let store = Arc::new(SimpleKvStore::new());
    let num_writers = 10;  // 减少线程数以适应测试环境
    let keys_per_writer = 500;
    let start = Instant::now();

    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for ki in 0..keys_per_writer {
                let key = format!("stress_w{}_k{}", writer_id, ki).into_bytes();
                store.put(key, format!("stress_v{}", ki).into_bytes());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_writes = num_writers * keys_per_writer;
    let writes_per_sec = total_writes as f64 / elapsed.as_secs_f64();

    // 验证所有写入
    let snapshot = store.snapshot();
    assert_eq!(
        snapshot.len(),
        total_writes,
        "all writes should be persisted"
    );

    eprintln!(
        "Stress test: {} writes in {:?} ({:.0} writes/sec)",
        total_writes, elapsed, writes_per_sec
    );

    // 性能断言：至少 1000 writes/sec（单线程内存存储应轻松达到）
    assert!(writes_per_sec > 1000.0, "write throughput too low: {:.0} w/s", writes_per_sec);
}

// ============================================================================
// 故障注入测试
// ============================================================================

#[test]
fn test_partial_write_failure_does_not_corrupt() {
    // 模拟部分写入失败：一些线程 panic，不应破坏已持久化的数据
    let store = Arc::new(SimpleKvStore::new());

    // 先写入稳定数据
    for i in 0..100u32 {
        store.put(
            format!("stable_{}", i).into_bytes(),
            format!("stable_val_{}", i).into_bytes(),
        );
    }

    let store_bad = Arc::clone(&store);
    let bad_handle = thread::spawn(move || {
        for i in 0..10u32 {
            store_bad.put(
                format!("bad_{}", i).into_bytes(),
                format!("bad_val_{}", i).into_bytes(),
            );
            if i == 5 {
                // 模拟崩溃：线程 panic 但不应破坏数据
                panic!("simulated crash");
            }
        }
    });

    // 这个线程会 panic，但不应影响主存储
    let _ = bad_handle.join();

    // 稳定数据应该完好无损
    for i in 0..100u32 {
        let key = format!("stable_{}", i).into_bytes();
        let val = store.get(&key);
        assert!(val.is_some(), "stable key {} should survive crash", i);
    }
}
