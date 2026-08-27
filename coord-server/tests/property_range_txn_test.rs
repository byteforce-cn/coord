// R-TST-21：确定性模型属性测试（property-based，无外部 fuzz 依赖）
//
// 目标：以 BTreeMap 为参考模型，对 MVCC 存储执行种子化随机操作序列
// （put / delete / delete_range / txn / range），每步之后校验存储状态与
// 模型完全一致。覆盖 R-SVC-07 半开区间语义与 Txn 可见性（已删 key 不复活）。
//
// 随机源：xorshift64 种子化 PRNG（确定性，无 proptest/quickcheck 依赖），
// 多组种子并行覆盖不同操作序列。口径：本套件为组件级属性验证，不构成
// 系统级证据（见 17 号文档 §六.3）。

use std::collections::BTreeMap;
use std::sync::Arc;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::txn::{CompareOp, CompareTarget, CompareValue, TxnCompare, TxnOp};

const KEYS: [&[u8]; 8] = [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"];

/// xorshift64 确定性 PRNG
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[(self.next() % items.len() as u64) as usize]
    }
}

/// 参考模型：key → value（None 表示已删除）
struct Model {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Model {
    fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.map.insert(key.to_vec(), value.to_vec());
    }

    fn delete(&mut self, key: &[u8]) {
        self.map.remove(key);
    }

    /// 半开区间 [start, end) 删除（与 R-SVC-07 语义一致）
    fn delete_range(&mut self, start: &[u8], end: &[u8]) {
        let doomed: Vec<Vec<u8>> = self
            .map
            .range(start.to_vec()..end.to_vec())
            .map(|(k, _)| k.clone())
            .collect();
        for k in doomed {
            self.map.remove(&k);
        }
    }

    /// 条件写：当前值 == expect 时写入 put_value，否则 no-op
    fn txn_compare_and_put(&mut self, key: &[u8], expect: &[u8], put_value: &[u8]) {
        if self.map.get(key).map(|v| v.as_slice()) == Some(expect) {
            self.put(key, put_value);
        }
    }

    fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.map
            .range(start.to_vec()..end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

fn verify_model(storage: &MvccStorage<RedbBackend>, model: &Model) {
    // 单键视图
    for key in KEYS {
        assert_eq!(
            storage.get(key).unwrap(),
            model.map.get(key).cloned(),
            "key {key:?} diverges from model"
        );
    }
    // 范围视图 [a, z)
    let expected = model.range(b"a", b"z");
    let actual = storage.range_in(b"a", b"z", usize::MAX).unwrap();
    assert_eq!(actual, expected, "range [a,z) diverges from model");
    // 子范围视图 [c, f)
    let expected = model.range(b"c", b"f");
    let actual = storage.range_in(b"c", b"f", usize::MAX).unwrap();
    assert_eq!(actual, expected, "range [c,f) diverges from model");
}

fn run_seed(seed: u64, ops: usize) {
    let tmp = tempfile::tempdir().unwrap();
    let backend = RedbBackend::open(tmp.path(), &StorageConfig::default()).unwrap();
    let storage = Arc::new(MvccStorage::new(backend).unwrap());
    let mut model = Model::new();
    let mut rng = Rng(seed | 1);

    for step in 0..ops {
        match rng.next() % 6 {
            0 => {
                // Put
                let key: &[u8] = rng.pick(&KEYS);
                let value = format!("v-{seed}-{step}").into_bytes();
                storage.put(key, &value, None).unwrap();
                model.put(key, &value);
            }
            1 => {
                // Delete
                let key: &[u8] = rng.pick(&KEYS);
                let _ = storage.delete(key).unwrap();
                model.delete(key);
            }
            2 => {
                // DeleteRange [start, end)
                let start: &[u8] = rng.pick(&KEYS);
                let end: &[u8] = rng.pick(&KEYS);
                let (start, end) = if start <= end {
                    (start, end)
                } else {
                    (end, start)
                };
                let (_, deleted) = storage.delete_range(start, end).unwrap();
                let model_before: usize = model.map.range(start.to_vec()..end.to_vec()).count();
                assert_eq!(
                    deleted as usize, model_before,
                    "delete_range [{start:?},{end:?}) deleted count diverges"
                );
                model.delete_range(start, end);
            }
            3 => {
                // Txn：compare value == current → put new value（已删 key 不可见）
                let key: &[u8] = rng.pick(&KEYS);
                if let Some(current) = model.map.get(key).cloned() {
                    let new_value = format!("txn-{seed}-{step}").into_bytes();
                    let compares = vec![TxnCompare {
                        key: key.to_vec(),
                        op: CompareOp::Equal,
                        target: CompareTarget::Value,
                        target_value: CompareValue::Value(current.clone()),
                    }];
                    let success_ops = vec![TxnOp::Put {
                        key: key.to_vec(),
                        value: new_value.clone(),
                        lease_id: None,
                    }];
                    let result = storage
                        .execute_txn(&compares, &success_ops, &[])
                        .expect("txn should execute");
                    assert!(
                        result.succeeded,
                        "compare-and-put must succeed when value matches"
                    );
                    model.txn_compare_and_put(key, &current, &new_value);
                }
            }
            4 => {
                // Range with random limit
                let limit = (rng.next() % 8 + 1) as usize;
                let expected = model.range(b"a", b"z");
                let actual = storage.range_in(b"a", b"z", limit).unwrap();
                assert_eq!(actual, expected[..expected.len().min(limit)].to_vec());
            }
            _ => {
                // Get + metadata 一致性
                let key: &[u8] = rng.pick(&KEYS);
                assert_eq!(
                    storage.get(key).unwrap(),
                    model.map.get(key).cloned(),
                    "get {key:?} diverges"
                );
            }
        }
        verify_model(storage.as_ref(), &model);
    }
}

#[test]
fn property_range_txn_matches_model() {
    for seed in [7u64, 13, 42, 99, 2026] {
        run_seed(seed, 200);
    }
}
