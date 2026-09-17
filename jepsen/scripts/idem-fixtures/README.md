;; T1.4 幂等 checker 的负控制 fixture（§0-1：没有负控制的 checker 不算完成）
;;
;; 运行：
;;   LEIN_ROOT=true lein run -m clojure.main scripts/run-checker-tests.clj \
;;       jepsen.coord.idem scripts/idem-fixtures '{:min-replay-attempts 0}'
;;
;; 命名约定（run-checker-tests.clj）：`expect-valid-*` 必须判 valid，
;; `expect-invalid-*` 必须判 invalid。
;;
;; 每个 fixture 都是**一个 completion op**（真实的 workload 也这样产出：
;; 一次 invoke! 把 k 次重放的响应汇总进 `:attempts`，见
;; jepsen.coord.client 的 idem 段落）。字段形状：
;;
;;   {:f :idem-put | :idem-delete | :idem-range-delete
;;    :type :ok|:info|:fail
;;    :request {:rid ... :key ... :val ... :replays n}
;;    :attempts [{:ok? true :node "n1" :revision 10 :prev-kv {...}} ...]
;;    :final {:ok? true :value "1" :version 1 ...}          ; 收尾点读
;;    :post-write-final {...}}                              ; 仅 range-delete
;;
;; fixture 清单：
;;   expect-valid-clean                     —— 幂等契约成立的基线（同一节点重放）
;;   expect-valid-small-sample              —— 单分组但全部自洽（样本门槛由 opts 调 0）
;;   expect-valid-info-first-attempt        —— 首次 :info（可能未生效）→ 不判生效次数
;;   expect-invalid-revision-advanced       —— F-03：重放落到别的节点 → revision 变了
;;   expect-invalid-prev-kv-mismatch        —— F-02：幂等命中丢 prev_kv
;;   expect-invalid-version-over-advance    —— 响应谎报 revision，但 version 前进了
;;   expect-invalid-delete-count-mismatch   —— F-01：Delete 无去重 → 重放 deleted 变 0
;;   expect-invalid-delete-prev-kvs-mismatch—— F-01：重放 prev_kvs 为空
;;   expect-invalid-delete-lost             —— 删除复活（P0）
;;   expect-invalid-replay-deleted-new-write—— F-01 最严重形态：重放删掉新写的数据
;;
;; 每个坏 fixture 都必须让 `:valid?` 为 false（不是只出现在报告里）。
