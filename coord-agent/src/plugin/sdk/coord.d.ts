// coord-agent 插件 SDK 类型定义（Phase 3.3：TS 类型面）
//
// 这是插件作者编写 JS/TS 编排插件时的类型依据：`coord` 全局由宿主注入
// （`plugin/sdk/bind.rs` 装宿主导入面，`plugin/sdk/stdlib.js` 在其上合成
// 组合原语）。TypeScript 源码编译到 ESM 后可直接作为插件 `entry`。
//
// 用法：
// ```ts
// /// <reference path="./coord.d.ts" />
// export async function handleInvoke(method: string, payload: Uint8Array) {
//   const lock = await coord.lock.acquire("/app/orders/lock", { waitMs: 2000 });
//   if (!lock) throw new Error("busy");
//   try {
//     await coord.kv.put("/app/orders/1", payload, { leaseId: lock.leaseId });
//   } finally {
//     await lock.release();
//   }
//   return coord.util.encode("ok");
// }
// ```
//
// 能力对齐：每个方法所需 capability 见注释；宿主在**门面层**（作用域守卫）与
// server 端（capability + scope 强制）双层校验。scope 不含资源键的能力
// （lease / watch / storage / txn）必须以**空 scope** 声明。

/** 字节输入：字符串按 UTF-8 编码，或原始字节。 */
type CoordBytes = string | Uint8Array;

/** SDK 错误（宿主拒绝/后端失败）：`name` 为 `Err*` 稳定判别名。 */
interface CoordError extends Error {
  /** 稳定短码：`not_found` / `unavailable` / `forbidden` / `resource_exhausted` … */
  readonly code: string;
}

interface CoordRecord {
  key: Uint8Array;
  value: Uint8Array;
  leaseId: number;
  /** 键被修改次数（非隔离令牌）。 */
  version: number;
}

interface CoordCompare {
  key: CoordBytes;
  target: "value" | "version" | "modRevision";
  op: "equal" | "notEqual" | "greater" | "less";
  /** `target: "value"` 时使用。 */
  value?: CoordBytes;
  /** `target: "version" | "modRevision"` 时使用。 */
  version?: number;
  modRevision?: number;
}

type CoordTxnOp =
  | { type: "put"; key: CoordBytes; value: CoordBytes; leaseId?: number; prevKv?: boolean }
  | {
      type: "range";
      key: CoordBytes;
      rangeEnd?: CoordBytes;
      limit?: number;
      revision?: number;
      keysOnly?: boolean;
      countOnly?: boolean;
    }
  | { type: "delete"; key: CoordBytes; rangeEnd?: CoordBytes; prevKv?: boolean };

interface CoordTxnResponse {
  succeeded: boolean;
  revision: number;
  responses: Array<{ type: "put" | "range" | "delete" } & Record<string, unknown>>;
}

/** 租约保活句柄（宿主后台续约；`stop()` 只停止续约，不撤销租约）。 */
interface CoordLeaseKeeper {
  id: number;
  stop(): Promise<void>;
}

interface CoordWatchEvent {
  type: "PUT" | "DELETE" | "BUFFER_OVERFLOW" | "HISTORY_UNAVAILABLE";
  revision: number;
  kvs: CoordRecord[];
  prevKv?: CoordRecord;
}

interface CoordWatchSubscription {
  id: number;
  /** 下一条事件；`null` 表示流结束；溢出抛 `ErrResourceExhausted`。 */
  next(): Promise<CoordWatchEvent | null>;
  /** 幂等关闭。 */
  close(): Promise<void>;
}

interface CoordObjectStat {
  bucket: string;
  objectId: Uint8Array;
  size: number;
  chunks: number;
  revision: number;
  exists: boolean;
  committed: boolean;
}

/** 分块上传会话（大对象不整块驻留插件内存）。 */
interface CoordObjectWriter {
  id: number;
  /** 打开时声明的总字节数；**未知长度**上传为 `null`（`commit` 时按实际字节定长）。 */
  totalSize: number | null;
  /** 追加一个 chunk；返回累计已写字节数。声明模式下超出 `totalSize` 抛 `ErrInvalidArgument`。 */
  write(chunk: CoordBytes): Promise<number>;
  /** 提交上传；声明模式下写入量须等于 `totalSize`，否则抛 `ErrInvalidArgument`。 */
  commit(): Promise<{ revision: number; size: number; chunks: number }>;
  /** 放弃上传（幂等）。忘记调用时插件停止会兜底回收。 */
  abort(): Promise<void>;
}

/** 分块下载会话（大对象不整块驻留插件内存）。 */
interface CoordObjectReader {
  id: number;
  /** 打开时已取得的元数据。 */
  stat(): Promise<CoordObjectStat>;
  /** 读取 ≤ `maxLen` 字节；`null` = 读完。 */
  read(maxLen: number): Promise<Uint8Array | null>;
  /** 提前关闭（幂等）。 */
  close(): Promise<void>;
}

/** 锁 / 选举句柄。 */
interface CoordLockHandle {
  key: string;
  owner: string;
  leaseId: number;
  /**
   * 隔离令牌（fencing token）：持锁写入的 MVCC revision，在同一锁键上严格单调。
   * 受保护资源侧必须记录已见最大值并拒绝 `fencing <= max_seen` 的写。
   */
  fencing: number;
  /** 仍持有？（读锁键比对；非原子，用于健康检查与观测） */
  held(): Promise<boolean>;
  /** 释放：停保活 → 撤销租约（server 端连带删键）。幂等。 */
  release(): Promise<void>;
  /** 只停保活（租约到期后锁自动释放）。 */
  stopKeepAlive(): Promise<void>;
  /** 本地快照（不访问集群）。 */
  snapshot(): { key: string; owner: string; leaseId: number; fencing: number };
}

interface CoordLeaderHandle {
  key: string;
  owner: string;
  leaseId: number;
  fencing: number;
  /** 仍是领导？ */
  leader(): Promise<boolean>;
  /** 辞去领导权（幂等）。 */
  resign(): Promise<void>;
  stopKeepAlive(): Promise<void>;
  snapshot(): { key: string; owner: string; leaseId: number; fencing: number };
}

interface CoordLockAcquireOptions {
  /** 持有者标识（默认 `coord.plugin`）。 */
  owner?: string;
  /** 租约 TTL，毫秒（默认 15000；下限 1000，上限 24h）。 */
  ttlMs?: number;
  /** 抢锁等待上限，毫秒（默认 0 = 只试一次，未抢到返回 `null`）。 */
  waitMs?: number;
  /** 是否后台保活（默认 true）。 */
  keepAlive?: boolean;
  retryDelayMs?: number;
  maxRetryDelayMs?: number;
}

interface Coord {
  /** 插件名。 */
  readonly plugin: string;
  /** 插件资源预算（只读）。 */
  readonly limits: { maxExecMs: number; maxMemoryMb: number; maxObjects: number };

  readonly kv: {
    /** 需要 `data:kv:write`（`prevKv` 另需 `data:kv:read`）。 */
    put(
      key: CoordBytes,
      value: CoordBytes,
      opts?: { leaseId?: number; prevKv?: boolean; requestId?: CoordBytes }
    ): Promise<{ revision: number; prevKv?: CoordRecord }>;
    /**
     * 单键读取（不存在 → 抛 `ErrNotFound`）。
     *
     * 需要 `data:kv:read`。需要元数据（version / leaseId）时用 `range`。
     */
    get(key: CoordBytes): Promise<Uint8Array>;
    /** 需要 `data:kv:read`。 */
    range(
      key: CoordBytes,
      opts?: {
        rangeEnd?: CoordBytes;
        limit?: number;
        revision?: number;
        keysOnly?: boolean;
        countOnly?: boolean;
      }
    ): Promise<{ kvs: CoordRecord[]; count: number; revision: number }>;
    /** 需要 `data:kv:delete`（`prevKv` 另需 `data:kv:read`）。 */
    delete(
      key: CoordBytes,
      opts?: { rangeEnd?: CoordBytes; prevKv?: boolean; requestId?: CoordBytes }
    ): Promise<{ deleted: number; prevKvs: CoordRecord[]; revision: number }>;
    /**
     * create-if-absent CAS（`version === 0` 才写入）；键已存在 → 抛 `ErrConflict`。
     *
     * 需要 `data:kv:read` + `data:kv:write` + `data:txn:execute`（实现走
     * compare-version-0 事务）。这是 `coord.lock` / `coord.idgen` 的原子基础。
     */
    create(
      key: CoordBytes,
      value: CoordBytes,
      opts?: { leaseId?: number }
    ): Promise<{ revision: number }>;
  };

  /**
   * 事务（CAS）：`succeeded === false` 不是错误。
   * 需要 `data:txn:execute` + 逐分支的 kv 能力。
   */
  txn(
    compares: CoordCompare[],
    success: CoordTxnOp[],
    failure: CoordTxnOp[],
    opts?: { requestId?: CoordBytes }
  ): Promise<CoordTxnResponse>;

  readonly lease: {
    /** 需要 `data:lease:grant`（空 scope）。 */
    grant(ttlSeconds: number, id?: number): Promise<{ id: number; ttl: number }>;
    /** 需要 `data:lease:revoke`（空 scope）；server 端删除绑定该租约的键。 */
    revoke(id: number): Promise<void>;
    /** 需要 `data:lease:keepalive`（空 scope）。 */
    keepAlive(id: number): Promise<CoordLeaseKeeper>;
  };

  readonly watch: {
    /** 需要 `data:watch:subscribe`（空 scope）。 */
    subscribe(
      key: CoordBytes,
      opts?: { rangeEnd?: CoordBytes; startRevision?: number; prevKv?: boolean }
    ): Promise<CoordWatchSubscription>;
  };

  readonly storage: {
    /** 需要 `data:storage:write`（空 scope）。 */
    put(
      bucket: string,
      objectId: CoordBytes,
      data: CoordBytes
    ): Promise<{ revision: number; size: number; chunks: number }>;
    /** 需要 `data:storage:read`（空 scope）。 */
    get(
      bucket: string,
      objectId: CoordBytes
    ): Promise<{ stat: CoordObjectStat; data: Uint8Array }>;
    /** 不存在 → `null`。需要 `data:storage:read`。 */
    stat(bucket: string, objectId: CoordBytes): Promise<CoordObjectStat | null>;
    /** 需要 `data:storage:write`。 */
    delete(bucket: string, objectId: CoordBytes): Promise<{ deleted: boolean }>;
    /**
     * 打开**分块上传**会话：大对象逐块 `write`，不必整块驻留内存。
     *
     * `totalSize` 可为 `0` 或省略 = **未知长度**（`commit` 时按实际字节定长，
     * 上限 = server `max_object_size`）；`> 0` = 声明长度（须写满才能 `commit`）。
     * 需要 `data:storage:write`（空 scope）。
     */
    openWrite(
      bucket: string,
      objectId: CoordBytes,
      totalSize?: number
    ): Promise<CoordObjectWriter>;
    /**
     * 打开**分块下载**会话：逐块 `read`，不必整块驻留内存。
     *
     * 需要 `data:storage:read`（空 scope）。
     */
    openRead(bucket: string, objectId: CoordBytes): Promise<CoordObjectReader>;
  };

  /** 宿主日志（`level`: trace|debug|info|warn|error）。 */
  log(level: string, msg: string): void;
  /** 配置注入（`[plugins].env`）的只读查询。 */
  env(key: string): string | undefined;

  readonly util: {
    /** UTF-8 编码 → `Uint8Array`。 */
    encode(s: string): Uint8Array;
    /** 字节 → 字符串（非法 UTF-8 用替换字符）。 */
    decode(b: CoordBytes): string;
    /** 判定 SDK 拒绝类错误。 */
    isForbidden(e: unknown): boolean;
    /** 宿主侧异步等待（毫秒，≤ 1h）。 */
    sleep(ms: number): Promise<void>;
    /** 读计数器视图（CAS 循环小工具）。 */
    readCounter(
      key: CoordBytes
    ): Promise<{ exists: boolean; version: number; value: number }>;
  };

  /**
   * 分布式互斥锁（租约 + version-CAS + 隔离令牌）。
   *
   * 能力：`data:kv:read` · `data:kv:write` · `data:txn:execute` ·
   * `data:lease:grant` · `data:lease:revoke` ·（`keepAlive` 时）`data:lease:keepalive`。
   *
   * 注意：`waitMs` 必须小于插件的 `limits.max_exec_ms`（内置 SDK 会提前抛
   * `RangeError`），否则宿主会中断并丢弃 isolate。
   */
  readonly lock: {
    acquire(key: string, opts?: CoordLockAcquireOptions): Promise<CoordLockHandle | null>;
    get(
      key: string
    ): Promise<{
      key: string;
      owner: string | null;
      foreign: boolean;
      version: number;
      leaseId: number;
    } | null>;
  };

  /** 领导选举（同一套租约锁；另需 `data:watch:subscribe` 才能 `observe`）。 */
  readonly election: {
    /** 竞选：抢到返回句柄（保活中），`waitMs` 内未当选返回 `null`。 */
    campaign(key: string, opts?: CoordLockAcquireOptions): Promise<CoordLeaderHandle | null>;
    /** 当前领导视图。 */
    leader(key: string): Promise<{ owner: string | null; version: number; leaseId: number } | null>;
    /** 等待下一次领导权变更；超时返回 `null`。 */
    observe(
      key: string,
      opts?: { timeoutMs?: number }
    ): Promise<{
      type: "PUT" | "DELETE" | "CLOSED";
      revision: number;
      leader: { owner: string | null; version: number; leaseId: number } | null;
    } | null>;
  };

  /** 单调递增 ID 发生器（读 version + CAS 写循环）。 */
  readonly idgen: {
    next(key: CoordBytes, opts?: { step?: number }): Promise<number>;
  };
}

/** 宿主注入的全局对象。 */
declare const coord: Coord;
