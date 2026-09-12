// coord-agent 内置 JS SDK（Phase 3.3）：在宿主导入面（§7）之上合成的组合原语。
//
// 约定：
// - 本文件在宿主 `coord` 对象安装之后、插件入口模块求值之前，以脚本形式在同一
//   isolate 内求值，因此插件代码可直接使用（整体包在 IIFE 内，不污染插件全局）；
// - 只使用 §7 的宿主导入原语（kv / txn / lease / watch / log / env / util），
//   不新增协调语义；
// - 键必须在插件声明的 capability scope 内：作用域守卫在宿主门面
//   （`plugin/sdk/mod.rs::PluginSdk`）强制执行，越界在此直接抛 `ErrForbidden`。
//
// ──── 锁的正确性边界（务必阅读）────
// `coord.lock` 是「租约 + version-CAS」实现的分布式锁，并提供**隔离令牌
// （fencing token）**：令牌取自持锁写入的 MVCC revision（`txn` 响应中 put 的
// `revision`），在同一锁键上严格单调递增。
//
// - 互斥由 CAS 保证：同一时刻只有一个持有者能写入锁键；
// - 活性由租约保证：持有者崩溃/分区时租约到期，键随租约过期被删除（自动释放）；
// - **受保护的资源侧必须校验令牌单调**：资源守卫应记录已见的最大令牌并拒绝
//   `fencing <= max_seen` 的写。锁本身无法阻止「旧持有者因分区自认为仍持锁」
//   这类情况（GC pause / 网络分区），只有令牌校验能。
//
// ──── 释放语义 ────
// `release()` / `resign()` 先停止保活，再 `lease.revoke`。server 端
// `LeaseOp::Revoke { delete_keys: true }` 会删除绑定到该租约的键，因此
// **不需要** `data:kv:delete` 能力；且撤销的是自己的租约，不可能误删他人的锁键。
//
// ──── 需要声明的能力 ────
//   data:kv:write        抢锁写锁键
//   data:kv:read         `coord.kv.create` 的 compare 目标（version == 0），
//                        以及 `held()` / `lock.get()` / `election.leader()`
//   data:txn:execute     create-CAS 事务（`coord.kv.create` 实现走 txn）
//   data:lease:grant     申请租约
//   data:lease:revoke    释放（server 端连带删键）
//   data:lease:keepalive 保活（keepAlive: true，默认）
//   data:watch:subscribe 仅 `election.observe()` 需要
//
// 类型定义（TS）：`plugin/sdk/coord.d.ts`。

(function () {
  "use strict";

  // ──── 内部：选项校验 ────

  function _intOpt(opts, key, dflt, min, max) {
    if (!opts || opts[key] === undefined || opts[key] === null) {
      return dflt;
    }
    var n = Number(opts[key]);
    if (!isFinite(n) || Math.floor(n) !== n || n < min || n > max) {
      throw new TypeError(
        "coord: option '" +
          key +
          "' must be an integer in [" +
          min +
          ", " +
          max +
          "], got " +
          opts[key]
      );
    }
    return n;
  }

  function _boolOpt(opts, key, dflt) {
    if (!opts || opts[key] === undefined || opts[key] === null) {
      return dflt;
    }
    return !!opts[key];
  }

  /// 单次调用的执行预算（毫秒），留 1s 余量给中断处理。
  /// 超出预算的等待会被中断处理器打断并丢弃 isolate，必须在 JS 侧提前失败。
  function _execBudgetMs() {
    var budget = 30000;
    if (coord.limits && typeof coord.limits.maxExecMs === "number") {
      budget = coord.limits.maxExecMs;
    }
    return Math.max(0, budget - 1000);
  }

  // ──── 内部：锁值编解码 ────

  var _nonceSeq = 0;

  /// 锁值：`{"owner": "...", "nonce": "..."}`（nonce 用于精确判定「锁仍归我」）。
  function _newLockValue(owner) {
    _nonceSeq += 1;
    return {
      owner: String(owner),
      nonce: _nonceSeq.toString(36) + "-" + Math.random().toString(36).slice(2, 10),
    };
  }

  function _decodeLockValue(bytes) {
    var parsed = null;
    try {
      parsed = JSON.parse(coord.util.decode(bytes));
    } catch (e) {
      parsed = null;
    }
    if (parsed && typeof parsed === "object" && typeof parsed.owner === "string") {
      return parsed;
    }
    return null;
  }

  // ──── 内部：锁句柄 ────

  function _lockHandle(key, owner, leaseId, fencing, encoded, keeper) {
    var released = false;

    return {
      key: key,
      owner: owner,
      leaseId: leaseId,
      /// 隔离令牌：持锁写入的 MVCC revision（同键严格单调）。
      fencing: fencing,

      /// 仍持有？（读锁键比对 nonce；非原子，仅用于健康检查/选举观测）
      async held() {
        if (released) {
          return false;
        }
        var cur = await coord.kv.range(key);
        if (cur.kvs.length === 0) {
          return false;
        }
        return coord.util.decode(cur.kvs[0].value) === encoded;
      },

      /// 释放：停保活 → 撤销租约（server 端连带删键）。幂等。
      async release() {
        if (released) {
          return;
        }
        released = true;
        if (keeper) {
          var h = keeper;
          keeper = null;
          try {
            await h.stop();
          } catch (e) {
            // 保活可能已自行结束
          }
        }
        try {
          await coord.lease.revoke(leaseId);
        } catch (e) {
          // 租约已过期/已被撤销 → 释放目标已达成
          if (!e || e.name !== "ErrNotFound") {
            throw e;
          }
        }
      },

      /// 只停保活（不释放锁；谨慎使用：租约到期后锁会自动释放）。
      async stopKeepAlive() {
        if (keeper) {
          var h = keeper;
          keeper = null;
          await h.stop();
        }
      },

      /// 句柄快照（纯本地，不访问集群）。
      snapshot() {
        return { key: key, owner: owner, leaseId: leaseId, fencing: fencing };
      },
    };
  }

  /// 尽力撤销租约（用于抢锁失败后的清理；错误忽略 —— 租约可能已过期）。
  async function _quietRevoke(leaseId) {
    try {
      await coord.lease.revoke(leaseId);
    } catch (e) {
      // 忽略：清理路径不应掩盖抢锁结果
    }
  }

  // ──── 内部：租约 + version-CAS 抢键 ────

  /// 抢同一个键：返回句柄（成功）/ `null`（waitMs 内未抢到）。
  ///
  /// 单次尝试 = 申请租约 → **create-CAS**（`version == 0`，即键不存在或被删除时
  /// 才写入）→ 写入绑租约的锁值。server 端的比较对软删除（tombstone）视为不存在，
  /// 因此租约过期/释放后同一键可被重新抢占。失败即撤销租约并退避重试，直到
  /// `waitMs` 用尽。
  async function _acquireLeaseKey(key, opts, label) {
    if (typeof key !== "string" || key.length === 0) {
      throw new TypeError("coord." + label + ": key must be a non-empty string");
    }
    opts = opts || {};

    var owner =
      opts.owner !== undefined && opts.owner !== null
        ? String(opts.owner)
        : coord.plugin || "plugin";
    var ttlMs = _intOpt(opts, "ttlMs", 15000, 1000, 86400000);
    var retryDelayMs = _intOpt(opts, "retryDelayMs", 100, 10, 10000);
    var maxRetryDelayMs = _intOpt(opts, "maxRetryDelayMs", 1000, retryDelayMs, 30000);
    var keepAlive = _boolOpt(opts, "keepAlive", true);

    var budget = _execBudgetMs();
    var waitMs = opts.waitMs === undefined || opts.waitMs === null ? 0 : Number(opts.waitMs);
    if (!isFinite(waitMs) || waitMs < 0 || Math.floor(waitMs) !== waitMs) {
      throw new TypeError("coord." + label + ": waitMs must be a non-negative integer");
    }
    if (waitMs > budget) {
      throw new RangeError(
        "coord." +
          label +
          ": waitMs=" +
          waitMs +
          "ms exceeds the execution budget (" +
          budget +
          "ms); lower waitMs or raise limits.max_exec_ms"
      );
    }

    var ttlSeconds = Math.max(1, Math.ceil(ttlMs / 1000));
    var deadline = Date.now() + waitMs;
    var delay = retryDelayMs;

    for (;;) {
      var lease = await coord.lease.grant(ttlSeconds, 0);
      var encoded = JSON.stringify(_newLockValue(owner));
      var fencing = 0;

      try {
        // 创建语义：仅当锁键不存在（或被删除）时写入 —— 这是互斥的唯一来源。
        // `coord.kv.create` 就是 create-if-absent CAS（version == 0），返回写入
        // 所在 revision（= 隔离令牌）。注意不能用「读 version 再比较 version」：
        // 那只是条件更新，未持有者读到的就是当前 version，条件恒真
        // （曾经实现错误，真实并发测试暴露）。
        var created = await coord.kv.create(key, coord.util.encode(encoded), {
          leaseId: lease.id,
        });
        fencing = Number(created.revision);
      } catch (e) {
        // 键已存在 = 本次抢锁失败（不是错误）：走下面的退避重试路径。
        // 权限/作用域类错误不可重试；其余（leader 切换、租约竞争等）当作一次失败尝试
        if (e && e.name !== "ErrConflict" && (coord.util.isForbidden(e) || waitMs <= 0)) {
          await _quietRevoke(lease.id);
          throw e;
        }
      }

      if (fencing > 0) {
        var keeper = null;
        if (keepAlive) {
          keeper = await coord.lease.keepAlive(lease.id);
        }
        return _lockHandle(key, owner, lease.id, fencing, encoded, keeper);
      }

      await _quietRevoke(lease.id);
      if (waitMs <= 0 || Date.now() >= deadline) {
        return null;
      }
      await coord.util.sleep(delay);
      delay = Math.min(delay * 2, maxRetryDelayMs);
    }
  }

  // ──── coord.lock：互斥锁（租约 + CAS + fencing token）────

  coord.lock = {
    /// acquire(key, {owner?, ttlMs?, waitMs?, keepAlive?, retryDelayMs?, maxRetryDelayMs?})
    ///   -> handle | null
    ///
    /// - `ttlMs`（默认 15000，下限 1000，上限 24h）：租约 TTL，也是持有者崩溃后
    ///   锁自动释放的时间上界；
    /// - `waitMs`（默认 0 = 只试一次）内未抢到 → 返回 `null`（不是错误）；
    /// - `keepAlive`（默认 true）由宿主后台续约，长临界区不会被 TTL 打断；
    /// - handle：`{key, owner, leaseId, fencing, held(), release(), stopKeepAlive(), snapshot()}`。
    async acquire(key, opts) {
      return _acquireLeaseKey(key, opts, "lock.acquire");
    },

    /// get(key) -> {key, owner, version, leaseId, foreign} | null
    ///
    /// 只读观测当前锁键（`foreign: true` 表示值不是本 SDK 写入的格式）。
    /// 注意：`version` 是键被修改次数，**不是**隔离令牌 —— 需要令牌请用
    /// `acquire()/campaign()` 的句柄，或比较 `version` 的单调性。
    async get(key) {
      var cur = await coord.kv.range(key);
      if (cur.kvs.length === 0) {
        return null;
      }
      var rec = cur.kvs[0];
      var decoded = _decodeLockValue(rec.value);
      return {
        key: key,
        owner: decoded ? decoded.owner : null,
        foreign: decoded === null,
        version: Number(rec.version),
        leaseId: Number(rec.leaseId),
      };
    },
  };

  // ──── coord.election：领导选举（同一套租约锁，语义包装）────

  coord.election = {
    /// campaign(key, opts) -> leader handle | null
    ///
    /// 竞选领导权：抢到返回句柄（保活中），`waitMs` 内未当选返回 `null`。
    /// handle：`{key, owner, leaseId, fencing, leader(), resign(), stopKeepAlive(), snapshot()}`。
    async campaign(key, opts) {
      var h = await _acquireLeaseKey(key, opts, "election.campaign");
      if (h === null) {
        return null;
      }
      return {
        key: h.key,
        owner: h.owner,
        leaseId: h.leaseId,
        fencing: h.fencing,
        leader: function () {
          return h.held();
        },
        resign: function () {
          return h.release();
        },
        stopKeepAlive: function () {
          return h.stopKeepAlive();
        },
        snapshot: function () {
          return h.snapshot();
        },
      };
    },

    /// leader(key) -> {owner, version, leaseId} | null
    async leader(key) {
      var view = await coord.lock.get(key);
      if (view === null) {
        return null;
      }
      return { owner: view.owner, version: view.version, leaseId: view.leaseId };
    },

    /// observe(key, {timeoutMs?}) -> {type, revision, leader} | null
    ///
    /// 等待下一次领导权变更（PUT = 新领导上任，DELETE = 领导卸任），超时返回 `null`。
    /// `leader` 为事件后的持有者视图（DELETE 时为 `null`）。有界调用：阻塞不超过
    /// `timeoutMs`（默认 5s，并被单次调用执行预算夹紧）。
    async observe(key, opts) {
      opts = opts || {};
      var budget = _execBudgetMs();
      var maxTimeout = Math.max(1, Math.min(5000, budget === 0 ? 1 : budget));
      var timeoutMs = _intOpt(opts, "timeoutMs", maxTimeout, 1, maxTimeout);

      var sub = await coord.watch.subscribe(key, { prevKv: true });
      var event;
      try {
        event = await Promise.race([
          sub.next(),
          coord.util.sleep(timeoutMs).then(function () {
            return "TIMEOUT";
          }),
        ]);
      } finally {
        await sub.close();
      }

      if (event === "TIMEOUT") {
        return null;
      }
      if (event === null) {
        return { type: "CLOSED", revision: 0, leader: null };
      }

      var leader = null;
      if (event.kvs && event.kvs.length > 0) {
        var decoded = _decodeLockValue(event.kvs[0].value);
        leader = {
          owner: decoded ? decoded.owner : null,
          version: Number(event.kvs[0].version),
          leaseId: Number(event.kvs[0].leaseId),
        };
      }
      return { type: event.type, revision: Number(event.revision), leader: leader };
    },
  };

  // ──── coord.idgen：单调递增 ID 发生器 ────
  //
  // 以「读 version + CAS 写」循环实现：键不存在时比较 version = 0（创建语义），
  // 存在时比较 version = 当前值，因此并发调用只会有一个成功，其余重试。
  coord.idgen = {
    // next(key, { step }) -> number
    async next(key, opts) {
      var step = (opts && opts.step) || 1;
      if (!Number.isInteger(step) || step <= 0) {
        throw new TypeError("idgen step must be a positive integer");
      }
      for (var attempt = 0; attempt < 32; attempt++) {
        var cur = await coord.kv.range(key);
        var has = cur.kvs.length > 0;
        var version = has ? Number(cur.kvs[0].version) : 0;
        var base = has ? Number(coord.util.decode(cur.kvs[0].value)) : 0;
        var next = base + step;
        var txn = await coord.txn(
          [{ key: key, target: "version", op: "equal", version: version }],
          [{ type: "put", key: key, value: coord.util.encode(String(next)) }],
          []
        );
        if (txn.succeeded) {
          return next;
        }
      }
      throw new Error("ErrInternal: idgen contention limit reached after 32 attempts");
    },
  };

  // ──── coord.util.readCounter：CAS 循环共享的小工具 ────
  //
  // 生成随后可被 CAS 更新的当前值视图（idgen / 计数器共享的小工具）。
  coord.util.readCounter = async function (key) {
    var cur = await coord.kv.range(key);
    if (cur.kvs.length === 0) {
      return { exists: false, version: 0, value: 0 };
    }
    return {
      exists: true,
      version: Number(cur.kvs[0].version),
      value: Number(coord.util.decode(cur.kvs[0].value)),
    };
  };
})();
