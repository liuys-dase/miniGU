# TP 事务乐观锁设计方案

## 背景与目标
- Issue #108 期望在 OLTP (minigu/storage/src/tp) 层为事务管理提供乐观锁模式，并允许配置悲观/乐观锁策略。
- 现状：存储层已实现基于时间戳的 MVCC，但写操作在进入时直接检测写冲突（`check_write_conflict`），行为接近悲观锁；锁策略不可配置。
- 目标：在保持现有 MVCC 语义和 WAL/Checkpoint 机制的前提下，引入可配置的乐观锁路径，降低不必要的写冲突失败，支持按场景选择策略，暂不涉及 AP 存储与事务。

## 当前实现梳理（tp）
- 读路径：
  - `MemoryGraph::get_vertex/get_edge` 根据 `start_ts` 选择可见版本；Serializable 记录 `vertex_reads/edge_reads` 以便提交校验。
- 写路径：
  - `create_vertex/create_edge/delete_* /set_*` 中调用 `check_write_conflict`。当记录被其他事务修改（`commit_ts` 为其他 txn_id）或已提交且 `commit_ts > start_ts` 时直接报 `WriteWriteConflict`/`VersionNotVisible`。
  - 写入直接更新 `current`，`commit_ts` 置为当前 txn_id，并将旧版本写入 undo 链，redo 先写入 `redo_buffer`。
- 提交流程（`MemTransaction::commit_at`）：
  - 获取全局 `commit_lock`，Serializable 模式执行 `validate_read_sets`（检测读到的版本是否被更新），然后写 redo/WAL，设置最新 `commit_ts`，触发 GC/Checkpoint。
- 结果：任何在事务开始后被其他事务提交过的记录都会在写入时失败，形成“读后写”/“写后写”早期拒绝的悲观行为。

## 需求与设计原则
- 提供可配置的锁策略：`Pessimistic`（保持现状）与 `Optimistic`（提交阶段校验）。
- 默认保持兼容（沿用悲观锁），避免破坏现有调用方。
- 乐观模式要求：
  - 允许在读取快照后尝试写入，冲突在提交阶段统一校验。
  - 并发未提交写仍需互斥（避免同一记录存在多个未提交版本）。
  - 读写集校验与 WAL/GC/Checkpoint 逻辑保持可预测。
- 支持按图实例/事务维度选择策略（建议图级默认 + 事务可覆盖）。

## 方案概览
1) 新增锁策略配置
- 定义 `LockStrategy` 枚举（`Pessimistic` | `Optimistic`）放置在公共 transaction crate 中供各层复用。
- 在 `MemoryGraph` 构造参数中增加 TP 事务配置（示例：`TpTxnOptions { default_lock: LockStrategy, default_isolation: IsolationLevel }`），对现有 `with_config_*` 提供带默认值的构造，保持向后兼容。
- `MemTxnManager::begin_transaction` 接收可选覆盖参数（新方法 `begin_transaction_with_options`），将锁策略写入 `MemTransaction`。

2) 事务上下文扩展
- `MemTransaction` 新增：
  - `lock_strategy: LockStrategy`
  - `write_set_vertices: DashMap<VertexId, Timestamp>`
  - `write_set_edges: DashMap<EdgeId, Timestamp>`
  - 封装方法 `record_write_guard(entity_id, prev_commit_ts)`。
- `UndoEntry` 与 redo 不改动；write set 仅用于校验，避免破坏 WAL/GC。

3) 写路径差异化
- 抽象 `prewrite_check(commit_ts, txn)` 代替直接调用 `check_write_conflict`：
  - `Pessimistic`：沿用当前逻辑（阻塞未提交写；阻止写入不可见已提交版本）。
  - `Optimistic`：仅在存在其他未提交写 (`commit_ts.is_txn_id() && != txn_id`) 时拒绝；对已提交且 `commit_ts > start_ts` 的版本不再拒绝，改为将 `commit_ts` 记录进 write set 作为期望版本。
- 写入成功后记录 write set：`write_set_vertices.insert(vid, prev_commit_ts)` 或 `write_set_edges`。

4) 提交流程扩展（乐观验证阶段）
- 提交时在持有 `commit_lock` 之前或之后（推荐锁后校验，利用顺序化提交）执行：
  - 读取 `write_set_*`，检查当前 `current.commit_ts` 是否仍等于记录的 `prev_commit_ts`。
  - 若当前为 tombstone 或版本已被其他事务提交，则报 `WriteWriteConflict`/`ValidationFailed` 并触发 `abort`。
  - 保持 `validate_read_sets` 用于 Serializable；Snapshot 模式下可选是否对 read set 做轻量校验（防止丢失更新，可通过配置开关，默认仅校验 write set）。
- 校验通过后继续原有 WAL 追加与 `commit_ts` 分配。

5) 可见性与读取
- 读路径不变；Serializable 仍记录 read set。
- 乐观写允许基于较旧快照写入，但在提交前会被阻断，满足主流 OCC“读-校验-写”三阶段。

6) GC/Checkpoint/WAL 影响
- Undo/Redo 的格式与 GC 入口不变；新增 write set 仅存在于事务生命周期内，对日志和持久化无额外开销。
- `commit_lock` 仍确保提交顺序，避免版本校验与写入之间的竞态。

## 关键流程伪码
```rust
// 写入前
match (txn.lock_strategy, current.commit_ts) {
    (LockStrategy::Pessimistic, ts) => check_write_conflict(ts, txn)?,
    (LockStrategy::Optimistic, ts) => {
        if ts.is_txn_id() && ts != txn.txn_id() {
            return Err(WriteWriteConflict)
        }
        txn.record_write_guard(eid_or_vid, ts);
    }
}
// 提交阶段
fn validate_write_sets(txn, graph) -> Result<(), StorageError> {
    for (vid, expected_ts) in txn.write_set_vertices.iter() {
        let cur = graph.vertices.get(vid).unwrap().chain.current.read().unwrap();
        if cur.commit_ts != *expected_ts { return Err(ValidationFailed) }
    }
    // edges 同理
    Ok(())
}
```

## 与主流数据库实践的对齐
- OCC 三阶段（读/验证/写）模式与 PostgreSQL SSI 的验证思路一致，冲突检测在提交端完成，减少长事务写入失败。
- 保留全局提交序（类似 TiDB/Spanner 的提交阶段锁 + 校验），避免写写交错导致不可序列化。
- 仅对未提交写保持互斥，相当于 InnoDB 的行级锁行为，确保不会出现多未提交版本覆盖。

## 兼容性与迁移
- 默认锁策略设为 `Pessimistic`，现有调用方/测试不受影响。
- 新增 API（如 `begin_transaction_with_options`）提供平滑迁移；若上层已有配置文件，可在存储配置结构中增加 `tp.lock_strategy` 字段，缺省时回退到悲观模式。
- WAL/Checkpoint 文件格式不变，无需数据迁移。

## 测试计划（建议新增）
- 单元测试：
  - 乐观模式下，两事务读取同一版本，先提交者成功，后提交者在 `validate_write_sets` 报冲突。
  - 乐观模式允许在 `start_ts` 之前已提交的新版本上写入并延迟冲突。
  - 悲观模式行为保持：对已提交且 `commit_ts > start_ts` 的版本写入立即失败。
  - 写集合覆盖顶点/边，确保 GC 后仍不误报。
- 集成/并发测试：
  - 交叉提交多事务，验证提交顺序下 redo/WAL 仍可恢复。
  - 结合 Checkpoint，确保乐观写集在恢复场景下不需要额外信息。

## 后续迭代点
- Snapshot 模式下是否需要可选的读集验证开关，用于防止读偏差/丢失更新。
- 失败重试策略（如指数退避）和锁等待超时配置。
- 在 `minigu-cli` 层暴露锁策略配置项，便于用户按 workload 切换。


## 关联 issue
https://github.com/TuGraph-family/miniGU/issues/108
