# TP 存储乐观锁设计（MVCC）

## 现状与问题判断
- `MemoryGraph` 的写路径在 `create_vertex` / `create_edge` / `delete_*` / `set_*_property` 等接口中统一调用 `check_write_conflict`，当目标版本的 `commit_ts` 为其他事务的事务号，或 `commit_ts` 晚于当前事务 `start_ts` 时立即返回冲突（`minigu/storage/src/tp/memory_graph.rs` 约 620–840、1200+ 行）。这等价于在写入前抢占式检测，行为接近悲观锁。
- `check_write_conflict` 逻辑与事务隔离级无关，未暴露配置项；提交阶段仅做可串行化时的读集验证，没有为乐观策略设计的写集/版本校验。
- 因此 issue 中“数据操作直接检查写冲突，表现为悲观锁；希望支持乐观锁并可配置”的描述成立，当前 TP/MVCC 路径缺少乐观锁选项。

## 设计目标
- 为 TP 内存存储（MVCC）提供可配置的锁策略：保留现有悲观策略为默认值，新增乐观策略，在提交阶段检测冲突。
- 范围限定：仅 TP 存储（`minigu/storage/src/tp`），AP 不改动。
- 兼容性：默认配置与现有行为一致，不影响 WAL/检查点/GC 语义。

## 方案概览
- 新增 `TxnLockStrategy`（`Pessimistic` | `Optimistic`），通过图实例配置注入到每个事务。
- 写路径：
  - `Pessimistic`：沿用当前 `check_write_conflict` 早期拒绝。
  - `Optimistic`：跳过写前冲突拒绝，记录写集基线版本信息，允许并发写，冲突延迟到提交时处理。
- 提交阶段（乐观策略）：
  - 读集校验：确认读到的版本在事务开始后未被他人提交（Snapshot 与 Serializable 都校验，Serializble 继续保留现有读集验证逻辑）。
  - 写集校验：确认当前版本的 `commit_ts` 仍等于写入时记录的基线，否则冲突回滚。
  - 校验通过后再获取全局提交锁、分配 `commit_ts`、写 WAL、推进 `latest_commit_ts`。

## 详细设计

### 配置与注入
- 定义 `TxnLockStrategy` 枚举以及 `TxnOptions { lock_strategy: TxnLockStrategy }`，默认 `Pessimistic`。
- 在 `MemoryGraph::with_config_fresh/with_config_recovered` 增加可选事务配置参数（保留现有签名的默认实现以兼容调用处），`MemTxnManager` 持有 `TxnOptions`。
- `begin_transaction_at` 创建 `MemTransaction` 时将 `lock_strategy` 写入事务实例，后续逻辑按事务内字段分支。

### 数据结构与记录
- `MemTransaction` 新增：
  - `lock_strategy: TxnLockStrategy`
  - `write_set: DashSet<WriteKey>`（或 `DashMap<WriteKey, Timestamp>`），记录写操作时看到的基线 `commit_ts`。`WriteKey` 可包含 `{kind: Vertex|Edge, id: u64}`。
- 读集：在乐观模式下，即使 `IsolationLevel::Snapshot` 也记录 `vertex_reads` / `edge_reads`，方便提交验证。
- 现有 `UndoEntry::timestamp` 已保留旧版本 `commit_ts`，可在提交时用于回滚与基线校验的兜底数据。

### 写路径调整
- `check_write_conflict` 改为根据事务 `lock_strategy` 分支：
  - `Pessimistic`：维持现有逻辑（他人持有写或提交在事务之后则报错）。
  - `Optimistic`：仅做可见性基础校验（目标版本不存在或 tombstone 时报错），不拒绝并发写；记录 `WriteKey` 与当前 `commit_ts` 到 `write_set`。
- `update_properties!` 宏及 `create_*`/`delete_*` 等接口在进入写操作时，将被修改实体的当前 `commit_ts` 推入 `write_set`。

### 提交阶段校验（乐观策略）
- 在 `MemTransaction::commit_at` 的 Step 1 前增加乐观校验分支：
  1. 读集校验：遍历 `vertex_reads` / `edge_reads`，若当前版本的 `commit_ts` 既不是本事务，也大于 `start_ts`，则 `ReadWriteConflict`。
  2. 写集校验：对记录的 `WriteKey`，再读当前版本的 `commit_ts`，若与写入时保存的基线不相等且不等于本事务 ID，则判定写写冲突（或不可见版本），返回 `WriteWriteConflict` 并触发回滚。
  3. Serializable 级别继续执行原有 `validate_read_sets`（防止快照后插入/删除导致的不可串行化）。
- 校验通过后继续现有提交流程：分配 `commit_ts` → 更新 `current.commit_ts` → 写 WAL → `finish_transaction`。

### 回滚、恢复与 GC
- 回滚：沿用 `undo_buffer` 逻辑即可，乐观模式下 `write_set` 仅用于校验，回滚时清空/丢弃即可。
- WAL/恢复：Redo/Undo 结构不变；恢复时按 WAL 重放仍会走写路径，需确保恢复时使用 `Pessimistic` 或绕过冲突检测（当前 `skip_wal` 分支可复用）。
- GC：基于 `undo_buffer` 与 watermark 的逻辑无需调整；`write_set` 生命周期限于事务。

### 兼容性与配置入口
- 对外 API 保持不变，默认 `Pessimistic`，现有调用无需改动。
- 需要新增配置注入路径（示例）：
  - 图创建时可传 `TxnOptions`，或在 `MemoryGraph` 上提供 `with_txn_options` 辅助构造。
  - 后续可在 Session/GraphCatalog 层暴露配置开关，便于 GQL/CLI 选择策略。

## 待办与测试建议
- 单元测试：
  - 乐观模式下并发写同一顶点/边，提交时发生冲突并回滚。
  - 乐观模式下 Snapshot 事务读旧版本、并发写后提交，读集校验能捕获冲突。
  - 悲观模式保持现有行为（原测试回归）。
- 集成测试：
  - 事务配置切换接口可用性（默认值为悲观；切换为乐观后行为符合预期）。
  - WAL 恢复路径在乐观模式下不误判冲突（使用 `skip_wal` 分支或默认悲观）。
- 文档与配置：
  - 在用户文档/CLI 帮助中说明锁策略含义与适用场景。
