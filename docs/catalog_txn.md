**背景与意义**
- Catalog 的职责：管理目录/Schema、图与图类型（顶点/边类型与属性）、过程等元数据，是查询规划与执行的“真相来源”。
- 为什么需要事务：
  - 原子性：例如一次 `CREATE GRAPH TYPE` 同时新增多个 Label、顶点/边类型的定义，要么全部可见、要么全部回滚。
  - 隔离性：长查询在快照下读取稳定的元数据；并发 DDL/过程注册不会打破可见性规则。
  - 一致性：名称唯一性、引用完整性（边类型引用的顶点类型）在事务边界内被严格校验。
  - 与数据层一致：`minigu-transaction` 已提供时间戳与事务抽象，Catalog 采用同一套 MVCC 基础，便于统一的事务语义与后续持久化演进。

**现状综述**
- 代码位置：
  - Catalog 抽象与内存实现：`minigu/catalog`（`provider.rs`、`memory/*`）。
  - 事务基础设施：`minigu/transaction`（`timestamp.rs`、`transaction.rs`、`manager.rs`）。
- 当前 Catalog 使用 `RwLock<HashMap<...>>` 直接增删改（如 `MemorySchemaCatalog` 的 `graph_map`、`graph_type_map`、`procedure_map`），没有版本管理或事务可见性；`Session` 侧 `TransactionActivity` 也尚未接通。

**目标与范围**
- 在 Catalog 层引入 MVCC 事务：
  - 支持快照隔离（Snapshot Isolation）读取；为将来的可串行化保留扩展点。
  - 写集包括：目录/Schema 的增删、Graph 与 GraphType 定义增删、Label/VertexType/EdgeType 及其属性增删改、Procedure 注册/卸载。
  - 提供 `BEGIN/COMMIT/ROLLBACK` 的生命周期管理，并集成到 GQL 的事务语法处理与 `Session`。
  - 基础 GC：基于低水位（low watermark）清理过时版本。

**设计总览**
- 可见性与版本链（MVCC）：
  - 每个命名实体（如 `graph_type_map[name]`）不再直接存 `V`，而是存 `VersionChain<V>`：一条从新到旧的版本链，节点含（创建事务 ID、提交时间戳、是否删除 tombstone、值/增量）。
  - 读：根据事务的 `start_ts`/`txn_id` 从链表挑选“可见”版本：
    - 已提交且 `commit_ts <= start_ts`；或
    - 本事务创建（未提交但 `creator_txn_id == self.txn_id`）。
  - 写：在链头追加一个“未提交版本”（记录 `creator_txn_id`），`COMMIT` 时写入 `commit_ts`；`ABORT` 时丢弃该节点。
- 冲突控制：
  - 名称唯一性冲突在写入时使用当前事务视图检查；若另一个并发事务已经提交同名对象，当前事务在 `COMMIT` 校验时检测到冲突并失败（避免 SI 下“丢失写”）。
  - 先实现名称级别冲突检测；可扩展到引用级/范围校验（如删除被引用的类型时需检测）。
- Undo 记录与 GC：
  - 直接用版本链存储“可见历史”，无需单独的全局撤销日志；`minigu-transaction::UndoEntry` 可用于对接更复杂的物化撤销策略（可选）。
  - 维护低水位（所有活跃事务 `start_ts` 的最小值），链上早于低水位且被后继版本覆盖的节点即可回收。

**核心数据结构**
- 版本节点与链：
  - 文件：`minigu/catalog/src/txn/versioned.rs`（新增）
  - 结构（示意）：
    - `CatalogVersionNode<V> { value: Option<V>, tombstone: bool, creator_txn: Timestamp, commit_ts: Option<Timestamp>, prev: Option<Arc<CatalogVersionNode<V>>> }`
    - `CatalogVersionChain<V> { head: Arc<CatalogVersionNode<V>> }`
  - 关键方法：
    - `visible_at(&self, start_ts, txn_id) -> Option<Arc<CatalogVersionNode<V>>>`
    - `append_uncommitted(value|tombstone, creator_txn) -> Arc<Node>`
    - `commit(head, commit_ts)` / `abort(head)`
- 版本化 Map：
  - 文件：`minigu/catalog/src/txn/versioned_map.rs`（新增）
  - `VersionedMap<K, V>`：用 `RwLock<HashMap<K, CatalogVersionChain<V>>>` 封装 `get/put/remove` 的事务化实现。

**Catalog 事务接口**
- 事务类型：
  - 文件：`minigu/catalog/src/txn/txn.rs`（新增）
  - `CatalogTxn` 实现 `minigu_transaction::Transaction`：
    - 字段：`txn_id: Timestamp`、`start_ts: Timestamp`、`commit_ts: Atomic<Option<Timestamp>>`、`isolation_level: IsolationLevel`、`state: Atomic`。
    - `commit()`：通过 `minigu_transaction::global_timestamp_generator()` 分配 `commit_ts`，对当前事务写入过的所有 `VersionedMap` 头结点打上 `commit_ts`，提交后调用 `finish_transaction`。
    - `abort()`：撤销当前事务写入的未提交节点，调用 `finish_transaction`。
- 事务管理器：
  - 文件：`minigu/catalog/src/txn/manager.rs`（新增）
  - `CatalogTxnManager` 实现 `minigu_transaction::GraphTxnManager`：
    - `type Transaction = CatalogTxn`；`type GraphContext = CatalogRuntime`（见下）；
    - 维护活跃事务集合以计算低水位；提供 `begin/finish/garbage_collect/low_watermark`。
- Catalog 运行环境：
  - 文件：`minigu/catalog/src/txn/runtime.rs`（新增）
  - 持有对顶层 `MemoryCatalog` 的可变引用/Arc，并暴露 GC 钩子，供 `garbage_collect` 清理版本。

**对现有内存实现的改造**
- `MemorySchemaCatalog`：将如下字段替换为 `VersionedMap`：
  - `graph_map: VersionedMap<String, GraphRef>`
  - `graph_type_map: VersionedMap<String, Arc<MemoryGraphTypeCatalog>>`
  - `procedure_map: VersionedMap<String, ProcedureRef>`
- `MemoryDirectoryCatalog`：其 `children: HashMap<String, DirectoryOrSchema>` 改为 `VersionedMap<String, DirectoryOrSchema>`。
- 读 API（`SchemaProvider`/`DirectoryProvider`）：
  - 新增“在视图/快照上读取”的重载：例如 `get_graph_with(&self, name, read_view)`；
  - 或引入 `CatalogReadView { start_ts, txn_id }`，通过 `SessionContext` 注入当前事务并在 Provider 层透传；
  - 兼容旧接口：默认走“最新可见（无事务）”视图（`commit_ts == Some(max)` 的头结点）。

**关键代码骨架（示意）**
- 版本与可见性：
  - 文件：`minigu/catalog/src/txn/versioned.rs`
  - 片段：
```
pub struct CatalogVersionNode<V> {
    value: Option<V>,
    tombstone: bool,
    creator_txn: Timestamp,
    commit_ts: AtomicCell<Option<Timestamp>>,
    prev: Option<Arc<CatalogVersionNode<V>>>,
}

impl<V> CatalogVersionNode<V> {
    fn visible_for(&self, start_ts: Timestamp, txn_id: Timestamp) -> bool {
        match self.commit_ts.load() {
            Some(cts) => cts <= start_ts,
            None => self.creator_txn == txn_id,
        }
    }
}

pub struct CatalogVersionChain<V> {
    head: Arc<CatalogVersionNode<V>>,
}

impl<V: Clone> CatalogVersionChain<V> {
    pub fn get_visible(&self, start_ts: Timestamp, txn_id: Timestamp) -> Option<V> {
        let mut cur = Some(self.head.clone());
        while let Some(node) = cur {
            if node.visible_for(start_ts, txn_id) {
                return if node.tombstone { None } else { node.value.clone() };
            }
            cur = node.prev.clone();
        }
        None
    }
}
```

- 版本化 Map：
  - 文件：`minigu/catalog/src/txn/versioned_map.rs`
  - 片段：
```
pub struct VersionedMap<K, V> {
    inner: RwLock<HashMap<K, CatalogVersionChain<V>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> VersionedMap<K, V> {
    pub fn get(&self, k: &K, view: &ReadView) -> Option<V> { /* 按 view 取可见版本 */ }
    pub fn put(&self, k: K, v: V, txn: &CatalogTxn) -> Result<()> { /* 追加未提交版本 */ }
    pub fn delete(&self, k: &K, txn: &CatalogTxn) -> Result<()> { /* 追加 tombstone */ }
    pub fn commit(&self, touched: &[(K, Arc<CatalogVersionNode<V>>)], commit_ts: Timestamp) { /* 标记提交 */ }
    pub fn abort(&self, touched: &[(K, Arc<CatalogVersionNode<V>>)]) { /* 丢弃未提交节点 */ }
}
```

- Catalog 事务体：
  - 文件：`minigu/catalog/src/txn/txn.rs`
  - 片段：
```
pub struct CatalogTxn {
    txn_id: Timestamp,
    start_ts: Timestamp,
    commit_ts: AtomicCell<Option<Timestamp>>,
    isolation: IsolationLevel,
    // 记录本事务触达的 VersionedMap 节点，便于 commit/abort
    touched: Mutex<TouchedNodes>,
    mgr: Weak<CatalogTxnManagerInner>,
}

impl Transaction for CatalogTxn {
    type Error = CatalogTxnError;
    fn txn_id(&self) -> Timestamp { self.txn_id }
    fn start_ts(&self) -> Timestamp { self.start_ts }
    fn commit_ts(&self) -> Option<Timestamp> { self.commit_ts.get() }
    fn isolation_level(&self) -> &IsolationLevel { &self.isolation }
    fn commit(&self) -> Result<Timestamp, Self::Error> { /* 分配 commit_ts，批量标记提交 */ }
    fn abort(&self) -> Result<(), Self::Error> { /* 批量丢弃未提交节点 */ }
}
```

- 事务管理器：
  - 文件：`minigu/catalog/src/txn/manager.rs`
  - 片段：
```
impl GraphTxnManager for CatalogTxnManager {
    type Transaction = CatalogTxn;
    type GraphContext = CatalogRuntime;
    type Error = CatalogTxnError;

    fn begin_transaction(&self, lvl: IsolationLevel) -> Result<Arc<Self::Transaction>, Self::Error> {
        // 通过 `global_transaction_id_generator()` 与 `global_timestamp_generator()` 分配 txn_id / start_ts，登记活跃集合
    }

    fn finish_transaction(&self, txn: &Self::Transaction) -> Result<(), Self::Error> {
        // 移出活跃集合，更新低水位，可触发 GC
    }

    fn garbage_collect(&self, ctx: &Self::GraphContext) -> Result<(), Self::Error> {
        // 遍历 VersionedMap，清理 < low watermark 的不可见版本
    }

    fn low_watermark(&self) -> Timestamp { /* 计算活跃事务最小 start_ts */ }
}
```

**与 Provider 的衔接**
- 保持 `CatalogProvider/DirectoryProvider/SchemaProvider` 现有接口不变，新增“带视图”的读取：
  - 例如在 `SchemaProvider` 新增：`fn get_graph_with(&self, name: &str, view: &ReadView) -> CatalogResult<Option<GraphRef>>`；
  - `get_graph` 默认 `view = ReadView::latest()`（无事务场景）。
- 写接口（如 `MemorySchemaCatalog::add_graph/remove_graph`）改为接收 `&CatalogTxn` 以便登记未提交节点。

**Session 与 GQL 集成**
- `minigu/core/src/session.rs`：
  - 在 `handle_transaction_activity` 中接入事务：
    - `START TRANSACTION`：通过 `CatalogTxnManager::begin_transaction` 建立事务，保存在 `SessionContext`。
    - 执行中读取：`Planner/Executor` 从 `SessionContext` 取出事务，生成 `ReadView` 传给 Catalog Provider。
    - `COMMIT/ROLLBACK`：调用当前事务 `commit/abort`，并通过管理器 `finish_transaction`，清理 `SessionContext`。
- 若短期内仅支持过程调用内的读写：在 `Executor` 构建时将 `ReadView`/`CatalogTxn` 注入执行环境，内部调用 Provider 的 `*_with` 变体。

**错误与冲突处理**
- 新增错误类型 `CatalogTxnError`（如：重名、可见性冲突、非法状态变迁）。
- COMMIT 校验：
  - 对每个写入：确认名称在 `commit_ts` 视图下仍无冲突（防止 SI 的写偏差）。
  - 如有冲突：整个事务失败，返回 `CatalogTxnError::WriteConflict`。

**GC 策略**
- 低水位 `low watermark = min(active.start_ts)`。
- 回收规则：对于每条链，若存在某节点 `n`，其后继中有可见于任意未来读的更“新”节点，且 `n.commit_ts < low watermark`，则可安全删除 `n`。

**实现步骤建议**
1) 基础设施：在 `minigu/catalog/src/txn/` 下落地 `versioned.rs`、`versioned_map.rs`、`txn.rs`、`manager.rs`、`runtime.rs` 与 `error.rs`。
2) 接口扩展：为 `SchemaProvider/DirectoryProvider` 增加 `*_with(view)` 读取变体；现有无参接口默认走 `latest` 视图。
3) 内存实现改造：将 `MemoryDirectoryCatalog` 与 `MemorySchemaCatalog` 的 `HashMap` 字段替换为 `VersionedMap`，并将 `add/remove` 等写接口签名改为接受 `&CatalogTxn`。
4) Session 对接：实现 `handle_transaction_activity` 的 `start/end`，并在 Planner/Executor 读取路径透传 `ReadView`。
5) 冲突检测与单元测试：覆盖并发创建/删除同名对象、读写交叉、提交/回滚可见性等场景。
6) GC：在 `CatalogTxnManager::finish_transaction` 或周期性触发 `garbage_collect`，验证版本清理正确性。

**测试建议**
- 单元测试：
  - 可见性：T1 开启快照，T2 创建同名/不同名对象并提交；验证 T1/T2/新会话的可见结果。
  - 回滚：T1 创建对象后回滚；读取应不可见。
  - 冲突：T1、T2 同名创建，先后提交；后提交者应报冲突。
  - GC：构造多版本链，推进低水位后触发 GC，校验旧版本被清理。
- 系统/集成：
  - 通过 GQL `START TRANSACTION; <DDL/PROC>; COMMIT;` 组合验证会话级行为与指标。

**后续扩展**
- 可串行化：在 Snapshot 基础上增加读写集校验或两段锁（2PL）以实现更强隔离。
- 细粒度锁与批量提交：降低写热点、提高 DDL 吞吐。

**与现有代码的契合点**
- 只实现、不新增：直接实现 `minigu-transaction` 提供的 `Transaction` 与 `GraphTxnManager` trait，不自定义平行的事务/管理器接口，避免重复造轮子。
- 统一类型：复用 `minigu-transaction::{Timestamp, IsolationLevel}`；事务/提交时间戳与事务 ID 分配统一使用全局生成器 `global_transaction_id_generator()` 与 `global_timestamp_generator()`，不再单独定义生成器或状态枚举。
- Undo 复用：如需撤销/增量链，可直接使用 `minigu-transaction::UndoEntry<T>` 对接更复杂策略（可选）。
- Catalog 仅在 Provider 内部引入版本化，不影响上层 `Planner/Executor` 的抽象；通过 `ReadView` 可渐进式接入事务读取。

**小结**
- 以上方案以最小侵入方式为 Catalog 引入 MVCC：读路径通过视图选择可见版本，写路径通过链头的未提交节点在提交时发布，可回滚；管理器负责生命周期与 GC。该策略与存储层的 MVCC 基础一致，便于后续统一的恢复与持久化支持。

**实现 TODO 列表（按阶段推进）**
- 阶段 0：基础脚手架与错误类型
  - 新增 `minigu/catalog/src/txn/mod.rs` 统一导出子模块（`pub mod versioned; pub mod versioned_map; pub mod txn; pub mod manager; pub mod runtime; pub mod error;`）。
  - 新增 `minigu/catalog/src/txn/error.rs`：定义 `CatalogTxnError`（变更冲突、非法状态、可见性错误、GC 错误等）。

- 阶段 1：版本链与版本化容器
  - 新增 `minigu/catalog/src/txn/versioned.rs`：实现 `CatalogVersionNode<V>`、`CatalogVersionChain<V>`，支持：
    - `visible_for(start_ts, txn_id)`、`get_visible(start_ts, txn_id)`；
    - 追加未提交节点 `append_uncommitted(value|tombstone, creator_txn)`；
    - `commit(node, commit_ts)` / `abort(node)` 标记或丢弃节点；
    - 遍历与回收辅助（返回可回收断点）。
  - 新增 `minigu/catalog/src/txn/versioned_map.rs`：
    - 封装 `RwLock<HashMap<K, CatalogVersionChain<V>>>`；
    - 提供 `get(k, view)`、`put(k, v, txn)`、`delete(k, txn)`；
    - 在 `put/delete` 返回本事务创建的节点句柄，便于后续 `commit/abort`；
    - 提供 `commit_batch(nodes, commit_ts)`、`abort_batch(nodes)`；
    - 提供 `gc(low_watermark)` 以按链清理旧版本。

- 阶段 2：事务体与管理器
  - 新增 `minigu/catalog/src/txn/txn.rs`：实现 `CatalogTxn`（实现 `minigu_transaction::Transaction`）。
    - 字段：`txn_id`、`start_ts`、`commit_ts`、`isolation`、`touched`（记录 `VersionedMap` 节点引用）、`mgr` 弱引用。
    - 方法：`commit()` 分配 `commit_ts` 并批量提交；`abort()` 批量丢弃；提交前做名称级冲突校验钩子（可通过回调或 `VersionedMap` 视图再查）。
  - 新增 `minigu/catalog/src/txn/manager.rs`：实现 `CatalogTxnManager`（实现 `minigu_transaction::GraphTxnManager`）。
    - 维护活跃事务集合（`start_ts` 最小值为低水位）；
    - `begin_transaction(level)` 分配 `txn_id/start_ts` 并登记；
    - `finish_transaction(txn)` 移除活跃、推进低水位并触发 GC；
    - `garbage_collect(ctx)` 遍历容器调用 `gc`；`low_watermark()` 返回当前低水位。
  - 新增 `minigu/catalog/src/txn/runtime.rs`：封装对顶层 `MemoryCatalog` 的引用及容器枚举接口（便于 GC 统一访问）。

- 阶段 3：可读视图与 Provider 接口扩展
  - 新增 `ReadView { start_ts, txn_id }` 类型（放在 `txn/mod.rs` 或单独 `read_view.rs`）。
  - 在 `minigu/catalog/src/provider.rs` 中为只读接口增加带视图变体：
    - `DirectoryProvider::get_child_with(&self, name: &str, view: &ReadView)`；
    - `SchemaProvider::get_graph_with(&self, name: &str, view: &ReadView)`；
    - `SchemaProvider::get_graph_type_with(&self, name: &str, view: &ReadView)`；
    - `SchemaProvider::get_procedure_with(&self, name: &str, view: &ReadView)`；
  - 默认旧接口走 `ReadView::latest()`（无事务兼容）。

- 阶段 4：内存实现改造（写路径事务化，读路径可视图）
  - `minigu/catalog/memory/directory.rs`：将 `children: RwLock<HashMap<..>>` 改为 `VersionedMap<String, DirectoryOrSchema>`；
    - 新增 `add_child_txn(&self, name: String, child: DirectoryOrSchema, txn: &CatalogTxn)`；
    - `remove_child_txn(&self, name: &str, txn: &CatalogTxn)`；
    - 新增 `get_child_with` 调用 `VersionedMap::get`；
  - `minigu/catalog/memory/schema.rs`：将 `graph_map/graph_type_map/procedure_map` 全部替换为 `VersionedMap`；
    - 新增 `add_graph_txn/remove_graph_txn`、`add_graph_type_txn/remove_graph_type_txn`、`add_procedure_txn/remove_procedure_txn`；
    - 实现 `*_with(view)` 读取；旧只读接口调用 `*_with(ReadView::latest())`。

- 阶段 5：冲突检测与提交校验
  - 在 `VersionedMap::put/delete` 前做“当前事务视图下重名检查”；
  - 在 `CatalogTxn::commit()` 再做一次基于最新提交视图的名称级冲突校验（防写偏差）；
  - 发生冲突返回 `CatalogTxnError::WriteConflict{ key }` 并拒绝提交。

- 阶段 6：Session 与 GQL 对接
  - `minigu/core/src/session.rs`：
    - `handle_transaction_activity`：实现 `start`（begin+保存到 `SessionContext`），`end`（解析 COMMIT/ROLLBACK，调用 `txn.commit/abort` 与 `manager.finish_transaction`）。
    - 在执行路径（Planner/Executor 构建时）从 `SessionContext` 取出事务，生成 `ReadView` 并注入。
  - `minigu/context/src/session.rs` 或 `minigu/context/src/database.rs`：
    - 为 `SessionContext` 增加可选的 `current_txn: Arc<CatalogTxn>` 字段与访问器。

- 阶段 7：GC 接入
  - 在 `CatalogTxnManager::finish_transaction` 后尝试触发 `garbage_collect`；
  - `runtime` 中提供迭代所有 `VersionedMap` 的方法，统一对 `directory/schema` 的容器执行 `gc(low_watermark)`。

- 阶段 8：测试
  - 单元测试（`minigu/catalog` 新增 `tests` 或 `#[cfg(test)]`）：
    - 可见性（提交前本事务可见、他事务不可见；提交后快照与新会话可见性）；
    - 回滚后不可见；
    - 并发重名写提交冲突；
    - GC 清理低水位之前的旧版本。
  - 集成/系统测试（在 `minigu-test`）：
    - GQL `START TRANSACTION; <DDL/PROC>; COMMIT;` 行为；
    - 事务中读写与多会话可见性。

- 阶段 9：文档与 CI
  - 更新 `docs/catalog_txn.md` 的 API 片段为最终签名；
  - 在 `scripts/run_ci.sh` 下跑通 fmt、clippy、tests；根据需要补充 `deny.toml` 依赖检查说明。
