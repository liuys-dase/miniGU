# 系统级事务设计方案

## 背景 (Background)

当前 minigu 数据库系统存在以下问题：
- 事务支持仅限于存储层（storage/tp），catalog 层完全没有事务支持
- 无法实现跨 catalog 和 storage 的系统级事务，导致 DDL 操作无法保证原子性
- 缺乏统一的事务协调机制，无法保证元数据变更和数据变更的一致性
- 这严重阻碍了端到端功能的实现，特别是需要同时修改 schema 和数据的操作

## 需求 (Requirements)

### 核心功能需求
1. **系统级事务管理器**：协调 catalog 和 storage 层的事务操作
2. **DDL 事务支持**：支持 CREATE/ALTER/DROP 等 DDL 操作的事务化
3. **DML-DDL 混合事务**：支持在同一事务中执行 DDL 和 DML 操作
4. **ACID 保证**：跨 catalog 和 storage 的原子性、一致性、隔离性和持久性
5. **版本化 Catalog**：支持 catalog 的 MVCC，实现 DDL 操作的并发控制

### 技术限制条件
1. 必须兼容现有的存储层事务实现（MemTxnManager）
2. 保持向后兼容，不破坏现有 API
3. 性能开销最小化，避免引入不必要的锁竞争
4. 支持崩溃恢复，确保系统一致性

## 方案设计 (Solution Design)

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Application Layer                             │
│                    (SQL/GQL Query Processing)                        │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    System Transaction Manager                        │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                   SystemTransaction                          │   │
│  │  - txn_id: Timestamp                                        │   │
│  │  - catalog_txn: Arc<CatalogTransaction>                    │   │
│  │  - storage_txn: Arc<MemTransaction>                        │   │
│  │  - state: TransactionState                                 │   │
│  │  - isolation_level: IsolationLevel                         │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌────────────────────┐  ┌────────────────────┐                    │
│  │ Global Timestamp   │  │ Transaction ID     │                    │
│  │ Generator          │  │ Generator          │                    │
│  └────────────────────┘  └────────────────────┘                    │
└─────────────────────────────────────────────────────────────────────┘
                    │                              │
        ┌───────────┴───────────┐      ┌──────────┴──────────┐
        ▼                       ▼      ▼                      ▼
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  Catalog Layer   │    │  Storage Layer   │    │   Checkpoint     │
│                  │    │                  │    │    Manager       │
│ VersionedCatalog │    │  MemTxnManager   │    │                  │
│ CatalogTxnMgr    │    │  MemTransaction  │    │(System Snapshot) │
│ (MVCC-based)     │    │  (MVCC-based)    │    │                  │
└──────────────────┘    └──────────────────┘    └──────────────────┘
```

### 模块划分与职责

#### 1. System Transaction Manager (系统事务管理器)
- **职责**：统一协调 catalog 和 storage 层的事务
- **核心组件**：
  - `SystemTxnManager`: 系统级事务管理器
  - `SystemTransaction`: 系统级事务实例
  - `GlobalTimestampGenerator`: 全局时间戳生成器
  - `TransactionIdGenerator`: 事务ID生成器

#### 2. Versioned Catalog (版本化目录)
- **职责**：为 catalog 提供 MVCC 支持
- **核心组件**：
  - `VersionedCatalog`: 版本化的 catalog 包装器
  - `CatalogTransaction`: catalog 事务实例（基于 MVCC）
  - `CatalogVersionManager`: catalog 版本管理器
  - `CatalogUndoLog`: catalog 操作的 undo 日志

#### 3. Common Infrastructure (公共基础设施)
- **职责**：提供跨层共享的基础组件
- **核心组件**：
  - `Timestamp`: 统一的时间戳类型
  - `TransactionId`: 统一的事务ID类型
  - `IsolationLevel`: 统一的隔离级别定义
  - `TransactionState`: 统一的事务状态定义

#### 4. System Checkpoint (系统检查点)
- **职责**：创建包含 catalog 和 storage 的一致性快照
- **核心组件**：
  - `SystemCheckpoint`: 系统级检查点
  - `CatalogSnapshot`: catalog 快照
  - `StorageSnapshot`: storage 快照

## 关键实现 (Key Implementation)

### 1. 公共基础设施定义

```rust
// minigu/common/src/transaction/mod.rs

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// 时间戳类型，用于 MVCC 版本控制
pub type Timestamp = u64;

/// 事务ID类型
pub type TransactionId = u64;

/// 全局时间戳生成器
pub struct GlobalTimestampGenerator {
    counter: AtomicU64,
}

impl GlobalTimestampGenerator {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }
    
    pub fn next(&self) -> Timestamp {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
    
    pub fn current(&self) -> Timestamp {
        self.counter.load(Ordering::SeqCst)
    }
}

/// 事务ID生成器
pub struct TransactionIdGenerator {
    counter: AtomicU64,
}

impl TransactionIdGenerator {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }
    
    pub fn next(&self) -> TransactionId {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
}

/// 系统级事务状态
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    Active,
    Committing,
    Committed,
    Aborting,
    Aborted,
}
```

### 2. 系统事务管理器接口

```rust
// minigu/context/src/system_transaction.rs

use std::sync::{Arc, Mutex, RwLock};
use crossbeam_skiplist::SkipMap;
use minigu_common::transaction::{
    GlobalTimestampGenerator, TransactionIdGenerator, 
    TransactionState, Timestamp, TransactionId
};
use minigu_storage::tp::transaction::{MemTransaction, IsolationLevel};
use minigu_catalog::transaction::CatalogTransaction;

/// 系统级事务管理器
pub struct SystemTxnManager {
    /// Catalog 事务管理器
    catalog_txn_mgr: Arc<CatalogTxnManager>,
    /// Storage 事务管理器
    storage_txn_mgr: Arc<MemTxnManager>,
    /// 全局时间戳生成器（共享）
    timestamp_generator: Arc<GlobalTimestampGenerator>,
    /// 事务ID生成器
    txn_id_generator: Arc<TransactionIdGenerator>,
    /// 活跃的系统事务
    active_txns: SkipMap<TransactionId, Arc<SystemTransaction>>,
}

impl SystemTxnManager {
    /// 开始一个新的系统事务
    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Arc<SystemTransaction>> {
        let txn_id = self.txn_id_generator.next();
        let start_ts = self.timestamp_generator.next();
        
        // 创建 catalog 事务（MVCC模式）
        let catalog_txn = self.catalog_txn_mgr.begin_transaction(
            txn_id,
            start_ts,
            isolation_level,
        )?;
        
        // 创建 storage 事务
        let storage_txn = self.storage_txn_mgr.begin_transaction(
            txn_id,
            isolation_level,
        )?;
        
        // 创建系统事务
        let system_txn = Arc::new(SystemTransaction {
            txn_id,
            start_ts,
            catalog_txn,
            storage_txn,
            state: RwLock::new(TransactionState::Active),
            isolation_level,
        });
        
        // 注册到活跃事务表
        self.active_txns.insert(txn_id, system_txn.clone());
        
        Ok(system_txn)
    }
    
    /// 提交系统事务（统一的 MVCC 提交）
    pub async fn commit_transaction(
        &self,
        txn: Arc<SystemTransaction>,
    ) -> Result<()> {
        // 获取提交时间戳
        let commit_ts = self.timestamp_generator.next();
        
        // 验证并提交事务
        self.do_commit_transaction(&txn, commit_ts).await?;
        
        Ok(())
    }
}
```

### 3. 系统事务实现

```rust
/// 系统级事务
pub struct SystemTransaction {
    /// 事务ID
    pub txn_id: TransactionId,
    /// 开始时间戳
    pub start_ts: Timestamp,
    /// Catalog 层事务
    catalog_txn: Arc<CatalogTransaction>,
    /// Storage 层事务
    storage_txn: Arc<MemTransaction>,
    /// 事务状态
    state: RwLock<TransactionState>,
    /// 隔离级别
    isolation_level: IsolationLevel,
}

impl SystemTransaction {
    /// 执行 DDL 操作
    pub fn execute_ddl(&self, ddl: DdlOperation) -> Result<()> {
        // 验证事务状态
        self.check_active()?;
        
        // 在 catalog 事务中执行（MVCC模式）
        self.catalog_txn.execute_ddl(&ddl)?;
        
        Ok(())
    }
    
    /// 执行 DML 操作
    pub fn execute_dml(&self, dml: DmlOperation) -> Result<()> {
        // 验证事务状态
        self.check_active()?;
        
        // 在 storage 事务中执行
        match dml {
            DmlOperation::CreateVertex(vertex) => {
                self.storage_txn.create_vertex(vertex)
            }
            DmlOperation::CreateEdge(edge) => {
                self.storage_txn.create_edge(edge)
            }
            // ... 其他 DML 操作
        }?;
        
        Ok(())
    }
    
    /// 检查事务是否处于活跃状态
    fn check_active(&self) -> Result<()> {
        let state = self.state.read().unwrap();
        if *state != TransactionState::Active {
            return Err(TransactionError::InvalidState(*state));
        }
        Ok(())
    }
}
```

### 4. 版本化 Catalog 实现（MVCC）

```rust
// minigu/catalog/src/transaction.rs

/// Catalog 事务（基于 MVCC）
pub struct CatalogTransaction {
    /// 事务ID
    txn_id: TransactionId,
    /// 开始时间戳
    start_ts: Timestamp,
    /// 读集：记录读取的 catalog 对象版本
    read_set: Mutex<Vec<CatalogReadEntry>>,
    /// 写集：记录修改的 catalog 对象
    write_set: Mutex<Vec<CatalogWriteEntry>>,
    /// 版本管理器引用
    version_manager: Arc<CatalogVersionManager>,
}

/// Catalog 读条目
#[derive(Clone)]
struct CatalogReadEntry {
    object_type: CatalogObjectType,
    object_id: String,
    version: u64,
}

/// Catalog 写条目
#[derive(Clone)]
struct CatalogWriteEntry {
    object_type: CatalogObjectType,
    object_id: String,
    operation: CatalogOperation,
    old_value: Option<CatalogObject>,
    new_value: Option<CatalogObject>,
}

impl CatalogTransaction {
    /// 创建 Graph Type（MVCC 版本）
    pub fn create_graph_type(
        &self,
        name: &str,
        definition: GraphTypeDefinition,
    ) -> Result<()> {
        // 使用 MVCC 读取当前版本，检查名称冲突
        let existing = self.version_manager.get_at_timestamp(
            CatalogObjectType::GraphType,
            name,
            self.start_ts,
        )?;
        
        if existing.is_some() {
            return Err(CatalogError::AlreadyExists(name.to_string()));
        }
        
        // 创建 graph type
        let graph_type = GraphType::new(definition);
        
        // 记录到写集
        self.write_set.lock().unwrap().push(CatalogWriteEntry {
            object_type: CatalogObjectType::GraphType,
            object_id: name.to_string(),
            operation: CatalogOperation::Create,
            old_value: None,
            new_value: Some(CatalogObject::GraphType(graph_type)),
        });
        
        Ok(())
    }
    
    /// 提交事务（MVCC 版本）
    pub fn commit(&self, commit_ts: Timestamp) -> Result<()> {
        // 验证读集（检查是否有并发修改）
        self.validate_read_set()?;
        
        // 将写集应用到版本管理器
        let write_set = self.write_set.lock().unwrap();
        for entry in write_set.iter() {
            self.version_manager.create_version(
                entry.object_type,
                &entry.object_id,
                commit_ts,
                entry.new_value.clone(),
            )?;
        }
        
        Ok(())
    }
    
    /// 验证读集
    fn validate_read_set(&self) -> Result<()> {
        let read_set = self.read_set.lock().unwrap();
        for entry in read_set.iter() {
            let current_version = self.version_manager.get_latest_version(
                entry.object_type,
                &entry.object_id,
            )?;
            
            // 如果版本号不同，说明有并发修改
            if current_version != entry.version {
                return Err(TransactionError::ConflictDetected);
            }
        }
        Ok(())
    }
}
```

### 5. Catalog 版本管理器

```rust
// minigu/catalog/src/version_manager.rs

/// Catalog 版本管理器
pub struct CatalogVersionManager {
    /// 版本存储：object_type -> object_id -> 版本链
    versions: DashMap<(CatalogObjectType, String), VersionChain>,
}

/// 版本链
struct VersionChain {
    /// 版本列表，按时间戳排序
    versions: Vec<CatalogVersion>,
}

/// Catalog 版本
struct CatalogVersion {
    /// 版本时间戳
    timestamp: Timestamp,
    /// 版本数据
    data: Option<CatalogObject>,
    /// 是否已删除
    deleted: bool,
}

impl CatalogVersionManager {
    /// 在指定时间戳获取对象
    pub fn get_at_timestamp(
        &self,
        object_type: CatalogObjectType,
        object_id: &str,
        timestamp: Timestamp,
    ) -> Result<Option<CatalogObject>> {
        let key = (object_type, object_id.to_string());
        
        if let Some(chain) = self.versions.get(&key) {
            // 二分查找合适的版本
            let version = chain.versions
                .binary_search_by_key(&timestamp, |v| v.timestamp)
                .map(|idx| &chain.versions[idx])
                .or_else(|idx| {
                    if idx > 0 {
                        Some(&chain.versions[idx - 1])
                    } else {
                        None
                    }
                })?;
            
            if version.deleted {
                Ok(None)
            } else {
                Ok(version.data.clone())
            }
        } else {
            Ok(None)
        }
    }
    
    /// 创建新版本
    pub fn create_version(
        &self,
        object_type: CatalogObjectType,
        object_id: &str,
        timestamp: Timestamp,
        data: Option<CatalogObject>,
    ) -> Result<()> {
        let key = (object_type, object_id.to_string());
        let version = CatalogVersion {
            timestamp,
            data: data.clone(),
            deleted: data.is_none(),
        };
        
        self.versions
            .entry(key)
            .or_insert_with(|| VersionChain { versions: Vec::new() })
            .versions
            .push(version);
        
        Ok(())
    }
}
```

### 6. 系统事务提交实现（基于 MVCC）

```rust
// minigu/context/src/system_transaction.rs

impl SystemTxnManager {
    /// 提交系统事务
    async fn do_commit_transaction(
        &self,
        txn: &Arc<SystemTransaction>,
        commit_ts: Timestamp,
    ) -> Result<()> {
        // 更新事务状态
        txn.set_state(TransactionState::Committing)?;
        
        // 并行验证 catalog 和 storage 的读集
        let (catalog_valid, storage_valid) = tokio::join!(
            self.validate_catalog_transaction(&txn.catalog_txn),
            self.validate_storage_transaction(&txn.storage_txn)
        );
        
        // 如果验证失败，回滚事务
        if !catalog_valid? || !storage_valid? {
            self.abort_transaction(txn).await?;
            return Err(TransactionError::ValidationFailed);
        }
        
        // 提交 catalog 事务
        self.catalog_txn_mgr
            .commit_transaction(&txn.catalog_txn, commit_ts)
            .await?;
        
        // 提交 storage 事务
        self.storage_txn_mgr
            .commit_transaction(&txn.storage_txn, commit_ts)
            .await?;
        
        // 更新事务状态
        txn.set_state(TransactionState::Committed)?;
        
        // 从活跃事务表中移除
        self.active_txns.remove(&txn.txn_id);
        
        Ok(())
    }
    
    /// 回滚事务
    async fn abort_transaction(
        &self,
        txn: &Arc<SystemTransaction>,
    ) -> Result<()> {
        // 更新事务状态
        txn.set_state(TransactionState::Aborting)?;
        
        // 回滚 catalog 事务
        self.catalog_txn_mgr.rollback_transaction(&txn.catalog_txn).await?;
        
        // 回滚 storage 事务
        self.storage_txn_mgr.rollback_transaction(&txn.storage_txn).await?;
        
        // 更新事务状态
        txn.set_state(TransactionState::Aborted)?;
        
        // 从活跃事务表中移除
        self.active_txns.remove(&txn.txn_id);
        
        Ok(())
    }
}

### 7. 系统检查点与恢复

```rust
// minigu/context/src/checkpoint.rs

/// 系统检查点管理器
pub struct SystemCheckpointManager {
    catalog_txn_mgr: Arc<CatalogTxnManager>,
    storage_txn_mgr: Arc<MemTxnManager>,
    timestamp_generator: Arc<GlobalTimestampGenerator>,
    checkpoint_dir: PathBuf,
}

impl SystemCheckpointManager {
    /// 创建系统检查点
    pub async fn create_checkpoint(&self) -> Result<SystemCheckpoint> {
        // 获取一致性时间戳
        let checkpoint_ts = self.timestamp_generator.next();
        
        // 创建 catalog 快照
        let catalog_snapshot = self.catalog_txn_mgr
            .create_snapshot(checkpoint_ts)
            .await?;
        
        // 创建 storage 快照
        let storage_snapshot = self.storage_txn_mgr
            .create_snapshot(checkpoint_ts)
            .await?;
        
        let checkpoint = SystemCheckpoint {
            timestamp: checkpoint_ts,
            catalog_snapshot,
            storage_snapshot,
        };
        
        // 持久化检查点
        self.persist_checkpoint(&checkpoint).await?;
        
        Ok(checkpoint)
    }
    
    /// 从检查点恢复
    pub async fn restore_from_checkpoint(
        &self,
        checkpoint: &SystemCheckpoint,
    ) -> Result<()> {
        // 恢复 catalog 状态
        self.catalog_txn_mgr
            .restore_from_snapshot(&checkpoint.catalog_snapshot)
            .await?;
        
        // 恢复 storage 状态
        self.storage_txn_mgr
            .restore_from_snapshot(&checkpoint.storage_snapshot)
            .await?;
        
        Ok(())
    }
}
```

### 8. 性能优化考虑

```rust
/// 批量操作优化
impl SystemTransaction {
    /// 批量执行操作
    pub fn batch_execute<F>(&self, f: F) -> Result<()> 
    where
        F: FnOnce(&mut BatchContext) -> Result<()>,
    {
        let mut batch_ctx = BatchContext::new(self);
        
        // 执行批量操作
        f(&mut batch_ctx)?;
        
        // 批量应用到写集
        self.apply_batch_writes(batch_ctx.operations)?;
        
        Ok(())
    }
}

/// 并发控制优化
impl CatalogVersionManager {
    /// 垃圾回收旧版本
    pub fn garbage_collect(&self, watermark: Timestamp) -> Result<()> {
        for mut entry in self.versions.iter_mut() {
            let chain = &mut entry.value_mut();
            // 保留 watermark 之后的版本和最新版本
            chain.versions.retain(|v| {
                v.timestamp >= watermark || 
                v.timestamp == chain.versions.last().unwrap().timestamp
            });
        }
        Ok(())
    }
}

/// 缓存优化
impl CatalogTxnManager {
    /// 版本缓存，减少版本链遍历
    struct VersionCache {
        cache: LruCache<(CatalogObjectType, String, Timestamp), Option<CatalogObject>>,
    }
    
    /// 批量预取优化
    pub fn prefetch_versions(
        &self,
        objects: &[(CatalogObjectType, String)],
        timestamp: Timestamp,
    ) -> Result<()> {
        // 并行预取多个对象的版本
        let handles: Vec<_> = objects.iter()
            .map(|(obj_type, obj_id)| {
                let mgr = self.version_manager.clone();
                tokio::spawn(async move {
                    mgr.get_at_timestamp(*obj_type, obj_id, timestamp)
                })
            })
            .collect();
        
        // 等待所有预取完成
        for handle in handles {
            handle.await??;
        }
        
        Ok(())
    }
}
```

## 实施计划 (Implementation Plan)

### 第一阶段：公共基础设施 (1周)
1. 实现 `GlobalTimestampGenerator` 和 `TransactionIdGenerator`
2. 定义统一的事务状态和类型
3. 创建共享的基础结构

### 第二阶段：Catalog MVCC 实现 (2周)
1. 实现 `CatalogVersionManager` 版本管理器
2. 实现 `CatalogTransaction` 的 MVCC 逻辑
3. 实现版本链管理和垃圾回收

### 第三阶段：系统事务管理器 (2周)
1. 实现 `SystemTxnManager` 和 `SystemTransaction`
2. 实现统一的事务提交和回滚逻辑
3. 实现事务隔离级别支持

### 第四阶段：检查点和恢复 (2周)
1. 实现系统检查点机制
2. 实现快照创建和恢复
3. 实现崩溃恢复流程

### 第五阶段：测试和优化 (2周)
1. 编写全面的单元测试和集成测试
2. 性能测试和优化
3. 文档完善

## 注意事项 (Notes)

### 潜在风险
1. **版本链过长**：长时间运行的事务可能导致版本链过长，影响查询性能
2. **时间戳耗尽**：需要考虑时间戳生成器的上限和回绕处理
3. **内存占用**：版本化 catalog 会增加内存使用，需要及时垃圾回收
4. **并发冲突**：高并发 DDL 操作可能导致频繁的事务回滚

### 优化建议
1. **版本缓存**：为频繁访问的 catalog 对象版本添加 LRU 缓存
2. **批量操作**：将多个 DDL/DML 操作批量执行，减少事务开销
3. **预取优化**：在事务开始时预取可能需要的 catalog 版本
4. **垃圾回收策略**：基于活跃事务的最小时间戳定期清理旧版本

### 扩展思路
1. **分布式 MVCC**：未来可扩展为支持分布式环境下的 MVCC
2. **在线 DDL**：利用 MVCC 实现不阻塞读写的在线 schema 变更
3. **时间旅行查询**：利用 MVCC 实现历史版本查询功能
4. **多版本索引**：为 catalog 对象实现多版本索引，加速版本查找
5. **增量检查点**：实现增量检查点机制，减少检查点创建开销

## 参考资料 (References)

1. DuckDB Transaction Management: https://duckdb.org/internals/transactions
2. PostgreSQL MVCC: https://www.postgresql.org/docs/current/mvcc.html
3. FoundationDB MVCC Design: https://apple.github.io/foundationdb/developer-guide.html
4. CockroachDB Transactions: https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer.html
5. Spanner: Google's Globally-Distributed Database: https://research.google/pubs/pub39966/