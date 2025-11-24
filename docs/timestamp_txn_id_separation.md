# Timestamp / TxnId 分离设计

## 背景与问题
- 现有 `Timestamp(u64)` 通过最高位区分 txn-id 与 commit-ts，类型合一导致比较/接口容易混用，缺少编译期防护。
- `GlobalTimestampGenerator` 与 `TransactionIdGenerator` 共享同一类型，`with_start` / `init_global_*` 未做域校验，易被传入错误起始值；生成器默认从 1 开始而 `Timestamp::Default` 为 0，语义不一致。
- commit-ts 上界判断存在 off-by-one（永远拿不到 `max_commit_ts()`），同时缺少持久化/恢复约束，重启后可能回退。
- 目标：拆分语义、显式域校验、为后续持久化/分布式扩展留接口。

## 设计目标
- 类型分离：使用两个新类型 `CommitTs`、`TxnId`，杜绝跨域比较/误用；`Timestamp` 仅做过渡或弃用。
- 生成器分离：`CommitTsGenerator`、`TxnIdGenerator` 各自维护起始值、上界和域校验，提供全局单例访问。
- 接口明确：公共 API 只暴露各自类型，不再接受裸 `u64`；必要时通过 `TryFrom<u64>` / `into_raw()` 做显式转换。
- 兼容与迁移：提供过渡层减少大范围修改，可逐步替换调用点；默认行为与现有数值单调性保持一致。
- 可扩展：预留持久化/恢复钩子，便于后续在元数据/日志中存储最后分配值。

## 新模型与数据结构
- 新类型
  - `struct CommitTs(u64);`
  - `struct TxnId(u64);`
  - 仅实现必要的 `Copy/Clone/PartialEq/Ord/Serialize/Deserialize`，`Ord` 仅在同域比较；跨域比较不提供。
  - 提供 `fn into_raw(self) -> u64`，`TryFrom<u64>` 做域校验（`CommitTs` 拒绝最高位为 1，`TxnId` 要求最高位为 1，或采用全分离取值区间）。
- 生成器
  - `CommitTsGenerator { counter: AtomicU64 }`，默认从 1（或 0，如需保留 0 则禁止发放）开始，溢出检查为 `>` 而非 `>=`。
  - `TxnIdGenerator { counter: AtomicU64 }`，默认从 `TXN_ID_START` 或 `TXN_ID_START + 1`，同样 `>` 检查。
  - `update_if_greater` 以各自类型入参，内部 `fetch_max`；拒绝跨域类型传入。
- 全局单例
  - `global_commit_ts_generator()` / `global_txn_id_generator()` 替换现有接口。
  - 初始化函数 `init_global_commit_ts_generator(start: CommitTs)` 等，保证只初始化一次，且在设置时验证域。

## API 变更与兼容策略
- 公开 API
  - 事务接口 `Transaction` 保持签名，但将 `Timestamp` 改为 `TxnId` / `CommitTs`：`fn txn_id(&self) -> TxnId`，`fn start_ts(&self) -> CommitTs`，`fn commit_ts(&self) -> Option<CommitTs>`。
  - 管理器接口 `GraphTxnManager::low_watermark() -> CommitTs`。
  - 辅助函数 `global_*` 返回新生成器类型。
- 过渡层
  - 在 `timestamp.rs` 保留旧 `Timestamp`，实现 `From<CommitTs>`、`From<TxnId>`、`TryFrom<Timestamp>` 便于增量迁移。
  - 编译期守护：对旧类型加 `#[deprecated]`（在完成迁移后再移除），或在 Clippy 配置中添加 deny 规则。
- 存储/版本结构调整
  - 数据版本字段使用 `CommitTs`，事务结构使用 `TxnId`；Undo 记录保持 `CommitTs`。
  - 如需写 WAL/快照，序列化时用 `into_raw()`，反序列化时 `TryFrom` 校验。

## 溢出与持久化策略
- 溢出处理
  - `CommitTs::MAX = 0x7fff…`（若保留最高位给 txn-id），`TxnId::MAX = u64::MAX`。
  - 生成器达到上界时报错，并提供统计指标计数。
- 持久化/恢复（预留）
  - 生成器提供 `fn restore(&self, last: CommitTs/TxnId)`，启动时由存储层读取元数据后调用，保证单调递增（使用 `fetch_max(last + 1)`）。
  - 若未来需要分布式/TSO，对 `CommitTsGenerator` 抽象成 trait，当前实现为本地计数器。

## 迁移步骤
1) 引入新类型与生成器，增加全局访问函数，保留旧 `Timestamp` 作为兼容层。
2) 将事务接口、管理器接口的类型替换为新类型；更新编译错误处的调用。
3) 存储/版本字段、undo 结构、WAL 读写改用新类型；在边缘调用处用 `From/TryFrom` 过渡。
4) 调整溢出判断与默认值一致性（生成器起始值与 `Default` 对齐）。
5) 删除或标记弃用旧接口，补充文档与 changelog。

## 测试与验证
- 单元测试
  - `CommitTs::try_from` 与 `TxnId::try_from` 域校验。
  - 生成器溢出边界、`update_if_greater` 单调性。
  - `global_*` 单例行为。
  - 旧 `Timestamp` 与新类型互转的兼容测试。
- 集成/端到端
  - 事务读写流程使用新类型仍能通过现有 e2e 测试。
  - 重启/恢复（如有 WAL）后时间戳不回退。

## 风险与缓解
- 调用层改动面较广：采用过渡层与编译警告逐步推进。
- 序列化兼容：使用原始 `u64` 存档，加载时强校验，避免旧数据导致跨域。
- 溢出与上界：修正 off-by-one，增加日志/指标便于观测。

## 开放问题
- 是否保留最高位区分 txn-id/commit-ts，还是完全分区间？当前方案沿用最高位分域，便于兼容旧数据。
- 生成器持久化位置：是放在 catalog 元数据、WAL 末尾，还是独立状态文件？需结合现有存储设计决定。
