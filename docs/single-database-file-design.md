# MiniGU 数据库单文件化设计方案

## 背景与问题
- Issue #121 目标：将当前分散的检查点文件与 WAL 日志整合为一个“数据库文件”，便于分发、备份和未来扩展的持久化内容。
- 现状（代码参考：`minigu/storage/src/common/wal/graph_wal.rs`，`minigu/storage/src/tp/checkpoint.rs`）：
  - WAL：默认路径为当前工作目录下的 `.wal` 单文件，记录格式为 `[len|crc32|payload]`，payload 以 `postcard` 序列化 `RedoEntry`。
  - 检查点：默认目录 `.checkpoint/`，文件名 `checkpoint_<uuid>.bin`，文件内部同样以长度 + 校验 + `postcard` 序列化整图快照；默认最多保留 5 份，自动间隔 30s。
  - 恢复流程：加载最新检查点（若存在），再回放 LSN 大于等于检查点 LSN 的 WAL 记录；创建检查点后会调用 `WalManager::truncate_until` 将旧 WAL 截断。
- 缺口：存储碎片化（目录 + 文件）、原子迁移/复制成本高、缺少统一的格式版本管理和未来扩展位（如统计/索引快照），不利于“一份文件代表一个数据库实例”的目标。

## 业界调研（单文件或近似单文件的最佳实践）
- SQLite（page-based 单文件 + 可选 WAL/SHM 辅助文件）  
  - 页式布局 + 文件头版本号；WAL 模式将写入先追加到 `dbname-wal`，checkpoint 时合并回主文件并可 VACUUM 压缩。  
  - 参考：SQLite File Format / WAL Mode 文档（https://www.sqlite.org/fileformat.html，https://www.sqlite.org/wal.html）。
- DuckDB（主文件单一 `.duckdb`，外置 WAL，checkpoint 合并）  
  - 追加式存储，checkpoint 将 WAL 合并并写新的元数据尾部，使用双元数据指针保证崩溃一致性。  
  - 参考：DuckDB Storage/Checkpoints 文档（https://duckdb.org/docs/internals/storage.html，https://duckdb.org/docs/internals/checkpoints_and_wal.html）。
- LMDB（单数据文件 + 锁文件）  
  - 基于 mmap 的 COW 页式存储，双元数据页交替写入保证崩溃安全；锁文件仅用于并发控制。  
  - 参考：LMDB Internals（https://symas.com/lmdb/internals/）。
- sled（Rust）  
  - 日志结构化存储，追加段 + 物化索引，周期性快照/压缩，将“日志 + manifest”保存在同一数据文件路径附近。  
  - 参考：sled architecture notes（https://sled.rs/docs/design/）。
- （对照）LevelDB/RocksDB  
  - 多文件（SST + MANIFEST + WAL）便于分层压缩，但缺少“单文件即可迁移”的能力；MANIFEST 记录版本化元数据的思想可借鉴。

### 可借鉴的模式
- 以“超级块/元数据块”双写或尾部写入的方式，提供单点入口和版本校验（SQLite 头 + DuckDB 双元数据指针 + LMDB 双 meta page）。
- “追加 + 清单（manifest）”策略：WAL/检查点段只追加，新元数据记录在清单表，旧段通过 retention + vacuum 清理（DuckDB、sled）。
- 明确页大小/对齐规则、强校验（CRC）和格式版本号，便于后向兼容与检测部分写入。

## 设计目标
- 单文件：默认产生一个 `minigu.db`（可配置），内部包含检查点、WAL 及后续扩展段。
- 崩溃一致性：采用双超级块 + CRC 校验，保证最新有效版本可被识别和恢复。
- 追加友好：尽量追加写，避免频繁重写整文件；提供按段截断与 vacuum。
- 可扩展：格式包含段类型与版本号，便于未来加入统计、索引、向量快照等。
- 兼容迁移：支持从现有 `.checkpoint/` + `.wal` 导入；可通过配置切换新旧格式。

## 方案概述
### 文件容器格式（草案）
- 超级块（Superblock，固定大小，双份：文件头与文件尾各一份）  
  - 字段：magic `MNGUDB00`、格式版本、上次安全写入的 manifest 偏移、块大小（默认 4 KiB）、校验（CRC32/64）、写入 epoch（单调递增）、选项标志（压缩/加密预留位）。  
  - 双写规则：先写新的 manifest，再写尾部超级块，最后写头部超级块，崩溃恢复时选取校验通过且 epoch 最大的超级块。
- Manifest（追加式的段目录）  
  - 记录当前活跃段的偏移/长度/类型/LSN 范围/创建时间/校验。  
  - 段类型：`Checkpoint`、`WalSegment`、`Meta`、`FreeList`、`Reserved`。  
  - 每次生成新检查点或切换 WAL 段时追加一条 manifest 记录。
- 段（Chunk）封装  
  - 段头：`type` | `version` | `lsn_start` | `lsn_end` | `created_at` | `payload_len` | `payload_crc`.  
  - `Checkpoint` 段 payload：沿用 `GraphCheckpoint` 的 `postcard` 序列化字节流。  
  - `WalSegment` 段 payload：沿用现有 `[len|crc32|payload]` 记录格式，允许多个段串联。

### 读写流程
- 写 WAL：仍使用现有 `RedoEntry` 序列化，目标从单独文件改为定位到“当前活跃 WalSegment”的偏移；WAL 段写满或触发切换（如 checkpoint 后）时，关闭当前段并追加新的空段记录到文件末尾，追加 manifest，刷新双超级块。
- 创建检查点：  
  1) 获取 `checkpoint_lock`，等待事务静默；2) 将内存快照序列化为新 `Checkpoint` 段追加；3) 关闭现有 WAL 段，开始新的 WAL 段（LSN 起点为 checkpoint LSN）；4) 追加 manifest 记录；5) 写双超级块。  
  旧 WAL 段逻辑上可回收（retention），物理清理由 vacuum 完成。
- 恢复：  
  - 读取双超级块，选择校验通过且 epoch 最大者；加载其指向的最新 manifest。  
  - 找到最新有效 `Checkpoint` 段，构造 `MemoryGraph`，再按 manifest 中顺序回放所有 LSN ≥ checkpoint LSN 的 WAL 段记录。  
  - 若文件缺少 manifest 或校验失败，降级尝试旧格式（见兼容策略）。

### 清理与压缩（Vacuum）
- 触发条件：文件膨胀、超过保留检查点数量、空间占用阈值。
- 实现思路：新建临时文件，写入最新检查点 + 活跃 WAL 段 + 精简 manifest + 新超级块，再原子替换（`rename`）。该流程类似 SQLite `VACUUM` 与 DuckDB “合并 WAL”。

### 并发与锁
- 单进程假设下，可复用现有 `checkpoint_lock` 与 WAL 内部锁。若未来支持多进程，需增加文件锁（建议放在轻量级 `.db.lock` 辅助文件或使用 `flock`），避免破坏单文件数据的一致性。

### 兼容与迁移
- 配置项：`StorageFormat = { SingleFile | LegacySplit }`，默认 SingleFile（新实例）；老实例若检测到 `.checkpoint/` + `.wal` 且无 `.db`，自动执行导入：  
  1) 读取最新检查点及 WAL；2) 在内存中恢复；3) 以单文件格式写出首个检查点 + 空 WAL 段；4) 标记迁移完成。  
  - 保留工具命令：`minigu-cli` 增加 `export --legacy`/`import` 子命令，方便显式转换。

### 迭代实现计划（里程碑）
1. 文件格式与读写抽象
   - 定义 `DbFile`/`Superblock`/`ManifestEntry` 数据结构与序列化校验。
   - 将 WAL writer/reader 抽象成基于 offset 的 I/O，复用现有编码格式。
2. Checkpoint/WAL 管理改造
   - `CheckpointManager` 写入单文件段，保留原有 retention 逻辑；`WalManager` 支持段切换与 truncate 逻辑（通过 manifest 而非直接截断文件）。
   - 恢复路径更新：从单文件读取 manifest，选择最新检查点 + WAL 段回放。
3. Vacuum 与工具
   - 提供 `vacuum`/`compact` API 与 CLI；实现旧格式导入/导出。
4. 测试与验证
   - 单元测试：超级块校验、manifest 解析、WAL/Checkpoint 段读写一致性。  
   - 集成测试：崩溃场景（中途崩溃后恢复）、多次 checkpoint + WAL 回放、vacuum 后的数据比对。  
   - 兼容测试：从旧目录格式导入生成单文件，再回放等价性。

### 风险与缓解
- 文件持续追加导致膨胀：引入阈值触发的 vacuum；manifest 记录逻辑删除的段。
- 崩溃时超级块撰写失败：双超级块 + CRC 校验 + epoch 选择保证可恢复到上一个稳定点。
- 未来加密/压缩需求：在超级块标志位预留，压缩可基于段级别（如 WAL 段启用 LZ4）。

## 下一步建议
- 确认格式字段与默认路径（建议 `minigu.db`），以及是否需要单独锁文件。
- 先实现无压缩/无加密的最小可用版本，完成迁移工具与基础恢复测试，再考虑 vacuum 与高级特性。
