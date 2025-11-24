# 事务相关调用点汇总（begin_explicit_txn / commit_explicit_txn / rollback_explicit_txn / with_statement_txn）

本文档按函数分组，列出当前仓库中对 SessionContext 事务相关 API 的所有调用点，包含文件路径、行号范围与调用上下文，并对用途作简要说明。便于统一审查与后续重构。

更新时间：依据 ripgrep 全局搜索结果自动整理（人工核对）。

## begin_explicit_txn

- 文件：`minigu/core/src/session.rs`（`handle_transaction_activity`）
  - 行：约 110–118
  - 代码：`self.context.begin_explicit_txn()?;`
  - 说明：处理 GQL 的 `BEGIN` 事务语句，开启会话级显式事务（遵循会话默认隔离级）。

- 文件：`minigu/catalog/tests/catalog_txn_executor_integration_tests.rs`
  - 行：42、62、83、99
  - 代码：`s2.begin_explicit_txn().unwrap();`
  - 说明：集成测试中用于验证多会话隔离与可见性，开启显式事务。

## commit_explicit_txn

- 文件：`minigu/core/src/session.rs`（`handle_transaction_activity`）
  - 行：约 121–128
  - 代码：`self.context.commit_explicit_txn()?;`
  - 说明：处理 GQL 的 `COMMIT` 事务语句，提交显式事务。

（测试中未直接出现 commit，覆盖在不同路径上由 rollback 测试代替。）

## rollback_explicit_txn

- 文件：`minigu/core/src/session.rs`（`handle_transaction_activity`）
  - 行：约 129–137
  - 代码：`self.context.rollback_explicit_txn()?;`
  - 说明：处理 GQL 的 `ROLLBACK` 事务语句，回滚显式事务。

- 文件：`minigu/catalog/tests/catalog_txn_executor_integration_tests.rs`
  - 行：58、96
  - 代码：`s2.rollback_explicit_txn().ok();`
  - 说明：集成测试中用于结束显式事务并验证快照可见性变更。

## with_statement_txn（语句级事务）

应用层（core / planner / execution）：

- 文件：`minigu/core/src/procedures/create_test_graph_data.rs`
  - 行：48
  - 代码：`context.with_statement_txn(|txn| { schema.add_graph_txn(..., txn) })`
  - 说明：在当前 schema 下注册内存图容器，隐式语句级事务（成功自动提交）。

- 文件：`minigu/core/src/procedures/show_graph.rs`
  - 行：23
  - 代码：`context.with_statement_txn(|txn| Ok(current_schema.graph_names(txn)))`
  - 说明：查询当前 schema 下图名称列表，语句级只读事务。

- 文件：`minigu/core/src/procedures/show_procedures.rs`
  - 行：22
  - 代码：`context.with_statement_txn(|txn| { ... current_schema.get_procedure(name, txn) ... })`
  - 说明：查询当前 schema 下的 procedure 列表，统一创建/提交语句级事务（或复用显式事务）。

- 文件：`minigu/core/src/procedures/export_import/export.rs`
  - 行：266
  - 代码：`context.with_statement_txn(|txn| { schema.get_graph(..., txn) ... })`
  - 说明：导出前解析图对象与图类型。

- 文件：`minigu/core/src/procedures/export_import/export.rs`
  - 行：281
  - 代码：`context.with_statement_txn(|txn| { export(..., txn) })`
  - 说明：执行导出操作，封装为语句级事务（错误回滚）。

- 文件：`minigu/gql/planner/src/lib.rs`（`Planner::plan_query`）
  - 行：约 20–60（最新实现，签名为 `(&mut self, &CatalogTxn, &Procedure)`）
  - 代码：`let binder = Binder::new(..., txn); binder.bind(...); ...`
  - 说明：Planner 不再自行开启事务，直接使用外部传入的 `txn` 完成绑定与生成物理计划。

- 文件：`minigu/gql/execution/src/builder.rs`（`ExecutorBuilder::build_executor`，NodeScan 分支）
  - 行：64
  - 代码：`self.session.with_statement_txn(|txn| { cur_schema.get_graph("test", txn) ... })`
  - 说明：执行阶段按需解析图对象，使用语句级只读事务。

- 文件：`minigu/gql/execution/src/executor/catalog.rs`（多处 DDL 执行）
  - 行：75、130、157、223、330、419 等
  - 代码：`self.session.with_statement_txn(|txn| { ... })`
  - 说明：所有 DDL 修改均在语句级事务中执行（若存在显式事务则复用）。

上下文内部（SessionContext 自身实现中复用）：

- 文件：`minigu/context/src/session.rs`（`set_current_schema`）
  - 行：约 160–216
  - 代码：`self.with_statement_txn(|txn| { ... get_child(..., txn) ... })`
  - 说明：解析并设置当前 schema 时，目录遍历在语句级事务中进行。

- 文件：`minigu/context/src/session.rs`（`set_current_graph`）
  - 行：约 218–247
  - 代码：`self.with_statement_txn(|txn| { schema.get_graph(..., txn) ... })`
  - 说明：解析并设置当前图时，读取在语句级事务中进行。

---

## 备注与建议（最新）

- 显式事务优先级：`with_statement_txn` 会优先复用显式事务（`begin_explicit_txn` 开启），否则创建隐式事务并在闭包成功时自动提交、失败时回滚。
- 隔离级别：隐式事务使用 `SessionContext` 中配置的默认隔离级（未设置则默认 Serializable）。
- 规划/执行阶段事务：已由 `minigu/core/src/session.rs::handle_procedure` 在一次 `with_statement_txn` 中统一包裹规划与执行，确保无显式事务时也在同一快照内完成（显式事务时自动复用）。
