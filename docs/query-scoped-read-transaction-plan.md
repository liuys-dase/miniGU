# Query-Scoped Graph Read Transaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure every graph read operator in one query reads from one consistent storage snapshot instead of opening independent storage transactions per operator.

**Architecture:** Add a query-scoped `GraphReadSession` that owns one `Arc<MemTransaction>` for the current graph, pass that session through scan/expand/property/vector-read paths, and wrap the root executor so the read transaction is committed or aborted exactly once. Keep the change narrow: no label index implementation, no physical storage rewrite, no planner rewrite.

**Tech Stack:** Rust, miniGU workspace crates (`minigu-context`, `minigu-execution`, `minigu-storage`, `minigu-transaction`), MVCC `MemoryGraph`, `Arc`, existing `Executor` trait, cargo tests.

---

## 1. Problem Statement

miniGU storage already exposes MVCC reads through `MemTransaction`, but execution currently creates separate storage transactions in independent read operators:

- `GraphContainer::vertex_source` starts a transaction for `NodeScan`.
- `GraphContainer::expand_from_vertex` starts a transaction for every expand source.
- `GraphContainer::scan_vertex_properties` starts a transaction for property fetch.
- `VectorIndexScanExecutor::execute_scan` starts another transaction before vector search.

That means one logical query can read vertex IDs from snapshot `S1`, edges from snapshot `S2`, properties from snapshot `S3`, and vector candidates from snapshot `S4`. The result can be a graph pattern that never existed in any single committed graph state.

The required fix is statement-level snapshot consistency: one query gets one read transaction, and all graph read APIs use that transaction.

## 2. Priority Decision

This plan implements the P0 correctness issue:

`storage/execution: bind graph reads to a single query transaction snapshot`

It intentionally does not implement the P1 performance issue:

`storage: add label-aware vertex scan and adjacency expansion access paths`

The P1 work should be implemented after this plan because future APIs such as `iter_vertices_by_label(txn, labels)` and `iter_adjacency_by_label(txn, vid, labels)` should accept the same query transaction introduced here.

## 3. Current Code Map

These are the relevant files before implementation:

- `minigu/context/src/graph.rs`
  - Defines `GraphStorage` and `GraphContainer`.
  - `GraphContainer::vertex_source` currently creates a transaction internally.
  - Implements helper `vertex_has_all_labels`.
  - Implements vector index catalog operations.

- `minigu/gql/execution/src/source/expand_source.rs`
  - Implements `ExpandSource for GraphContainer`.
  - `expand_from_vertex` currently creates a transaction internally.

- `minigu/gql/execution/src/source/property_scan_source.rs`
  - Implements `VertexPropertySource for GraphContainer`.
  - `scan_vertex_properties` currently creates a transaction internally.

- `minigu/gql/execution/src/executor/vector_index_scan.rs`
  - `VectorIndexScanExecutor::execute_scan` currently creates a transaction internally.

- `minigu/gql/execution/src/builder.rs`
  - `ExecutorBuilder` recursively builds executors.
  - It passes `GraphContainer` directly into scan/expand/property paths.

- `minigu/gql/execution/src/executor/mod.rs`
  - Exports executor modules and common executor traits.

- `minigu/core/src/session.rs`
  - `Session::handle_procedure` builds and drains the executor.

## 4. Target Design

### 4.1 New Query Read Object

Create `GraphReadSession` in `minigu/context/src/graph.rs`.

Responsibilities:

- Hold the `Arc<GraphContainer>` for the current graph.
- Hold the `Arc<MemoryGraph>` resolved from `GraphStorage::Memory`.
- Hold one `Arc<MemTransaction>`.
- Expose `txn()`, `graph()`, and `container()` accessors.
- Expose `commit()` and `abort()` for the root executor wrapper.
- Provide graph read helper methods that use the owned transaction.

Use `IsolationLevel::Snapshot` for the query read transaction. A read-only statement should see a stable snapshot and should not fail at the end only because another transaction committed a write after the snapshot started.

### 4.2 Source Trait Implementations Move to GraphReadSession

Keep existing trait definitions:

- `VertexPropertySource`
- `ExpandSource`

Add implementations for `GraphReadSession`.

Remove production `GraphContainer` implementations of `ExpandSource` and `VertexPropertySource` after migrating tests. Keeping only `GraphReadSession` implementations gives compile-time protection against accidentally reintroducing hidden transactions in the execution builder.

### 4.3 Root Executor Owns Transaction Finalization

Create a root wrapper executor in `minigu/gql/execution/src/executor/query_read.rs`.

Responsibilities:

- Own the inner `BoxedExecutor`.
- Own one optional `GraphReadSession`.
- Forward `next_chunk`.
- If inner returns `None`, commit the read transaction and finish.
- If inner returns `Err`, abort the read transaction and return the original error.
- If dropped before finishing, abort the read transaction.

This avoids relying on `MemTransaction::drop`, which currently aborts unhandled transactions.

### 4.4 ExecutorBuilder Opens One Read Session

Modify `ExecutorBuilder::build` so it opens a `GraphReadSession` once when the physical plan contains graph read operators.

Graph read operators for this plan:

- `PhysicalNodeScan`
- `PhysicalExpand`
- `PhysicalVectorIndexScan`
- Any `PhysicalProject` path that needs `scan_vertex_property`

The simplest robust implementation is:

- Traverse `PlanNode` before building.
- Return `true` if any node requires graph reads.
- If true, resolve the current graph and create `GraphReadSession`.
- Pass clones of that session into graph read sources.
- Wrap the root executor with `QueryReadExecutor`.

Do not use the query read session for graph-modifying commands such as `PhysicalCreateVectorIndex`, `PhysicalDropVectorIndex`, or catalog-modify executors.

## 5. Implementation Tasks

### Task 1: Add `GraphReadSession`

**Files:**

- Modify: `minigu/context/src/graph.rs`

- [ ] **Step 1: Add imports**

Add these imports near the existing imports in `minigu/context/src/graph.rs`:

```rust
use minigu_storage::tp::transaction::MemTransaction;
use minigu_transaction::{GraphTxnManager, IsolationLevel, Transaction};
```

If `GraphTxnManager` and `IsolationLevel` are already imported in the file, only add `Transaction`.

- [ ] **Step 2: Add the struct**

Insert this after `pub enum GraphStorage`:

```rust
#[derive(Clone)]
pub struct GraphReadSession {
    container: Arc<GraphContainer>,
    graph: Arc<MemoryGraph>,
    txn: Arc<MemTransaction>,
}
```

- [ ] **Step 3: Add the implementation**

Insert this after `impl GraphContainer { pub fn new... }` or immediately before it:

```rust
impl GraphReadSession {
    pub fn new(container: Arc<GraphContainer>, isolation: IsolationLevel) -> StorageResult<Self> {
        let graph = match container.graph_storage() {
            GraphStorage::Memory(graph) => Arc::clone(graph),
        };
        let txn = graph.txn_manager().begin_transaction(isolation)?;
        Ok(Self {
            container,
            graph,
            txn,
        })
    }

    #[inline]
    pub fn container(&self) -> &Arc<GraphContainer> {
        &self.container
    }

    #[inline]
    pub fn graph(&self) -> &Arc<MemoryGraph> {
        &self.graph
    }

    #[inline]
    pub fn txn(&self) -> &Arc<MemTransaction> {
        &self.txn
    }

    pub fn commit(&self) -> StorageResult<()> {
        self.txn.commit().map(|_| ())
    }

    pub fn abort(&self) -> StorageResult<()> {
        self.txn.abort()
    }
}
```

- [ ] **Step 4: Add container helper**

Inside `impl GraphContainer`, add:

```rust
pub fn open_read_session(self: &Arc<Self>) -> StorageResult<GraphReadSession> {
    GraphReadSession::new(Arc::clone(self), IsolationLevel::Snapshot)
}
```

This method must take `self: &Arc<Self>` so the read session can own an `Arc<GraphContainer>` without cloning the container internals.

- [ ] **Step 5: Compile context crate**

Run:

```bash
cargo check -p minigu-context
```

Expected result:

```text
Finished `dev` profile ...
```

If the compiler reports duplicate imports, remove the duplicate import and rerun the command.

### Task 2: Add Transaction-Explicit Vertex Source

**Files:**

- Modify: `minigu/context/src/graph.rs`

- [ ] **Step 1: Extract the existing vertex source logic**

Keep the existing `GraphContainer::vertex_source` behavior, but move the actual scan into a transaction-explicit helper.

Add this method to `impl GraphContainer`:

```rust
pub fn vertex_source_with_txn(
    &self,
    txn: &Arc<MemTransaction>,
    label_ids: &Option<Vec<Vec<LabelId>>>,
    batch_size: usize,
) -> StorageResult<Box<dyn Iterator<Item = Arc<VertexIdArray>> + Send + 'static>> {
    let mem = match self.graph_storage() {
        GraphStorage::Memory(m) => Arc::clone(m),
    };
    let mut ids: Vec<u64> = Vec::new();
    {
        let it = mem.iter_vertices(txn)?;
        for v in it {
            let v = v?;
            let vid = v.vid();
            if vertex_has_all_labels(&mem, txn, vid, label_ids)? {
                ids.push(vid);
            }
        }
    }

    ids.sort_unstable();

    assert!(
        batch_size > 0,
        "vertex source batch size must be greater than 0"
    );
    let mut pos = 0usize;
    let iter = std::iter::from_fn(move || {
        if pos >= ids.len() {
            return None;
        }
        let end = (pos + batch_size).min(ids.len());
        let slice = &ids[pos..end];
        pos = end;
        Some(Arc::new(VertexIdArray::from_iter(slice.iter().copied())))
    });

    Ok(Box::new(iter))
}
```

- [ ] **Step 2: Reimplement the old method through the helper**

Replace the body of `GraphContainer::vertex_source` with:

```rust
let mem = match self.graph_storage() {
    GraphStorage::Memory(m) => Arc::clone(m),
};
let txn = mem
    .txn_manager()
    .begin_transaction(IsolationLevel::Snapshot)?;
let result = self.vertex_source_with_txn(&txn, label_ids, batch_size);
match result {
    Ok(iter) => {
        txn.commit()?;
        Ok(iter)
    }
    Err(err) => {
        let _ = txn.abort();
        Err(err)
    }
}
```

This keeps existing callers working while making the new execution path transaction-explicit.

- [ ] **Step 3: Add `GraphReadSession` vertex source helper**

Add this to `impl GraphReadSession`:

```rust
pub fn vertex_source(
    &self,
    label_ids: &Option<Vec<Vec<LabelId>>>,
    batch_size: usize,
) -> StorageResult<Box<dyn Iterator<Item = Arc<VertexIdArray>> + Send + 'static>> {
    self.container.vertex_source_with_txn(&self.txn, label_ids, batch_size)
}
```

- [ ] **Step 4: Compile context crate**

Run:

```bash
cargo check -p minigu-context
```

Expected result:

```text
Finished `dev` profile ...
```

### Task 3: Implement `ExpandSource` for `GraphReadSession`

**Files:**

- Modify: `minigu/gql/execution/src/source/expand_source.rs`

- [ ] **Step 1: Update imports**

Replace:

```rust
use minigu_context::graph::{GraphContainer, GraphStorage};
```

with:

```rust
use minigu_context::graph::{GraphContainer, GraphReadSession, GraphStorage};
```

- [ ] **Step 2: Keep iterator lifetime ownership explicit**

Keep the existing `GraphExpandIter` owner fields:

```rust
_graph_storage: GraphStorage,
_txn: Arc<minigu_storage::tp::transaction::MemTransaction>,
```

The new `GraphReadSession` implementation will populate these fields with the query transaction. Keeping the existing fields avoids a broad test rewrite and keeps standalone source tests from dropping the transaction while the iterator is consumed.

- [ ] **Step 3: Add helper function**

Add this private helper above the trait implementations:

```rust
fn expand_from_read_session(
    read_session: GraphReadSession,
    vertex: VertexId,
    edge_labels: Option<Vec<Vec<LabelId>>>,
    target_vertex_labels: Option<Vec<Vec<LabelId>>>,
) -> Option<GraphExpandIter> {
    let mem = Arc::clone(read_session.graph());
    let txn = Arc::clone(read_session.txn());

    if mem.get_vertex(&txn, vertex).is_err() {
        return None;
    }

    let mut neighbors = Vec::new();
    let adj_batch_size = read_session.container().adjacency_batch_size();
    let mut adj_iter = txn.iter_adjacency_outgoing(vertex, adj_batch_size);

    if let Some(labels) = &edge_labels {
        use std::collections::HashSet;
        let allowed_labels: HashSet<LabelId> = labels.iter().flatten().copied().collect();
        adj_iter = AdjacencyIteratorTrait::filter(adj_iter, move |neighbor| {
            allowed_labels.contains(&neighbor.label_id())
        });
    }

    for neighbor_result in adj_iter {
        match neighbor_result {
            Ok(neighbor) => {
                if let Some(target_labels) = &target_vertex_labels {
                    match mem.get_vertex(&txn, neighbor.neighbor_id()) {
                        Ok(neighbor_vertex) => {
                            let neighbor_label = neighbor_vertex.label_id;
                            let mut matches = false;
                            for and_labels in target_labels {
                                if and_labels.is_empty() {
                                    matches = true;
                                    break;
                                }
                                if and_labels.contains(&neighbor_label) {
                                    matches = true;
                                    break;
                                }
                            }
                            if !matches {
                                continue;
                            }
                        }
                        Err(_) => continue,
                    }
                }
                neighbors.push(neighbor);
            }
            Err(_) => continue,
        }
    }

    let expand_batch_size = read_session.container().expand_batch_size();
    Some(GraphExpandIter {
        neighbors,
        offset: 0,
        batch_size: expand_batch_size,
        _graph_storage: GraphStorage::Memory(Arc::clone(read_session.graph())),
        _txn: txn,
    })
}
```

- [ ] **Step 4: Implement `ExpandSource for GraphReadSession`**

Add:

```rust
impl ExpandSource for GraphReadSession {
    type ExpandIter = GraphExpandIter;

    fn expand_from_vertex(
        &self,
        vertex: VertexId,
        edge_labels: Option<Vec<Vec<LabelId>>>,
        target_vertex_labels: Option<Vec<Vec<LabelId>>>,
    ) -> Option<Self::ExpandIter> {
        expand_from_read_session(self.clone(), vertex, edge_labels, target_vertex_labels)
    }
}
```

- [ ] **Step 5: Remove production `ExpandSource for GraphContainer`**

After the `GraphReadSession` implementation compiles, migrate tests in this file to call `container.open_read_session().unwrap()` and invoke `read_session.expand_from_vertex(...)`. Then delete the old `impl ExpandSource for GraphContainer` block.

This makes accidental production fallback impossible: if `ExecutorBuilder` passes `GraphContainer` into `.expand(...)`, the code will fail to compile because `GraphContainer` no longer implements `ExpandSource`.

- [ ] **Step 6: Compile execution crate**

Run:

```bash
cargo check -p minigu-execution
```

Expected result:

```text
Finished `dev` profile ...
```

### Task 4: Implement `VertexPropertySource` for `GraphReadSession`

**Files:**

- Modify: `minigu/gql/execution/src/source/property_scan_source.rs`

- [ ] **Step 1: Update imports**

Replace:

```rust
use minigu_context::graph::{GraphContainer, GraphStorage};
```

with:

```rust
use minigu_context::graph::{GraphContainer, GraphReadSession, GraphStorage};
```

- [ ] **Step 2: Extract property scan helper**

Move the body of the current implementation into a helper that accepts `&GraphReadSession`:

```rust
fn scan_vertex_properties_with_read_session(
    read_session: &GraphReadSession,
    vertices: &VertexIdArray,
    property_list: &[PropertyId],
) -> ExecutionResult<Vec<ArrayRef>> {
    let mem = Arc::clone(read_session.graph());
    let txn = Arc::clone(read_session.txn());

    let property_list = if property_list.is_empty() {
        if let Some(&first_vid) = vertices.values().first() {
            let sample_vertex = mem
                .get_vertex(&txn, first_vid)
                .map_err(|e| ExecutionError::Custom(Box::new(e)))?;
            let num_properties = sample_vertex.properties().len();
            (0..num_properties as u32).collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::from(property_list)
    };

    let mut results = Vec::new();

    for prop_id in property_list.iter() {
        let idx = *prop_id as usize;
        let mut values = Vec::new();

        for vid in vertices.values().iter().copied() {
            let v = mem
                .get_vertex(&txn, vid)
                .map_err(|e| ExecutionError::Custom(Box::new(e)))?;
            if v.is_tombstone {
                values.push(ScalarValue::Null);
            } else {
                let sv = v.properties.get(idx).unwrap_or(&ScalarValue::Null);
                values.push(sv.clone());
            }
        }

        let array_ref = scalar_values_to_array(values);
        results.push(array_ref);
    }

    Ok(results)
}
```

The helper above references `scalar_values_to_array`. Add this helper below the existing `convert_scalar_values_to_array!` macro:

```rust
fn scalar_values_to_array(values: Vec<ScalarValue>) -> ArrayRef {
    let sample_value = values
        .iter()
        .find(|v| !matches!(v, ScalarValue::Null))
        .unwrap_or(&ScalarValue::Null);

    match sample_value {
        ScalarValue::Int8(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::Int8, Int8Array, i8)
        }
        ScalarValue::Int16(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::Int16, Int16Array, i16)
        }
        ScalarValue::Int32(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::Int32, Int32Array, i32)
        }
        ScalarValue::Int64(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::Int64, Int64Array, i64)
        }
        ScalarValue::UInt8(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::UInt8, UInt8Array, u8)
        }
        ScalarValue::UInt16(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::UInt16, UInt16Array, u16)
        }
        ScalarValue::UInt32(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::UInt32, UInt32Array, u32)
        }
        ScalarValue::UInt64(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::UInt64, UInt64Array, u64)
        }
        ScalarValue::Float32(_) => {
            convert_scalar_values_to_array!(
                values,
                ScalarValue::Float32,
                Float32Array,
                f32,
                |f| f.into_inner()
            )
        }
        ScalarValue::Float64(_) => {
            convert_scalar_values_to_array!(
                values,
                ScalarValue::Float64,
                Float64Array,
                f64,
                |f| f.into_inner()
            )
        }
        ScalarValue::Boolean(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::Boolean, BooleanArray, bool)
        }
        ScalarValue::String(_) => {
            convert_scalar_values_to_array!(values, ScalarValue::String, StringArray, String)
        }
        ScalarValue::Vector { dimension, .. } => {
            let elem_field = Arc::new(Field::new("item", DataType::Float32, false));
            let list_size = *dimension as i32;
            let mut flat: Vec<f32> = Vec::with_capacity(values.len() * (*dimension));
            let mut nulls = NullBufferBuilder::new(values.len());
            let mut has_null = false;
            for value in values.iter() {
                match value {
                    ScalarValue::Vector {
                        value: Some(vector_value),
                        ..
                    } => {
                        flat.extend(vector_value.to_f32_vec().into_iter());
                        nulls.append_non_null();
                    }
                    ScalarValue::Vector { .. } | ScalarValue::Null => {
                        flat.extend(std::iter::repeat_n(0.0, *dimension));
                        nulls.append_null();
                        has_null = true;
                    }
                    _ => {
                        flat.extend(std::iter::repeat_n(0.0, *dimension));
                        nulls.append_null();
                        has_null = true;
                    }
                }
            }
            let values_array = Arc::new(Float32Array::from(flat));
            let null_buffer = if has_null {
                Some(
                    nulls
                        .finish()
                        .expect("vector null buffer should build successfully"),
                )
            } else {
                None
            };
            Arc::new(FixedSizeListArray::new(
                elem_field,
                list_size,
                values_array,
                null_buffer,
            )) as ArrayRef
        }
        ScalarValue::Null => {
            Arc::new(Int64Array::from(vec![None::<i64>; values.len()])) as ArrayRef
        }
        _ => Arc::new(Int64Array::from(vec![None::<i64>; values.len()])) as ArrayRef,
    }
}
```

- [ ] **Step 3: Add `GraphReadSession` implementation**

Add:

```rust
impl VertexPropertySource for GraphReadSession {
    fn scan_vertex_properties(
        &self,
        vertices: &VertexIdArray,
        property_list: &[PropertyId],
    ) -> ExecutionResult<Vec<ArrayRef>> {
        scan_vertex_properties_with_read_session(self, vertices, property_list)
    }
}
```

- [ ] **Step 4: Remove production `VertexPropertySource for GraphContainer`**

After adding `impl VertexPropertySource for GraphReadSession`, migrate property source tests to create a read session:

```rust
let container = Arc::new(create_test_graph_container());
create_test_vertices_with_properties(&container);
let read_session = container.open_read_session().unwrap();
let results = read_session
    .scan_vertex_properties(&vertices, &property_list)
    .unwrap();
read_session.commit().unwrap();
```

Then delete the old `impl VertexPropertySource for GraphContainer` block.

This gives a compile-time guard: any future production call that passes `GraphContainer` to `.scan_vertex_property(...)` will fail to compile instead of silently opening a hidden transaction.

- [ ] **Step 5: Compile execution crate**

Run:

```bash
cargo check -p minigu-execution
```

Expected result:

```text
Finished `dev` profile ...
```

### Task 5: Add Root `QueryReadExecutor`

**Files:**

- Create: `minigu/gql/execution/src/executor/query_read.rs`
- Modify: `minigu/gql/execution/src/executor/mod.rs`

- [ ] **Step 1: Create module file**

Create `minigu/gql/execution/src/executor/query_read.rs` with:

```rust
use minigu_common::data_chunk::DataChunk;
use minigu_context::graph::GraphReadSession;

use super::{BoxedExecutor, Executor};
use crate::error::{ExecutionError, ExecutionResult};

pub struct QueryReadExecutor {
    inner: BoxedExecutor,
    read_session: Option<GraphReadSession>,
    finished: bool,
}

impl QueryReadExecutor {
    pub fn new(inner: BoxedExecutor, read_session: Option<GraphReadSession>) -> Self {
        Self {
            inner,
            read_session,
            finished: false,
        }
    }

    fn finish_success(&mut self) -> ExecutionResult<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        if let Some(read_session) = self.read_session.take() {
            read_session.commit().map_err(ExecutionError::from)?;
        }
        Ok(())
    }

    fn finish_error(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(read_session) = self.read_session.take() {
            let _ = read_session.abort();
        }
    }
}

impl Executor for QueryReadExecutor {
    fn next_chunk(&mut self) -> Option<ExecutionResult<DataChunk>> {
        match self.inner.next_chunk() {
            Some(Ok(chunk)) => Some(Ok(chunk)),
            Some(Err(err)) => {
                self.finish_error();
                Some(Err(err))
            }
            None => match self.finish_success() {
                Ok(()) => None,
                Err(err) => Some(Err(err)),
            },
        }
    }
}

impl Drop for QueryReadExecutor {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_error();
        }
    }
}
```

- [ ] **Step 2: Export the module**

Add this line to `minigu/gql/execution/src/executor/mod.rs`:

```rust
pub mod query_read;
```

- [ ] **Step 3: Compile execution crate**

Run:

```bash
cargo check -p minigu-execution
```

Expected result:

```text
Finished `dev` profile ...
```

### Task 6: Thread `GraphReadSession` Through `ExecutorBuilder`

**Files:**

- Modify: `minigu/gql/execution/src/builder.rs`

- [ ] **Step 1: Add imports**

Add:

```rust
use minigu_context::error::Error as ContextError;
use minigu_context::graph::{GraphReadSession, GraphStorage};
use crate::executor::query_read::QueryReadExecutor;
```

Merge with existing `GraphContainer` imports instead of duplicating them.

- [ ] **Step 2: Add field to `ExecutorBuilder`**

Change:

```rust
pub struct ExecutorBuilder {
    session: SessionContext,
}
```

to:

```rust
pub struct ExecutorBuilder {
    session: SessionContext,
    graph_read_session: Option<GraphReadSession>,
}
```

- [ ] **Step 3: Update constructor**

Change `ExecutorBuilder::new` to:

```rust
pub fn new(session: SessionContext) -> Self {
    Self {
        session,
        graph_read_session: None,
    }
}
```

- [ ] **Step 4: Add plan traversal**

Add this helper inside `impl ExecutorBuilder`:

```rust
fn plan_needs_graph_read(plan: &PlanNode) -> bool {
    match plan {
        PlanNode::PhysicalNodeScan(_)
        | PlanNode::PhysicalExpand(_)
        | PlanNode::PhysicalVectorIndexScan(_)
        | PlanNode::PhysicalVertexPropertyFetch(_) => true,
        PlanNode::PhysicalCreateVectorIndex(_)
        | PlanNode::PhysicalDropVectorIndex(_)
        | PlanNode::PhysicalCreateGraph(_)
        | PlanNode::PhysicalDropGraph(_) => false,
        _ => plan.children().iter().any(Self::plan_needs_graph_read),
    }
}
```

These variant names must match `minigu/gql/planner/src/plan/mod.rs`: `PhysicalVertexPropertyFetch`, `PhysicalCreateGraph`, and `PhysicalDropGraph` exist; `PhysicalCatalogModify` does not.

- [ ] **Step 5: Add read session opener**

Add:

```rust
fn open_graph_read_session(&self) -> Result<Option<GraphReadSession>, ContextError> {
    let Some(graph_ref) = self.session.current_graph.clone() else {
        return Ok(None);
    };
    let provider = graph_ref.object().clone();
    let container = provider
        .downcast_arc::<GraphContainer>()
        .map_err(|_| ContextError::Internal("only in-memory graph reads are supported".into()))?;
    let read_session = container
        .open_read_session()
        .map_err(|e| ContextError::Internal(e.to_string()))?;
    Ok(Some(read_session))
}
```

- [ ] **Step 6: Update `build`**

Change:

```rust
pub fn build(self, plan: &PlanNode) -> BoxedExecutor {
    self.build_executor(plan)
}
```

to:

```rust
pub fn build(mut self, plan: &PlanNode) -> BoxedExecutor {
    if Self::plan_needs_graph_read(plan) {
        self.graph_read_session = self
            .open_graph_read_session()
            .expect("failed to open graph read session");
    }
    let executor = self.build_executor(plan);
    if self.graph_read_session.is_some() {
        Box::new(QueryReadExecutor::new(
            executor,
            self.graph_read_session.clone(),
        ))
    } else {
        executor
    }
}
```

- [ ] **Step 7: Add accessor**

Add:

```rust
fn graph_read_session(&self) -> GraphReadSession {
    self.graph_read_session
        .clone()
        .expect("graph read session should be initialized for graph read plans")
}
```

- [ ] **Step 8: Update `PhysicalNodeScan`**

In the `PlanNode::PhysicalNodeScan` arm, replace direct `GraphContainer` source usage:

```rust
let batches = container
    .vertex_source(
        &Some(node_scan.labels.clone()),
        config.execution.vertex_scan_batch_size,
    )
    .expect("failed to create vertex source");
```

with:

```rust
let read_session = self.graph_read_session();
let batches = read_session
    .vertex_source(
        &Some(node_scan.labels.clone()),
        config.execution.vertex_scan_batch_size,
    )
    .expect("failed to create vertex source");
```

The surrounding `container` resolution can be removed from this arm if it is no longer used.

- [ ] **Step 9: Update `PhysicalExpand`**

In the `PlanNode::PhysicalExpand` arm:

1. Keep resolving `container` only for setting batch sizes.
2. Pass `self.graph_read_session()` to `.expand(...)` instead of `container`.

The final call should look like:

```rust
let expand_executor = child.expand(
    expand.input_column_index,
    Some(expand.edge_labels.clone()),
    expand.target_vertex_labels.clone(),
    self.graph_read_session(),
);
```

- [ ] **Step 10: Update property fetch paths**

There are two places in `builder.rs` that call `.scan_vertex_property(..., container)`.

Replace the source argument with `self.graph_read_session()`:

```rust
child_executor = Box::new(child_executor.scan_vertex_property(
    vid_index,
    property_list.clone(),
    self.graph_read_session(),
));
```

and:

```rust
Box::new(child_executor.scan_vertex_property(
    fetch.input_column_index,
    fetch.property_ids.clone(),
    self.graph_read_session(),
))
```

Keep `container` resolution where it is still needed for catalog lookup of property IDs.

- [ ] **Step 11: Update vector index scan builder construction**

When constructing `VectorIndexScanBuilder`, pass `self.graph_read_session()` into the builder after Task 7 changes that builder signature.

- [ ] **Step 12: Compile execution crate**

Run:

```bash
cargo check -p minigu-execution
```

Expected result:

```text
Finished `dev` profile ...
```

### Task 7: Update Vector Index Scan to Reuse Query Snapshot

**Files:**

- Modify: `minigu/gql/execution/src/executor/vector_index_scan.rs`

- [ ] **Step 1: Change builder fields**

Replace `session_context: SessionContext` in `VectorIndexScanBuilder` and `VectorIndexScanExecutor` with:

```rust
read_session: GraphReadSession,
```

Update imports:

```rust
use minigu_context::graph::GraphReadSession;
```

Remove `SessionContext`, `GraphContainer`, `GraphStorage`, `GraphTxnManager`, `IsolationLevel`, and `Transaction` imports if they become unused.

- [ ] **Step 2: Change constructor**

Change `VectorIndexScanBuilder::new` signature to:

```rust
pub fn new(
    read_session: GraphReadSession,
    plan: Arc<VectorIndexScan>,
    child: BoxedExecutor,
    binding_column_index: usize,
) -> Self
```

Set `read_session` in both builder and executor.

- [ ] **Step 3: Remove internal transaction creation**

Replace this block in `execute_scan`:

```rust
let graph = self.resolve_memory_graph()?;
let txn = graph
    .txn_manager()
    .begin_transaction(IsolationLevel::Snapshot)
    .map_err(ExecutionError::from)?;

let result = self.scan_with_graph(graph.as_ref(), candidate_bitmap);
match result {
    Ok(chunk) => {
        txn.commit().map_err(ExecutionError::from)?;
        Ok(chunk)
    }
    Err(err) => {
        let _ = txn.abort();
        Err(err)
    }
}
```

with:

```rust
let graph = Arc::clone(self.read_session.graph());
self.scan_with_graph(graph.as_ref(), candidate_bitmap)
```

Delete `resolve_memory_graph`.

- [ ] **Step 4: Build a snapshot-visible vector filter before search**

Do not call `vector_search(k)` and then filter invisible IDs after the fact. That leaks no invisible IDs, but it can return fewer than `k` visible neighbors because invisible top-k candidates consumed slots before filtering. Instead, build a bitmap of vertices visible to the query snapshot and merge it with the existing child candidate bitmap before calling `MemoryGraph::vector_search`.

Add this helper to `impl VectorIndexScanExecutor`:

```rust
fn build_snapshot_filter_bitmap(
    &self,
    candidate_bitmap: Option<BooleanArray>,
) -> ExecutionResult<Option<BooleanArray>> {
    let txn = Arc::clone(self.read_session.txn());
    let visible_vertices = txn
        .iter_vertices()
        .map(|result| result.map(|vertex| vertex.vid()).map_err(ExecutionError::from))
        .collect::<ExecutionResult<Vec<_>>>()?;
    let visible_indices = visible_vertices
        .into_iter()
        .map(|vertex_id| {
            usize::try_from(vertex_id).map_err(|_| {
                ExecutionError::Custom(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("vertex id {vertex_id} exceeds usize range"),
                )))
            })
        })
        .collect::<ExecutionResult<Vec<_>>>()?;

    if visible_indices.is_empty() {
        return Ok(Some(BooleanArray::from(Vec::<bool>::new())));
    }

    let max_visible_id = visible_indices.iter().copied().max().unwrap_or(0);
    let bitmap_len = candidate_bitmap
        .as_ref()
        .map(|bitmap| bitmap.len())
        .unwrap_or(0)
        .max(max_visible_id + 1);

    let mut bits = vec![false; bitmap_len];
    for idx in visible_indices {
        let allowed_by_child = candidate_bitmap
            .as_ref()
            .map(|bitmap| idx < bitmap.len() && bitmap.value(idx))
            .unwrap_or(true);
        if allowed_by_child && idx < bits.len() {
            bits[idx] = true;
        }
    }

    Ok(Some(BooleanArray::from(bits)))
}
```

Then change `execute_scan` so `candidate_bitmap` is merged before search:

```rust
let graph = Arc::clone(self.read_session.graph());
let snapshot_bitmap = self.build_snapshot_filter_bitmap(candidate_bitmap)?;
self.scan_with_graph(graph.as_ref(), snapshot_bitmap)
```

Inside `scan_with_graph`, keep passing the merged bitmap to `graph.vector_search(...)`:

```rust
let filter_bitmap = candidate_bitmap.as_ref();
let results = graph
    .vector_search(
        self.plan.index_key,
        &vector_value,
        self.plan.limit,
        l_value,
        filter_bitmap,
        self.plan.approximate,
    )
    .map_err(ExecutionError::from)?;
```

This preserves top-k semantics over the visible candidate set because `MemoryGraph::vector_search` already accepts `filter_bitmap: Option<&BooleanArray>`.

- [ ] **Step 5: Update builder call site**

In `minigu/gql/execution/src/builder.rs`, change the `VectorIndexScanBuilder::new(...)` call to pass `self.graph_read_session()` as the first argument.

- [ ] **Step 6: Compile execution crate**

Run:

```bash
cargo check -p minigu-execution
```

Expected result:

```text
Finished `dev` profile ...
```

### Task 8: Add Source-Level Snapshot Regression Tests

**Files:**

- Modify: `minigu/gql/execution/src/source/expand_source.rs`
- Modify: `minigu/gql/execution/src/source/property_scan_source.rs`

- [ ] **Step 1: Add vertex source snapshot test in `property_scan_source.rs`**

The `property_scan_source.rs` test module already has `create_test_graph_container`, `create_test_vertices_with_properties`, `PERSON_LABEL_ID`, `GraphStorage`, `Vertex`, `PropertyRecord`, and `ScalarValue`. Add this test to that module:

```rust
#[test]
fn graph_read_session_vertex_source_uses_original_snapshot_after_insert() {
    let container = Arc::new(create_test_graph_container());
    create_test_vertices_with_properties(&container);
    let read = container.open_read_session().unwrap();

    let mem = match container.graph_storage() {
        GraphStorage::Memory(mem) => Arc::clone(mem),
    };
    let write_txn = mem
        .txn_manager()
        .begin_transaction(IsolationLevel::Serializable)
        .unwrap();
    let later_vertex = Vertex::new(
        VertexId::from(99u64),
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![
            ScalarValue::Int32(Some(99)),
            ScalarValue::Int64(Some(9900)),
            ScalarValue::Float32(Some(ordered_float::OrderedFloat(9.9))),
            ScalarValue::Float64(Some(ordered_float::OrderedFloat(99.0))),
            ScalarValue::Boolean(Some(true)),
        ]),
    );
    mem.create_vertex(&write_txn, later_vertex).unwrap();
    write_txn.commit().unwrap();

    let batches = read.vertex_source(&None, 64).unwrap().collect::<Vec<_>>();
    let ids = batches
        .iter()
        .flat_map(|array| array.values().iter().copied())
        .collect::<Vec<_>>();

    assert_eq!(ids, vec![0, 1, 2]);
    assert!(!ids.contains(&99));
    read.commit().unwrap();
}
```

- [ ] **Step 2: Add expand snapshot test in `expand_source.rs`**

Add this helper to the existing `expand_source.rs` test module:

```rust
fn insert_committed_edge(container: &GraphContainer, eid: u64, src: u64, dst: u64) {
    let mem = match container.graph_storage() {
        GraphStorage::Memory(mem) => Arc::clone(mem),
    };
    let txn = mem
        .txn_manager()
        .begin_transaction(IsolationLevel::Serializable)
        .unwrap();
    let friend_label_id = LabelId::new(1).unwrap();
    let edge = Edge::new(
        eid,
        src,
        dst,
        friend_label_id,
        PropertyRecord::new(vec![ScalarValue::Int32(Some(999))]),
    );
    mem.create_edge(&txn, edge).unwrap();
    txn.commit().unwrap();
}
```

Add this test to the same module:

```rust
#[test]
fn graph_read_session_expand_uses_original_snapshot_after_edge_insert() {
    let container = Arc::new(create_test_graph());
    setup_test_data(&container);
    let read = container.open_read_session().unwrap();

    insert_committed_edge(&container, 999, 1, 4);

    let mut neighbor_ids = Vec::new();
    let expand_iter = read
        .expand_from_vertex(1, None, None)
        .expect("vertex 1 should exist in the read snapshot");
    for batch in expand_iter {
        let columns = batch.unwrap();
        let ids = columns[1]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("neighbor id column should be UInt64");
        for row in 0..ids.len() {
            neighbor_ids.push(ids.value(row));
        }
    }

    assert_eq!(neighbor_ids, vec![2, 3]);
    assert!(!neighbor_ids.contains(&4));
    read.commit().unwrap();
}
```

If `UInt64Array` is not imported in the test module, add `use arrow::array::UInt64Array;`.

- [ ] **Step 3: Add property snapshot test**

Add this helper to the existing `property_scan_source.rs` test module:

```rust
fn update_committed_vertex0_int32(container: &GraphContainer, new_value: i32) {
    let mem = match container.graph_storage() {
        GraphStorage::Memory(mem) => Arc::clone(mem),
    };
    let txn = mem
        .txn_manager()
        .begin_transaction(IsolationLevel::Serializable)
        .unwrap();
    mem.set_vertex_property(
        &txn,
        VertexId::from(0u64),
        vec![0],
        vec![ScalarValue::Int32(Some(new_value))],
    )
    .unwrap();
    txn.commit().unwrap();
}
```

Add this test to the same module:

```rust
#[test]
fn graph_read_session_property_scan_uses_original_snapshot_after_property_update() {
    let container = Arc::new(create_test_graph_container());
    create_test_vertices_with_properties(&container);
    let read = container.open_read_session().unwrap();

    update_committed_vertex0_int32(&container, 99);

    let vertices = VertexIdArray::from_iter_values([0u64]);
    let columns = read
        .scan_vertex_properties(&vertices, &[PropertyId::from(0u32)])
        .unwrap();
    let values = columns[0].as_any().downcast_ref::<Int32Array>().unwrap();

    assert_eq!(values.value(0), 10);
    read.commit().unwrap();
}
```

- [ ] **Step 4: Run targeted tests**

Run:

```bash
cargo test -p minigu-execution graph_read_session -- --nocapture
```

Expected result:

```text
test result: ok
```

If test names do not all include `graph_read_session`, run the exact module tests:

```bash
cargo test -p minigu-execution source::expand_source -- --nocapture
cargo test -p minigu-execution source::property_scan_source -- --nocapture
```

Expected result for each:

```text
test result: ok
```

### Task 9: Add End-to-End Builder Ownership Test

**Files:**

- Modify: `minigu/gql/execution/src/builder.rs`

- [ ] **Step 1: Add builder-level read-session instrumentation**

Add this near the imports in `builder.rs`:

```rust
#[cfg(test)]
static GRAPH_READ_SESSION_OPEN_COUNT_FOR_TEST: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
```

In `open_graph_read_session`, increment the counter immediately before returning `Ok(Some(read_session))`:

```rust
#[cfg(test)]
GRAPH_READ_SESSION_OPEN_COUNT_FOR_TEST.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
Ok(Some(read_session))
```

Add these test helpers in a `#[cfg(test)] mod tests` in `builder.rs`:

```rust
fn reset_graph_read_session_open_count() {
    GRAPH_READ_SESSION_OPEN_COUNT_FOR_TEST.store(0, std::sync::atomic::Ordering::SeqCst);
}

fn graph_read_session_open_count() -> usize {
    GRAPH_READ_SESSION_OPEN_COUNT_FOR_TEST.load(std::sync::atomic::Ordering::SeqCst)
}
```

- [ ] **Step 2: Add a production-path builder test**

Add a `#[test]` in `builder.rs` that builds a real executor through `ExecutorBuilder::build` for this physical plan shape:

```text
PhysicalVertexPropertyFetch
  PhysicalExpand
    PhysicalNodeScan
```

Use these plan constructors:

```rust
use minigu_common::data_type::LogicalType;
use minigu_common::types::{LabelId, PropertyId};
use minigu_planner::plan::expand::{Expand, ExpandDirection};
use minigu_planner::plan::property_fetch::{PropertyOutput, VertexPropertyFetch};
use minigu_planner::plan::scan::NodeIdScan;

let person = LabelId::new(1).unwrap();
let friend = LabelId::new(1).unwrap();
let scan = PlanNode::PhysicalNodeScan(Arc::new(NodeIdScan::new("a", vec![vec![person]])));
let expand = PlanNode::PhysicalExpand(Arc::new(Expand::new(
    scan,
    0,
    vec![vec![friend]],
    Some(vec![vec![person]]),
    Some("e".to_string()),
    Some("b".to_string()),
    ExpandDirection::Outgoing,
)));
let plan = PlanNode::PhysicalVertexPropertyFetch(Arc::new(VertexPropertyFetch::new(
    expand,
    "b".to_string(),
    vec![PropertyId::from(1u32)],
    vec![PropertyOutput {
        column_alias: "b_age".to_string(),
        ty: LogicalType::Int8,
        nullable: false,
    }],
)));
```

Create a `SessionContext` with a current graph using the same minimal graph setup pattern already used in `expand_source.rs` tests: `MemoryGraph::in_memory()`, `MemoryGraphTypeCatalog`, `GraphContainer`, and `NamedGraphRef`.

The test body should:

```rust
reset_graph_read_session_open_count();
let executor = ExecutorBuilder::new(session_context).build(&plan);
let chunks = executor.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
assert!(!chunks.is_empty());
assert_eq!(graph_read_session_open_count(), 1);
```

This test must call `ExecutorBuilder::build`, not source traits directly. Its purpose is to prove the production builder path opens exactly one query read session for a plan that includes scan, expand, and property fetch.

- [ ] **Step 3: Add compile-time fallback guards**

Because Tasks 3 and 4 remove production `ExpandSource for GraphContainer` and `VertexPropertySource for GraphContainer`, the following production mistakes must fail at compile time:

```rust
child.expand(..., container)
child_executor.scan_vertex_property(..., container)
```

Run:

```bash
cargo check -p minigu-execution
```

Expected result:

```text
Finished `dev` profile ...
```

- [ ] **Step 4: Add no-internal-transaction regression search**

Run:

```bash
rg -n "begin_transaction\\(" minigu/gql/execution/src minigu/context/src/graph.rs
```

Expected remaining matches:

- Write operations such as create/drop vector index.
- `GraphReadSession::new`.
- Compatibility-only `GraphContainer::vertex_source`, if it remains.
- No match inside production `ExpandSource for GraphReadSession`, `VertexPropertySource for GraphReadSession`, or `VectorIndexScanExecutor`.

- [ ] **Step 5: Compile all changed crates**

Run:

```bash
cargo check -p minigu-context -p minigu-execution -p minigu-core
```

Expected result:

```text
Finished `dev` profile ...
```

### Task 10: Full Verification

**Files:**

- No new files.

- [ ] **Step 1: Run storage tests**

Run:

```bash
cargo test -p minigu-storage
```

Expected result:

```text
test result: ok
```

- [ ] **Step 2: Run execution tests**

Run:

```bash
cargo test -p minigu-execution
```

Expected result:

```text
test result: ok
```

- [ ] **Step 3: Run core tests**

Run:

```bash
cargo test -p minigu-core
```

Expected result:

```text
test result: ok
```

- [ ] **Step 4: Run end-to-end miniGU tests**

Run:

```bash
cargo test -p minigu-test
```

Expected result:

```text
test result: ok
```

- [ ] **Step 5: Search for hidden read transactions**

Run:

```bash
rg -n "begin_transaction\\(" minigu/gql/execution/src minigu/context/src/graph.rs
```

Expected result:

- `GraphReadSession::new` is allowed.
- write executors are allowed.
- graph read source methods used by query execution do not create their own transaction.

## 6. Acceptance Criteria

- One query creates at most one storage read transaction for graph reads.
- `NodeScan`, `Expand`, `VertexPropertyScan`, and vector scan reuse the same `GraphReadSession`.
- Source implementations no longer open hidden transactions in the production execution path.
- Read transaction finalization is deterministic: commit on successful executor exhaustion, abort on error or early drop.
- Snapshot tests prove that concurrent writes after query read session creation do not affect scan, expand, or property fetch results.
- Existing storage transaction tests still pass.
- Existing GQL end-to-end tests still pass.

## 7. Design Notes for Future P1 Work

After this P0 lands, implement label-aware access paths on top of the same transaction boundary:

```rust
MemoryGraph::iter_vertices_by_labels(txn, labels, batch_size)
MemoryGraph::iter_adjacency_by_labels(txn, vid, direction, edge_labels, batch_size)
```

Those APIs should reuse `GraphReadSession::txn()` in execution. Do not add a label index API that opens its own transaction.

## 8. Risk Register

- `Serializable` read transaction commits can fail after concurrent writes. This plan uses `Snapshot` for query reads to avoid read-only query failures caused by read-write validation.
- Vector index search is not fully MVCC-aware internally. This plan builds a snapshot-visible `BooleanArray` and passes it into `MemoryGraph::vector_search` before ANN search, preserving top-k semantics over visible candidates. A later vector-index issue can make the index maintain version-aware metadata instead of deriving the bitmap at query time.
- The compatibility `GraphContainer::vertex_source` method can still open its own transaction for non-builder callers. `ExecutorBuilder` must not use it after this plan; expand and property source compatibility impls are removed to make fallback compile-time impossible.
- If an executor is not drained by the caller, `QueryReadExecutor::Drop` aborts the read transaction. This is preferable to leaving the transaction active.

## 9. Suggested Commit Sequence

1. `feat(context): add graph read session`
2. `feat(context): add transaction-explicit vertex source`
3. `feat(execution): route expand and property reads through graph read session`
4. `feat(execution): finalize query read transactions at root executor`
5. `feat(execution): reuse query snapshot in vector index scan`
6. `test(execution): cover graph read snapshot consistency`

## 10. Self-Review Checklist

- [ ] The plan starts by fixing correctness before performance.
- [ ] Every production graph read path has a transaction-explicit variant.
- [ ] The root executor owns transaction finalization.
- [ ] Tests cover scan, expand, and property fetch after concurrent writes.
- [ ] The plan avoids implementing label-aware scan in the same change.
- [ ] The plan filters vector search before ANN top-k selection with a snapshot-visible bitmap.
