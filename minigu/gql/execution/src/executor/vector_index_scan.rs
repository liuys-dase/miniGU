use std::convert::TryFrom;
use std::io;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float32Array, UInt64Array};
use arrow::datatypes::UInt64Type;
use minigu_common::data_chunk::DataChunk;
use minigu_context::graph::GraphReadSession;
use minigu_planner::plan::vector_index_scan::VectorIndexScan;
use minigu_storage::error::{StorageError, VectorIndexError};
use minigu_storage::tp::MemoryGraph;

use super::{BoxedExecutor, Executor};
use crate::error::{ExecutionError, ExecutionResult};

/// Default L parameter for DiskANN search
const DEFAULT_L_VALUE: u32 = 100;

pub struct VectorIndexScanBuilder {
    read_session: GraphReadSession,
    plan: Arc<VectorIndexScan>,
    child: BoxedExecutor,
    binding_column_index: usize,
}

impl VectorIndexScanBuilder {
    pub fn new(
        read_session: GraphReadSession,
        plan: Arc<VectorIndexScan>,
        child: BoxedExecutor,
        binding_column_index: usize,
    ) -> Self {
        Self {
            read_session,
            plan,
            child,
            binding_column_index,
        }
    }

    pub fn into_executor(self) -> BoxedExecutor {
        Box::new(VectorIndexScanExecutor {
            read_session: self.read_session,
            plan: self.plan,
            child: Some(self.child),
            binding_column_index: self.binding_column_index,
            finished: false,
        })
    }
}

pub struct VectorIndexScanExecutor {
    read_session: GraphReadSession,
    plan: Arc<VectorIndexScan>,
    child: Option<BoxedExecutor>,
    binding_column_index: usize,
    finished: bool,
}

impl Executor for VectorIndexScanExecutor {
    fn next_chunk(&mut self) -> Option<ExecutionResult<DataChunk>> {
        if self.finished {
            return None;
        }
        self.finished = true;
        Some(self.execute_scan())
    }
}

impl VectorIndexScanExecutor {
    fn execute_scan(&mut self) -> ExecutionResult<DataChunk> {
        let candidate_bitmap = self.consume_child_bitmap()?;
        if self.plan.limit == 0 {
            return Ok(Self::empty_result_chunk());
        }
        let candidate_bitmap = match candidate_bitmap {
            CandidateBitmap::Empty => return Ok(Self::empty_result_chunk()),
            CandidateBitmap::Filter(bitmap) => Some(bitmap),
        };
        let graph = Arc::clone(self.read_session.graph());
        let snapshot_bitmap = self.build_snapshot_filter_bitmap(candidate_bitmap)?;
        self.scan_with_graph(graph.as_ref(), snapshot_bitmap)
    }

    fn build_snapshot_filter_bitmap(
        &self,
        candidate_bitmap: Option<BooleanArray>,
    ) -> ExecutionResult<Option<BooleanArray>> {
        let txn = Arc::clone(self.read_session.txn());
        let visible_vertices = txn
            .iter_vertices()
            .map(|result| {
                result
                    .map(|vertex| vertex.vid())
                    .map_err(ExecutionError::from)
            })
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

    fn scan_with_graph(
        &self,
        graph: &MemoryGraph,
        candidate_bitmap: Option<BooleanArray>,
    ) -> ExecutionResult<DataChunk> {
        // TODO(minigu-vector-search): support parameter/column vector expressions once binder
        // permits.
        let query_scalar = self.plan.query.clone().evaluate_scalar().ok_or_else(|| {
            ExecutionError::Custom(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "query vector must be a constant expression",
            )))
        })?;
        let vector_value = query_scalar.get_vector().map_err(|_| {
            ExecutionError::Custom(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "failed to extract vector from scalar value",
            )))
        })?;
        if vector_value.dimension() != self.plan.dimension {
            return Err(
                StorageError::VectorIndex(VectorIndexError::InvalidDimension {
                    expected: self.plan.dimension,
                    actual: vector_value.dimension(),
                })
                .into(),
            );
        }

        let l_value = DEFAULT_L_VALUE.max(self.plan.limit as u32);
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

        // [node_id, distance]
        let (vertex_ids, distances): (Vec<u64>, Vec<f32>) = results.into_iter().unzip();
        let mut columns: Vec<ArrayRef> = Vec::new();
        let id_array: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(vertex_ids.iter().copied()));
        let distance_array: ArrayRef =
            Arc::new(Float32Array::from_iter_values(distances.iter().copied()));
        columns.push(id_array);
        columns.push(distance_array);

        Ok(DataChunk::new(columns))
    }
}

impl VectorIndexScanExecutor {
    fn empty_result_chunk() -> DataChunk {
        let id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values(std::iter::empty::<u64>()));
        let distance_array: ArrayRef =
            Arc::new(Float32Array::from_iter_values(std::iter::empty::<f32>()));
        DataChunk::new(vec![id_array, distance_array])
    }

    fn consume_child_bitmap(&mut self) -> ExecutionResult<CandidateBitmap> {
        let child = match self.child.take() {
            Some(child) => child,
            None => {
                return Err(ExecutionError::Custom(Box::new(io::Error::other(
                    "vector index scan child executor has already been consumed",
                ))));
            }
        };

        let mut candidate_indices: Vec<usize> = Vec::new();
        let mut max_index: Option<usize> = None;

        for chunk in child.into_iter() {
            let chunk = chunk?;
            if chunk.is_empty() {
                continue;
            }
            let column = chunk
                .columns()
                .get(self.binding_column_index)
                .ok_or_else(|| {
                    ExecutionError::Custom(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "binding column missing from child output",
                    )))
                })?;
            let column = column.as_primitive::<UInt64Type>();
            for row in 0..column.len() {
                if column.is_null(row) {
                    continue;
                }
                let node_id = column.value(row);
                let idx = usize::try_from(node_id).map_err(|_| {
                    ExecutionError::Custom(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("vertex id {node_id} exceeds usize range"),
                    )))
                })?;
                if let Some(current_max) = max_index.as_mut() {
                    if idx > *current_max {
                        *current_max = idx;
                    }
                } else {
                    max_index = Some(idx);
                }
                candidate_indices.push(idx);
            }
        }

        let Some(max_index) = max_index else {
            return Ok(CandidateBitmap::Empty);
        };
        let bitmap_len = max_index.checked_add(1).ok_or_else(|| {
            ExecutionError::Custom(Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                "vertex id overflow while building candidate bitmap",
            )))
        })?;
        let mut filter_bits = vec![false; bitmap_len];
        for idx in candidate_indices {
            filter_bits[idx] = true;
        }

        Ok(CandidateBitmap::Filter(BooleanArray::from(filter_bits)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::BooleanArray;
    use minigu_catalog::memory::graph_type::MemoryGraphTypeCatalog;
    use minigu_common::IsolationLevel;
    use minigu_common::types::{LabelId, PropertyId, VectorIndexKey, VectorMetric, VertexId};
    use minigu_common::value::ScalarValue;
    use minigu_context::graph::{GraphContainer, GraphStorage};
    use minigu_planner::bound::{BoundExpr, BoundExprKind};
    use minigu_planner::plan::PlanBase;
    use minigu_planner::plan::vector_index_scan::VectorIndexScan;
    use minigu_storage::common::{PropertyRecord, Vertex};
    use minigu_storage::tp::MemoryGraph;

    use super::*;

    fn build_read_session_with_vertices() -> GraphReadSession {
        let graph = MemoryGraph::in_memory();
        let graph_type = Arc::new(MemoryGraphTypeCatalog::new());
        let container = Arc::new(GraphContainer::new(
            graph_type,
            GraphStorage::Memory(Arc::clone(&graph)),
        ));

        let label = LabelId::new(1).unwrap();
        let txn = graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();
        for i in 0u64..3u64 {
            let v = Vertex::new(
                VertexId::from(i),
                label,
                PropertyRecord::new(vec![ScalarValue::Int32(Some(i as i32))]),
            );
            graph.create_vertex(&txn, v).unwrap();
        }
        txn.commit().unwrap();

        container.open_read_session().unwrap()
    }

    #[test]
    fn build_snapshot_filter_bitmap_excludes_post_snapshot_inserted_vertex() {
        let read_session = build_read_session_with_vertices();

        // Insert vertex 3 in a write txn committed after the read session started
        let graph = Arc::clone(read_session.graph());
        let write_txn = graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();
        let label = LabelId::new(1).unwrap();
        let new_vertex = Vertex::new(
            VertexId::from(3u64),
            label,
            PropertyRecord::new(vec![ScalarValue::Int32(Some(999))]),
        );
        graph.create_vertex(&write_txn, new_vertex).unwrap();
        write_txn.commit().unwrap();

        let dummy_plan = Arc::new(VectorIndexScan {
            base: PlanBase::new(None, vec![]),
            binding: "v".into(),
            distance_alias: "dist".into(),
            index_key: VectorIndexKey::new(LabelId::new(1).unwrap(), PropertyId::from(0u32)),
            query: BoundExpr {
                kind: BoundExprKind::Value(ScalarValue::Null),
                logical_type: minigu_common::data_type::LogicalType::Float32,
                nullable: true,
            },
            metric: VectorMetric::L2,
            dimension: 1,
            limit: 10,
            approximate: false,
        });

        let executor = VectorIndexScanExecutor {
            read_session,
            plan: dummy_plan,
            child: None,
            binding_column_index: 0,
            finished: false,
        };

        // Candidate bitmap covers vertices 0, 1, 2, 3 (vertex 3 was added after snapshot)
        let candidate = Some(BooleanArray::from(vec![true, true, true, true]));
        let result = executor
            .build_snapshot_filter_bitmap(candidate)
            .unwrap()
            .expect("should return a bitmap");

        assert_eq!(result.len(), 4);
        assert!(result.value(0), "vertex 0 should be visible");
        assert!(result.value(1), "vertex 1 should be visible");
        assert!(result.value(2), "vertex 2 should be visible");
        assert!(
            !result.value(3),
            "vertex 3 was inserted after snapshot — must be excluded"
        );
    }
}

enum CandidateBitmap {
    Empty,
    Filter(BooleanArray),
}
