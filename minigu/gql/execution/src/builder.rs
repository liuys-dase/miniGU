use std::sync::Arc;

use arrow::array::{AsArray, Int32Array};
use minigu_catalog::label_set::LabelSet;
use minigu_catalog::provider::GraphTypeProvider;
use minigu_common::data_chunk::DataChunk;
use minigu_common::data_type::{DataField, DataSchema, LogicalType};
use minigu_common::types::VertexIdArray;
use minigu_context::error::Error as ContextError;
use minigu_context::graph::{GraphContainer, GraphReadSession};
use minigu_context::session::SessionContext;
use minigu_planner::bound::{BoundExpr, BoundExprKind};
use minigu_planner::plan::{PlanData, PlanNode};

use crate::evaluator::BoxedEvaluator;
use crate::evaluator::column_ref::ColumnRef;
use crate::evaluator::constant::Constant;
use crate::evaluator::vector_distance::VectorDistanceEvaluator;
use crate::evaluator::vertex_constructor::VertexConstructor;
use crate::executor::catalog_modify::{CreateGraphBuilder, DropGraphBuilder};
use crate::executor::create_vector_index::CreateVectorIndexBuilder;
use crate::executor::drop_vector_index::DropVectorIndexBuilder;
use crate::executor::join::JoinCond;
use crate::executor::procedure_call::ProcedureCallBuilder;
use crate::executor::sort::SortSpec;
use crate::executor::query_read::QueryReadExecutor;
use crate::executor::vector_index_scan::VectorIndexScanBuilder;
use crate::executor::{BoxedExecutor, Executor, IntoExecutor};
use crate::source::VertexSource;

#[cfg(test)]
static GRAPH_READ_SESSION_OPEN_COUNT_FOR_TEST: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub struct ExecutorBuilder {
    session: SessionContext,
    graph_read_session: Option<GraphReadSession>,
}

impl ExecutorBuilder {
    pub fn new(session: SessionContext) -> Self {
        Self {
            session,
            graph_read_session: None,
        }
    }

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
        #[cfg(test)]
        GRAPH_READ_SESSION_OPEN_COUNT_FOR_TEST.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Some(read_session))
    }

    fn graph_read_session(&self) -> GraphReadSession {
        self.graph_read_session
            .clone()
            .expect("graph read session should be initialized for graph read plans")
    }

    fn build_executor(&self, physical_plan: &PlanNode) -> BoxedExecutor {
        let children = physical_plan.children();
        match physical_plan {
            PlanNode::PhysicalFilter(filter) => {
                assert_eq!(children.len(), 1);
                let schema = children[0].schema().expect("child should have a schema");
                let predicate = self.build_evaluator(&filter.predicate, schema);
                Box::new(self.build_executor(&children[0]).filter(move |c| {
                    predicate
                        .evaluate(c)
                        .map(|a| a.into_array().as_boolean().clone())
                }))
            }
            PlanNode::PhysicalNodeScan(node_scan) => {
                // NodeScan provide graph id and label, Handle in next pr.
                assert_eq!(children.len(), 0);
                let config = self.session.database().config();
                let read_session = self.graph_read_session();
                let batches = read_session
                    .vertex_source(
                        &Some(node_scan.labels.clone()),
                        config.execution.vertex_scan_batch_size,
                    )
                    .expect("failed to create vertex source");
                let source = batches.map(|arr: Arc<VertexIdArray>| Ok(arr));
                Box::new(source.scan_vertex())
            }
            PlanNode::PhysicalExpand(expand) => {
                assert_eq!(children.len(), 1);
                let child = self.build_executor(&children[0]);
                let container: Arc<GraphContainer> = self
                    .session
                    .current_graph
                    .clone()
                    .expect("current graph should be set")
                    .object()
                    .clone()
                    .downcast_arc::<GraphContainer>()
                    .expect("failed to downcast to GraphContainer");

                // Inject batch sizes from config into the container
                let config = self.session.database().config();
                container.set_expand_batch_size(config.execution.expand_batch_size);
                container.set_adjacency_batch_size(config.storage.batch_size);

                // Get the number of columns before expand
                let child_schema = children[0].schema().expect("child should have a schema");
                let num_child_columns = child_schema.fields().len();

                // Expand adds new columns (as ListArray) that need to be flattened.
                // ExpandSource returns 2 columns: edge IDs and target vertex IDs
                // We need to flatten both columns at indices num_child_columns and
                // num_child_columns + 1
                let expand_executor = child.expand(
                    expand.input_column_index,
                    Some(expand.edge_labels.clone()),
                    expand.target_vertex_labels.clone(),
                    self.graph_read_session(),
                );
                let column_indices_to_flatten: Vec<usize> =
                    (num_child_columns..num_child_columns + 2).collect();
                Box::new(expand_executor.flatten(column_indices_to_flatten))
            }
            PlanNode::PhysicalProject(project) => {
                assert_eq!(children.len(), 1);
                let child_schema = children[0].schema().expect("child should have a schema");
                let mut child_executor = self.build_executor(&children[0]);
                let output_schema = physical_plan.schema().expect("there should be a schema");

                let mut updated_schema = child_schema.clone();

                // Check if any expression is a Vertex type that needs properties
                // If output type is Vertex, we need to scan properties
                for expr in &project.exprs {
                    if let LogicalType::Vertex(_) = &expr.logical_type
                        && let BoundExprKind::Variable(var_name) = &expr.kind
                    {
                        // Check child schema to see if this variable only has id (Int64)
                        let child_field = child_schema
                            .get_field_by_name(var_name)
                            .expect("variable should be present in child schema");

                        // If child schema only has id (Int64), need to add VertexPropertyScan
                        if matches!(child_field.ty(), LogicalType::Int64) {
                            let vid_index = child_schema
                                .get_field_index_by_name(var_name)
                                .expect("variable should be present in child schema");

                            let container: Arc<GraphContainer> = self
                                .session
                                .current_graph
                                .clone()
                                .expect("current graph should be set")
                                .object()
                                .clone()
                                .downcast_arc::<GraphContainer>()
                                .expect("failed to downcast to GraphContainer");

                            let mut property_names = Vec::new();
                            let property_list = if let Some(label_specs) =
                                output_schema.get_var_label(var_name.as_str())
                            {
                                let graph_type = container.graph_type();
                                let mut property_ids = Vec::new();
                                if let Some(first_label_set) = label_specs.first()
                                    && let Ok(Some(vertex_type)) = graph_type.get_vertex_type(
                                        &LabelSet::from_iter(first_label_set.clone()),
                                    )
                                {
                                    for property in vertex_type.properties().iter() {
                                        property_ids.push(property.0);
                                        property_names.push(property.1.name().to_string());
                                    }
                                }

                                property_ids
                            } else {
                                Vec::new()
                            };

                            child_executor = Box::new(child_executor.scan_vertex_property(
                                vid_index,
                                property_list.clone(),
                                self.graph_read_session(),
                            ));

                            // Format: {var_name}_{prop_name} to handle cases where multiple
                            // variables
                            let mut new_fields = updated_schema.fields().to_vec();
                            for prop_name in property_names.iter() {
                                let qualified_name = format!("{}_{}", var_name, prop_name);
                                new_fields.push(DataField::new(
                                    qualified_name,
                                    LogicalType::String,
                                    true,
                                ));
                            }
                            updated_schema = Arc::new(DataSchema::new(new_fields));
                        }
                    }
                }

                // Build evaluators with updated schema
                let evaluators = project
                    .exprs
                    .iter()
                    .map(|e| self.build_evaluator(e, &updated_schema))
                    .collect();
                Box::new(child_executor.project(evaluators))
            }
            PlanNode::PhysicalCall(call) => {
                assert!(children.is_empty());
                let procedure = call.procedure.object().clone();
                let session = self.session.clone();
                let args = call.args.clone();
                Box::new(ProcedureCallBuilder::new(procedure, session, args).into_executor())
            }
            // We don't need an independent executor for PhysicalOneRow. Returning a chunk with a
            // single row is enough.
            PlanNode::PhysicalOneRow(one_row) => {
                assert!(children.is_empty());
                let schema = &one_row.schema().expect("one_row should have a data schema");
                assert_eq!(schema.fields().len(), 1);
                let field = &schema.fields()[0];
                assert_eq!(field.ty(), &LogicalType::Int32);
                assert!(!field.is_nullable());
                let columns = vec![Arc::new(Int32Array::from_iter_values([0])) as _];
                let chunk = DataChunk::new(columns);
                Box::new([Ok(chunk)].into_executor())
            }
            PlanNode::PhysicalSort(sort) => {
                assert_eq!(children.len(), 1);
                let schema = children[0].schema().expect("child should have a schema");
                let specs = sort
                    .specs
                    .iter()
                    .map(|s| {
                        let key = self.build_evaluator(&s.key, schema);
                        SortSpec::new(key, s.ordering, s.null_ordering)
                    })
                    .collect();
                let chunk_size = self.session.database().config().execution.sort_chunk_size;
                Box::new(self.build_executor(&children[0]).sort(specs, chunk_size))
            }
            PlanNode::PhysicalLimit(limit) => {
                assert_eq!(children.len(), 1);
                Box::new(self.build_executor(&children[0]).limit(limit.limit))
            }
            PlanNode::PhysicalOffset(offset) => {
                assert_eq!(children.len(), 1);
                Box::new(self.build_executor(&children[0]).offset(offset.offset))
            }
            PlanNode::PhysicalVectorIndexScan(vector_scan) => {
                assert_eq!(children.len(), 1);
                let child_schema = children[0].schema().expect("child should have a schema");
                let binding_column_index = child_schema
                    .get_field_index_by_name(&vector_scan.binding)
                    .expect("binding column should exist in child schema");
                let child_executor = self.build_executor(&children[0]);
                VectorIndexScanBuilder::new(
                    self.graph_read_session(),
                    vector_scan.clone(),
                    child_executor,
                    binding_column_index,
                )
                .into_executor()
            }
            PlanNode::PhysicalHashJoin(join) => {
                assert_eq!(children.len(), 2);
                let left_executor = self.build_executor(&children[0]);
                let right_executor = self.build_executor(&children[1]);
                let left_schema = children[0].schema().expect("left schema");
                let right_schema = children[1].schema().expect("right schema");
                let conds = join
                    .conds
                    .iter()
                    .map(|cond| {
                        let left_key = self.build_evaluator(&cond.left_key, left_schema);
                        let right_key = self.build_evaluator(&cond.right_key, right_schema);
                        JoinCond::new(left_key, right_key)
                    })
                    .collect();
                Box::new(left_executor.join(right_executor, conds))
            }
            PlanNode::PhysicalVertexPropertyFetch(fetch) => {
                assert_eq!(children.len(), 1);
                let child_executor = self.build_executor(&children[0]);
                let binding_idx = children[0]
                    .schema()
                    .expect("child schema should exist")
                    .get_field_index_by_name(&fetch.binding)
                    .expect("binding column should exist");
                Box::new(child_executor.scan_vertex_property(
                    binding_idx,
                    fetch.property_ids.clone(),
                    self.graph_read_session(),
                ))
            }
            PlanNode::PhysicalExplain(explain) => {
                let explain_str = explain.explain(0).unwrap_or_default();
                let lines: Vec<&str> = explain_str.lines().collect();
                let string_array = arrow::array::StringArray::from_iter_values(lines);
                let columns = vec![Arc::new(string_array) as _];
                let chunk = DataChunk::new(columns);
                Box::new([Ok(chunk)].into_executor())
            }
            PlanNode::PhysicalCreateVectorIndex(create_index) => {
                assert!(children.is_empty());
                CreateVectorIndexBuilder::new(self.session.clone(), create_index.clone())
                    .into_executor()
            }
            PlanNode::PhysicalDropVectorIndex(drop_index) => {
                assert!(children.is_empty());
                DropVectorIndexBuilder::new(self.session.clone(), drop_index.clone())
                    .into_executor()
            }
            PlanNode::PhysicalCreateGraph(create_graph) => {
                assert!(children.is_empty());
                let plan = (**create_graph).clone();
                let session = self.session.clone();
                Box::new(CreateGraphBuilder::new(plan, session).into_executor())
            }
            PlanNode::PhysicalDropGraph(drop_graph) => {
                assert!(children.is_empty());
                let plan = (**drop_graph).clone();
                let session = self.session.clone();
                Box::new(DropGraphBuilder::new(plan, session).into_executor())
            }
            _ => unreachable!(),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn build_evaluator(&self, expr: &BoundExpr, schema: &DataSchema) -> BoxedEvaluator {
        match &expr.kind {
            BoundExprKind::Value(value) => Box::new(Constant::new(value.clone())),
            BoundExprKind::Variable(variable) => {
                // Check if this is a Vertex type that needs to be constructed
                if let LogicalType::Vertex(vertex_fields) = &expr.logical_type {
                    // Find the vertex ID column
                    let vid_index = schema
                        .get_field_index_by_name(variable)
                        .expect("variable should be present in the schema");

                    let label_specs = schema.get_var_label(variable);

                    // Find property columns by their names: {var_name}_{prop_name}
                    let mut property_column_indices = Vec::new();
                    let mut property_names = Vec::new();
                    for field in vertex_fields {
                        let prop_name = field.name();
                        // Look for qualified name: {variable}_{prop_name}
                        let qualified_name = format!("{}_{}", variable, prop_name);
                        if let Some(prop_idx) = schema.get_field_index_by_name(&qualified_name) {
                            property_column_indices.push(prop_idx);
                            property_names.push(prop_name.to_string());
                        }
                    }

                    return Box::new(VertexConstructor::new(
                        vid_index,
                        property_column_indices,
                        property_names,
                        label_specs,
                    ));
                }

                // Default: just return the column reference
                let index = schema
                    .get_field_index_by_name(variable)
                    .expect("variable should be present in the schema");
                Box::new(ColumnRef::new(index))
            }
            BoundExprKind::Property {
                binding, property, ..
            } => {
                // Prefer qualified column name `{binding}_{property}`
                let qualified = format!("{}_{}", binding, property);
                let index = schema
                    .get_field_index_by_name(&qualified)
                    .expect("property column should be present in the schema");
                Box::new(ColumnRef::new(index))
            }
            BoundExprKind::VectorDistance {
                lhs,
                rhs,
                metric,
                dimension,
            } => {
                let lhs = self.build_evaluator(lhs.as_ref(), schema);
                let rhs = self.build_evaluator(rhs.as_ref(), schema);
                Box::new(VectorDistanceEvaluator::new(lhs, rhs, *metric, *dimension))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use minigu_catalog::label_set::LabelSet;
    use minigu_catalog::memory::directory::MemoryDirectoryCatalog;
    use minigu_catalog::memory::graph_type::{
        MemoryEdgeTypeCatalog, MemoryGraphTypeCatalog, MemoryVertexTypeCatalog,
    };
    use minigu_catalog::memory::MemoryCatalog;
    use minigu_catalog::property::Property;
    use minigu_catalog::provider::DirectoryOrSchema;
    use minigu_catalog::provider::DirectoryProvider;
    use minigu_common::data_type::LogicalType;
    use minigu_common::types::{LabelId, PropertyId};
    use minigu_common::value::ScalarValue;
    use minigu_context::database::{DatabaseConfig, DatabaseContext};
    use minigu_context::graph::{GraphContainer, GraphStorage};
    use minigu_context::runtime::DatabaseRuntime;
    use minigu_context::session::SessionContext;
    use minigu_planner::plan::expand::{Expand, ExpandDirection};
    use minigu_planner::plan::property_fetch::{PropertyOutput, VertexPropertyFetch};
    use minigu_planner::plan::scan::NodeIdScan;
    use minigu_planner::plan::PlanNode;
    use minigu_storage::common::{Edge, PropertyRecord, Vertex};
    use minigu_storage::tp::MemoryGraph;
    use minigu_transaction::{GraphTxnManager, IsolationLevel, Transaction};

    use super::*;

    fn reset_graph_read_session_open_count() {
        GRAPH_READ_SESSION_OPEN_COUNT_FOR_TEST.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    fn graph_read_session_open_count() -> usize {
        GRAPH_READ_SESSION_OPEN_COUNT_FOR_TEST.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn build_test_session() -> SessionContext {
        let graph = MemoryGraph::in_memory();
        let mut graph_type = MemoryGraphTypeCatalog::new();
        let person_label = graph_type.add_label("PERSON".to_string()).unwrap();
        let friend_label = graph_type.add_label("FRIEND".to_string()).unwrap();

        let person_label_set: LabelSet = vec![person_label].into_iter().collect();
        let person_vt = Arc::new(MemoryVertexTypeCatalog::new(
            person_label_set.clone(),
            vec![Property::new("age".to_string(), LogicalType::Int8, false)],
        ));
        let friend_label_set: LabelSet = vec![friend_label].into_iter().collect();
        let friend_et = Arc::new(MemoryEdgeTypeCatalog::new(
            friend_label_set.clone(),
            person_vt.clone(),
            person_vt.clone(),
            vec![],
        ));
        graph_type.add_vertex_type(person_label_set, person_vt);
        graph_type.add_edge_type(friend_label_set, friend_et);

        let container = Arc::new(GraphContainer::new(
            Arc::new(graph_type),
            GraphStorage::Memory(Arc::clone(&graph)),
        ));

        // Populate vertices and edges
        let txn = graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();
        for i in 1u64..=3u64 {
            let v = Vertex::new(
                i,
                person_label,
                PropertyRecord::new(vec![ScalarValue::Int8(Some(21 + i as i8))]),
            );
            graph.create_vertex(&txn, v).unwrap();
        }
        // 1 -> 2, 1 -> 3
        for (eid, src, dst) in [(1, 1, 2), (2, 1, 3)] {
            let e = Edge::new(
                eid,
                src,
                dst,
                friend_label,
                PropertyRecord::new(vec![]),
            );
            graph.create_edge(&txn, e).unwrap();
        }
        txn.commit().unwrap();

        let dir: DirectoryOrSchema =
            (Arc::new(MemoryDirectoryCatalog::new(None)) as Arc<dyn DirectoryProvider>).into();
        let catalog = MemoryCatalog::new(dir);
        let runtime = DatabaseRuntime::new(1).expect("runtime should be constructable");
        let db = Arc::new(DatabaseContext::new(
            catalog,
            runtime,
            DatabaseConfig::default(),
        ));
        let mut session = SessionContext::new(db);
        session.current_graph = Some(minigu_catalog::named_ref::NamedGraphRef::new(
            "g".into(),
            container,
        ));
        session
    }

    #[test]
    fn builder_opens_exactly_one_read_session_for_scan_expand_fetch() {
        reset_graph_read_session_open_count();
        let session = build_test_session();

        let person = LabelId::new(1).unwrap();
        let friend = LabelId::new(2).unwrap();
        let scan = PlanNode::PhysicalNodeScan(Arc::new(NodeIdScan::new(
            "a",
            vec![vec![person]],
        )));
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
            vec![PropertyId::from(0u32)],
            vec![PropertyOutput {
                column_alias: "b_age".to_string(),
                ty: LogicalType::Int8,
                nullable: false,
            }],
        )));

        let executor = ExecutorBuilder::new(session).build(&plan);
        let chunks = executor
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(!chunks.is_empty(), "should produce at least one chunk");
        assert_eq!(
            graph_read_session_open_count(),
            1,
            "should open exactly one graph read session"
        );
    }
}
