use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use minigu_catalog::memory::graph_type::MemoryGraphTypeCatalog;
use minigu_catalog::provider::{GraphProvider, GraphTypeRef};
use minigu_catalog::txn::CatalogTxnManager;
use minigu_common::IsolationLevel;
use minigu_common::types::{LabelId, VertexIdArray};
use minigu_storage::error::StorageResult;
use minigu_storage::tp::MemoryGraph;
use minigu_transaction::{Transaction, TransactionManager, TxnError};

pub enum GraphStorage {
    Memory(Arc<MemoryGraph>),
}

pub struct GraphContainer {
    graph_type: Arc<MemoryGraphTypeCatalog>,
    graph_storage: GraphStorage,
    txn_mgr: TransactionManager,
}

impl GraphContainer {
    pub fn new(graph_type: Arc<MemoryGraphTypeCatalog>, graph_storage: GraphStorage) -> Self {
        let catalog_txn_mgr = CatalogTxnManager::new();
        let graph = match &graph_storage {
            GraphStorage::Memory(mem) => Arc::clone(mem),
        };
        let txn_mgr =
            TransactionManager::new(graph, Arc::clone(&graph_type), Arc::clone(&catalog_txn_mgr));
        Self {
            graph_type,
            graph_storage,
            txn_mgr,
        }
    }

    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Transaction, TxnError> {
        self.txn_mgr.begin_transaction(isolation_level)
    }

    #[inline]
    pub fn graph_storage(&self) -> &GraphStorage {
        &self.graph_storage
    }

    #[inline]
    pub fn graph_type(&self) -> Arc<MemoryGraphTypeCatalog> {
        self.graph_type.clone()
    }
}

// TODO: Remove and use a checker.
fn vertex_has_all_labels(
    _mem: &Arc<MemoryGraph>,
    _txn: &Arc<minigu_storage::tp::transaction::MemTransaction>,
    _vid: u64,
    _label_ids: &Option<Vec<Vec<LabelId>>>,
) -> StorageResult<bool> {
    let Some(label_specs) = _label_ids else {
        return Ok(true);
    };

    let vertex = _mem.get_vertex(_txn, _vid)?;
    let vertex_label = vertex.label_id;

    for and_labels in label_specs {
        if and_labels.is_empty() {
            return Ok(true);
        }
        if and_labels.contains(&vertex_label) {
            return Ok(true);
        }
    }
    Ok(false)
}

impl GraphContainer {
    pub fn vertex_source(
        &self,
        label_ids: &Option<Vec<Vec<LabelId>>>,
        batch_size: usize,
    ) -> StorageResult<Box<dyn Iterator<Item = Arc<VertexIdArray>> + Send + 'static>> {
        let mem = match self.graph_storage() {
            GraphStorage::Memory(m) => Arc::clone(m),
        };
        let txn = mem
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)?;
        let mut ids: Vec<u64> = Vec::new();
        {
            let it = mem.iter_vertices(&txn)?;
            for v in it {
                let v = v?;
                let vid = v.vid();
                if vertex_has_all_labels(&mem, &txn, vid, label_ids)? {
                    ids.push(vid);
                }
            }
        }

        // TODO(Colin): Sort IDs to ensure deterministic output in tests.
        // Remove once ORDER BY is supported.
        ids.sort_unstable();

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
}

impl Debug for GraphContainer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphContainer")
            .field("graph_type", &self.graph_type)
            .finish()
    }
}

impl GraphProvider for GraphContainer {
    #[inline]
    fn graph_type(&self) -> GraphTypeRef {
        self.graph_type.clone()
    }

    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use minigu_catalog::memory::graph_type::MemoryGraphTypeCatalog;
    use minigu_catalog::provider::GraphTypeProvider;
    use minigu_catalog::txn::versioned::{VersionedMap, WriteOp};
    use minigu_catalog::txn::{CatalogTxnManager, CatalogTxnView};
    use minigu_common::IsolationLevel;
    use minigu_storage::tp::{GraphTxnView, memory_graph};

    use super::{GraphContainer, GraphStorage};

    #[test]
    fn begin_transaction_aligns_core_and_sub_txns() {
        let (graph, _cleaner) = memory_graph::tests::mock_empty_graph();
        let graph_type = Arc::new(MemoryGraphTypeCatalog::new());
        let container = GraphContainer::new(Arc::clone(&graph_type), GraphStorage::Memory(graph));

        let txn = container
            .begin_transaction(IsolationLevel::Serializable)
            .expect("begin_transaction should succeed");

        let mem_txn = txn.mem_txn();
        let catalog_txn = txn.catalog_txn();

        assert_eq!(txn.core.txn_id, mem_txn.txn_id());
        assert_eq!(txn.core.start_ts, mem_txn.start_ts());
        assert!(matches!(
            txn.core.isolation_level,
            IsolationLevel::Serializable
        ));
        assert!(matches!(
            mem_txn.isolation_level(),
            IsolationLevel::Serializable
        ));

        assert_eq!(txn.core.txn_id, catalog_txn.txn_id());
        assert_eq!(txn.core.start_ts, catalog_txn.start_ts());
        assert!(matches!(
            catalog_txn.isolation_level(),
            IsolationLevel::Serializable
        ));
    }

    #[test]
    fn catalog_write_is_visible_after_transaction_commit() {
        let (graph, _cleaner) = memory_graph::tests::mock_empty_graph();
        let graph_type = Arc::new(MemoryGraphTypeCatalog::new());
        let container = GraphContainer::new(Arc::clone(&graph_type), GraphStorage::Memory(graph));

        let mut txn = container
            .begin_transaction(IsolationLevel::Serializable)
            .expect("begin_transaction should succeed");

        let label_name = "Person".to_string();
        let id = graph_type
            .add_label_txn(label_name.clone(), txn.catalog_txn())
            .expect("add_label_txn should succeed");

        txn.commit().expect("commit should succeed");

        let visible = graph_type
            .get_label_id(&label_name)
            .expect("get_label_id should succeed");
        assert_eq!(visible, Some(id));
    }

    #[test]
    fn catalog_txn_drop_aborts_and_cleans_uncommitted_versions() {
        let mgr = CatalogTxnManager::new();
        let map: Arc<VersionedMap<String, u64>> = Arc::new(VersionedMap::new());

        let key = "k".to_string();

        let txn = mgr
            .begin_transaction(IsolationLevel::Serializable)
            .expect("begin_transaction should succeed");
        let start_ts = txn.start_ts();

        assert_eq!(mgr.low_watermark(), start_ts);

        let node = map
            .put(key.clone(), Arc::new(1), txn.as_ref())
            .expect("put should succeed");
        txn.record_write(&map, key.clone(), node, WriteOp::Create);

        drop(txn);

        // Dropping the txn should best-effort abort it and remove it from the manager's active set.
        assert!(
            mgr.low_watermark().raw() > start_ts.raw(),
            "low watermark should advance after dropping the last active txn"
        );

        // The uncommitted head should have been removed by abort.
        let read_txn = mgr
            .begin_transaction(IsolationLevel::Serializable)
            .expect("begin_transaction should succeed");
        let val = map.get(&key, read_txn.as_ref());
        assert!(val.is_none(), "aborted write should not be visible");
    }
}
