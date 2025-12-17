use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use minigu_catalog::memory::graph_type::MemoryGraphTypeCatalog;
use minigu_catalog::provider::{GraphProvider, GraphTypeRef};
use minigu_catalog::txn::CatalogTxnManager;
use minigu_common::types::{LabelId, VertexIdArray};
use minigu_common::{IsolationLevel, global_timestamp_generator, global_transaction_id_generator};
use minigu_storage::error::StorageResult;
use minigu_storage::tp::MemoryGraph;
use minigu_transaction::{CatalogTxnState, GraphTxnState, Transaction, TransactionCore, TxnError};

pub enum GraphStorage {
    Memory(Arc<MemoryGraph>),
}

pub struct GraphContainer {
    graph_type: Arc<MemoryGraphTypeCatalog>,
    graph_storage: GraphStorage,
    catalog_txn_mgr: Arc<CatalogTxnManager>,
}

impl GraphContainer {
    pub fn new(graph_type: Arc<MemoryGraphTypeCatalog>, graph_storage: GraphStorage) -> Self {
        Self {
            graph_type,
            graph_storage,
            catalog_txn_mgr: Arc::new(CatalogTxnManager::new()),
        }
    }

    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Transaction, TxnError> {
        let txn_id = global_transaction_id_generator().next()?;
        let start_ts = global_timestamp_generator().next()?;

        let mem = match &self.graph_storage {
            GraphStorage::Memory(mem) => mem,
        };

        let mem_txn = mem.txn_manager().begin_transaction_at(
            Some(txn_id),
            Some(start_ts),
            isolation_level,
            false,
        )?;
        let graph_state = GraphTxnState::new(mem_txn);

        let catalog_txn = self.catalog_txn_mgr.begin_transaction_at(
            Some(txn_id),
            Some(start_ts),
            isolation_level,
        )?;
        let catalog_state = Some(CatalogTxnState::new(
            Arc::clone(&self.graph_type),
            catalog_txn,
        ));

        let core = TransactionCore::new(txn_id, start_ts, isolation_level);
        Ok(Transaction::new(core, graph_state, catalog_state))
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
