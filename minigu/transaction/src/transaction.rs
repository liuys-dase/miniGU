use std::fmt;
use std::sync::Arc;

use minigu_catalog::provider::CatalogProvider;
use minigu_catalog::txn::{CatalogTxn, CatalogTxnError, CatalogTxnView};
use minigu_common::types::{EdgeId, VectorIndexKey, VertexId};
use minigu_common::value::ScalarValue;
use minigu_common::{IsolationLevel, Timestamp, TimestampError, global_timestamp_generator};
use minigu_storage::error::StorageError;
use minigu_storage::model::edge::{Edge, Neighbor};
use minigu_storage::model::vertex::Vertex;
use minigu_storage::tp::{GraphTxnView, MemTransaction};
use thiserror::Error;

/// Result alias for transaction operations.
pub type TxnResult<T> = Result<T, TxnError>;

/// Global transaction that coordinates graph and catalog updates.
#[derive(Debug)]
pub struct Transaction {
    pub core: TransactionCore,
    pub graph: GraphTxnState,
    pub catalog: Option<CatalogTxnState>,
}

impl Transaction {
    pub fn new(
        core: TransactionCore,
        graph: GraphTxnState,
        catalog: Option<CatalogTxnState>,
    ) -> Self {
        Self {
            core,
            graph,
            catalog,
        }
    }

    /// Commit catalog first (when available), then graph, sharing a single commit timestamp.
    pub fn commit(&mut self) -> TxnResult<Timestamp> {
        if self.core.state != TxnState::Active {
            return Err(TxnError::InvalidState(
                "transaction already finished or not active",
            ));
        }

        let commit_ts = global_timestamp_generator().next()?;

        if let Some(catalog) = self.catalog.as_ref() {
            catalog.commit_at(commit_ts)?;
        }

        match self.graph.commit_at(commit_ts) {
            Ok(ts) => {
                self.core.mark_committed(ts);
                Ok(ts)
            }
            Err(err) => {
                if let Some(catalog) = self.catalog.as_ref() {
                    let _ = catalog.abort();
                }
                Err(TxnError::from(err))
            }
        }
    }

    /// Abort graph and catalog sub-transactions.
    pub fn abort(&mut self) -> TxnResult<()> {
        if self.core.state != TxnState::Active {
            return Ok(());
        }

        if let Some(catalog) = self.catalog.as_ref() {
            let _ = catalog.abort();
        }

        self.graph.abort()?;
        self.core.mark_aborted();
        Ok(())
    }

    pub fn state(&self) -> TxnState {
        self.core.state
    }

    pub fn graph(&self) -> &GraphTxnState {
        &self.graph
    }

    pub fn catalog(&self) -> Option<&CatalogTxnState> {
        self.catalog.as_ref()
    }

    pub fn core(&self) -> &TransactionCore {
        &self.core
    }

    pub fn get_vertex(&self, vid: minigu_common::types::VertexId) -> TxnResult<Vertex> {
        let graph = self.graph.mem().graph();
        graph.get_vertex(self, vid).map_err(TxnError::from)
    }

    pub fn get_edge(&self, eid: EdgeId) -> TxnResult<Edge> {
        let graph = self.graph.mem().graph();
        graph.get_edge(self, eid).map_err(TxnError::from)
    }

    pub fn iter_vertices(
        &self,
    ) -> TxnResult<Box<dyn Iterator<Item = minigu_storage::error::StorageResult<Vertex>> + '_>>
    {
        let graph = self.graph.mem().graph();
        graph.iter_vertices(self).map_err(TxnError::from)
    }

    pub fn iter_edges(
        &self,
    ) -> TxnResult<Box<dyn Iterator<Item = minigu_storage::error::StorageResult<Edge>> + '_>> {
        let graph = self.graph.mem().graph();
        graph.iter_edges(self).map_err(TxnError::from)
    }

    pub fn iter_adjacency(
        &self,
        vid: VertexId,
    ) -> TxnResult<Box<dyn Iterator<Item = minigu_storage::error::StorageResult<Neighbor>> + '_>>
    {
        let graph = self.graph.mem().graph();
        graph.iter_adjacency(self, vid).map_err(TxnError::from)
    }

    pub fn create_vertex(&self, vertex: Vertex) -> TxnResult<VertexId> {
        let graph = self.graph.mem().graph();
        graph.create_vertex(self, vertex).map_err(TxnError::from)
    }

    pub fn create_edge(&self, edge: Edge) -> TxnResult<EdgeId> {
        let graph = self.graph.mem().graph();
        graph.create_edge(self, edge).map_err(TxnError::from)
    }

    pub fn delete_vertex(&self, vid: VertexId) -> TxnResult<()> {
        let graph = self.graph.mem().graph();
        graph.delete_vertex(self, vid).map_err(TxnError::from)
    }

    pub fn delete_edge(&self, eid: EdgeId) -> TxnResult<()> {
        let graph = self.graph.mem().graph();
        graph.delete_edge(self, eid).map_err(TxnError::from)
    }

    pub fn set_vertex_property(
        &self,
        vid: VertexId,
        indices: Vec<usize>,
        props: Vec<ScalarValue>,
    ) -> TxnResult<()> {
        let graph = self.graph.mem().graph();
        graph
            .set_vertex_property(self, vid, indices, props)
            .map_err(TxnError::from)
    }

    pub fn set_edge_property(
        &self,
        eid: EdgeId,
        indices: Vec<usize>,
        props: Vec<ScalarValue>,
    ) -> TxnResult<()> {
        let graph = self.graph.mem().graph();
        graph
            .set_edge_property(self, eid, indices, props)
            .map_err(TxnError::from)
    }

    pub fn build_vector_index(&self, index_key: VectorIndexKey) -> TxnResult<()> {
        let graph = self.graph.mem().graph();
        graph
            .build_vector_index(self, index_key)
            .map_err(TxnError::from)
    }

    pub fn insert_into_vector_index(
        &self,
        index_key: VectorIndexKey,
        node_ids: &[u64],
    ) -> TxnResult<()> {
        let graph = self.graph.mem().graph();
        graph
            .insert_into_vector_index(self, index_key, node_ids)
            .map_err(TxnError::from)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.core.state == TxnState::Active {
            let _ = self.abort();
        }
    }
}

/// The lifecycle state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed,
    Aborted,
}

/// Shared metadata used by all sub-transactions.
#[derive(Debug, Clone)]
pub struct TransactionCore {
    pub txn_id: Timestamp,
    pub start_ts: Timestamp,
    pub isolation_level: IsolationLevel,
    pub state: TxnState,
    pub commit_ts: Option<Timestamp>,
}

impl TransactionCore {
    pub fn new(txn_id: Timestamp, start_ts: Timestamp, isolation_level: IsolationLevel) -> Self {
        Self {
            txn_id,
            start_ts,
            isolation_level,
            state: TxnState::Active,
            commit_ts: None,
        }
    }

    pub fn mark_committed(&mut self, commit_ts: Timestamp) {
        self.state = TxnState::Committed;
        self.commit_ts = Some(commit_ts);
    }

    pub fn mark_aborted(&mut self) {
        self.state = TxnState::Aborted;
    }
}

/// Graph-side transaction state. Wraps the existing MemTransaction to avoid duplicating fields.
#[derive(Clone)]
pub struct GraphTxnState {
    mem: Arc<MemTransaction>,
}

impl GraphTxnState {
    pub fn new(mem: Arc<MemTransaction>) -> Self {
        Self { mem }
    }

    pub fn commit_at(&self, commit_ts: Timestamp) -> Result<Timestamp, StorageError> {
        self.mem.commit_at(Some(commit_ts), false)
    }

    pub fn abort(&self) -> Result<(), StorageError> {
        self.mem.abort_at(false)
    }

    pub fn mem(&self) -> &Arc<MemTransaction> {
        &self.mem
    }
}

impl GraphTxnView for GraphTxnState {
    fn mem_txn(&self) -> &Arc<MemTransaction> {
        &self.mem
    }
}

impl GraphTxnView for Transaction {
    fn mem_txn(&self) -> &Arc<MemTransaction> {
        self.graph.mem()
    }
}

impl CatalogTxnView for Transaction {
    fn catalog_txn(&self) -> &CatalogTxn {
        self.catalog
            .as_ref()
            .expect("catalog transaction missing")
            .txn()
    }
}

impl fmt::Debug for GraphTxnState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphTxnState")
            .field("txn_id", &self.mem.txn_id())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct CatalogTxnState {
    catalog: Arc<dyn CatalogProvider>,
    txn: Arc<CatalogTxn>,
}

impl CatalogTxnState {
    pub fn new(catalog: Arc<dyn CatalogProvider>, txn: Arc<CatalogTxn>) -> Self {
        Self { catalog, txn }
    }

    pub fn catalog(&self) -> &Arc<dyn CatalogProvider> {
        &self.catalog
    }

    pub fn txn(&self) -> &Arc<CatalogTxn> {
        &self.txn
    }

    pub fn commit_at(&self, commit_ts: Timestamp) -> TxnResult<Timestamp> {
        self.txn.commit_at(commit_ts).map_err(TxnError::from)
    }

    pub fn abort(&self) -> TxnResult<()> {
        self.txn.abort().map_err(TxnError::from)
    }
}

/// Errors surfaced by the transaction layer.
#[derive(Error, Debug)]
pub enum TxnError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("timestamp error: {0}")]
    Timestamp(#[from] TimestampError),
    #[error("invalid transaction state: {0}")]
    InvalidState(&'static str),
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogTxnError),
}
