use std::fmt;
use std::sync::Arc;

use minigu_catalog::provider::CatalogProvider;
use minigu_common::{IsolationLevel, global_timestamp_generator, global_transaction_id_generator};
use minigu_storage::tp::MemoryGraph;

use crate::transaction::{CatalogTxnState, GraphTxnState, Transaction, TransactionCore, TxnError};

/// Provides a single entrypoint to start global transactions over graph and catalog.
#[derive(Clone)]
pub struct LightningGraph {
    memory_graph: Arc<MemoryGraph>,
    catalog: Option<Arc<dyn CatalogProvider>>,
}

impl LightningGraph {
    pub fn new(memory_graph: Arc<MemoryGraph>, catalog: Option<Arc<dyn CatalogProvider>>) -> Self {
        Self {
            memory_graph,
            catalog,
        }
    }

    pub fn memory_graph(&self) -> &Arc<MemoryGraph> {
        &self.memory_graph
    }

    pub fn catalog(&self) -> Option<&Arc<dyn CatalogProvider>> {
        self.catalog.as_ref()
    }

    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Transaction, TxnError> {
        let txn_id = global_transaction_id_generator().next()?;
        let start_ts = global_timestamp_generator().next()?;

        let mem_txn = self.memory_graph.txn_manager().begin_transaction_at(
            Some(txn_id),
            Some(start_ts),
            isolation_level,
            false,
        )?;
        let graph_state = GraphTxnState::new(mem_txn);

        let catalog_state = self
            .catalog
            .as_ref()
            .map(|catalog| CatalogTxnState::new_placeholder(Arc::clone(catalog)));

        let core = TransactionCore::new(txn_id, start_ts, isolation_level);

        Ok(Transaction::new(core, graph_state, catalog_state))
    }
}

impl fmt::Debug for LightningGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LightningGraph").finish_non_exhaustive()
    }
}
