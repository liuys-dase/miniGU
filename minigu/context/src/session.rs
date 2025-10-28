use std::sync::Arc;

use minigu_catalog::memory::schema::MemorySchemaCatalog;
use minigu_catalog::named_ref::NamedGraphRef;
use minigu_catalog::txn::catalog_txn::CatalogTxn;
use minigu_catalog::txn::error::CatalogTxnResult;
use minigu_catalog::txn::manager::CatalogTxnManager;
use minigu_transaction::{GraphTxnManager, IsolationLevel};

use crate::database::DatabaseContext;

#[derive(Clone)]
pub struct SessionContext {
    database: Arc<DatabaseContext>,
    pub home_schema: Option<Arc<MemorySchemaCatalog>>,
    pub current_schema: Option<Arc<MemorySchemaCatalog>>,
    pub home_graph: Option<NamedGraphRef>,
    pub current_graph: Option<NamedGraphRef>,
    pub current_txn: Option<Arc<CatalogTxn>>,
    pub catalog_txn_mgr: CatalogTxnManager,
}

impl SessionContext {
    pub fn new(database: Arc<DatabaseContext>) -> Self {
        Self {
            database,
            home_schema: None,
            current_schema: None,
            home_graph: None,
            current_graph: None,
            current_txn: None,
            catalog_txn_mgr: CatalogTxnManager::new(),
        }
    }

    pub fn database(&self) -> &DatabaseContext {
        &self.database
    }

    /// Begin a new transaction using the catalog transaction manager.
    /// Returns a `Arc<CatalogTxn>`.
    pub fn begin_txn(&self) -> CatalogTxnResult<Arc<CatalogTxn>> {
        let txn_arc = self
            .catalog_txn_mgr
            .begin_transaction(IsolationLevel::Snapshot)?;

        Ok(txn_arc)
    }

    /// Get the current transaction if present; otherwise begin one and store it.
    pub fn get_or_begin_txn(&mut self) -> CatalogTxnResult<Arc<CatalogTxn>> {
        if let Some(txn) = &self.current_txn {
            return Ok(txn.clone());
        }

        let txn = self.begin_txn()?;
        self.current_txn = Some(txn.clone());
        Ok(txn)
    }
}
