use std::sync::Arc;

use minigu_catalog::memory::schema::MemorySchemaCatalog;
use minigu_catalog::named_ref::NamedGraphRef;
use minigu_catalog::txn::catalog_txn::CatalogTxn;
use minigu_catalog::txn::manager::CatalogTxnManager;

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
}
