use std::sync::Arc;

use gql_parser::ast::{Ident, SchemaPathSegment, SchemaRef};
use minigu_catalog::memory::schema::MemorySchemaCatalog;
use minigu_catalog::named_ref::NamedGraphRef;
use minigu_catalog::provider::{CatalogProvider, SchemaProvider};
use minigu_catalog::txn::catalog_txn::CatalogTxn;
use minigu_catalog::txn::error::CatalogTxnResult;
use minigu_catalog::txn::manager::CatalogTxnManager;
use minigu_transaction::{GraphTxnManager, IsolationLevel, Transaction};

use crate::database::DatabaseContext;
use crate::error::{Error, SessionResult};

#[derive(Clone, Debug)]
pub struct SessionContext {
    database: Arc<DatabaseContext>,
    pub home_schema: Option<Arc<MemorySchemaCatalog>>,
    pub current_schema: Option<Arc<MemorySchemaCatalog>>,
    // currently home_graph and current_graph is same.
    // In the future, home_graph is a default graph named default.
    pub home_graph: Option<NamedGraphRef>,
    pub current_graph: Option<NamedGraphRef>,
    pub current_txn: Option<Arc<CatalogTxn>>,
    pub catalog_txn_mgr: CatalogTxnManager,
    isolation_level: Option<IsolationLevel>,
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
            isolation_level: None,
        }
    }

    pub fn database(&self) -> &DatabaseContext {
        &self.database
    }

    /// Set default isolation level for implicitly created transactions.
    #[inline]
    pub fn set_default_isolation(&mut self, iso: IsolationLevel) {
        self.isolation_level = Some(iso);
    }

    /// Begin a new transaction using the default isolation level.
    fn begin_txn(&self) -> CatalogTxnResult<Arc<CatalogTxn>> {
        if let Some(iso) = self.isolation_level {
            let txn_arc = self.catalog_txn_mgr.begin_transaction(iso)?;
            return Ok(txn_arc);
        }
        let txn_arc = self
            .catalog_txn_mgr
            .begin_transaction(IsolationLevel::Serializable)?;
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

    /// Clear the current transaction reference (without committing or rolling back).
    /// This is used in auto-commit mode, where the transaction is committed successfully,
    /// and the `current_txn` is removed to avoid mistakenly reusing the committed transaction.
    /// Note: This method only clears the session-level reference, does not affect the lifecycle of
    /// the transaction itself.
    #[inline]
    pub fn clear_current_txn(&mut self) {
        self.current_txn = None;
    }

    pub fn set_current_schema(&mut self, schema: SchemaRef) -> SessionResult<()> {
        match schema {
            SchemaRef::Absolute(schema_path) => {
                let txn = self
                    .get_or_begin_txn()
                    .map_err(|e| minigu_catalog::error::CatalogError::External(Box::new(e)))?;
                let mut current = self.database.catalog().get_root()?;
                let mut current_path = vec![];
                for segment in schema_path {
                    let name = match segment.value() {
                        SchemaPathSegment::Name(name) => name,
                        SchemaPathSegment::Parent => unreachable!(),
                    };
                    let current_dir = current
                        .into_directory()
                        .ok_or_else(|| Error::SchemaPathInvalid)?;
                    current_path.push(segment.value().clone());
                    let child = current_dir
                        .get_child(name, txn.as_ref())?
                        .ok_or_else(|| Error::SchemaPathInvalid)?;
                    current = child;
                }
                let schema_arc: minigu_catalog::provider::SchemaRef = current
                    .into_schema()
                    .ok_or_else(|| Error::SchemaPathInvalid)?;

                let msc: Arc<MemorySchemaCatalog> = schema_arc
                    .downcast_arc::<MemorySchemaCatalog>()
                    .map_err(|_| Error::SchemaPathInvalid)?;
                self.current_schema = Some(msc);
                txn.commit()
                    .map_err(|e| minigu_catalog::error::CatalogError::External(Box::new(e)))?;
                self.clear_current_txn();
                Ok(())
            }
            _ => {
                todo!()
            }
        }
    }

    pub fn set_current_graph(&mut self, graph_name: String) -> SessionResult<()> {
        if self.current_schema.is_none() {
            return Err(Error::CurrentSchemaNotSet);
        };
        let txn = self
            .get_or_begin_txn()
            .map_err(|e| minigu_catalog::error::CatalogError::External(Box::new(e)))?;
        let schema = self
            .current_schema
            .as_ref()
            .ok_or_else(|| Error::CurrentSchemaNotSet)?
            .as_ref();
        let graph = schema
            .get_graph(graph_name.as_str(), txn.as_ref())?
            .ok_or_else(|| Error::GraphNotExists(graph_name.clone()))?;
        self.current_graph = Some(NamedGraphRef::new(Ident::new(graph_name), graph));
        txn.commit()
            .map_err(|e| minigu_catalog::error::CatalogError::External(Box::new(e)))?;
        self.clear_current_txn();
        Ok(())
    }

    pub fn reset_current_graph(&mut self) {
        self.current_graph = self.home_graph.clone();
    }

    pub fn reset_current_schema(&mut self) {
        self.current_schema = self.home_schema.clone();
    }
}
