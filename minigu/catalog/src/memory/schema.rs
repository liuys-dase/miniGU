use std::fmt;
use std::sync::{Arc, Weak};

use minigu_transaction::Transaction;

use crate::error::CatalogResult;
use crate::provider::{
    DirectoryProvider, DirectoryRef, GraphRef, GraphTypeRef, ProcedureRef, SchemaProvider,
};
use crate::txn::ReadView;
use crate::txn::catalog_txn::CatalogTxn;
use crate::txn::versioned_map::{VersionedMap, WriteOp};

pub struct MemorySchemaCatalog {
    parent: Option<Weak<dyn DirectoryProvider>>,
    graph_map: Arc<VersionedMap<String, GraphRef>>,
    graph_type_map: Arc<VersionedMap<String, GraphTypeRef>>,
    procedure_map: Arc<VersionedMap<String, ProcedureRef>>,
}

impl fmt::Debug for MemorySchemaCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySchemaCatalog").finish()
    }
}

impl MemorySchemaCatalog {
    #[inline]
    pub fn new(parent: Option<Weak<dyn DirectoryProvider>>) -> Self {
        Self {
            parent,
            graph_map: Arc::new(VersionedMap::new()),
            graph_type_map: Arc::new(VersionedMap::new()),
            procedure_map: Arc::new(VersionedMap::new()),
        }
    }

    // ===== Transactional write APIs =====
    #[inline]
    pub fn add_graph_txn(
        &self,
        name: String,
        graph: GraphRef,
        txn: &CatalogTxn,
    ) -> Result<(), crate::txn::error::CatalogTxnError> {
        if self.graph_map.get(&name, txn).is_some() {
            return Err(crate::txn::error::CatalogTxnError::AlreadyExists { key: name });
        }
        let node = self.graph_map.put(name.clone(), Arc::new(graph), txn)?;
        txn.record_write(&self.graph_map, name, node, WriteOp::Create);
        Ok(())
    }

    #[inline]
    pub fn remove_graph_txn(
        &self,
        name: &str,
        txn: &CatalogTxn,
    ) -> Result<(), crate::txn::error::CatalogTxnError> {
        let key = name.to_string();
        let _base = self
            .graph_map
            .get_node_visible(&key, txn.start_ts(), txn.txn_id())
            .ok_or_else(|| crate::txn::error::CatalogTxnError::NotFound { key: key.clone() })?;
        let node = self.graph_map.delete(&key, txn)?;
        txn.record_write(&self.graph_map, key, node, WriteOp::Delete);
        Ok(())
    }

    #[inline]
    pub fn add_graph_type_txn(
        &self,
        name: String,
        graph_type: GraphTypeRef,
        txn: &CatalogTxn,
    ) -> Result<(), crate::txn::error::CatalogTxnError> {
        if self.graph_type_map.get(&name, txn).is_some() {
            return Err(crate::txn::error::CatalogTxnError::AlreadyExists { key: name });
        }
        let node = self
            .graph_type_map
            .put(name.clone(), Arc::new(graph_type), txn)?;
        txn.record_write(&self.graph_type_map, name, node, WriteOp::Create);
        Ok(())
    }

    #[inline]
    pub fn remove_graph_type_txn(
        &self,
        name: &str,
        txn: &CatalogTxn,
    ) -> Result<(), crate::txn::error::CatalogTxnError> {
        let key = name.to_string();
        let _base = self
            .graph_type_map
            .get_node_visible(&key, txn.start_ts(), txn.txn_id())
            .ok_or_else(|| crate::txn::error::CatalogTxnError::NotFound { key: key.clone() })?;
        let node = self.graph_type_map.delete(&key, txn)?;
        txn.record_write(&self.graph_type_map, key, node, WriteOp::Delete);
        Ok(())
    }

    #[inline]
    pub fn add_procedure_txn(
        &self,
        name: String,
        procedure: ProcedureRef,
        txn: &CatalogTxn,
    ) -> Result<(), crate::txn::error::CatalogTxnError> {
        if self.procedure_map.get(&name, txn).is_some() {
            return Err(crate::txn::error::CatalogTxnError::AlreadyExists { key: name });
        }
        let node = self
            .procedure_map
            .put(name.clone(), Arc::new(procedure), txn)?;
        txn.record_write(&self.procedure_map, name, node, WriteOp::Create);
        Ok(())
    }

    #[inline]
    pub fn remove_procedure_txn(
        &self,
        name: &str,
        txn: &CatalogTxn,
    ) -> Result<(), crate::txn::error::CatalogTxnError> {
        let key = name.to_string();
        let _base = self
            .procedure_map
            .get_node_visible(&key, txn.start_ts(), txn.txn_id())
            .ok_or_else(|| crate::txn::error::CatalogTxnError::NotFound { key: key.clone() })?;
        let node = self.procedure_map.delete(&key, txn)?;
        txn.record_write(&self.procedure_map, key, node, WriteOp::Delete);
        Ok(())
    }
}

impl SchemaProvider for MemorySchemaCatalog {
    #[inline]
    fn parent(&self) -> Option<DirectoryRef> {
        self.parent.clone().and_then(|p| p.upgrade())
    }

    #[inline]
    fn graph_names(&self) -> Vec<String> {
        self.graph_names_with(&ReadView::latest())
    }

    #[inline]
    fn get_graph(&self, name: &str) -> CatalogResult<Option<GraphRef>> {
        self.get_graph_with(name, &ReadView::latest())
    }

    #[inline]
    fn get_graph_with(&self, name: &str, _view: &ReadView) -> CatalogResult<Option<GraphRef>> {
        let view = _view;
        Ok(self
            .graph_map
            .get(&name.to_string(), &CatalogTxn::from_view(view))
            .map(|arc| (*arc).clone()))
    }

    #[inline]
    fn graph_names_with(&self, view: &ReadView) -> Vec<String> {
        self.graph_map.visible_keys(view.start_ts, view.txn_id)
    }

    #[inline]
    fn graph_type_names(&self) -> Vec<String> {
        self.graph_type_names_with(&ReadView::latest())
    }

    #[inline]
    fn get_graph_type(&self, name: &str) -> CatalogResult<Option<GraphTypeRef>> {
        self.get_graph_type_with(name, &ReadView::latest())
    }

    #[inline]
    fn get_graph_type_with(
        &self,
        name: &str,
        _view: &ReadView,
    ) -> CatalogResult<Option<GraphTypeRef>> {
        let view = _view;
        Ok(self
            .graph_type_map
            .get(&name.to_string(), &CatalogTxn::from_view(view))
            .map(|arc| (*arc).clone()))
    }

    #[inline]
    fn graph_type_names_with(&self, view: &ReadView) -> Vec<String> {
        self.graph_type_map.visible_keys(view.start_ts, view.txn_id)
    }

    #[inline]
    fn procedure_names(&self) -> Vec<String> {
        self.procedure_names_with(&ReadView::latest())
    }

    #[inline]
    fn get_procedure(&self, name: &str) -> CatalogResult<Option<ProcedureRef>> {
        self.get_procedure_with(name, &ReadView::latest())
    }

    #[inline]
    fn get_procedure_with(
        &self,
        name: &str,
        _view: &ReadView,
    ) -> CatalogResult<Option<ProcedureRef>> {
        let view = _view;
        Ok(self
            .procedure_map
            .get(&name.to_string(), &CatalogTxn::from_view(view))
            .map(|arc| (*arc).clone()))
    }

    #[inline]
    fn procedure_names_with(&self, view: &ReadView) -> Vec<String> {
        self.procedure_map.visible_keys(view.start_ts, view.txn_id)
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
