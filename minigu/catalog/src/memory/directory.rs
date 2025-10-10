use std::fmt;
use std::sync::{Arc, Weak};

use minigu_transaction::Transaction;

use crate::error::CatalogResult;
use crate::provider::{DirectoryOrSchema, DirectoryProvider, DirectoryRef};
use crate::txn::ReadView;
use crate::txn::catalog_txn::CatalogTxn;
use crate::txn::versioned_map::{VersionedMap, WriteOp};

pub struct MemoryDirectoryCatalog {
    parent: Option<Weak<dyn DirectoryProvider>>,
    children: Arc<VersionedMap<String, DirectoryOrSchema>>,
}

impl fmt::Debug for MemoryDirectoryCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryDirectoryCatalog").finish()
    }
}

impl MemoryDirectoryCatalog {
    #[inline]
    pub fn new(parent: Option<Weak<dyn DirectoryProvider>>) -> Self {
        Self {
            parent,
            children: Arc::new(VersionedMap::new()),
        }
    }

    /// Add child with transaction. If the child is visible in the read view, return
    /// `AlreadyExists` error; otherwise, add a uncommitted version and record it in the
    /// transaction write set
    #[inline]
    pub fn add_child_txn(
        &self,
        name: String,
        child: DirectoryOrSchema,
        txn: &CatalogTxn,
    ) -> Result<(), crate::txn::error::CatalogTxnError> {
        if self.children.get(&name, txn).is_some() {
            return Err(crate::txn::error::CatalogTxnError::AlreadyExists { key: name });
        }
        let node = self.children.put(name.clone(), Arc::new(child), txn)?;
        txn.record_write(&self.children, name, node, WriteOp::Create);
        Ok(())
    }

    /// Remove child with transaction. If the child is not visible in the read view, return
    /// `NotFound` error; otherwise, add a tombstone and record it in the transaction write set
    #[inline]
    pub fn remove_child_txn(
        &self,
        name: &str,
        txn: &CatalogTxn,
    ) -> Result<(), crate::txn::error::CatalogTxnError> {
        let key = name.to_string();
        let _base = self
            .children
            .get_node_visible(&key, txn.start_ts(), txn.txn_id())
            .ok_or_else(|| crate::txn::error::CatalogTxnError::NotFound { key: key.clone() })?;
        let node = self.children.delete(&key, txn)?;
        txn.record_write(&self.children, key, node, WriteOp::Delete);
        Ok(())
    }
}

impl DirectoryProvider for MemoryDirectoryCatalog {
    #[inline]
    fn parent(&self) -> Option<DirectoryRef> {
        self.parent.clone().and_then(|p| p.upgrade())
    }

    #[inline]
    fn get_child(&self, name: &str) -> CatalogResult<Option<DirectoryOrSchema>> {
        self.get_child_with(name, &ReadView::latest())
    }

    #[inline]
    fn get_child_with(
        &self,
        name: &str,
        _view: &ReadView,
    ) -> CatalogResult<Option<DirectoryOrSchema>> {
        let view = _view;
        Ok(self
            .children
            .get(&name.to_string(), &CatalogTxn::from_view(view))
            .map(|arc| arc.as_ref().clone()))
    }

    #[inline]
    fn children_names(&self) -> Vec<String> {
        self.children_names_with(&ReadView::latest())
    }

    #[inline]
    fn children_names_with(&self, view: &ReadView) -> Vec<String> {
        self.children.visible_keys(view.start_ts, view.txn_id)
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
