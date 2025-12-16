use std::sync::{Arc, Mutex};

use minigu_common::IsolationLevel;
use minigu_common::types::{LabelId, PropertyId};

use crate::error::{CatalogError, CatalogResult};
use crate::label_set::LabelSet;
use crate::memory::txn_manager;
use crate::property::Property;
use crate::provider::{
    EdgeTypeProvider, EdgeTypeRef, GraphTypeProvider, PropertiesProvider, VertexTypeProvider,
    VertexTypeRef,
};
use crate::txn::CatalogTxnError;
use crate::txn::catalog_txn::{CatalogTxn, TxnHook};
use crate::txn::versioned_map::{VersionedMap, WriteOp};

#[derive(Debug)]
pub struct MemoryGraphTypeCatalog {
    next_label_id: Mutex<LabelId>,
    label_map: Arc<VersionedMap<String, LabelId>>,
    vertex_type_map: Arc<VersionedMap<LabelSet, VertexTypeRef>>,
    edge_type_map: Arc<VersionedMap<LabelSet, EdgeTypeRef>>,
}

impl Default for MemoryGraphTypeCatalog {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryGraphTypeCatalog {
    #[inline]
    pub fn new() -> Self {
        Self {
            next_label_id: Mutex::new(LabelId::new(1).expect("label id should be non-zero")),
            label_map: Arc::new(VersionedMap::new()),
            vertex_type_map: Arc::new(VersionedMap::new()),
            edge_type_map: Arc::new(VersionedMap::new()),
        }
    }

    // ============== Transactional write APIs ==============
    #[inline]
    pub fn add_label_txn(
        &self,
        name: String,
        txn: &CatalogTxn,
    ) -> Result<LabelId, CatalogTxnError> {
        if self.label_map.get(&name, txn).is_some() {
            return Err(CatalogTxnError::AlreadyExists { key: name });
        }
        let mut guard = self.next_label_id.lock().expect("poisoned label id mutex");
        let label_id = *guard;
        *guard = guard
            .checked_add(1)
            .ok_or_else(|| CatalogTxnError::IllegalState {
                reason: "label id overflow".to_string(),
            })?;
        drop(guard);

        let node = self.label_map.put(name.clone(), Arc::new(label_id), txn)?;
        txn.record_write(&self.label_map, name, node, WriteOp::Create);
        Ok(label_id)
    }

    #[inline]
    pub fn remove_label_txn(&self, name: &str, txn: &CatalogTxn) -> Result<(), CatalogTxnError> {
        let key = name.to_string();
        let base = self
            .label_map
            .get_node_visible(&key, txn.start_ts(), txn.txn_id())
            .ok_or_else(|| CatalogTxnError::NotFound { key: key.clone() })?;
        let label_id = *base.value().ok_or_else(|| CatalogTxnError::IllegalState {
            reason: "label node has no value".to_string(),
        })?;
        txn.add_hook(Box::new(GraphTypeIntegrityHook::label_delete(
            self.vertex_type_map.clone(),
            self.edge_type_map.clone(),
            label_id,
        )));
        let node = self.label_map.delete(&key, txn)?;
        txn.record_write(&self.label_map, key, node, WriteOp::Delete);
        Ok(())
    }

    #[inline]
    pub fn add_vertex_type_txn(
        &self,
        label_set: LabelSet,
        vertex_type: Arc<MemoryVertexTypeCatalog>,
        txn: &CatalogTxn,
    ) -> Result<(), CatalogTxnError> {
        if self.vertex_type_map.get(&label_set, txn).is_some() {
            return Err(CatalogTxnError::AlreadyExists {
                key: format!("{:?}", label_set),
            });
        }
        let vt_ref: VertexTypeRef = vertex_type;
        let node = self
            .vertex_type_map
            .put(label_set.clone(), Arc::new(vt_ref), txn)?;
        txn.record_write(&self.vertex_type_map, label_set, node, WriteOp::Create);
        Ok(())
    }

    #[inline]
    pub fn remove_vertex_type_txn(
        &self,
        label_set: &LabelSet,
        txn: &CatalogTxn,
    ) -> Result<(), CatalogTxnError> {
        let _base = self
            .vertex_type_map
            .get_node_visible(label_set, txn.start_ts(), txn.txn_id())
            .ok_or_else(|| CatalogTxnError::NotFound {
                key: format!("{:?}", label_set),
            })?;
        txn.add_hook(Box::new(GraphTypeIntegrityHook::vertex_delete(
            self.edge_type_map.clone(),
            label_set.clone(),
        )));
        let node = self.vertex_type_map.delete(label_set, txn)?;
        txn.record_write(
            &self.vertex_type_map,
            label_set.clone(),
            node,
            WriteOp::Delete,
        );
        Ok(())
    }

    #[inline]
    pub fn add_edge_type_txn(
        &self,
        label_set: LabelSet,
        edge_type: Arc<MemoryEdgeTypeCatalog>,
        txn: &CatalogTxn,
    ) -> Result<(), CatalogTxnError> {
        let src_ls = edge_type.src().label_set();
        let dst_ls = edge_type.dst().label_set();
        if self.vertex_type_map.get(&src_ls, txn).is_none()
            || self.vertex_type_map.get(&dst_ls, txn).is_none()
        {
            return Err(CatalogTxnError::ReferentialIntegrity {
                reason: "edge src/dst vertex type not found".to_string(),
            });
        }
        if self.edge_type_map.get(&label_set, txn).is_some() {
            return Err(CatalogTxnError::AlreadyExists {
                key: format!("{:?}", label_set),
            });
        }
        txn.add_hook(Box::new(GraphTypeIntegrityHook::edge_src_dst_exist(
            self.vertex_type_map.clone(),
            src_ls.clone(),
            dst_ls.clone(),
        )));
        let et_ref: EdgeTypeRef = edge_type;
        let node = self
            .edge_type_map
            .put(label_set.clone(), Arc::new(et_ref), txn)?;
        txn.record_write(&self.edge_type_map, label_set, node, WriteOp::Create);
        Ok(())
    }

    #[inline]
    pub fn remove_edge_type_txn(
        &self,
        label_set: &LabelSet,
        txn: &CatalogTxn,
    ) -> Result<(), CatalogTxnError> {
        let _base = self
            .edge_type_map
            .get_node_visible(label_set, txn.start_ts(), txn.txn_id())
            .ok_or_else(|| CatalogTxnError::NotFound {
                key: format!("{:?}", label_set),
            })?;
        let node = self.edge_type_map.delete(label_set, txn)?;
        txn.record_write(
            &self.edge_type_map,
            label_set.clone(),
            node,
            WriteOp::Delete,
        );
        Ok(())
    }

    #[inline]
    pub fn replace_vertex_type_txn(
        &self,
        label_set: &LabelSet,
        new_vertex_type: Arc<MemoryVertexTypeCatalog>,
        txn: &CatalogTxn,
    ) -> Result<(), CatalogTxnError> {
        let _ = self
            .vertex_type_map
            .get_node_visible(label_set, txn.start_ts(), txn.txn_id())
            .ok_or_else(|| CatalogTxnError::NotFound {
                key: format!("{:?}", label_set),
            })?;
        let vt_ref: VertexTypeRef = new_vertex_type;
        let node = self
            .vertex_type_map
            .put(label_set.clone(), Arc::new(vt_ref), txn)?;
        txn.record_write(
            &self.vertex_type_map,
            label_set.clone(),
            node,
            WriteOp::Replace,
        );
        Ok(())
    }

    #[inline]
    pub fn replace_edge_type_txn(
        &self,
        label_set: &LabelSet,
        new_edge_type: Arc<MemoryEdgeTypeCatalog>,
        txn: &CatalogTxn,
    ) -> Result<(), CatalogTxnError> {
        let src_ls = new_edge_type.src().label_set();
        let dst_ls = new_edge_type.dst().label_set();
        if self.vertex_type_map.get(&src_ls, txn).is_none()
            || self.vertex_type_map.get(&dst_ls, txn).is_none()
        {
            return Err(CatalogTxnError::ReferentialIntegrity {
                reason: "edge src/dst vertex type not found".to_string(),
            });
        }
        let _ = self
            .edge_type_map
            .get_node_visible(label_set, txn.start_ts(), txn.txn_id())
            .ok_or_else(|| CatalogTxnError::NotFound {
                key: format!("{:?}", label_set),
            })?;
        txn.add_hook(Box::new(GraphTypeIntegrityHook::edge_src_dst_exist(
            self.vertex_type_map.clone(),
            src_ls.clone(),
            dst_ls.clone(),
        )));
        let et_ref: EdgeTypeRef = new_edge_type;
        let node = self
            .edge_type_map
            .put(label_set.clone(), Arc::new(et_ref), txn)?;
        txn.record_write(
            &self.edge_type_map,
            label_set.clone(),
            node,
            WriteOp::Replace,
        );
        Ok(())
    }

    // ============== Compatibility wrappers (auto-commit) ==============
    #[inline]
    pub fn add_label(&mut self, name: String) -> Option<LabelId> {
        let txn = txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .ok()?;
        let res = self.add_label_txn(name, txn.as_ref());
        match res {
            Ok(id) => {
                if txn.commit().is_ok() {
                    Some(id)
                } else {
                    None
                }
            }
            Err(_) => {
                txn.abort().ok();
                None
            }
        }
    }

    #[inline]
    pub fn remove_label(&mut self, name: &str) -> bool {
        let txn = match txn_manager().begin_transaction(IsolationLevel::Serializable) {
            Ok(txn) => txn,
            Err(_) => return false,
        };
        let res = self.remove_label_txn(name, txn.as_ref());
        match res {
            Ok(_) => txn.commit().is_ok(),
            Err(_) => {
                txn.abort().ok();
                false
            }
        }
    }

    #[inline]
    pub fn add_vertex_type(
        &mut self,
        label_set: LabelSet,
        vertex_type: Arc<MemoryVertexTypeCatalog>,
    ) -> bool {
        let txn = match txn_manager().begin_transaction(IsolationLevel::Serializable) {
            Ok(txn) => txn,
            Err(_) => return false,
        };
        let res = self.add_vertex_type_txn(label_set, vertex_type, txn.as_ref());
        match res {
            Ok(_) => txn.commit().is_ok(),
            Err(_) => {
                txn.abort().ok();
                false
            }
        }
    }

    #[inline]
    pub fn remove_vertex_type(&mut self, label_set: &LabelSet) -> bool {
        let txn = match txn_manager().begin_transaction(IsolationLevel::Serializable) {
            Ok(txn) => txn,
            Err(_) => return false,
        };
        let res = self.remove_vertex_type_txn(label_set, txn.as_ref());
        match res {
            Ok(_) => txn.commit().is_ok(),
            Err(_) => {
                txn.abort().ok();
                false
            }
        }
    }

    #[inline]
    pub fn add_edge_type(
        &mut self,
        label_set: LabelSet,
        edge_type: Arc<MemoryEdgeTypeCatalog>,
    ) -> bool {
        let txn = match txn_manager().begin_transaction(IsolationLevel::Serializable) {
            Ok(txn) => txn,
            Err(_) => return false,
        };
        let res = self.add_edge_type_txn(label_set, edge_type, txn.as_ref());
        match res {
            Ok(_) => txn.commit().is_ok(),
            Err(_) => {
                txn.abort().ok();
                false
            }
        }
    }

    #[inline]
    pub fn remove_edge_type(&mut self, label_set: &LabelSet) -> bool {
        let txn = match txn_manager().begin_transaction(IsolationLevel::Serializable) {
            Ok(txn) => txn,
            Err(_) => return false,
        };
        let res = self.remove_edge_type_txn(label_set, txn.as_ref());
        match res {
            Ok(_) => txn.commit().is_ok(),
            Err(_) => {
                txn.abort().ok();
                false
            }
        }
    }
}

// ================ Pre-commit validation hook implementation ================
#[derive(Debug)]
struct GraphTypeIntegrityHook {
    vertex_type_map: Arc<VersionedMap<LabelSet, VertexTypeRef>>,
    edge_type_map: Arc<VersionedMap<LabelSet, EdgeTypeRef>>,
    kind: IntegrityKind,
}

#[derive(Debug)]
enum IntegrityKind {
    LabelDelete { label_id: LabelId },
    VertexDelete { vertex_label_set: LabelSet },
    EdgeSrcDstExist { src: LabelSet, dst: LabelSet },
}

impl GraphTypeIntegrityHook {
    fn label_delete(
        vertex_type_map: Arc<VersionedMap<LabelSet, VertexTypeRef>>,
        edge_type_map: Arc<VersionedMap<LabelSet, EdgeTypeRef>>,
        label_id: LabelId,
    ) -> Self {
        Self {
            vertex_type_map,
            edge_type_map,
            kind: IntegrityKind::LabelDelete { label_id },
        }
    }

    fn vertex_delete(
        edge_type_map: Arc<VersionedMap<LabelSet, EdgeTypeRef>>,
        vertex_label_set: LabelSet,
    ) -> Self {
        Self {
            vertex_type_map: Arc::new(VersionedMap::new()),
            edge_type_map,
            kind: IntegrityKind::VertexDelete { vertex_label_set },
        }
    }

    fn edge_src_dst_exist(
        vertex_type_map: Arc<VersionedMap<LabelSet, VertexTypeRef>>,
        src: LabelSet,
        dst: LabelSet,
    ) -> Self {
        Self {
            vertex_type_map,
            edge_type_map: Arc::new(VersionedMap::new()),
            kind: IntegrityKind::EdgeSrcDstExist { src, dst },
        }
    }
}

impl TxnHook for GraphTypeIntegrityHook {
    fn precommit(&self, txn: &CatalogTxn) -> Result<(), CatalogTxnError> {
        match &self.kind {
            IntegrityKind::LabelDelete { label_id } => {
                let v_keys = self
                    .vertex_type_map
                    .visible_keys(txn.start_ts(), txn.txn_id());
                if v_keys.iter().any(|k| k.contains(*label_id)) {
                    return Err(CatalogTxnError::ReferentialIntegrity {
                        reason: format!("label {:?} is still used by vertex types", label_id),
                    });
                }
                let e_keys = self
                    .edge_type_map
                    .visible_keys(txn.start_ts(), txn.txn_id());
                if e_keys.iter().any(|k| k.contains(*label_id)) {
                    return Err(CatalogTxnError::ReferentialIntegrity {
                        reason: format!("label {:?} is still used by edge types", label_id),
                    });
                }
                Ok(())
            }
            IntegrityKind::VertexDelete { vertex_label_set } => {
                let e_keys = self
                    .edge_type_map
                    .visible_keys(txn.start_ts(), txn.txn_id());
                for k in e_keys.into_iter() {
                    if let Some(node) =
                        self.edge_type_map
                            .get_node_visible(&k, txn.start_ts(), txn.txn_id())
                        && let Some(edge) = node.value()
                    {
                        let edge = edge.as_ref();
                        if edge.src().label_set() == *vertex_label_set
                            || edge.dst().label_set() == *vertex_label_set
                        {
                            return Err(CatalogTxnError::ReferentialIntegrity {
                                reason: "vertex type is still referenced by edge types".to_string(),
                            });
                        }
                    }
                }
                Ok(())
            }
            IntegrityKind::EdgeSrcDstExist { src, dst } => {
                let s = self
                    .vertex_type_map
                    .get_node_visible(src, txn.start_ts(), txn.txn_id())
                    .is_some();
                let d = self
                    .vertex_type_map
                    .get_node_visible(dst, txn.start_ts(), txn.txn_id())
                    .is_some();
                if s && d {
                    Ok(())
                } else {
                    Err(CatalogTxnError::ReferentialIntegrity {
                        reason: "edge src/dst vertex type not found at commit".to_string(),
                    })
                }
            }
        }
    }
}

impl GraphTypeProvider for MemoryGraphTypeCatalog {
    #[inline]
    fn get_label_id(&self, name: &str) -> CatalogResult<Option<LabelId>> {
        let txn = txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .map_err(|e| CatalogError::External(Box::new(e)))?;
        let res = self.get_label_id_txn(name, txn.as_ref());
        txn.abort().ok();
        res
    }

    #[inline]
    fn get_label_id_txn(&self, name: &str, txn: &CatalogTxn) -> CatalogResult<Option<LabelId>> {
        Ok(self
            .label_map
            .get(&name.to_string(), txn)
            .map(|arc| *arc.as_ref()))
    }

    #[inline]
    fn label_names(&self) -> Vec<String> {
        let txn = match txn_manager().begin_transaction(IsolationLevel::Serializable) {
            Ok(txn) => txn,
            Err(_) => return Vec::new(),
        };
        let res = self.label_names_txn(txn.as_ref());
        txn.abort().ok();
        res
    }

    #[inline]
    fn label_names_txn(&self, txn: &CatalogTxn) -> Vec<String> {
        self.label_map.visible_keys(txn.start_ts(), txn.txn_id())
    }

    #[inline]
    fn get_vertex_type(&self, key: &LabelSet) -> CatalogResult<Option<VertexTypeRef>> {
        let txn = txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .map_err(|e| CatalogError::External(Box::new(e)))?;
        let res = self.get_vertex_type_txn(key, txn.as_ref());
        txn.abort().ok();
        res
    }

    #[inline]
    fn get_vertex_type_txn(
        &self,
        key: &LabelSet,
        txn: &CatalogTxn,
    ) -> CatalogResult<Option<VertexTypeRef>> {
        Ok(self
            .vertex_type_map
            .get(key, txn)
            .map(|arc| arc.as_ref().clone()))
    }

    #[inline]
    fn vertex_type_keys(&self) -> Vec<LabelSet> {
        let txn = match txn_manager().begin_transaction(IsolationLevel::Serializable) {
            Ok(txn) => txn,
            Err(_) => return Vec::new(),
        };
        let res = self.vertex_type_keys_txn(txn.as_ref());
        txn.abort().ok();
        res
    }

    #[inline]
    fn vertex_type_keys_txn(&self, txn: &CatalogTxn) -> Vec<LabelSet> {
        self.vertex_type_map
            .visible_keys(txn.start_ts(), txn.txn_id())
    }

    #[inline]
    fn get_edge_type(&self, key: &LabelSet) -> CatalogResult<Option<EdgeTypeRef>> {
        let txn = txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .map_err(|e| CatalogError::External(Box::new(e)))?;
        let res = self.get_edge_type_txn(key, txn.as_ref());
        txn.abort().ok();
        res
    }

    #[inline]
    fn get_edge_type_txn(
        &self,
        key: &LabelSet,
        txn: &CatalogTxn,
    ) -> CatalogResult<Option<EdgeTypeRef>> {
        Ok(self
            .edge_type_map
            .get(key, txn)
            .map(|arc| arc.as_ref().clone()))
    }

    #[inline]
    fn edge_type_keys(&self) -> Vec<LabelSet> {
        let txn = match txn_manager().begin_transaction(IsolationLevel::Serializable) {
            Ok(txn) => txn,
            Err(_) => return Vec::new(),
        };
        let res = self.edge_type_keys_txn(txn.as_ref());
        txn.abort().ok();
        res
    }

    #[inline]
    fn edge_type_keys_txn(&self, txn: &CatalogTxn) -> Vec<LabelSet> {
        self.edge_type_map
            .visible_keys(txn.start_ts(), txn.txn_id())
    }
}

#[derive(Debug)]
pub struct MemoryVertexTypeCatalog {
    label_set: LabelSet,
    properties: Vec<Property>,
}

impl MemoryVertexTypeCatalog {
    #[inline]
    pub fn new(label_set: LabelSet, properties: Vec<Property>) -> Self {
        Self {
            label_set,
            properties,
        }
    }
}

impl VertexTypeProvider for MemoryVertexTypeCatalog {
    #[inline]
    fn label_set(&self) -> LabelSet {
        self.label_set.clone()
    }
}

impl PropertiesProvider for MemoryVertexTypeCatalog {
    fn get_property(&self, name: &str) -> CatalogResult<Option<(PropertyId, &Property)>> {
        Ok(self
            .properties
            .iter()
            .enumerate()
            .find(|(_, p)| p.name() == name)
            .map(|(i, p)| (i as PropertyId, p)))
    }

    #[inline]
    fn properties(&self) -> Vec<(PropertyId, Property)> {
        self.properties
            .iter()
            .enumerate()
            .map(|(i, p)| (i as PropertyId, p.clone()))
            .collect()
    }
}

#[derive(Debug)]
pub struct MemoryEdgeTypeCatalog {
    label_set: LabelSet,
    src: VertexTypeRef,
    dst: VertexTypeRef,
    properties: Vec<Property>,
}

impl MemoryEdgeTypeCatalog {
    #[inline]
    pub fn new(
        label_set: LabelSet,
        src: VertexTypeRef,
        dst: VertexTypeRef,
        properties: Vec<Property>,
    ) -> Self {
        Self {
            label_set,
            src,
            dst,
            properties,
        }
    }
}

impl EdgeTypeProvider for MemoryEdgeTypeCatalog {
    #[inline]
    fn label_set(&self) -> LabelSet {
        self.label_set.clone()
    }

    #[inline]
    fn src(&self) -> VertexTypeRef {
        self.src.clone()
    }

    #[inline]
    fn dst(&self) -> VertexTypeRef {
        self.dst.clone()
    }
}

impl PropertiesProvider for MemoryEdgeTypeCatalog {
    fn get_property(&self, name: &str) -> CatalogResult<Option<(PropertyId, &Property)>> {
        Ok(self
            .properties
            .iter()
            .enumerate()
            .find(|(_, p)| p.name() == name)
            .map(|(i, p)| (i as PropertyId, p)))
    }

    #[inline]
    fn properties(&self) -> Vec<(PropertyId, Property)> {
        self.properties
            .iter()
            .enumerate()
            .map(|(i, p)| (i as PropertyId, p.clone()))
            .collect()
    }
}
