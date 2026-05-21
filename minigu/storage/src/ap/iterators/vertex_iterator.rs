use std::num::NonZeroU32;
use std::sync::Arc;

use minigu_common::types::{LabelId, VertexId};

use super::adjacency_iterator::OlapAdjacencyIter;
use crate::ap::olap_graph::{OlapStorage, OlapVertex};
use crate::common::iterators::{ChunkData, VertexIteratorTrait};
use crate::common::model::vertex::Vertex;
use crate::error::{StorageError, StorageResult};

/// Sentinel label for AP vertices/edges that have no label information.
///
/// Uses NonZeroU32::MAX to avoid collision with real business labels (e.g. label 1 =
/// PERSON/FRIEND in tests). Note: u32::MAX is also used as `min_label_id` initialization
/// sentinel elsewhere in the codebase, so this is not entirely risk-free.
///
/// # Adapter-only invariant
/// This sentinel MUST only appear in common adapter output. It MUST NOT be:
/// - written back to AP storage
/// - used in AP block min/max label statistics
/// - exposed as a user-creatable label through the catalog
pub(crate) const AP_UNLABELED_LABEL: LabelId = match NonZeroU32::new(u32::MAX) {
    Some(v) => v,
    None => unreachable!(),
};

impl From<&OlapVertex> for Vertex {
    fn from(v: &OlapVertex) -> Self {
        Vertex {
            vid: v.vid,
            label_id: AP_UNLABELED_LABEL, // AP has no per-vertex label
            properties: v.properties.clone(),
            is_tombstone: false,
        }
    }
}

pub struct VertexIter<'a> {
    pub storage: &'a OlapStorage,
    // Vertex index
    pub idx: usize,
}

impl Iterator for VertexIter<'_> {
    type Item = Result<OlapVertex, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.storage.vertices.read().unwrap().len() {
            return None;
        }

        while self
            .storage
            .vertices
            .read()
            .unwrap()
            .get(self.idx)
            .is_none()
        {
            self.idx += 1;
        }

        let clone = self
            .storage
            .vertices
            .read()
            .unwrap()
            .get(self.idx)
            .cloned()?;
        self.idx += 1;
        Some(Ok(clone))
    }
}

type OlapVertexFilter<'a> = Box<dyn Fn(&Vertex) -> bool + 'a>;

/// Wrapper around [`VertexIter`] that outputs common [`Vertex`] and implements
/// [`VertexIteratorTrait`], bridging AP storage to the shared iterator interface.
pub struct OlapVertexIter<'a> {
    inner: VertexIter<'a>,
    current: Option<Vertex>,
    filters: Vec<OlapVertexFilter<'a>>,
}

impl<'a> OlapVertexIter<'a> {
    pub fn new(inner: VertexIter<'a>) -> Self {
        OlapVertexIter {
            inner,
            current: None,
            filters: Vec::new(),
        }
    }
}

impl Iterator for OlapVertexIter<'_> {
    type Item = StorageResult<Vertex>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.inner.by_ref() {
            let olap_v = match item {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };
            let vertex = Vertex::from(&olap_v);
            if self.filters.iter().all(|f| f(&vertex)) {
                self.current = Some(vertex.clone());
                return Some(Ok(vertex));
            }
        }
        self.current = None;
        None
    }
}

impl<'a> VertexIteratorTrait<'a> for OlapVertexIter<'a> {
    type AdjacencyIterator = OlapAdjacencyIter<'a>;

    fn filter<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&Vertex) -> bool + 'a,
    {
        self.filters.push(Box::new(predicate));
        self
    }

    fn seek(&mut self, id: VertexId) -> StorageResult<bool> {
        for result in self.by_ref() {
            match result {
                Ok(vertex) if vertex.vid() == id => return Ok(true),
                Err(e) => return Err(e),
                _ => continue,
            }
        }
        Ok(false)
    }

    fn vertex(&self) -> Option<&Vertex> {
        self.current.as_ref()
    }

    fn properties(&self) -> ChunkData {
        if let Some(v) = &self.current {
            vec![Arc::new(v.properties().clone())]
        } else {
            ChunkData::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use minigu_common::value::ScalarValue;

    use super::*;
    use crate::common::model::properties::PropertyRecord;

    #[test]
    fn test_ap_vertex_to_common_vertex_unlabeled() {
        let olap_v = OlapVertex {
            vid: 42,
            properties: PropertyRecord::new(vec![ScalarValue::UInt32(Some(100))]),
            block_offset: 0,
        };
        let vertex = Vertex::from(&olap_v);
        assert_eq!(vertex.vid(), 42);
        assert_eq!(vertex.label_id, AP_UNLABELED_LABEL);
        assert!(!vertex.is_tombstone());
        assert_eq!(vertex.properties(), &vec![ScalarValue::UInt32(Some(100))]);
    }

    #[test]
    fn test_ap_unlabeled_label_not_equal_to_label_one() {
        // AP_UNLABELED_LABEL (u32::MAX) must not collide with a real label like 1 (PERSON/FRIEND).
        let label_one = NonZeroU32::new(1).unwrap();
        assert_ne!(AP_UNLABELED_LABEL, label_one);
    }

    #[test]
    fn test_olap_vertex_iter_wrapper_next() {
        use std::sync::RwLock;
        use std::sync::atomic::AtomicU64;

        use dashmap::DashMap;

        use crate::ap::olap_graph::OlapStorage;

        let storage = OlapStorage {
            logic_id_counter: AtomicU64::new(0),
            edge_id_counter: AtomicU64::new(1),
            dense_id_map: DashMap::new(),
            edge_id_map: DashMap::new(),
            vertices: RwLock::new(vec![
                OlapVertex {
                    vid: 1,
                    properties: PropertyRecord::new(vec![ScalarValue::UInt32(Some(10))]),
                    block_offset: 0,
                },
                OlapVertex {
                    vid: 2,
                    properties: PropertyRecord::new(vec![ScalarValue::UInt32(Some(20))]),
                    block_offset: 0,
                },
            ]),
            edges: RwLock::new(Vec::new()),
            property_columns: RwLock::new(Vec::new()),
            is_edge_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_edges: RwLock::new(Vec::new()),
            is_property_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_properties: RwLock::new(vec![]),
        };

        let inner = VertexIter {
            storage: &storage,
            idx: 0,
        };
        let wrapper = OlapVertexIter::new(inner);
        let results: Vec<_> = wrapper.collect();
        assert_eq!(results.len(), 2);
        let v1 = results[0].as_ref().unwrap();
        assert_eq!(v1.vid(), 1);
        let v2 = results[1].as_ref().unwrap();
        assert_eq!(v2.vid(), 2);
    }

    #[test]
    fn test_olap_vertex_iter_filter() {
        use std::sync::RwLock;
        use std::sync::atomic::AtomicU64;

        use dashmap::DashMap;

        use crate::ap::olap_graph::OlapStorage;

        let storage = OlapStorage {
            logic_id_counter: AtomicU64::new(0),
            edge_id_counter: AtomicU64::new(1),
            dense_id_map: DashMap::new(),
            edge_id_map: DashMap::new(),
            vertices: RwLock::new(vec![
                OlapVertex {
                    vid: 1,
                    properties: PropertyRecord::new(vec![ScalarValue::UInt32(Some(10))]),
                    block_offset: 0,
                },
                OlapVertex {
                    vid: 2,
                    properties: PropertyRecord::new(vec![ScalarValue::UInt32(Some(20))]),
                    block_offset: 0,
                },
                OlapVertex {
                    vid: 3,
                    properties: PropertyRecord::new(vec![ScalarValue::UInt32(Some(30))]),
                    block_offset: 0,
                },
            ]),
            edges: RwLock::new(Vec::new()),
            property_columns: RwLock::new(Vec::new()),
            is_edge_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_edges: RwLock::new(Vec::new()),
            is_property_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_properties: RwLock::new(vec![]),
        };

        let inner = VertexIter {
            storage: &storage,
            idx: 0,
        };
        let wrapper =
            VertexIteratorTrait::filter(OlapVertexIter::new(inner), |v: &Vertex| v.vid() > 1);
        let results: Vec<_> = wrapper.collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().vid(), 2);
        assert_eq!(results[1].as_ref().unwrap().vid(), 3);
    }

    #[test]
    fn test_olap_vertex_iter_seek() {
        use std::sync::RwLock;
        use std::sync::atomic::AtomicU64;

        use dashmap::DashMap;

        use crate::ap::olap_graph::OlapStorage;

        let storage = OlapStorage {
            logic_id_counter: AtomicU64::new(0),
            edge_id_counter: AtomicU64::new(1),
            dense_id_map: DashMap::new(),
            edge_id_map: DashMap::new(),
            vertices: RwLock::new(vec![
                OlapVertex {
                    vid: 1,
                    properties: PropertyRecord::new(vec![]),
                    block_offset: 0,
                },
                OlapVertex {
                    vid: 5,
                    properties: PropertyRecord::new(vec![]),
                    block_offset: 0,
                },
                OlapVertex {
                    vid: 10,
                    properties: PropertyRecord::new(vec![]),
                    block_offset: 0,
                },
            ]),
            edges: RwLock::new(Vec::new()),
            property_columns: RwLock::new(Vec::new()),
            is_edge_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_edges: RwLock::new(Vec::new()),
            is_property_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_properties: RwLock::new(vec![]),
        };

        let inner = VertexIter {
            storage: &storage,
            idx: 0,
        };
        let mut wrapper = OlapVertexIter::new(inner);
        let found = wrapper.seek(5).unwrap();
        assert!(found);
        assert_eq!(wrapper.vertex().unwrap().vid(), 5);
    }

    #[test]
    fn test_olap_vertex_iter_vertex_and_properties() {
        use std::sync::RwLock;
        use std::sync::atomic::AtomicU64;

        use dashmap::DashMap;

        use crate::ap::olap_graph::OlapStorage;

        let storage = OlapStorage {
            logic_id_counter: AtomicU64::new(0),
            edge_id_counter: AtomicU64::new(1),
            dense_id_map: DashMap::new(),
            edge_id_map: DashMap::new(),
            vertices: RwLock::new(vec![OlapVertex {
                vid: 1,
                properties: PropertyRecord::new(vec![ScalarValue::String(Some(
                    "test".to_string(),
                ))]),
                block_offset: 0,
            }]),
            edges: RwLock::new(Vec::new()),
            property_columns: RwLock::new(Vec::new()),
            is_edge_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_edges: RwLock::new(Vec::new()),
            is_property_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_properties: RwLock::new(vec![]),
        };

        let inner = VertexIter {
            storage: &storage,
            idx: 0,
        };
        let mut wrapper = OlapVertexIter::new(inner);
        let _ = wrapper.next();
        assert!(wrapper.vertex().is_some());
        let props = wrapper.properties();
        assert!(!props.is_empty());
    }

    #[test]
    fn test_olap_vertex_iter_seek_non_monotonic() {
        // Vertices stored in insertion order, NOT vid order.
        // vids: 50, 10, 100 — seek(10) must find it even though 50 > 10 appears first.
        use std::sync::RwLock;
        use std::sync::atomic::AtomicU64;

        use dashmap::DashMap;

        use crate::ap::olap_graph::OlapStorage;

        let storage = OlapStorage {
            logic_id_counter: AtomicU64::new(0),
            edge_id_counter: AtomicU64::new(1),
            dense_id_map: DashMap::new(),
            edge_id_map: DashMap::new(),
            vertices: RwLock::new(vec![
                OlapVertex {
                    vid: 50,
                    properties: PropertyRecord::new(vec![]),
                    block_offset: 0,
                },
                OlapVertex {
                    vid: 10,
                    properties: PropertyRecord::new(vec![]),
                    block_offset: 0,
                },
                OlapVertex {
                    vid: 100,
                    properties: PropertyRecord::new(vec![]),
                    block_offset: 0,
                },
            ]),
            edges: RwLock::new(Vec::new()),
            property_columns: RwLock::new(Vec::new()),
            is_edge_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_edges: RwLock::new(Vec::new()),
            is_property_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_properties: RwLock::new(vec![]),
        };

        let inner = VertexIter {
            storage: &storage,
            idx: 0,
        };
        let mut wrapper = OlapVertexIter::new(inner);
        // seek vid=10 which appears AFTER vid=50 in storage order
        let found = wrapper.seek(10).unwrap();
        assert!(found);
        assert_eq!(wrapper.vertex().unwrap().vid(), 10);
    }

    #[test]
    fn test_common_filter_by_unlabeled_label() {
        // filter(|v| v.label_id == AP_UNLABELED_LABEL) should match all AP
        // vertices (since all AP vertices lack a real label), while
        // filter(|v| v.label_id == label_1) should match none.
        use std::sync::RwLock;
        use std::sync::atomic::AtomicU64;

        use dashmap::DashMap;

        use crate::ap::olap_graph::OlapStorage;

        let storage = OlapStorage {
            logic_id_counter: AtomicU64::new(0),
            edge_id_counter: AtomicU64::new(1),
            dense_id_map: DashMap::new(),
            edge_id_map: DashMap::new(),
            vertices: RwLock::new(vec![
                OlapVertex {
                    vid: 1,
                    properties: PropertyRecord::new(vec![]),
                    block_offset: 0,
                },
                OlapVertex {
                    vid: 2,
                    properties: PropertyRecord::new(vec![]),
                    block_offset: 0,
                },
            ]),
            edges: RwLock::new(Vec::new()),
            property_columns: RwLock::new(Vec::new()),
            is_edge_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_edges: RwLock::new(Vec::new()),
            is_property_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_properties: RwLock::new(vec![]),
        };

        // All AP vertices should match AP_UNLABELED_LABEL filter
        let all = VertexIteratorTrait::filter(
            OlapVertexIter::new(VertexIter {
                storage: &storage,
                idx: 0,
            }),
            |v: &Vertex| v.label_id == AP_UNLABELED_LABEL,
        );
        assert_eq!(all.count(), 2);

        // A real label (e.g. 1 for PERSON/FRIEND) should match no AP vertices
        let label_1 = NonZeroU32::new(1).unwrap();
        let none = VertexIteratorTrait::filter(
            OlapVertexIter::new(VertexIter {
                storage: &storage,
                idx: 0,
            }),
            |v: &Vertex| v.label_id == label_1,
        );
        assert_eq!(none.count(), 0);
    }
}
