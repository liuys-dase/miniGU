use std::num::NonZeroU32;
use std::sync::Arc;

use minigu_common::types::EdgeId;
use minigu_common::value::ScalarValue;
use minigu_transaction::Timestamp;

use super::vertex_iterator::AP_UNLABELED_LABEL;
use crate::ap::olap_graph::{OlapEdge, OlapPropertyStore, OlapStorage, OlapStorageEdge};
use crate::common::iterators::{ChunkData, EdgeIteratorTrait};
use crate::common::model::edge::{Edge, Neighbor};
use crate::common::model::properties::PropertyRecord;
use crate::error::{StorageError, StorageResult};

const BLOCK_CAPACITY: usize = 256;

impl From<&OlapEdge> for Edge {
    fn from(e: &OlapEdge) -> Self {
        Edge {
            label_id: e.label_id.unwrap_or(AP_UNLABELED_LABEL),
            src_id: e.src_id,
            dst_id: e.dst_id,
            eid: e.eid,
            // Preserve column positions: map None → ScalarValue::Null
            // instead of filter_map, to keep property indices aligned.
            properties: PropertyRecord::new(
                e.properties
                    .properties
                    .iter()
                    .map(|v| v.clone().unwrap_or(ScalarValue::Null))
                    .collect(),
            ),
            is_tombstone: false,
        }
    }
}

impl From<&OlapEdge> for Neighbor {
    fn from(e: &OlapEdge) -> Self {
        Neighbor::new(e.label_id.unwrap_or(AP_UNLABELED_LABEL), e.dst_id, e.eid)
    }
}

pub struct EdgeIter<'a> {
    pub storage: &'a OlapStorage,
    // Index of the current block
    pub block_idx: usize,
    // Offset within block
    pub offset: usize,
}
impl Iterator for EdgeIter<'_> {
    type Item = Result<OlapEdge, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        // 1. Scan Block
        let edges = self.storage.edges.read().unwrap();
        while self.block_idx < edges.len() {
            // 1.1 If none,move to next block
            let borrow = self.storage.edges.read().unwrap();
            let block = match borrow.get(self.block_idx) {
                Some(block) => block,
                None => {
                    self.block_idx += 1;
                    self.offset = 0;
                    continue;
                }
            };
            if block.is_tombstone {
                self.block_idx += 1;
                self.offset = 0;
                continue;
            }
            // 1.2 If one block has been finished,move to next
            if self.offset == BLOCK_CAPACITY {
                self.offset = 0;
                self.block_idx += 1;
                continue;
            }
            // 2. Scan within block
            if self.offset < block.edges.len() {
                let raw: &OlapStorageEdge = &block.edges[self.offset];
                // 2.1 Scan next block once scanned empty edge
                if raw.label_id == NonZeroU32::new(1) && raw.dst_id == 1 {
                    self.offset = 0;
                    self.block_idx += 1;
                    continue;
                }
                // 2.2 Build edge result
                let edge = OlapEdge {
                    eid: raw.eid,
                    label_id: raw.label_id,
                    src_id: block.src_id,
                    dst_id: raw.dst_id,
                    properties: {
                        let mut props = OlapPropertyStore::default();

                        for (col_idx, column) in self
                            .storage
                            .property_columns
                            .read()
                            .unwrap()
                            .iter()
                            .enumerate()
                        {
                            if let Some(val) = column
                                .blocks
                                .get(self.block_idx)
                                .and_then(|blk| blk.values.get(self.offset))
                                .and_then(|versions| {
                                    crate::ap::olap_graph::latest_committed_prop_value(versions)
                                })
                            {
                                props.set_prop(col_idx, Some(val));
                            }
                        }
                        props
                    },
                };
                // 2.3 Increase offset
                self.offset += 1;
                return Some(Ok(edge));
            }
        }
        None
    }
}

pub struct EdgeIterAtTs<'a> {
    pub storage: &'a OlapStorage,
    // Index of the current block
    pub block_idx: usize,
    // Offset within block
    pub offset: usize,
    pub txn_id: Option<Timestamp>,
    pub start_ts: Timestamp,
}
impl Iterator for EdgeIterAtTs<'_> {
    type Item = Result<OlapEdge, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        // 1. Scan Block
        let edges = self.storage.edges.read().unwrap();
        while self.block_idx < edges.len() {
            // 1.1 If none,move to next block
            let borrow = self.storage.edges.read().unwrap();
            let block = match borrow.get(self.block_idx) {
                Some(block) => block,
                None => {
                    self.block_idx += 1;
                    self.offset = 0;
                    continue;
                }
            };
            if block.is_tombstone {
                self.block_idx += 1;
                self.offset = 0;
                continue;
            }
            // Block-level timestamp filter
            if block.min_ts.is_commit_ts() && self.start_ts.raw() < block.min_ts.raw() {
                self.block_idx += 1;
                self.offset = 0;
                continue;
            }
            // 1.2 If one block has been finished,move to next
            if self.offset == BLOCK_CAPACITY {
                self.offset = 0;
                self.block_idx += 1;
                continue;
            }
            // 2. Scan within block
            if self.offset < block.edges.len() {
                let raw: &OlapStorageEdge = &block.edges[self.offset];
                // 2.1 Determine logical end of block and skip tombstones
                // Use eid == 0 as the end-of-block sentinel to avoid
                // confusing transactional tombstones with padding.
                if raw.eid == 0 {
                    // No more valid edges in this block; move to next block.
                    self.offset = 0;
                    self.block_idx += 1;
                    continue;
                }
                // Transactional delete tombstone: skip this edge but continue scanning.
                if raw.label_id == NonZeroU32::new(1) && raw.dst_id == 1 {
                    self.offset += 1;
                    continue;
                }
                // 2.2 Visibility filtering by edge commit_ts
                let is_visible = if raw.commit_ts.is_txn_id() {
                    self.txn_id == Some(raw.commit_ts)
                } else {
                    raw.commit_ts.raw() <= self.start_ts.raw()
                };

                if !is_visible {
                    self.offset += 1;
                    continue;
                }

                // 2.3 Build edge result
                let edge = OlapEdge {
                    eid: raw.eid,
                    label_id: raw.label_id,
                    src_id: block.src_id,
                    dst_id: raw.dst_id,
                    properties: {
                        let mut props = OlapPropertyStore::default();

                        for (col_idx, column) in self
                            .storage
                            .property_columns
                            .read()
                            .unwrap()
                            .iter()
                            .enumerate()
                        {
                            if let Some(val) = column
                                .blocks
                                .get(self.block_idx)
                                .and_then(|blk| blk.values.get(self.offset))
                                .and_then(|versions| {
                                    crate::ap::olap_graph::prop_value_visible_at(
                                        versions,
                                        self.txn_id,
                                        self.start_ts,
                                    )
                                })
                            {
                                props.set_prop(col_idx, Some(val));
                            }
                        }
                        props
                    },
                };
                // 2.4 Increase offset
                self.offset += 1;
                return Some(Ok(edge));
            }
        }
        None
    }
}

type OlapEdgeFilter<'a> = Box<dyn Fn(&Edge) -> bool + 'a>;

/// Wrapper around [`EdgeIter`] that outputs common [`Edge`] and implements
/// [`EdgeIteratorTrait`], bridging AP storage to the shared iterator interface.
pub struct OlapEdgeIter<'a> {
    inner: EdgeIter<'a>,
    current: Option<Edge>,
    filters: Vec<OlapEdgeFilter<'a>>,
}

impl<'a> OlapEdgeIter<'a> {
    pub fn new(inner: EdgeIter<'a>) -> Self {
        OlapEdgeIter {
            inner,
            current: None,
            filters: Vec::new(),
        }
    }
}

impl Iterator for OlapEdgeIter<'_> {
    type Item = StorageResult<Edge>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.inner.by_ref() {
            let olap_e = match item {
                Ok(e) => e,
                Err(e) => return Some(Err(e)),
            };
            let edge = Edge::from(&olap_e);
            if self.filters.iter().all(|f| f(&edge)) {
                self.current = Some(edge.clone());
                return Some(Ok(edge));
            }
        }
        self.current = None;
        None
    }
}

impl<'a> EdgeIteratorTrait<'a> for OlapEdgeIter<'a> {
    fn filter<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&Edge) -> bool + 'a,
        Self: Sized,
    {
        self.filters.push(Box::new(predicate));
        self
    }

    fn seek(&mut self, id: EdgeId) -> StorageResult<bool> {
        for result in self.by_ref() {
            match result {
                Ok(edge) if edge.eid() == id => return Ok(true),
                Err(e) => return Err(e),
                _ => continue,
            }
        }
        Ok(false)
    }

    fn edge(&self) -> Option<&Edge> {
        self.current.as_ref()
    }

    fn properties(&self) -> ChunkData {
        if let Some(e) = &self.current {
            vec![Arc::new(e.properties().clone())]
        } else {
            ChunkData::new()
        }
    }
}

/// Wrapper around [`EdgeIterAtTs`] that outputs common [`Edge`] and implements
/// [`EdgeIteratorTrait`], bridging timestamp-aware AP edge iteration to the shared
/// iterator interface.
pub struct OlapEdgeIterAtTs<'a> {
    inner: EdgeIterAtTs<'a>,
    current: Option<Edge>,
    filters: Vec<OlapEdgeFilter<'a>>,
}

impl<'a> OlapEdgeIterAtTs<'a> {
    pub fn new(inner: EdgeIterAtTs<'a>) -> Self {
        OlapEdgeIterAtTs {
            inner,
            current: None,
            filters: Vec::new(),
        }
    }
}

impl Iterator for OlapEdgeIterAtTs<'_> {
    type Item = StorageResult<Edge>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.inner.by_ref() {
            let olap_e = match item {
                Ok(e) => e,
                Err(e) => return Some(Err(e)),
            };
            let edge = Edge::from(&olap_e);
            if self.filters.iter().all(|f| f(&edge)) {
                self.current = Some(edge.clone());
                return Some(Ok(edge));
            }
        }
        self.current = None;
        None
    }
}

impl<'a> EdgeIteratorTrait<'a> for OlapEdgeIterAtTs<'a> {
    fn filter<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&Edge) -> bool + 'a,
        Self: Sized,
    {
        self.filters.push(Box::new(predicate));
        self
    }

    fn seek(&mut self, id: EdgeId) -> StorageResult<bool> {
        for result in self.by_ref() {
            match result {
                Ok(edge) if edge.eid() == id => return Ok(true),
                Err(e) => return Err(e),
                _ => continue,
            }
        }
        Ok(false)
    }

    fn edge(&self) -> Option<&Edge> {
        self.current.as_ref()
    }

    fn properties(&self) -> ChunkData {
        if let Some(e) = &self.current {
            vec![Arc::new(e.properties().clone())]
        } else {
            ChunkData::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use minigu_common::value::ScalarValue;

    use super::*;

    #[test]
    fn test_ap_edge_to_common_edge_with_eid() {
        let olap_e = OlapEdge {
            eid: 42,
            label_id: NonZeroU32::new(100),
            src_id: 1,
            dst_id: 2,
            properties: OlapPropertyStore::new(vec![Some(ScalarValue::UInt32(Some(99)))]),
        };
        let edge = Edge::from(&olap_e);
        assert_eq!(edge.eid(), 42);
        assert_eq!(edge.src_id(), 1);
        assert_eq!(edge.dst_id(), 2);
        assert_eq!(edge.label_id(), NonZeroU32::new(100).unwrap());
        assert!(!edge.is_tombstone());
    }

    #[test]
    fn test_ap_edge_none_label_converts_to_unlabeled() {
        let olap_e = OlapEdge {
            eid: 1,
            label_id: None,
            src_id: 1,
            dst_id: 2,
            properties: OlapPropertyStore::default(),
        };
        let edge = Edge::from(&olap_e);
        assert_eq!(edge.label_id(), AP_UNLABELED_LABEL);
        // Must not equal a real label like 1 (PERSON/FRIEND).
        assert_ne!(edge.label_id(), NonZeroU32::new(1).unwrap());
    }

    #[test]
    fn test_ap_edge_to_neighbor() {
        let olap_e = OlapEdge {
            eid: 7,
            label_id: NonZeroU32::new(200),
            src_id: 10,
            dst_id: 20,
            properties: OlapPropertyStore::default(),
        };
        let neighbor = Neighbor::from(&olap_e);
        assert_eq!(neighbor.eid(), 7);
        assert_eq!(neighbor.neighbor_id(), 20);
        assert_eq!(neighbor.label_id(), NonZeroU32::new(200).unwrap());
    }

    #[test]
    fn test_ap_edge_none_label_to_neighbor_unlabeled() {
        let olap_e = OlapEdge {
            eid: 1,
            label_id: None,
            src_id: 1,
            dst_id: 2,
            properties: OlapPropertyStore::default(),
        };
        let neighbor = Neighbor::from(&olap_e);
        assert_eq!(neighbor.label_id(), AP_UNLABELED_LABEL);
    }

    #[test]
    fn test_ap_property_null_preserves_column_index() {
        // None in OlapPropertyStore should become ScalarValue::Null (not skip),
        // so column positions are preserved.
        let olap_e = OlapEdge {
            eid: 1,
            label_id: None,
            src_id: 1,
            dst_id: 2,
            properties: OlapPropertyStore::new(vec![
                Some(ScalarValue::UInt32(Some(10))),
                None, // column 1 — should become Null, not removed
                Some(ScalarValue::String(Some("hello".to_string()))), // column 2
            ]),
        };
        let edge = Edge::from(&olap_e);
        let props = edge.properties();
        // All 3 columns preserved.
        assert_eq!(props.len(), 3);
        assert_eq!(props[0], ScalarValue::UInt32(Some(10)));
        assert_eq!(props[1], ScalarValue::Null);
        assert_eq!(props[2], ScalarValue::String(Some("hello".to_string())));
    }

    // ── OlapEdgeIter wrapper behaviour tests ──

    fn make_edge_storage(edges_data: Vec<(u64, Option<u32>, u64, u64)>) -> OlapStorage {
        // edges_data: (eid, label_id, src_id, dst_id)
        use std::sync::RwLock;
        use std::sync::atomic::AtomicU64;

        use dashmap::DashMap;

        use crate::ap::olap_graph::{BLOCK_CAPACITY, EdgeBlock, OlapStorageEdge};

        let mut storage_edges: [OlapStorageEdge; BLOCK_CAPACITY] = [OlapStorageEdge {
            eid: 0,
            label_id: NonZeroU32::new(1),
            dst_id: 1,
            commit_ts: Timestamp::with_ts(0),
        }; BLOCK_CAPACITY];

        let edge_count = edges_data.len();
        for (i, (eid, label_id, _src_id, dst_id)) in edges_data.iter().enumerate() {
            storage_edges[i] = OlapStorageEdge {
                eid: *eid,
                label_id: label_id.and_then(NonZeroU32::new),
                dst_id: *dst_id,
                commit_ts: Timestamp::with_ts(0),
            };
        }

        let src_id = edges_data.first().map(|e| e.2).unwrap_or(1);

        OlapStorage {
            logic_id_counter: AtomicU64::new(0),
            edge_id_counter: AtomicU64::new(1),
            dense_id_map: DashMap::new(),
            edge_id_map: DashMap::new(),
            vertices: RwLock::new(Vec::new()),
            edges: RwLock::new(vec![EdgeBlock {
                pre_block_index: None,
                cur_block_index: 0,
                is_tombstone: false,
                max_label_id: NonZeroU32::new(200),
                min_label_id: NonZeroU32::new(100),
                max_dst_id: 100,
                min_dst_id: 1,
                min_ts: Timestamp::with_ts(0),
                max_ts: Timestamp::with_ts(0),
                src_id,
                edge_counter: edge_count,
                edges: storage_edges,
            }]),
            property_columns: RwLock::new(Vec::new()),
            is_edge_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_edges: RwLock::new(Vec::new()),
            is_property_compressed: std::sync::atomic::AtomicBool::new(false),
            compressed_properties: RwLock::new(Vec::new()),
        }
    }

    #[test]
    fn test_olap_edge_iter_wrapper_next() {
        // eids: 100, 50, 200 — deliberately non-monotonic by eid
        let storage = make_edge_storage(vec![
            (100, Some(200), 1, 20),
            (50, Some(200), 1, 10),
            (200, Some(200), 1, 30),
        ]);
        let inner = EdgeIter {
            storage: &storage,
            block_idx: 0,
            offset: 0,
        };
        let wrapper = OlapEdgeIter::new(inner);
        let results: Vec<_> = wrapper.collect();
        assert_eq!(results.len(), 3);
        let eids: Vec<u64> = results.iter().map(|r| r.as_ref().unwrap().eid()).collect();
        // iteration order is block storage order, not eid order
        assert_eq!(eids, vec![100, 50, 200]);
    }

    #[test]
    fn test_olap_edge_iter_filter() {
        let storage = make_edge_storage(vec![
            (100, Some(200), 1, 20),
            (50, Some(300), 1, 10),
            (200, Some(200), 1, 30),
        ]);
        let inner = EdgeIter {
            storage: &storage,
            block_idx: 0,
            offset: 0,
        };
        let wrapper = EdgeIteratorTrait::filter(OlapEdgeIter::new(inner), |e: &Edge| {
            e.label_id() == NonZeroU32::new(200).unwrap()
        });
        let results: Vec<_> = wrapper.collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().eid(), 100);
        assert_eq!(results[1].as_ref().unwrap().eid(), 200);
    }

    #[test]
    fn test_olap_edge_iter_seek_non_monotonic() {
        // eids are NOT in order: 100, 50, 200
        let storage = make_edge_storage(vec![
            (100, Some(200), 1, 20),
            (50, Some(200), 1, 10),
            (200, Some(200), 1, 30),
        ]);
        let inner = EdgeIter {
            storage: &storage,
            block_idx: 0,
            offset: 0,
        };
        let mut wrapper = OlapEdgeIter::new(inner);
        // seek eid=50 which sits AFTER eid=100 in iteration order
        let found = wrapper.seek(50).unwrap();
        assert!(found);
        assert_eq!(wrapper.edge().unwrap().eid(), 50);
    }

    #[test]
    fn test_olap_edge_iter_seek_not_found() {
        let storage = make_edge_storage(vec![(100, Some(200), 1, 20), (50, Some(200), 1, 10)]);
        let inner = EdgeIter {
            storage: &storage,
            block_idx: 0,
            offset: 0,
        };
        let mut wrapper = OlapEdgeIter::new(inner);
        let found = wrapper.seek(999).unwrap();
        assert!(!found);
        assert!(wrapper.edge().is_none());
    }

    #[test]
    fn test_olap_edge_iter_edge_and_properties() {
        let storage = make_edge_storage(vec![(100, Some(200), 1, 20)]);
        // Need properties too — use property_columns
        // Drop storage and make a new one with property columns
        std::mem::drop(storage);
        let storage = make_edge_storage(vec![(100, Some(200), 1, 20)]);
        let inner = EdgeIter {
            storage: &storage,
            block_idx: 0,
            offset: 0,
        };
        let mut wrapper = OlapEdgeIter::new(inner);
        let _ = wrapper.next();
        assert!(wrapper.edge().is_some());
        assert_eq!(wrapper.edge().unwrap().eid(), 100);
        let props = wrapper.properties();
        assert!(!props.is_empty());
    }

    // ── OlapEdgeIterAtTs wrapper behaviour tests ──

    #[test]
    fn test_olap_edge_iter_at_ts_wrapper_next() {
        let storage = make_edge_storage(vec![(100, Some(200), 1, 20), (50, Some(200), 1, 10)]);
        let inner = EdgeIterAtTs {
            storage: &storage,
            block_idx: 0,
            offset: 0,
            txn_id: None,
            start_ts: Timestamp::with_ts(100),
        };
        let wrapper = OlapEdgeIterAtTs::new(inner);
        let results: Vec<_> = wrapper.collect();
        // All edges have commit_ts=0 which is <= start_ts=100, so all visible
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_olap_edge_iter_at_ts_filter() {
        let storage = make_edge_storage(vec![(100, Some(200), 1, 20), (50, Some(300), 1, 10)]);
        let inner = EdgeIterAtTs {
            storage: &storage,
            block_idx: 0,
            offset: 0,
            txn_id: None,
            start_ts: Timestamp::with_ts(100),
        };
        let wrapper =
            EdgeIteratorTrait::filter(OlapEdgeIterAtTs::new(inner), |e: &Edge| e.src_id() == 1);
        let results: Vec<_> = wrapper.collect();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_olap_edge_iter_at_ts_seek_non_monotonic() {
        let storage = make_edge_storage(vec![(100, Some(200), 1, 20), (50, Some(200), 1, 10)]);
        let inner = EdgeIterAtTs {
            storage: &storage,
            block_idx: 0,
            offset: 0,
            txn_id: None,
            start_ts: Timestamp::with_ts(100),
        };
        let mut wrapper = OlapEdgeIterAtTs::new(inner);
        // seek should find eid=50 even though it comes after eid=100
        let found = wrapper.seek(50).unwrap();
        assert!(found);
        assert_eq!(wrapper.edge().unwrap().eid(), 50);
    }
}
