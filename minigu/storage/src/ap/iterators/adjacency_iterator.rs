use std::num::NonZeroU32;

use minigu_common::types::{EdgeId, VertexId};
use minigu_transaction::Timestamp;

use crate::ap::olap_graph::{OlapEdge, OlapPropertyStore, OlapStorage, OlapStorageEdge};
use crate::common::iterators::AdjacencyIteratorTrait;
use crate::common::model::edge::Neighbor;
use crate::error::{StorageError, StorageResult};

const BLOCK_CAPACITY: usize = 256;

#[allow(dead_code)]
pub struct AdjacencyIterator<'a> {
    pub storage: &'a OlapStorage,
    // Vertex ID
    pub vertex_id: VertexId,
    // Index of the current block
    pub block_idx: usize,
    // Offset within block
    pub offset: usize,
}
impl Iterator for AdjacencyIterator<'_> {
    type Item = Result<OlapEdge, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.block_idx != usize::MAX {
            let temporary = self.storage.edges.read().unwrap();
            let block = match temporary.get(self.block_idx) {
                Some(block) => block,
                None => {
                    self.block_idx = usize::MAX;
                    return None;
                }
            };

            // Return if tombstone
            if block.is_tombstone {
                if block.pre_block_index.is_none() {
                    self.block_idx = usize::MAX;
                    return None;
                }
                self.block_idx = block.pre_block_index.unwrap();
                continue;
            }

            // Move to next block
            if self.offset == BLOCK_CAPACITY {
                self.offset = 0;
                self.block_idx = block.pre_block_index.unwrap_or(usize::MAX);
                continue;
            }

            if self.offset < BLOCK_CAPACITY {
                let raw: &OlapStorageEdge = &block.edges[self.offset];
                if raw.label_id == NonZeroU32::new(1) && raw.dst_id == 1 {
                    self.offset = 0;
                    self.block_idx = block.pre_block_index.unwrap_or(usize::MAX);
                    continue;
                }

                // Build edge result
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
                self.offset += 1;
                return Some(Ok(edge));
            }
            self.block_idx = block.pre_block_index.unwrap_or(usize::MAX);
        }
        None
    }
}

#[allow(dead_code)]
pub struct AdjacencyIteratorAtTs<'a> {
    pub storage: &'a OlapStorage,
    // Vertex ID
    pub vertex_id: VertexId,
    // Index of the current block
    pub block_idx: usize,
    // Offset within block
    pub offset: usize,
    pub txn_id: Option<Timestamp>,
    pub start_ts: Timestamp,
}
impl Iterator for AdjacencyIteratorAtTs<'_> {
    type Item = Result<OlapEdge, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.block_idx != usize::MAX {
            let temporary = self.storage.edges.read().unwrap();
            let block = match temporary.get(self.block_idx) {
                Some(block) => block,
                None => {
                    self.block_idx = usize::MAX;
                    return None;
                }
            };

            // Return if tombstone
            if block.is_tombstone {
                if block.pre_block_index.is_none() {
                    self.block_idx = usize::MAX;
                    return None;
                }
                self.block_idx = block.pre_block_index.unwrap();
                continue;
            }

            if block.min_ts.is_commit_ts() && self.start_ts.raw() < block.min_ts.raw() {
                self.block_idx = block.pre_block_index.unwrap_or(usize::MAX);
                self.offset = 0;
                continue;
            }

            // Move to next block
            if self.offset == BLOCK_CAPACITY {
                self.offset = 0;
                self.block_idx = block.pre_block_index.unwrap_or(usize::MAX);
                continue;
            }

            if self.offset < BLOCK_CAPACITY {
                let raw: &OlapStorageEdge = &block.edges[self.offset];
                // Scan next block once scanned empty edge
                if raw.label_id == NonZeroU32::new(1) && raw.dst_id == 1 {
                    self.offset = 0;
                    self.block_idx = block.pre_block_index.unwrap_or(usize::MAX);
                    continue;
                }

                // Visibility filtering by edge commit_ts using snapshot start_ts
                if raw.commit_ts.is_txn_id() {
                    if let Some(txn_id) = self.txn_id {
                        if raw.commit_ts != txn_id {
                            self.offset += 1;
                            continue;
                        }
                    } else {
                        self.offset += 1;
                        continue;
                    }
                } else if raw.commit_ts.raw() > self.start_ts.raw() {
                    self.offset += 1;
                    continue;
                }

                // Build edge result
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
                self.offset += 1;
                return Some(Ok(edge));
            }

            self.block_idx = block.pre_block_index.unwrap_or(usize::MAX);
        }
        None
    }
}

type OlapAdjacencyFilter<'a> = Box<dyn Fn(&Neighbor) -> bool + 'a>;

/// Wrapper around [`AdjacencyIterator`] that outputs common [`Neighbor`] and implements
/// [`AdjacencyIteratorTrait`], bridging AP storage to the shared iterator interface.
pub struct OlapAdjacencyIter<'a> {
    inner: AdjacencyIterator<'a>,
    current: Option<Neighbor>,
    filters: Vec<OlapAdjacencyFilter<'a>>,
}

impl<'a> OlapAdjacencyIter<'a> {
    pub fn new(inner: AdjacencyIterator<'a>) -> Self {
        OlapAdjacencyIter {
            inner,
            current: None,
            filters: Vec::new(),
        }
    }
}

impl Iterator for OlapAdjacencyIter<'_> {
    type Item = StorageResult<Neighbor>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.inner.by_ref() {
            let olap_e = match item {
                Ok(e) => e,
                Err(e) => return Some(Err(e)),
            };
            let neighbor = Neighbor::from(&olap_e);
            if self.filters.iter().all(|f| f(&neighbor)) {
                self.current = Some(neighbor);
                return Some(Ok(neighbor));
            }
        }
        self.current = None;
        None
    }
}

impl<'a> AdjacencyIteratorTrait<'a> for OlapAdjacencyIter<'a> {
    fn filter<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&Neighbor) -> bool + 'a,
        Self: Sized,
    {
        self.filters.push(Box::new(predicate));
        self
    }

    fn seek(&mut self, id: EdgeId) -> StorageResult<bool> {
        for result in self.by_ref() {
            match result {
                Ok(neighbor) if neighbor.eid() == id => return Ok(true),
                Err(e) => return Err(e),
                _ => continue,
            }
        }
        Ok(false)
    }

    fn current_entry(&self) -> Option<&Neighbor> {
        self.current.as_ref()
    }
}

/// Wrapper around [`AdjacencyIteratorAtTs`] that outputs common [`Neighbor`] and implements
/// [`AdjacencyIteratorTrait`], bridging timestamp-aware AP adjacency iteration to the
/// shared iterator interface.
pub struct OlapAdjacencyIterAtTs<'a> {
    inner: AdjacencyIteratorAtTs<'a>,
    current: Option<Neighbor>,
    filters: Vec<OlapAdjacencyFilter<'a>>,
}

impl<'a> OlapAdjacencyIterAtTs<'a> {
    pub fn new(inner: AdjacencyIteratorAtTs<'a>) -> Self {
        OlapAdjacencyIterAtTs {
            inner,
            current: None,
            filters: Vec::new(),
        }
    }
}

impl Iterator for OlapAdjacencyIterAtTs<'_> {
    type Item = StorageResult<Neighbor>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.inner.by_ref() {
            let olap_e = match item {
                Ok(e) => e,
                Err(e) => return Some(Err(e)),
            };
            let neighbor = Neighbor::from(&olap_e);
            if self.filters.iter().all(|f| f(&neighbor)) {
                self.current = Some(neighbor);
                return Some(Ok(neighbor));
            }
        }
        self.current = None;
        None
    }
}

impl<'a> AdjacencyIteratorTrait<'a> for OlapAdjacencyIterAtTs<'a> {
    fn filter<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&Neighbor) -> bool + 'a,
        Self: Sized,
    {
        self.filters.push(Box::new(predicate));
        self
    }

    fn seek(&mut self, id: EdgeId) -> StorageResult<bool> {
        for result in self.by_ref() {
            match result {
                Ok(neighbor) if neighbor.eid() == id => return Ok(true),
                Err(e) => return Err(e),
                _ => continue,
            }
        }
        Ok(false)
    }

    fn current_entry(&self) -> Option<&Neighbor> {
        self.current.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ap::iterators::vertex_iterator::AP_UNLABELED_LABEL;
    use crate::ap::olap_graph::OlapPropertyStore;

    #[test]
    fn test_ap_adjacency_to_neighbor() {
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
    fn test_ap_adjacency_none_label_to_unlabeled() {
        let olap_e = OlapEdge {
            eid: 1,
            label_id: None,
            src_id: 1,
            dst_id: 2,
            properties: OlapPropertyStore::default(),
        };
        let neighbor = Neighbor::from(&olap_e);
        assert_eq!(neighbor.label_id(), AP_UNLABELED_LABEL);
        assert_ne!(neighbor.label_id(), NonZeroU32::new(1).unwrap());
    }

    // ── OlapAdjacencyIter / OlapAdjacencyIterAtTs wrapper behaviour tests ──

    fn make_adjacency_storage(edges_data: Vec<(u64, Option<u32>, u64, u64)>) -> OlapStorage {
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
    fn test_olap_adjacency_iter_wrapper_next() {
        // eids: 100, 50, 200 — deliberately non-monotonic
        let storage = make_adjacency_storage(vec![
            (100, Some(200), 1, 20),
            (50, Some(200), 1, 10),
            (200, Some(200), 1, 30),
        ]);
        let inner = AdjacencyIterator {
            storage: &storage,
            vertex_id: 1,
            block_idx: 0,
            offset: 0,
        };
        let wrapper = OlapAdjacencyIter::new(inner);
        let results: Vec<_> = wrapper.collect();
        assert_eq!(results.len(), 3);
        let eids: Vec<u64> = results.iter().map(|r| r.as_ref().unwrap().eid()).collect();
        assert_eq!(eids, vec![100, 50, 200]);
    }

    #[test]
    fn test_olap_adjacency_iter_filter() {
        let storage = make_adjacency_storage(vec![
            (100, Some(200), 1, 20),
            (50, Some(300), 1, 10),
            (200, Some(200), 1, 30),
        ]);
        let inner = AdjacencyIterator {
            storage: &storage,
            vertex_id: 1,
            block_idx: 0,
            offset: 0,
        };
        let wrapper =
            AdjacencyIteratorTrait::filter(OlapAdjacencyIter::new(inner), |n: &Neighbor| {
                n.label_id() == NonZeroU32::new(200).unwrap()
            });
        let results: Vec<_> = wrapper.collect();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_olap_adjacency_iter_seek_non_monotonic() {
        // eids in non-monotonic order: 100, 50, 200
        let storage = make_adjacency_storage(vec![
            (100, Some(200), 1, 20),
            (50, Some(200), 1, 10),
            (200, Some(200), 1, 30),
        ]);
        let inner = AdjacencyIterator {
            storage: &storage,
            vertex_id: 1,
            block_idx: 0,
            offset: 0,
        };
        let mut wrapper = OlapAdjacencyIter::new(inner);
        let found = wrapper.seek(50).unwrap();
        assert!(found);
        assert_eq!(wrapper.current_entry().unwrap().eid(), 50);
    }

    #[test]
    fn test_olap_adjacency_iter_seek_not_found() {
        let storage = make_adjacency_storage(vec![(100, Some(200), 1, 20)]);
        let inner = AdjacencyIterator {
            storage: &storage,
            vertex_id: 1,
            block_idx: 0,
            offset: 0,
        };
        let mut wrapper = OlapAdjacencyIter::new(inner);
        let found = wrapper.seek(999).unwrap();
        assert!(!found);
        assert!(wrapper.current_entry().is_none());
    }

    #[test]
    fn test_olap_adjacency_iter_current_entry() {
        let storage = make_adjacency_storage(vec![(100, Some(200), 1, 20)]);
        let inner = AdjacencyIterator {
            storage: &storage,
            vertex_id: 1,
            block_idx: 0,
            offset: 0,
        };
        let mut wrapper = OlapAdjacencyIter::new(inner);
        let _ = wrapper.next();
        assert!(wrapper.current_entry().is_some());
        assert_eq!(wrapper.current_entry().unwrap().eid(), 100);
    }

    // ── OlapAdjacencyIterAtTs wrapper behaviour tests ──

    #[test]
    fn test_olap_adjacency_iter_at_ts_wrapper_next() {
        let storage = make_adjacency_storage(vec![(100, Some(200), 1, 20), (50, Some(200), 1, 10)]);
        let inner = AdjacencyIteratorAtTs {
            storage: &storage,
            vertex_id: 1,
            block_idx: 0,
            offset: 0,
            txn_id: None,
            start_ts: Timestamp::with_ts(100),
        };
        let wrapper = OlapAdjacencyIterAtTs::new(inner);
        let results: Vec<_> = wrapper.collect();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_olap_adjacency_iter_at_ts_filter() {
        let storage = make_adjacency_storage(vec![(100, Some(200), 1, 20), (50, Some(300), 1, 10)]);
        let inner = AdjacencyIteratorAtTs {
            storage: &storage,
            vertex_id: 1,
            block_idx: 0,
            offset: 0,
            txn_id: None,
            start_ts: Timestamp::with_ts(100),
        };
        let wrapper =
            AdjacencyIteratorTrait::filter(OlapAdjacencyIterAtTs::new(inner), |n: &Neighbor| {
                n.label_id() == NonZeroU32::new(200).unwrap()
            });
        let results: Vec<_> = wrapper.collect();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_olap_adjacency_iter_at_ts_seek_non_monotonic() {
        let storage = make_adjacency_storage(vec![(100, Some(200), 1, 20), (50, Some(200), 1, 10)]);
        let inner = AdjacencyIteratorAtTs {
            storage: &storage,
            vertex_id: 1,
            block_idx: 0,
            offset: 0,
            txn_id: None,
            start_ts: Timestamp::with_ts(100),
        };
        let mut wrapper = OlapAdjacencyIterAtTs::new(inner);
        let found = wrapper.seek(50).unwrap();
        assert!(found);
        assert_eq!(wrapper.current_entry().unwrap().eid(), 50);
    }
}
