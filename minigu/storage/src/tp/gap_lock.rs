use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};
use std::ops::Bound;
use std::sync::RwLock;

use dashmap::DashMap;
use minigu_common::types::{EdgeId, LabelId, VertexId};
use minigu_common::value::ScalarValue;

use crate::common::transaction::Timestamp;
use crate::error::{StorageError, StorageResult};

/// Entity identifier for locking purposes
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EntityId {
    Vertex(VertexId),
    Edge(EdgeId),
}

/// Type of entity for gap locking
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EntityType {
    Vertex,
    Edge,
}

/// Represents different types of gap ranges for locking
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapRange {
    /// Property value range for vertices/edges with specific label
    PropertyRange {
        entity_type: EntityType,
        label_id: LabelId,
        property_key: String,
        lower_bound: Bound<ScalarValue>,
        upper_bound: Bound<ScalarValue>,
    },
    /// ID range for vertices or edges
    IdRange {
        entity_type: EntityType,
        lower_bound: Bound<u64>,
        upper_bound: Bound<u64>,
    },
    /// Adjacency range for graph structure queries
    AdjacencyRange {
        src_vertex: VertexId,
        label_id: Option<LabelId>,
        dst_range: Option<Box<GapRange>>,
    },
    /// Label range for entities with specific labels
    LabelRange {
        entity_type: EntityType,
        label_ids: Vec<LabelId>,
    },
}

impl Hash for GapRange {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            GapRange::PropertyRange {
                entity_type,
                label_id,
                property_key,
                lower_bound,
                upper_bound,
            } => {
                0u8.hash(state);
                entity_type.hash(state);
                label_id.hash(state);
                property_key.hash(state);
                // Hash bounds by their discriminant and value
                match lower_bound {
                    Bound::Unbounded => 0u8.hash(state),
                    Bound::Included(v) => {
                        1u8.hash(state);
                        v.hash(state);
                    }
                    Bound::Excluded(v) => {
                        2u8.hash(state);
                        v.hash(state);
                    }
                }
                match upper_bound {
                    Bound::Unbounded => 0u8.hash(state),
                    Bound::Included(v) => {
                        1u8.hash(state);
                        v.hash(state);
                    }
                    Bound::Excluded(v) => {
                        2u8.hash(state);
                        v.hash(state);
                    }
                }
            }
            GapRange::IdRange {
                entity_type,
                lower_bound,
                upper_bound,
            } => {
                1u8.hash(state);
                entity_type.hash(state);
                match lower_bound {
                    Bound::Unbounded => 0u8.hash(state),
                    Bound::Included(v) => {
                        1u8.hash(state);
                        v.hash(state);
                    }
                    Bound::Excluded(v) => {
                        2u8.hash(state);
                        v.hash(state);
                    }
                }
                match upper_bound {
                    Bound::Unbounded => 0u8.hash(state),
                    Bound::Included(v) => {
                        1u8.hash(state);
                        v.hash(state);
                    }
                    Bound::Excluded(v) => {
                        2u8.hash(state);
                        v.hash(state);
                    }
                }
            }
            GapRange::AdjacencyRange {
                src_vertex,
                label_id,
                dst_range,
            } => {
                2u8.hash(state);
                src_vertex.hash(state);
                label_id.hash(state);
                dst_range.hash(state);
            }
            GapRange::LabelRange {
                entity_type,
                label_ids,
            } => {
                3u8.hash(state);
                entity_type.hash(state);
                label_ids.hash(state);
            }
        }
    }
}

impl PartialOrd for GapRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GapRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (
                GapRange::PropertyRange {
                    entity_type: et1,
                    label_id: lid1,
                    property_key: pk1,
                    ..
                },
                GapRange::PropertyRange {
                    entity_type: et2,
                    label_id: lid2,
                    property_key: pk2,
                    ..
                },
            ) => et1
                .cmp(et2)
                .then_with(|| lid1.cmp(lid2))
                .then_with(|| pk1.cmp(pk2)),
            (
                GapRange::IdRange {
                    entity_type: et1, ..
                },
                GapRange::IdRange {
                    entity_type: et2, ..
                },
            ) => et1.cmp(et2),
            (
                GapRange::AdjacencyRange {
                    src_vertex: sv1, ..
                },
                GapRange::AdjacencyRange {
                    src_vertex: sv2, ..
                },
            ) => sv1.cmp(sv2),
            (
                GapRange::LabelRange {
                    entity_type: et1, ..
                },
                GapRange::LabelRange {
                    entity_type: et2, ..
                },
            ) => et1.cmp(et2),
            (GapRange::PropertyRange { .. }, _) => Ordering::Less,
            (GapRange::IdRange { .. }, GapRange::PropertyRange { .. }) => Ordering::Greater,
            (GapRange::IdRange { .. }, _) => Ordering::Less,
            (GapRange::AdjacencyRange { .. }, GapRange::PropertyRange { .. }) => Ordering::Greater,
            (GapRange::AdjacencyRange { .. }, GapRange::IdRange { .. }) => Ordering::Greater,
            (GapRange::AdjacencyRange { .. }, _) => Ordering::Less,
            (GapRange::LabelRange { .. }, _) => Ordering::Greater,
        }
    }
}

impl GapRange {
    /// Check if this range overlaps with another range
    pub fn overlaps_with(&self, other: &GapRange) -> bool {
        match (self, other) {
            (
                GapRange::PropertyRange {
                    entity_type: et1,
                    label_id: lid1,
                    property_key: pk1,
                    lower_bound: lb1,
                    upper_bound: ub1,
                },
                GapRange::PropertyRange {
                    entity_type: et2,
                    label_id: lid2,
                    property_key: pk2,
                    lower_bound: lb2,
                    upper_bound: ub2,
                },
            ) => {
                et1 == et2 && lid1 == lid2 && pk1 == pk2 && Self::ranges_overlap(lb1, ub1, lb2, ub2)
            }
            (
                GapRange::IdRange {
                    entity_type: et1,
                    lower_bound: lb1,
                    upper_bound: ub1,
                },
                GapRange::IdRange {
                    entity_type: et2,
                    lower_bound: lb2,
                    upper_bound: ub2,
                },
            ) => et1 == et2 && Self::id_ranges_overlap(lb1, ub1, lb2, ub2),
            (
                GapRange::AdjacencyRange {
                    src_vertex: sv1,
                    label_id: lid1,
                    dst_range: dr1,
                },
                GapRange::AdjacencyRange {
                    src_vertex: sv2,
                    label_id: lid2,
                    dst_range: dr2,
                },
            ) => {
                sv1 == sv2
                    && lid1 == lid2
                    && match (dr1, dr2) {
                        (Some(r1), Some(r2)) => r1.overlaps_with(r2),
                        _ => true, // If either has no destination range constraint, they overlap
                    }
            }
            (
                GapRange::LabelRange {
                    entity_type: et1,
                    label_ids: lids1,
                },
                GapRange::LabelRange {
                    entity_type: et2,
                    label_ids: lids2,
                },
            ) => et1 == et2 && lids1.iter().any(|lid| lids2.contains(lid)),
            _ => false, // Different range types don't overlap
        }
    }

    /// Check if two scalar value ranges overlap
    fn ranges_overlap(
        _lb1: &Bound<ScalarValue>,
        _ub1: &Bound<ScalarValue>,
        _lb2: &Bound<ScalarValue>,
        _ub2: &Bound<ScalarValue>,
    ) -> bool {
        // For now, we conservatively assume all scalar value ranges overlap
        // This can be optimized later with proper ScalarValue comparison
        true
    }

    /// Check if two ID ranges overlap
    fn id_ranges_overlap(
        lb1: &Bound<u64>,
        ub1: &Bound<u64>,
        lb2: &Bound<u64>,
        ub2: &Bound<u64>,
    ) -> bool {
        use Bound::*;
        match (lb1, ub1, lb2, ub2) {
            (Unbounded, _, _, _) | (_, _, Unbounded, _) => true,
            (_, Unbounded, _, _) | (_, _, _, Unbounded) => true,
            // All included bounds
            (Included(l1), Included(u1), Included(l2), Included(u2)) => l1 <= u2 && l2 <= u1,
            // All excluded bounds
            (Excluded(l1), Excluded(u1), Excluded(l2), Excluded(u2)) => l1 < u2 && l2 < u1,
            // Mixed bounds - conservative approach: assume they overlap
            _ => true,
        }
    }
}

/// Type of lock
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockType {
    /// Record lock on a specific entity
    Record(EntityId),
    /// Gap lock on a range
    Gap(GapRange),
    /// Next-Key lock (combination of record lock and gap lock)
    NextKey(EntityId, GapRange),
}

impl Hash for LockType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            LockType::Record(entity) => {
                0u8.hash(state);
                entity.hash(state);
            }
            LockType::Gap(range) => {
                1u8.hash(state);
                range.hash(state);
            }
            LockType::NextKey(entity, range) => {
                2u8.hash(state);
                entity.hash(state);
                range.hash(state);
            }
        }
    }
}

impl LockType {
    /// Check if this lock conflicts with another lock
    pub fn conflicts_with(&self, other: &LockType) -> bool {
        match (self, other) {
            // Record locks conflict with record locks on the same entity
            (LockType::Record(e1), LockType::Record(e2)) => e1 == e2,
            // Record locks conflict with gaps that contain the record
            (LockType::Record(entity), LockType::Gap(range))
            | (LockType::Gap(range), LockType::Record(entity)) => {
                Self::record_in_gap_range(entity, range)
            }
            // Gap locks conflict if they overlap
            (LockType::Gap(r1), LockType::Gap(r2)) => r1.overlaps_with(r2),
            // Next-Key locks are complex combinations
            (LockType::NextKey(e1, r1), LockType::NextKey(e2, r2)) => {
                e1 == e2 || r1.overlaps_with(r2)
            }
            (LockType::NextKey(entity, range), LockType::Record(other_entity))
            | (LockType::Record(other_entity), LockType::NextKey(entity, range)) => {
                entity == other_entity || Self::record_in_gap_range(other_entity, range)
            }
            (LockType::NextKey(entity, range), LockType::Gap(other_range))
            | (LockType::Gap(other_range), LockType::NextKey(entity, range)) => {
                Self::record_in_gap_range(entity, other_range) || range.overlaps_with(other_range)
            }
        }
    }

    /// Check if a record entity falls within a gap range
    fn record_in_gap_range(entity: &EntityId, range: &GapRange) -> bool {
        match (entity, range) {
            (
                EntityId::Vertex(vid),
                GapRange::IdRange {
                    entity_type,
                    lower_bound,
                    upper_bound,
                },
            ) => {
                entity_type == &EntityType::Vertex
                    && Self::id_in_range(*vid, lower_bound, upper_bound)
            }
            (
                EntityId::Edge(eid),
                GapRange::IdRange {
                    entity_type,
                    lower_bound,
                    upper_bound,
                },
            ) => {
                entity_type == &EntityType::Edge
                    && Self::id_in_range(*eid, lower_bound, upper_bound)
            }
            // For other gap types, we would need additional entity metadata
            // This is a simplified implementation
            _ => false,
        }
    }

    /// Check if an ID falls within a range
    fn id_in_range(id: u64, lower: &Bound<u64>, upper: &Bound<u64>) -> bool {
        use Bound::*;
        let lower_ok = match lower {
            Unbounded => true,
            Included(l) => id >= *l,
            Excluded(l) => id > *l,
        };
        let upper_ok = match upper {
            Unbounded => true,
            Included(u) => id <= *u,
            Excluded(u) => id < *u,
        };
        lower_ok && upper_ok
    }
}

/// Lock manager for handling gap locks and record locks
pub struct LockManager {
    /// Active locks per transaction: txn_id -> set of locks
    active_locks: DashMap<Timestamp, HashSet<LockType>>,
    /// Gap lock index for fast conflict detection: range -> set of transaction IDs
    gap_index: RwLock<BTreeMap<GapRange, HashSet<Timestamp>>>,
    /// Record lock index: entity_id -> set of transaction IDs
    record_index: DashMap<EntityId, HashSet<Timestamp>>,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    /// Create a new lock manager
    pub fn new() -> Self {
        Self {
            active_locks: DashMap::new(),
            gap_index: RwLock::new(BTreeMap::new()),
            record_index: DashMap::new(),
        }
    }

    /// Acquire a lock for a transaction
    pub fn acquire_lock(&self, txn_id: Timestamp, lock: LockType) -> StorageResult<()> {
        // Check for conflicts with existing locks
        if self.has_conflict(&lock, txn_id)? {
            return Err(StorageError::Transaction(
                crate::error::TransactionError::LockConflict(format!(
                    "Transaction {:?} cannot acquire lock {:?} due to conflict",
                    txn_id, lock
                )),
            ));
        }

        // Add lock to transaction's lock set
        self.active_locks
            .entry(txn_id)
            .or_insert_with(HashSet::new)
            .insert(lock.clone());

        // Update indices
        match &lock {
            LockType::Record(entity) => {
                self.record_index
                    .entry(entity.clone())
                    .or_insert_with(HashSet::new)
                    .insert(txn_id);
            }
            LockType::Gap(range) => {
                self.gap_index
                    .write()
                    .unwrap()
                    .entry(range.clone())
                    .or_insert_with(HashSet::new)
                    .insert(txn_id);
            }
            LockType::NextKey(entity, range) => {
                self.record_index
                    .entry(entity.clone())
                    .or_insert_with(HashSet::new)
                    .insert(txn_id);
                self.gap_index
                    .write()
                    .unwrap()
                    .entry(range.clone())
                    .or_insert_with(HashSet::new)
                    .insert(txn_id);
            }
        }

        Ok(())
    }

    /// Release all locks for a transaction
    pub fn release_locks(&self, txn_id: Timestamp) -> StorageResult<()> {
        if let Some((_, locks)) = self.active_locks.remove(&txn_id) {
            for lock in locks {
                match lock {
                    LockType::Record(entity) => {
                        if let Some(mut txn_set) = self.record_index.get_mut(&entity) {
                            txn_set.remove(&txn_id);
                            if txn_set.is_empty() {
                                drop(txn_set);
                                self.record_index.remove(&entity);
                            }
                        }
                    }
                    LockType::Gap(range) => {
                        let mut gap_index = self.gap_index.write().unwrap();
                        if let Some(txn_set) = gap_index.get_mut(&range) {
                            txn_set.remove(&txn_id);
                            if txn_set.is_empty() {
                                gap_index.remove(&range);
                            }
                        }
                    }
                    LockType::NextKey(entity, range) => {
                        // Remove from both indices
                        if let Some(mut txn_set) = self.record_index.get_mut(&entity) {
                            txn_set.remove(&txn_id);
                            if txn_set.is_empty() {
                                drop(txn_set);
                                self.record_index.remove(&entity);
                            }
                        }
                        let mut gap_index = self.gap_index.write().unwrap();
                        if let Some(txn_set) = gap_index.get_mut(&range) {
                            txn_set.remove(&txn_id);
                            if txn_set.is_empty() {
                                gap_index.remove(&range);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Check if acquiring a lock would cause a conflict
    fn has_conflict(&self, lock: &LockType, requesting_txn: Timestamp) -> StorageResult<bool> {
        match lock {
            LockType::Record(entity) => {
                // Check for conflicting record locks
                if let Some(txn_set) = self.record_index.get(entity) {
                    for &txn_id in txn_set.iter() {
                        if txn_id != requesting_txn {
                            return Ok(true);
                        }
                    }
                }

                // Check for conflicting gap locks that cover this record
                let gap_index = self.gap_index.read().unwrap();
                for (existing_range, txn_set) in gap_index.iter() {
                    if LockType::record_in_gap_range(entity, existing_range) {
                        for &txn_id in txn_set {
                            if txn_id != requesting_txn {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
            LockType::Gap(range) => {
                // Check for overlapping gap locks
                let gap_index = self.gap_index.read().unwrap();
                for (existing_range, txn_set) in gap_index.iter() {
                    if range.overlaps_with(existing_range) {
                        for &txn_id in txn_set {
                            if txn_id != requesting_txn {
                                return Ok(true);
                            }
                        }
                    }
                }

                // Check for conflicting record locks within the gap
                for entry in self.record_index.iter() {
                    if LockType::record_in_gap_range(entry.key(), range) {
                        for &txn_id in entry.value() {
                            if txn_id != requesting_txn {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
            LockType::NextKey(entity, range) => {
                // Check both record and gap conflicts
                let record_lock = LockType::Record(entity.clone());
                let gap_lock = LockType::Gap(range.clone());
                return Ok(self.has_conflict(&record_lock, requesting_txn)?
                    || self.has_conflict(&gap_lock, requesting_txn)?);
            }
        }
        Ok(false)
    }

    /// Get all locks held by a transaction
    pub fn get_transaction_locks(&self, txn_id: Timestamp) -> Vec<LockType> {
        self.active_locks
            .get(&txn_id)
            .map(|locks| locks.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Check if a transaction holds a specific lock
    pub fn holds_lock(&self, txn_id: Timestamp, lock: &LockType) -> bool {
        self.active_locks
            .get(&txn_id)
            .map(|locks| locks.contains(lock))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::*;

    use super::*;

    #[test]
    fn test_gap_range_overlap() {
        let range1 = GapRange::IdRange {
            entity_type: EntityType::Vertex,
            lower_bound: Included(10),
            upper_bound: Included(20),
        };
        let range2 = GapRange::IdRange {
            entity_type: EntityType::Vertex,
            lower_bound: Included(15),
            upper_bound: Included(25),
        };
        let range3 = GapRange::IdRange {
            entity_type: EntityType::Vertex,
            lower_bound: Included(30),
            upper_bound: Included(40),
        };

        assert!(range1.overlaps_with(&range2));
        assert!(!range1.overlaps_with(&range3));
    }

    #[test]
    fn test_lock_conflict() {
        let entity = EntityId::Vertex(100);
        let record_lock1 = LockType::Record(entity.clone());
        let record_lock2 = LockType::Record(entity);

        assert!(record_lock1.conflicts_with(&record_lock2));

        let gap_range = GapRange::IdRange {
            entity_type: EntityType::Vertex,
            lower_bound: Included(50),
            upper_bound: Included(150),
        };
        let gap_lock = LockType::Gap(gap_range);
        let record_lock = LockType::Record(EntityId::Vertex(100));

        assert!(gap_lock.conflicts_with(&record_lock));
    }

    #[test]
    fn test_lock_manager() {
        let manager = LockManager::new();
        let txn1 = Timestamp::with_ts(1);
        let txn2 = Timestamp::with_ts(2);

        let entity = EntityId::Vertex(100);
        let lock1 = LockType::Record(entity.clone());
        let lock2 = LockType::Record(entity);

        // First transaction acquires the lock
        assert!(manager.acquire_lock(txn1, lock1).is_ok());

        // Second transaction should fail to acquire conflicting lock
        assert!(manager.acquire_lock(txn2, lock2).is_err());

        // Release locks for first transaction
        assert!(manager.release_locks(txn1).is_ok());

        // Now second transaction should be able to acquire the lock
        let lock3 = LockType::Record(EntityId::Vertex(100));
        assert!(manager.acquire_lock(txn2, lock3).is_ok());
    }
}
