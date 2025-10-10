use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use minigu_transaction::{Timestamp, Transaction, global_timestamp_generator};

use crate::txn::catalog_txn::CatalogTxn;
use crate::txn::error::CatalogTxnError;
use crate::txn::versioned::{CatalogVersionChain, CatalogVersionNode};

/// Type of write operation (used for commit validation)
#[derive(Clone, Copy, Debug)]
pub enum WriteOp {
    Create,
    Delete,
    Replace,
}

/// Touched item: records the written node, operation, and expected base version
pub struct TouchedItem<K, V> {
    pub key: K,
    pub node: Arc<CatalogVersionNode<V>>,
    pub op: WriteOp,
}

pub struct VersionedMap<K, V>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    V: Send + Sync + 'static,
{
    inner: RwLock<HashMap<K, CatalogVersionChain<V>>>,
}

impl<K, V> Default for VersionedMap<K, V>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl<K, V> VersionedMap<K, V>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    V: Send + Sync + 'static,
{
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the visible node of the key (including commit/creator information)
    pub fn get_node_visible(
        &self,
        key: &K,
        start_ts: Timestamp,
        txn_id: Timestamp,
    ) -> Option<Arc<CatalogVersionNode<V>>> {
        let guard = self.inner.read().unwrap();
        let chain = guard.get(key)?;
        chain.visible_at(start_ts, txn_id)
    }

    /// Get the visible value of the key (returns None if it's a tombstone or nonexistent)
    pub fn get(&self, key: &K, txn: &CatalogTxn) -> Option<Arc<V>> {
        let guard = self.inner.read().unwrap();
        let chain: &CatalogVersionChain<V> = guard.get(key)?;
        let node = chain.visible_at(txn.start_ts(), txn.txn_id())?;
        if node.is_tombstone() {
            None
        } else {
            node.value()
        }
    }

    /// Return the set of keys that are visible and non-tombstone under the given read view
    pub fn visible_keys(&self, start_ts: Timestamp, txn_id: Timestamp) -> Vec<K> {
        let guard = self.inner.read().unwrap();
        guard
            .iter()
            .filter_map(|(k, chain)| {
                chain
                    .visible_at(start_ts, txn_id)
                    .filter(|node| !node.is_tombstone())
                    .map(|_| k.clone())
            })
            .collect()
    }

    // Get or create the chain for the key
    fn get_or_create_chain<'a>(
        inner: &'a mut HashMap<K, CatalogVersionChain<V>>,
        key: &K,
    ) -> &'a mut CatalogVersionChain<V> {
        inner.entry(key.clone()).or_default()
    }

    /// Append an uncommitted value version and return the created version node handle.
    /// Precondition: the current head (if any) must be visible for the caller's read view.
    pub fn put(
        &self,
        key: K,
        value: Arc<V>,
        txn: &CatalogTxn,
    ) -> Result<Arc<CatalogVersionNode<V>>, CatalogTxnError> {
        let mut guard = self.inner.write().unwrap();
        let chain = Self::get_or_create_chain(&mut guard, &key);
        let start_ts = txn.start_ts();
        let txn_id = txn.txn_id();
        // If there is a head, check if it is visible for the caller's read view
        if let Some(head) = chain.head() {
            if !head.visible_for(start_ts, txn_id) {
                return Err(CatalogTxnError::WriteConflict {
                    key: format!("{:?}", &key),
                });
            }
            // If head is our own uncommitted, overwrite in place instead of creating a new node
            if head.commit_ts().is_none() && head.creator_txn() == txn_id {
                head.overwrite_uncommitted(txn_id, Some(value), false)?;
                return Ok(head);
            }
        }
        Ok(chain.append_uncommitted(Some(value), false, txn_id))
    }

    /// Append an uncommitted tombstone version and return the created version node handle.
    /// Precondition: the current head (if any) must be visible for the caller's read view.
    pub fn delete(
        &self,
        key: &K,
        txn: &CatalogTxn,
    ) -> Result<Arc<CatalogVersionNode<V>>, CatalogTxnError> {
        let mut guard = self.inner.write().unwrap();
        let chain = Self::get_or_create_chain(&mut guard, key);
        let start_ts = txn.start_ts();
        let txn_id = txn.txn_id();
        // If there is a head, check if it is visible for the caller's read view
        if let Some(head) = chain.head() {
            if !head.visible_for(start_ts, txn_id) {
                return Err(CatalogTxnError::WriteConflict {
                    key: format!("{:?}", &key),
                });
            }
            // If head is our own uncommitted, overwrite in place instead of creating a new node
            if head.commit_ts().is_none() && head.creator_txn() == txn_id {
                head.overwrite_uncommitted(txn_id, None, true)?;
                return Ok(head);
            }
        }
        Ok(chain.append_uncommitted(None, true, txn_id))
    }

    /// Batch commit nodes with conflict validation.
    /// Note: If any node is not in its corresponding chain or already committed, returns an error.
    pub fn commit_batch(
        &self,
        items: &[TouchedItem<K, V>],
        commit_ts: Timestamp,
    ) -> Result<(), CatalogTxnError> {
        // Use write lock to serialize commit validation and application per map, ensuring
        // first-committer-wins under concurrent commits on the same key.
        let guard = self.inner.read().unwrap();

        // Pre-commit validation: name-level conflict detection using the latest committed view
        // captured while holding the write lock (no concurrent commit in this map can interleave).
        let latest_view_start = global_timestamp_generator().current();
        let latest_view_txn = Timestamp::with_ts(Timestamp::TXN_ID_START);

        for it in items.iter() {
            let chain = guard
                .get(&it.key)
                .ok_or_else(|| CatalogTxnError::IllegalState {
                    reason: "commit key not found: (exists node, missing chain)".to_string(),
                })?;

            // Name-level validation based on the latest committed view
            let latest = chain.visible_at(latest_view_start, latest_view_txn);
            match it.op {
                WriteOp::Create => {
                    if latest.is_some() {
                        return Err(CatalogTxnError::WriteConflict {
                            key: format!("{:?}", &it.key),
                        });
                    }
                }
                // For Delete/Replace: put/delete preconditions ensure no other txn appended
                // concurrent uncommitted versions on the same key. Therefore name-level latest
                // committed view validation is not required here; simply allow commit.
                WriteOp::Delete | WriteOp::Replace => {}
            }
        }

        // Validation passed, commit each node
        for it in items.iter() {
            let chain = guard
                .get(&it.key)
                .ok_or_else(|| CatalogTxnError::IllegalState {
                    reason: "commit key not found during apply: (exists node, missing chain)"
                        .to_string(),
                })?;
            chain.commit_node(&it.node, commit_ts)?;
        }
        Ok(())
    }

    /// Batch rollback nodes.
    /// If the node is the head and uncommitted, it will be physically removed; otherwise, it
    /// remains for safety.
    pub fn abort_batch(&self, items: &[TouchedItem<K, V>]) -> Result<(), CatalogTxnError> {
        let guard = self.inner.read().unwrap();
        // Abort in reverse append order so that we always try to remove the current head first,
        // then older nodes become head and can be removed subsequently.
        for it in items.iter().rev() {
            let chain = guard
                .get(&it.key)
                .ok_or_else(|| CatalogTxnError::IllegalState {
                    reason: "abort key not found: (exists node, missing chain)".to_string(),
                })?;
            chain.abort_node(&it.node)?;
        }
        Ok(())
    }

    /// Reserved GC interface: will reclaim obsolete versions based on low watermark in later stages
    pub fn gc(&self, _low_watermark: Timestamp) -> Result<(), CatalogTxnError> {
        // TODO: Not yet implemented.
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use minigu_transaction::{GraphTxnManager, IsolationLevel, Transaction};

    use super::*;
    use crate::txn::manager::CatalogTxnManager;

    fn begin_txn(mgr: &CatalogTxnManager) -> std::sync::Arc<crate::txn::catalog_txn::CatalogTxn> {
        mgr.begin_transaction(IsolationLevel::Snapshot)
            .expect("begin txn")
    }

    #[test]
    fn mvcc_visibility_read_writes_and_commit() {
        let mgr = CatalogTxnManager::new();
        let map: std::sync::Arc<VersionedMap<String, i32>> =
            std::sync::Arc::new(VersionedMap::new());

        let txn1 = begin_txn(&mgr);
        let key = "g1".to_string();

        assert!(map.get(&key, &txn1).is_none());

        // Uncommitted write: only visible to txn1
        let node = map
            .put(key.clone(), std::sync::Arc::new(1), &txn1)
            .expect("put ok");
        txn1.record_write(&map, key.clone(), node, WriteOp::Create);

        assert_eq!(map.get(&key, &txn1).map(|v| *v), Some(1));

        // After commit, the latest view is visible to txn2
        txn1.commit().expect("commit ok");
        let txn2 = begin_txn(&mgr);
        assert_eq!(map.get(&key, &txn2).map(|v| *v), Some(1));
    }

    #[test]
    fn mvcc_abort_discard_head_uncommitted() {
        let mgr = CatalogTxnManager::new();
        let map: std::sync::Arc<VersionedMap<String, i32>> =
            std::sync::Arc::new(VersionedMap::new());

        let key = "k_abort".to_string();

        let txn1 = begin_txn(&mgr);
        let node1 = map
            .put(key.clone(), std::sync::Arc::new(21), &txn1)
            .expect("put ok");
        txn1.record_write(&map, key.clone(), node1, WriteOp::Create);
        assert_eq!(map.get(&key, &txn1).map(|v| *v), Some(21));
        txn1.commit().expect("commit ok");

        let txn2 = begin_txn(&mgr);
        let node2 = map
            .put(key.clone(), std::sync::Arc::new(42), &txn2)
            .expect("put ok");
        txn2.record_write(&map, key.clone(), node2, WriteOp::Create);
        assert_eq!(map.get(&key, &txn2).map(|v| *v), Some(42));

        txn2.abort().expect("abort ok");

        let txn2 = begin_txn(&mgr);
        assert_eq!(map.get(&key, &txn2).map(|v| *v), Some(21));
    }

    #[test]
    fn mvcc_create_conflict() {
        let mgr = CatalogTxnManager::new();
        let map: std::sync::Arc<VersionedMap<String, i32>> =
            std::sync::Arc::new(VersionedMap::new());
        let key = "dup".to_string();

        let txn1 = begin_txn(&mgr);
        let node1 = map
            .put(key.clone(), std::sync::Arc::new(1), &txn1)
            .expect("put ok");
        txn1.record_write(&map, key.clone(), node1, WriteOp::Create);

        let txn2 = begin_txn(&mgr);
        let r2 = map.put(key.clone(), std::sync::Arc::new(2), &txn2);
        assert!(r2.is_err());

        let r1 = txn1.commit();
        assert!(r1.is_ok());
        let txn3 = begin_txn(&mgr);
        assert_eq!(map.get(&key, &txn3).map(|v| *v), Some(1));
    }

    #[test]
    fn mvcc_delete_conflict() {
        let mgr = CatalogTxnManager::new();
        let map: std::sync::Arc<VersionedMap<String, i32>> =
            std::sync::Arc::new(VersionedMap::new());
        let key = "del".to_string();

        // Create and commit first
        let txn1 = begin_txn(&mgr);
        let node1 = map
            .put(key.clone(), std::sync::Arc::new(7), &txn1)
            .expect("put ok");
        txn1.record_write(&map, key.clone(), node1, WriteOp::Create);
        txn1.commit().expect("seed commit");

        let txn_check = begin_txn(&mgr);
        assert_eq!(map.get(&key, &txn_check).map(|v| *v), Some(7));

        // txn2 delete the node without commit
        let txn2 = begin_txn(&mgr);
        let node2 = map.delete(&key, &txn2).expect("delete ok");
        txn2.record_write(&map, key.clone(), node2, WriteOp::Delete);

        // txn3 delete the node without commit
        let txn3 = begin_txn(&mgr);
        let r3 = map.delete(&key, &txn3);
        assert!(r3.is_err());

        let _ = txn2.commit().expect("first delete ok");
        let txn_check = begin_txn(&mgr);
        assert!(map.get(&key, &txn_check).is_none());
    }
}
