use std::sync::{Arc, Mutex};

use minigu_common::Timestamp;

use crate::txn::error::{CatalogTxnError, CatalogTxnResult};

#[derive(Debug)]
pub struct CatalogVersionNode<V> {
    value: Mutex<Option<Arc<V>>>,
    tombstone: Mutex<bool>,
    creator_txn: Timestamp,
    commit_ts: Mutex<Option<Timestamp>>,
    next: Mutex<Option<Arc<CatalogVersionNode<V>>>>,
}

impl<V> CatalogVersionNode<V> {
    pub fn new_uncommitted(value: Option<Arc<V>>, tombstone: bool, creator_txn: Timestamp) -> Self {
        Self {
            value: Mutex::new(value),
            tombstone: Mutex::new(tombstone),
            creator_txn,
            commit_ts: Mutex::new(None),
            next: Mutex::new(None),
        }
    }

    pub fn overwrite_uncommitted(
        &self,
        txn_id: Timestamp,
        value: Option<Arc<V>>,
        tombstone: bool,
    ) -> CatalogTxnResult<()> {
        if self.creator_txn != txn_id || self.commit_ts().is_some() {
            return Err(CatalogTxnError::IllegalState {
                reason: "overwrite_uncommitted on node not owned by txn or already committed"
                    .to_string(),
            });
        }
        *self.value.lock().unwrap() = value;
        *self.tombstone.lock().unwrap() = tombstone;
        Ok(())
    }

    pub fn set_committed(&self, commit_ts: Timestamp) {
        *self.commit_ts.lock().unwrap() = Some(commit_ts);
    }

    pub fn commit_ts(&self) -> Option<Timestamp> {
        *self.commit_ts.lock().unwrap()
    }

    pub fn creator_txn(&self) -> Timestamp {
        self.creator_txn
    }

    pub fn value(&self) -> Option<Arc<V>> {
        self.value.lock().unwrap().clone()
    }

    pub fn is_tombstone(&self) -> bool {
        *self.tombstone.lock().unwrap()
    }

    pub fn next(&self) -> Option<Arc<CatalogVersionNode<V>>> {
        self.next.lock().unwrap().clone()
    }

    pub fn set_next(&self, next: Option<Arc<CatalogVersionNode<V>>>) {
        *self.next.lock().unwrap() = next;
    }

    pub fn visible_for(&self, start_ts: Timestamp, txn_id: Timestamp) -> bool {
        if let Some(commit_ts) = self.commit_ts() {
            commit_ts <= start_ts
        } else {
            self.creator_txn == txn_id
        }
    }
}

#[derive(Debug)]
pub struct CatalogVersionChain<V> {
    pub(crate) head: Option<Arc<CatalogVersionNode<V>>>,
}

impl<V> Default for CatalogVersionChain<V> {
    fn default() -> Self {
        Self { head: None }
    }
}

impl<V> CatalogVersionChain<V> {
    pub fn head(&self) -> Option<Arc<CatalogVersionNode<V>>> {
        self.head.clone()
    }

    pub fn visible_at(
        &self,
        start_ts: Timestamp,
        txn_id: Timestamp,
    ) -> Option<Arc<CatalogVersionNode<V>>> {
        let mut cursor = self.head.clone();
        while let Some(node) = cursor {
            if node.visible_for(start_ts, txn_id) {
                return Some(node);
            }
            cursor = node.next();
        }
        None
    }

    pub fn append_uncommitted(
        &mut self,
        value: Option<Arc<V>>,
        tombstone: bool,
        creator_txn: Timestamp,
    ) -> Arc<CatalogVersionNode<V>> {
        let node = Arc::new(CatalogVersionNode::new_uncommitted(
            value,
            tombstone,
            creator_txn,
        ));
        node.set_next(self.head.clone());
        self.head = Some(node.clone());
        node
    }
}
