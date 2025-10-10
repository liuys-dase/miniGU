use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use minigu_transaction::timestamp::Timestamp;
use minigu_transaction::{IsolationLevel, Transaction, global_timestamp_generator};

use crate::txn::ReadView;
use crate::txn::error::{CatalogTxnError, CatalogTxnResult};
use crate::txn::manager::CatalogTxnManagerInner;
use crate::txn::versioned::CatalogVersionNode;
use crate::txn::versioned_map::{TouchedItem, VersionedMap, WriteOp};

fn encode_commit_ts(opt: Option<Timestamp>) -> u64 {
    match opt {
        Some(ts) => ts.raw(),
        None => 0,
    }
}

fn decode_commit_ts(raw: u64) -> Option<Timestamp> {
    if raw == 0 {
        None
    } else {
        Some(Timestamp::with_ts(raw))
    }
}

/// Abstract touched set that supports batch commit/abort.
trait TxnTouchedSet: Send + Sync {
    fn commit(&self, commit_ts: Timestamp) -> CatalogTxnResult<()>;
    fn abort(&self) -> CatalogTxnResult<()>;
}

/// Touched-set implementation for `VersionedMap<K, V>`.
struct VersionedMapTouched<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
    V: Send + Sync + 'static,
{
    map: Weak<VersionedMap<K, V>>,
    items: Vec<TouchedItem<K, V>>,
}

impl<K, V> TxnTouchedSet for VersionedMapTouched<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
    V: Send + Sync + 'static,
{
    fn commit(&self, commit_ts: Timestamp) -> CatalogTxnResult<()> {
        if let Some(map) = self.map.upgrade() {
            map.commit_batch(&self.items, commit_ts)
        } else {
            // The map has been dropped; treat as no-op.
            Ok(())
        }
    }

    fn abort(&self) -> CatalogTxnResult<()> {
        if let Some(map) = self.map.upgrade() {
            map.abort_batch(&self.items)
        } else {
            Ok(())
        }
    }
}

/// Catalog transaction object.
pub struct CatalogTxn {
    txn_id: Timestamp,
    start_ts: Timestamp,
    commit_ts_raw: AtomicU64, // 0 means not committed yet.
    isolation: IsolationLevel,
    touched: Mutex<Vec<Box<dyn TxnTouchedSet>>>, // Record the touched sets for batch commit/abort.
    hooks: Mutex<Vec<Box<dyn TxnHook>>>,         // Record the hooks for pre-commit validation.
    mgr: Weak<CatalogTxnManagerInner>,
}

impl CatalogTxn {
    pub(crate) fn new(
        txn_id: Timestamp,
        start_ts: Timestamp,
        isolation: IsolationLevel,
        mgr: Weak<CatalogTxnManagerInner>,
    ) -> Self {
        Self {
            txn_id,
            start_ts,
            commit_ts_raw: AtomicU64::new(0),
            isolation,
            touched: Mutex::new(Vec::new()),
            hooks: Mutex::new(Vec::new()),
            mgr,
        }
    }

    /// Create a transaction from a read view. Only used for `get_xxx_with`.
    pub fn from_view(view: &ReadView) -> Self {
        Self {
            txn_id: view.txn_id,
            start_ts: view.start_ts,
            commit_ts_raw: AtomicU64::new(0),
            isolation: IsolationLevel::Snapshot,
            touched: Mutex::new(Vec::new()),
            hooks: Mutex::new(Vec::new()),
            mgr: Weak::new(),
        }
    }

    /// Record a set of writes to a `VersionedMap` for subsequent batch commit/abort.
    pub fn record_versioned_map_writes<K, V>(
        &self,
        map: &Arc<VersionedMap<K, V>>,
        items: Vec<TouchedItem<K, V>>,
    ) where
        K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
        V: Send + Sync + 'static,
    {
        let touched = VersionedMapTouched {
            map: Arc::downgrade(map),
            items,
        };
        self.touched
            .lock()
            .expect("poisoned touched mutex")
            .push(Box::new(touched));
    }

    /// Transaction-level hook registration (e.g., consistency checks before commit).
    pub fn add_hook(&self, hook: Box<dyn TxnHook>) {
        self.hooks.lock().expect("poisoned hooks mutex").push(hook);
    }

    /// Construct and record a single write entry.
    pub fn record_write<K, V>(
        &self,
        map: &Arc<VersionedMap<K, V>>,
        key: K,
        node: Arc<CatalogVersionNode<V>>,
        op: WriteOp,
    ) where
        K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
        V: Send + Sync + 'static,
    {
        let item = TouchedItem { key, node, op };
        self.record_versioned_map_writes(map, vec![item]);
    }
}

impl Transaction for CatalogTxn {
    type Error = CatalogTxnError;

    fn txn_id(&self) -> Timestamp {
        self.txn_id
    }

    fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    fn commit_ts(&self) -> Option<Timestamp> {
        decode_commit_ts(self.commit_ts_raw.load(Ordering::SeqCst))
    }

    fn isolation_level(&self) -> &IsolationLevel {
        &self.isolation
    }

    fn commit(&self) -> Result<Timestamp, Self::Error> {
        // Run pre-commit validation hooks.
        {
            let hooks = self.hooks.lock().expect("poisoned hooks mutex");
            for h in hooks.iter() {
                h.precommit()?;
            }
        }

        let commit_ts = global_timestamp_generator().next()?;

        // Batch-commit touched sets.
        let touched = self.touched.lock().expect("poisoned touched mutex");
        for set in touched.iter() {
            set.commit(commit_ts)?;
        }

        // Set commit_ts.
        self.commit_ts_raw
            .store(encode_commit_ts(Some(commit_ts)), Ordering::SeqCst);

        // Remove from active set.
        if let Some(mgr) = self.mgr.upgrade() {
            mgr.finish_transaction(self)?;
        }

        Ok(commit_ts)
    }

    fn abort(&self) -> Result<(), Self::Error> {
        let touched = self.touched.lock().expect("poisoned touched mutex");
        for set in touched.iter() {
            set.abort()?;
        }

        if let Some(mgr) = self.mgr.upgrade() {
            mgr.finish_transaction(self)?;
        }

        Ok(())
    }
}

/// Transaction hook interface exposed to external users (e.g., pre-commit checks).
pub trait TxnHook: Send + Sync {
    fn precommit(&self) -> CatalogTxnResult<()>;
}
