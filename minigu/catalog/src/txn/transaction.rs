use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use minigu_common::{IsolationLevel, Timestamp, global_timestamp_generator};
use parking_lot::RawMutex;
use parking_lot::lock_api::ArcMutexGuard;

use crate::txn::error::{CatalogTxnError, CatalogTxnResult};
use crate::txn::manager::CatalogTxnManager;
use crate::txn::versioned::{CatalogVersionNode, CommitPlan, Entry, VersionedMap, WriteOp};

/// Unified abstraction of "the write sets touched by this transaction" - two-phase interface.
///
/// This is closer to a transaction "write set / write intents" than a generic access list:
/// the implementor is expected to validate and then apply its buffered writes at commit time.
trait TxnWriteSet: Send + Sync + std::fmt::Debug {
    fn validate(&self) -> CatalogTxnResult<()>;
    fn apply(&self, commit_ts: Timestamp) -> CatalogTxnResult<()>;
    fn abort(&self) -> CatalogTxnResult<()>;
}

/// Per-map write-set implementation for `VersionedMap<K, V>`.
#[derive(Debug)]
struct VersionedMapWriteSet<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
    V: Send + Sync + 'static + std::fmt::Debug,
{
    map: Weak<VersionedMap<K, V>>,
    entries: Vec<Entry<K, V>>,
    plans: Mutex<Option<Vec<CommitPlan<K, V>>>>,
}

impl<K, V> TxnWriteSet for VersionedMapWriteSet<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
    V: Send + Sync + 'static + std::fmt::Debug,
{
    fn validate(&self) -> CatalogTxnResult<()> {
        if let Some(map) = self.map.upgrade() {
            let plans = map.validate_batch(&self.entries)?;
            let mut slot = self.plans.lock().expect("plans mutex poisoned");
            *slot = Some(plans);
            Ok(())
        } else {
            Ok(())
        }
    }

    fn apply(&self, commit_ts: Timestamp) -> CatalogTxnResult<()> {
        if let Some(map) = self.map.upgrade() {
            if let Some(plans) = self.plans.lock().expect("plans mutex poisoned").as_ref() {
                map.apply_batch(plans, commit_ts)
            } else {
                Err(CatalogTxnError::IllegalState {
                    reason: "apply without prior validate".into(),
                })
            }
        } else {
            Ok(())
        }
    }

    fn abort(&self) -> CatalogTxnResult<()> {
        if let Some(map) = self.map.upgrade() {
            map.abort_batch(&self.entries)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
/// Catalog transaction object.
pub struct CatalogTxn {
    txn_id: Timestamp,
    start_ts: Timestamp,
    commit_ts: AtomicU64, // 0 means not committed yet.
    isolation_level: IsolationLevel,
    write_sets: Mutex<Vec<Box<dyn TxnWriteSet>>>,
    hooks: Mutex<Vec<Box<dyn TxnHook>>>, // Record the hooks for pre-commit validation.
    mgr: Weak<CatalogTxnManager>,
    op_mutex: Mutex<()>, // commit/abort mutex.
}

impl CatalogTxn {
    pub(crate) fn new(
        txn_id: Timestamp,
        start_ts: Timestamp,
        isolation: IsolationLevel,
        mgr: Weak<CatalogTxnManager>,
    ) -> Self {
        Self {
            txn_id,
            start_ts,
            commit_ts: AtomicU64::new(0),
            isolation_level: isolation,
            write_sets: Mutex::new(Vec::new()),
            hooks: Mutex::new(Vec::new()),
            mgr,
            op_mutex: Mutex::new(()),
        }
    }

    /// Record a set of writes to a `VersionedMap` for subsequent batch commit/abort.
    pub fn record_versioned_map_writes<K, V>(
        &self,
        map: &Arc<VersionedMap<K, V>>,
        entries: Vec<Entry<K, V>>,
    ) where
        K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
        V: Send + Sync + 'static + std::fmt::Debug,
    {
        let write_set = VersionedMapWriteSet {
            map: Arc::downgrade(map),
            entries,
            plans: Mutex::new(None),
        };
        self.write_sets
            .lock()
            .expect("poisoned write_sets mutex")
            .push(Box::new(write_set));
    }

    /// Transaction-level hook registration (e.g., consistency checks before commit).
    pub fn add_hook(&self, hook: Box<dyn TxnHook>) {
        self.hooks.lock().expect("poisoned hooks mutex").push(hook);
    }

    /// Whether this transaction runs under Serializable isolation
    #[inline]
    pub fn is_serializable(&self) -> bool {
        matches!(self.isolation_level, IsolationLevel::Serializable)
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
        V: Send + Sync + 'static + std::fmt::Debug,
    {
        let entry = Entry { key, node, op };
        self.record_versioned_map_writes(map, vec![entry]);
    }

    pub fn txn_id(&self) -> Timestamp {
        self.txn_id
    }

    pub fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    pub fn commit_ts(&self) -> Option<Timestamp> {
        let raw = self.commit_ts.load(Ordering::SeqCst);
        if raw == 0 {
            None
        } else {
            Some(Timestamp::with_ts(raw))
        }
    }

    pub fn isolation_level(&self) -> &IsolationLevel {
        &self.isolation_level
    }

    /// Prepare this transaction for commit by running precommit hooks and validating write sets.
    ///
    /// This method acquires a global commit lock to prevent other catalog transactions from
    /// committing between validation and apply. The returned guard must be consumed via
    /// [`PreparedCatalogTxn::apply`] to finalize the commit.
    pub fn prepare(&self) -> Result<PreparedCatalogTxn<'_>, CatalogTxnError> {
        if self.commit_ts().is_some() {
            return Err(CatalogTxnError::IllegalState {
                reason: "transaction already committed".into(),
            });
        }

        let mgr = self
            .mgr
            .upgrade()
            .ok_or_else(|| CatalogTxnError::IllegalState {
                reason: "transaction manager dropped".into(),
            })?;
        let commit_guard = mgr.commit_lock().lock_arc();
        let op_guard = self.op_mutex.lock().expect("op mutex poisoned");

        {
            let hooks = self.hooks.lock().expect("poisoned hooks mutex");
            for h in hooks.iter() {
                h.precommit(self)?;
            }
        }

        {
            let write_sets = self.write_sets.lock().expect("poisoned write_sets mutex");
            for set in write_sets.iter() {
                set.validate()?;
            }
        }

        Ok(PreparedCatalogTxn {
            txn: self,
            _commit_guard: commit_guard,
            _op_guard: op_guard,
        })
    }

    pub fn commit_at(&self, commit_ts: Timestamp) -> Result<Timestamp, CatalogTxnError> {
        self.prepare()?.apply(commit_ts)
    }

    pub fn commit(&self) -> Result<Timestamp, CatalogTxnError> {
        let commit_ts = global_timestamp_generator().next()?;
        self.commit_at(commit_ts)
    }

    pub fn abort(&self) -> Result<(), CatalogTxnError> {
        let _guard = self.op_mutex.lock().unwrap_or_else(|e| e.into_inner());

        {
            let write_sets = self.write_sets.lock().unwrap_or_else(|e| e.into_inner());
            for set in write_sets.iter().rev() {
                set.abort()?;
            }
        }

        if let Some(mgr) = self.mgr.upgrade() {
            mgr.finish_transaction(self)?;
        }

        Ok(())
    }
}

impl Drop for CatalogTxn {
    fn drop(&mut self) {
        let commit_ts = self.commit_ts();
        if commit_ts.is_some() {
            return;
        }
        {
            let _guard = self.op_mutex.lock().unwrap_or_else(|e| e.into_inner());

            let write_sets = self.write_sets.lock().unwrap_or_else(|e| e.into_inner());
            for set in write_sets.iter().rev() {
                let _ = set.abort();
            }

            if let Some(mgr) = self.mgr.upgrade() {
                let _ = mgr.finish_transaction(self);
            }
        }
    }
}

/// Transaction hook interface exposed to external users (e.g., pre-commit checks).
pub trait TxnHook: Send + Sync + std::fmt::Debug {
    fn precommit(&self, txn: &CatalogTxn) -> CatalogTxnResult<()>;
}

/// RAII guard representing a catalog transaction that has been successfully validated for commit.
///
/// Holding this guard prevents other catalog transactions from committing, ensuring that
/// serializable read-validation hooks remain valid until the changes are applied.
pub struct PreparedCatalogTxn<'a> {
    txn: &'a CatalogTxn,
    _commit_guard: ArcMutexGuard<RawMutex, ()>,
    _op_guard: MutexGuard<'a, ()>,
}

impl PreparedCatalogTxn<'_> {
    pub fn apply(self, commit_ts: Timestamp) -> Result<Timestamp, CatalogTxnError> {
        {
            let write_sets = self
                .txn
                .write_sets
                .lock()
                .expect("poisoned write_sets mutex");
            for set in write_sets.iter() {
                set.apply(commit_ts)?;
            }
        }

        self.txn.commit_ts.store(commit_ts.raw(), Ordering::SeqCst);

        if let Some(mgr) = self.txn.mgr.upgrade() {
            mgr.finish_transaction(self.txn)?;
        }

        Ok(commit_ts)
    }
}

/// View trait to allow accepting either `CatalogTxn` or higher-level `Transaction`.
pub trait CatalogTxnView {
    fn catalog_txn(&self) -> &CatalogTxn;
}

impl CatalogTxnView for CatalogTxn {
    fn catalog_txn(&self) -> &CatalogTxn {
        self
    }
}
