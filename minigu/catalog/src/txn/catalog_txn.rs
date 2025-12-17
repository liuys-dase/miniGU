use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use minigu_common::{IsolationLevel, Timestamp, global_timestamp_generator};

use crate::txn::error::{CatalogTxnError, CatalogTxnResult};
use crate::txn::manager::CatalogTxnManagerInner;
use crate::txn::versioned::CatalogVersionNode;
use crate::txn::versioned_map::{CommitPlan, TouchedItem, VersionedMap, WriteOp};

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

/// Unified abstraction of "the containers touched by this transaction" - two-phase interface.
trait TxnTouchedSet: Send + Sync + std::fmt::Debug {
    fn validate(&self) -> CatalogTxnResult<()>;
    fn apply(&self, commit_ts: Timestamp) -> CatalogTxnResult<()>;
    fn abort(&self) -> CatalogTxnResult<()>;
}

/// Touched-set implementation for `VersionedMap<K, V>`.
#[derive(Debug)]
struct VersionedMapTouched<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
    V: Send + Sync + 'static + std::fmt::Debug,
{
    map: Weak<VersionedMap<K, V>>,
    items: Vec<TouchedItem<K, V>>,
    plans: Mutex<Option<Vec<CommitPlan<K, V>>>>,
}

impl<K, V> TxnTouchedSet for VersionedMapTouched<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
    V: Send + Sync + 'static + std::fmt::Debug,
{
    fn validate(&self) -> CatalogTxnResult<()> {
        if let Some(map) = self.map.upgrade() {
            let plans = map.validate_batch(&self.items)?;
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
            map.abort_batch(&self.items)
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
    commit_ts_raw: AtomicU64, // 0 means not committed yet.
    isolation: IsolationLevel,
    touched: Mutex<Vec<Box<dyn TxnTouchedSet>>>, // Record the touched containers.
    hooks: Mutex<Vec<Box<dyn TxnHook>>>,         // Record the hooks for pre-commit validation.
    mgr: Weak<CatalogTxnManagerInner>,
    op_mutex: Mutex<()>, // commit/abort mutex.
    commit_lock: Arc<Mutex<()>>,
}

impl CatalogTxn {
    pub(crate) fn new(
        txn_id: Timestamp,
        start_ts: Timestamp,
        isolation: IsolationLevel,
        mgr: Weak<CatalogTxnManagerInner>,
        commit_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            txn_id,
            start_ts,
            commit_ts_raw: AtomicU64::new(0),
            isolation,
            touched: Mutex::new(Vec::new()),
            hooks: Mutex::new(Vec::new()),
            mgr,
            op_mutex: Mutex::new(()),
            commit_lock,
        }
    }

    /// Record a set of writes to a `VersionedMap` for subsequent batch commit/abort.
    pub fn record_versioned_map_writes<K, V>(
        &self,
        map: &Arc<VersionedMap<K, V>>,
        items: Vec<TouchedItem<K, V>>,
    ) where
        K: Eq + Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
        V: Send + Sync + 'static + std::fmt::Debug,
    {
        let touched = VersionedMapTouched {
            map: Arc::downgrade(map),
            items,
            plans: Mutex::new(None),
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

    /// Whether this transaction runs under Serializable isolation
    #[inline]
    pub fn is_serializable(&self) -> bool {
        matches!(self.isolation, IsolationLevel::Serializable)
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
        let item = TouchedItem { key, node, op };
        self.record_versioned_map_writes(map, vec![item]);
    }

    pub fn txn_id(&self) -> Timestamp {
        self.txn_id
    }

    pub fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    pub fn commit_ts(&self) -> Option<Timestamp> {
        decode_commit_ts(self.commit_ts_raw.load(Ordering::SeqCst))
    }

    pub fn isolation_level(&self) -> &IsolationLevel {
        &self.isolation
    }

    /// Prepare this transaction for commit by running precommit hooks and validating touched sets.
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

        let commit_guard = self.commit_lock.lock().expect("commit lock poisoned");
        let op_guard = self.op_mutex.lock().expect("op mutex poisoned");

        {
            let hooks = self.hooks.lock().expect("poisoned hooks mutex");
            for h in hooks.iter() {
                h.precommit(self)?;
            }
        }

        {
            let touched = self.touched.lock().expect("poisoned touched mutex");
            for set in touched.iter() {
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
        let _guard = self.op_mutex.lock().expect("op mutex poisoned");

        {
            let touched = self.touched.lock().expect("poisoned touched mutex");
            for set in touched.iter().rev() {
                set.abort()?;
            }
        }

        if let Some(mgr) = self.mgr.upgrade() {
            mgr.finish_transaction(self)?;
        }

        Ok(())
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
    _commit_guard: MutexGuard<'a, ()>,
    _op_guard: MutexGuard<'a, ()>,
}

impl PreparedCatalogTxn<'_> {
    pub fn apply(self, commit_ts: Timestamp) -> Result<Timestamp, CatalogTxnError> {
        {
            let touched = self.txn.touched.lock().expect("poisoned touched mutex");
            for set in touched.iter() {
                set.apply(commit_ts)?;
            }
        }

        self.txn
            .commit_ts_raw
            .store(encode_commit_ts(Some(commit_ts)), Ordering::SeqCst);

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
