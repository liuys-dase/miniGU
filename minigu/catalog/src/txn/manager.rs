use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use minigu_common::{
    IsolationLevel, Timestamp, global_timestamp_generator, global_transaction_id_generator,
};

use crate::txn::catalog_txn::CatalogTxn;
use crate::txn::error::{CatalogTxnError, CatalogTxnResult};

#[derive(Debug)]
pub struct CatalogTxnManagerInner {
    active: RwLock<HashMap<u64, Timestamp>>, // txn_id.raw() -> start_ts
    low_watermark_raw: AtomicU64,
}

impl CatalogTxnManagerInner {
    fn new() -> Self {
        Self {
            active: RwLock::new(HashMap::new()),
            low_watermark_raw: AtomicU64::new(0),
        }
    }

    fn update_low_watermark_locked(&self, guard: &HashMap<u64, Timestamp>) {
        let lw = guard
            .values()
            .map(|ts| ts.raw())
            .min()
            .unwrap_or_else(|| global_timestamp_generator().current().raw());
        self.low_watermark_raw.store(lw, Ordering::SeqCst);
    }

    pub(crate) fn finish_transaction(&self, txn: &CatalogTxn) -> CatalogTxnResult<()> {
        let mut guard = self.active.write().expect("poisoned active set");
        guard.remove(&txn.txn_id().raw());
        self.update_low_watermark_locked(&guard);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct CatalogTxnManager {
    inner: Arc<CatalogTxnManagerInner>,
}

impl Default for CatalogTxnManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogTxnManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CatalogTxnManagerInner::new()),
        }
    }

    pub fn inner(&self) -> Arc<CatalogTxnManagerInner> {
        self.inner.clone()
    }

    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Arc<CatalogTxn>, CatalogTxnError> {
        self.begin_transaction_at(None, None, isolation_level)
    }

    pub fn begin_transaction_at(
        &self,
        txn_id: Option<Timestamp>,
        start_ts: Option<Timestamp>,
        isolation_level: IsolationLevel,
    ) -> Result<Arc<CatalogTxn>, CatalogTxnError> {
        let txn_id = if let Some(id) = txn_id {
            global_transaction_id_generator().update_if_greater(id)?;
            id
        } else {
            global_transaction_id_generator().next()?
        };
        let start_ts = if let Some(ts) = start_ts {
            global_timestamp_generator().update_if_greater(ts)?;
            ts
        } else {
            global_timestamp_generator().next()?
        };

        let txn = Arc::new(CatalogTxn::new(
            txn_id,
            start_ts,
            isolation_level,
            Arc::downgrade(&self.inner),
        ));

        let mut guard = self.inner.active.write().expect("poisoned active set");
        guard.insert(txn_id.raw(), start_ts);
        self.inner.update_low_watermark_locked(&guard);

        Ok(txn)
    }

    pub fn finish_transaction(&self, txn: &CatalogTxn) -> Result<(), CatalogTxnError> {
        self.inner.finish_transaction(txn)
    }

    pub fn garbage_collect(&self) -> Result<(), CatalogTxnError> {
        Ok(())
    }

    pub fn low_watermark(&self) -> Timestamp {
        Timestamp::with_ts(self.inner.low_watermark_raw.load(Ordering::SeqCst))
    }
}
