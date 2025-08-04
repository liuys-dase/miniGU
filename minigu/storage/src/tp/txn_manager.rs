use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_skiplist::SkipMap;
use minigu_transaction::{GraphTxnManager, Transaction};

use super::graph_gc::{GcInfo, GcMonitor};
use super::memory_graph::MemoryGraph;
use super::transaction::{MemTransaction, UndoEntry};
use crate::common::transaction::Timestamp;
use crate::error::{StorageError, TransactionError};

const PERIODIC_GC_THRESHOLD: u64 = 50;

/// A manager for managing transactions.
pub struct MemTxnManager {
    /// Active transactions' txn.
    pub(super) active_txns: SkipMap<Timestamp, Arc<MemTransaction>>,
    /// All transactions, running or committed.
    pub(super) committed_txns: SkipMap<Timestamp, Arc<MemTransaction>>,
    /// Commit lock to enforce serial commit order
    pub(super) commit_lock: Mutex<()>,
    pub(super) latest_commit_ts: AtomicU64,
    /// The watermark is the minimum start timestamp of the active transactions.
    /// If there is no active transaction, the watermark is the latest commit timestamp.
    pub(super) watermark: AtomicU64,
    /// Last garbage collection timestamp
    last_gc_ts: AtomicU64,
}

impl Default for MemTxnManager {
    fn default() -> Self {
        Self {
            active_txns: SkipMap::new(),
            committed_txns: SkipMap::new(),
            commit_lock: Mutex::new(()),
            latest_commit_ts: AtomicU64::new(0),
            watermark: AtomicU64::new(0),
            last_gc_ts: AtomicU64::new(0),
        }
    }
}

impl MemTxnManager {
    /// Create a new MemTxnManager
    pub fn new() -> Self {
        Self::default()
    }
}

impl GraphTxnManager for MemTxnManager {
    type Error = StorageError;
    type GraphContext = MemoryGraph;
    type Transaction = MemTransaction;

    fn begin_transaction(&self, txn: Arc<Self::Transaction>) -> Result<(), Self::Error> {
        self.active_txns.insert(txn.txn_id(), txn.clone());
        self.update_watermark();
        Ok(())
    }

    fn finish_transaction(&self, txn: &Self::Transaction) -> Result<(), Self::Error> {
        let txn_entry = self.active_txns.remove(&txn.txn_id());
        if let Some(txn_arc) = txn_entry {
            // Check if the transaction has been committed (by checking if it has a commit_ts)
            if let Some(commit_ts) = txn.commit_ts() {
                self.committed_txns
                    .insert(commit_ts, txn_arc.value().clone());
            }
            self.update_watermark();
            return Ok(());
        }

        Err(StorageError::Transaction(
            TransactionError::TransactionNotFound(format!("{:?}", txn.txn_id())),
        ))
    }

    fn garbage_collect(&self, _graph: &Self::GraphContext) -> Result<(), Self::Error> {
        // Step1: Obtain the min read timestamp of the active transactions
        let min_read_ts = self.watermark.load(Ordering::Acquire);

        // Collect expired transactions (only handle transaction-level garbage collection)
        let mut expired_txns = Vec::new();

        for entry in self.committed_txns.iter() {
            // If the commit timestamp of the transaction is greater than the min read timestamp,
            // it means the transaction is still active, and the subsequent transactions are also
            // active.
            if entry.key().0 > min_read_ts {
                break;
            }

            // Mark transaction for removal
            expired_txns.push(entry.value().clone());
        }

        // Remove expired transactions from our tracking
        for txn in expired_txns {
            if let Some(commit_ts) = txn.commit_ts() {
                self.committed_txns.remove(&commit_ts);
            }
        }

        Ok(())
    }

    fn update_watermark(&self) {
        let min_ts = self
            .active_txns
            .front()
            .map(|v| v.value().start_ts().0)
            .unwrap_or(self.latest_commit_ts.load(Ordering::Acquire))
            .max(self.watermark.load(Ordering::Acquire));
        self.watermark.store(min_ts, Ordering::SeqCst);
    }
}

impl GcMonitor for MemTxnManager {
    fn should_trigger_gc(&self) -> bool {
        let current_watermark = self.watermark.load(Ordering::Acquire);
        let last_gc_ts = self.last_gc_ts.load(Ordering::Acquire);
        let committed_count = self.committed_txns.len();

        // If the watermark has changed or the number of committed transactions exceeds a threshold,
        // trigger garbage collection.
        (current_watermark - last_gc_ts > PERIODIC_GC_THRESHOLD) || (committed_count > 100)
    }

    fn get_gc_info(&self) -> GcInfo {
        GcInfo {
            watermark: self.watermark.load(Ordering::Acquire),
            committed_txns_count: self.committed_txns.len(),
            last_gc_timestamp: self.last_gc_ts.load(Ordering::Acquire),
        }
    }

    fn get_expired_entries(&self) -> (Vec<Arc<UndoEntry>>, u64) {
        let min_read_ts = self.watermark.load(Ordering::Acquire);
        let mut expired_undo_entries = Vec::new();

        for entry in self.committed_txns.iter() {
            // If the commit timestamp of the transaction is greater than the min read timestamp,
            // it means the transaction is still active, and the subsequent transactions are also
            // active.
            if entry.key().0 > min_read_ts {
                break;
            }

            // Collect all undo entries from this expired transaction
            for undo_entry in entry.value().undo_buffer().read().unwrap().iter() {
                expired_undo_entries.push(undo_entry.clone());
            }
        }

        (expired_undo_entries, min_read_ts)
    }

    fn update_last_gc_timestamp(&self, timestamp: u64) {
        self.last_gc_ts.store(timestamp, Ordering::SeqCst);
    }
}
