use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crossbeam_skiplist::SkipMap;
use minigu_transaction::{
    GraphTxnManager, Transaction, global_timestamp_generator, global_transaction_id_generator,
};

use super::graph_gc::{GcInfo, GcMonitor};
use super::memory_graph::MemoryGraph;
use super::transaction::{IsolationLevel, MemTransaction, UndoEntry};
use crate::common::transaction::Timestamp;
use crate::common::wal::StorageWal;
use crate::common::wal::graph_wal::{Operation, RedoEntry};
use crate::error::{StorageError, TransactionError};

const PERIODIC_GC_THRESHOLD: u64 = 50;

/// A manager for managing transactions.
pub struct MemTxnManager {
    /// Weak reference to the graph to avoid circular references
    pub(super) graph: Weak<MemoryGraph>,
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
            graph: Weak::new(),
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

    /// Set the graph reference after construction
    pub fn set_graph(&mut self, graph: &Arc<MemoryGraph>) {
        self.graph = Arc::downgrade(graph);
    }

    /// Begin a new transaction with specified parameters
    pub fn begin_transaction_at(
        &self,
        txn_id: Option<Timestamp>,
        start_ts: Option<Timestamp>,
        isolation_level: IsolationLevel,
        skip_wal: bool,
    ) -> Result<Arc<MemTransaction>, StorageError> {
        let graph = self.graph.upgrade().ok_or_else(|| {
            StorageError::Transaction(TransactionError::InvalidState(
                "Graph reference is no longer valid".to_string(),
            ))
        })?;

        // Update the counters
        let txn_id = if let Some(txn_id) = txn_id {
            global_transaction_id_generator().update_if_greater(txn_id);
            txn_id
        } else {
            global_transaction_id_generator().next()
        };
        let start_ts = if let Some(start_ts) = start_ts {
            global_timestamp_generator().update_if_greater(start_ts);
            start_ts
        } else {
            global_timestamp_generator().next()
        };

        // Acquire the checkpoint lock to prevent new transactions from being created
        // while we are creating a checkpoint
        let _checkpoint_lock = graph
            .checkpoint_manager
            .as_ref()
            .unwrap()
            .checkpoint_lock
            .read()
            .unwrap();

        // Create the transaction
        let txn = Arc::new(MemTransaction::with_memgraph(
            graph.clone(),
            txn_id,
            start_ts,
            isolation_level,
        ));
        self.active_txns.insert(txn.txn_id(), txn.clone());
        self.update_watermark();

        // Write `Operation::BeginTransaction` to WAL,
        // unless the function is called when recovering from WAL
        if !skip_wal {
            let wal_entry = RedoEntry {
                lsn: graph.wal_manager.next_lsn(),
                txn_id: txn.txn_id(),
                iso_level: *txn.isolation_level(),
                op: Operation::BeginTransaction(txn.start_ts()),
            };
            graph
                .wal_manager
                .wal()
                .write()
                .unwrap()
                .append(&wal_entry)
                .unwrap();
        }

        Ok(txn)
    }
}

impl GraphTxnManager for MemTxnManager {
    type Error = StorageError;
    type GraphContext = MemoryGraph;
    type Transaction = MemTransaction;

    fn begin_transaction(&self) -> Result<Arc<Self::Transaction>, Self::Error> {
        self.begin_transaction_at(None, None, IsolationLevel::Serializable, false)
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
