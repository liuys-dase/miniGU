use std::sync::atomic::{AtomicU64, Ordering};
use minigu_storage::common::transaction::Timestamp;

/// Transaction ID type for system transactions
pub type TransactionId = Timestamp;

/// Unified timestamp manager for system-level transactions
/// Ensures timestamp consistency across catalog and storage operations
pub struct TimestampManager {
    /// Global transaction ID counter
    txn_id_counter: AtomicU64,
    /// Global commit timestamp counter
    commit_ts_counter: AtomicU64,
    /// Latest committed timestamp
    latest_commit_ts: AtomicU64,
    /// Watermark (minimum active transaction timestamp)
    watermark: AtomicU64,
}

impl Default for TimestampManager {
    fn default() -> Self {
        Self {
            txn_id_counter: AtomicU64::new(Timestamp::TXN_ID_START + 1),
            commit_ts_counter: AtomicU64::new(1),
            latest_commit_ts: AtomicU64::new(0),
            watermark: AtomicU64::new(0),
        }
    }
}

impl TimestampManager {
    /// Create a new timestamp manager
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a new transaction ID
    pub fn new_transaction_id(&self) -> TransactionId {
        Timestamp(self.txn_id_counter.fetch_add(1, Ordering::SeqCst))
    }

    /// Allocate a new commit timestamp
    pub fn new_commit_timestamp(&self) -> Timestamp {
        Timestamp(self.commit_ts_counter.fetch_add(1, Ordering::SeqCst))
    }

    /// Get the latest commit timestamp
    pub fn latest_commit_timestamp(&self) -> Timestamp {
        Timestamp(self.latest_commit_ts.load(Ordering::Acquire))
    }

    /// Update the latest commit timestamp
    pub fn update_latest_commit_timestamp(&self, ts: Timestamp) {
        self.latest_commit_ts.store(ts.0, Ordering::SeqCst);
    }

    /// Get the current watermark
    pub fn watermark(&self) -> Timestamp {
        Timestamp(self.watermark.load(Ordering::Acquire))
    }

    /// Update the watermark based on active transactions
    pub fn update_watermark(&self, active_txn_start_timestamps: &[Timestamp]) {
        let min_ts = active_txn_start_timestamps
            .iter()
            .map(|ts| ts.0)
            .min()
            .unwrap_or_else(|| self.latest_commit_ts.load(Ordering::Acquire));
        
        self.watermark.store(min_ts, Ordering::SeqCst);
    }

    /// Get a snapshot timestamp for reading (for new transactions)
    pub fn get_snapshot_timestamp(&self) -> Timestamp {
        // For new transactions, use latest_commit_ts + 1 as start timestamp
        Timestamp(self.latest_commit_ts.load(Ordering::Acquire) + 1)
    }
}