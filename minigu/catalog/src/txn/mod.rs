pub mod catalog_txn;
pub mod error;
pub mod manager;
pub mod runtime;
pub mod versioned;
pub mod versioned_map;

use minigu_transaction::{Timestamp, Transaction, global_timestamp_generator};

/// ReadView: used to select visible versions in Catalog layer by transaction snapshot
#[derive(Debug, Clone, Copy)]
pub struct ReadView {
    pub start_ts: Timestamp,
    pub txn_id: Timestamp,
}

impl ReadView {
    /// Create a latest view:
    /// - `start_ts` is the current global timestamp (commit_ts field)
    /// - `txn_id` is `Timestamp::TXN_ID_START` (never equal to any valid transaction ID, to avoid
    ///   seeing uncommitted)
    #[inline]
    pub fn latest() -> Self {
        Self {
            start_ts: global_timestamp_generator().current(),
            txn_id: Timestamp::with_ts(Timestamp::TXN_ID_START),
        }
    }

    #[inline]
    pub fn new(start_ts: Timestamp, txn_id: Timestamp) -> Self {
        Self { start_ts, txn_id }
    }

    /// Create a read view from an object that implements `Transaction`
    #[inline]
    pub fn from_txn<T: Transaction>(txn: &T) -> Self {
        Self {
            start_ts: txn.start_ts(),
            txn_id: txn.txn_id(),
        }
    }
}
