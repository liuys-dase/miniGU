//! Timestamp / TxnId management for MVCC transactions
//!
//! This module separates transaction IDs and commit timestamps into distinct types to prevent
//! cross-domain comparisons and incorrect usage.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

use crate::error::TimestampError;

/// Commit timestamp type (highest bit must be 0).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct CommitTs(u64);

impl CommitTs {
    /// Maximum commit timestamp allowed, with the highest bit cleared.
    pub const MAX: u64 = TxnId::START - 1;

    /// Creates a commit timestamp from a raw value with domain validation.
    pub fn try_from_raw(raw: u64) -> Result<Self, TimestampError> {
        if raw > Self::MAX {
            return Err(TimestampError::CommitTsOverflow(raw));
        }
        Ok(Self(raw))
    }

    /// Returns the inner raw value.
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl From<CommitTs> for u64 {
    fn from(ts: CommitTs) -> Self {
        ts.0
    }
}

impl TryFrom<u64> for CommitTs {
    type Error = TimestampError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        CommitTs::try_from_raw(value)
    }
}

/// Transaction ID type (highest bit set to 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxnId(u64);

impl TxnId {
    /// Maximum transaction ID value.
    pub const MAX: u64 = u64::MAX;
    /// Starting value for transaction IDs (highest bit set).
    pub const START: u64 = 1 << 63;

    /// Creates a transaction ID from a raw value with domain validation.
    pub fn try_from_raw(raw: u64) -> Result<Self, TimestampError> {
        if raw < Self::START {
            return Err(TimestampError::WrongDomainTxnId(raw));
        }
        Ok(Self(raw))
    }

    /// Returns the inner raw value.
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl From<TxnId> for u64 {
    fn from(id: TxnId) -> Self {
        id.0
    }
}

impl TryFrom<u64> for TxnId {
    type Error = TimestampError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        TxnId::try_from_raw(value)
    }
}

/// Commit timestamp generator.
pub struct CommitTsGenerator {
    counter: AtomicU64,
}

impl CommitTsGenerator {
    /// Defaults to counting from 1 (0 reserved as an initial placeholder).
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }

    /// Initializes the generator with a specific starting value.
    pub fn with_start(start: CommitTs) -> Self {
        Self {
            counter: AtomicU64::new(start.raw()),
        }
    }

    /// Obtains the next commit timestamp.
    pub fn next(&self) -> Result<CommitTs, TimestampError> {
        let mut cur = self.counter.load(Ordering::SeqCst);
        loop {
            if cur > CommitTs::MAX {
                return Err(TimestampError::CommitTsOverflow(cur));
            }
            match self.counter.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return CommitTs::try_from_raw(cur),
                Err(actual) => cur = actual,
            }
        }
    }

    /// Returns the current counter value without incrementing.
    pub fn current(&self) -> CommitTs {
        // Safety: only valid commit timestamps (or MAX+1 before overflow) are stored internally.
        CommitTs(self.counter.load(Ordering::SeqCst))
    }

    /// Advances the counter if the provided timestamp is greater.
    pub fn update_if_greater(&self, ts: CommitTs) -> Result<(), TimestampError> {
        if ts.raw() > CommitTs::MAX {
            return Err(TimestampError::CommitTsOverflow(ts.raw()));
        }
        self.counter.fetch_max(ts.raw() + 1, Ordering::SeqCst);
        Ok(())
    }
}

impl Default for CommitTsGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction ID generator.
pub struct TxnIdGenerator {
    counter: AtomicU64,
}

impl TxnIdGenerator {
    /// Defaults to `TxnId::START + 1`, leaving the START value reserved.
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(TxnId::START + 1),
        }
    }

    /// Initializes the generator with a provided starting ID.
    pub fn with_start(start: TxnId) -> Self {
        Self {
            counter: AtomicU64::new(start.raw()),
        }
    }

    /// Returns the next transaction ID.
    pub fn next(&self) -> Result<TxnId, TimestampError> {
        let mut cur = self.counter.load(Ordering::SeqCst);
        loop {
            if cur == TxnId::MAX {
                return Err(TimestampError::TxnIdOverflow(cur));
            }
            match self.counter.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return TxnId::try_from_raw(cur),
                Err(actual) => cur = actual,
            }
        }
    }

    /// Advances the counter if the provided ID is greater.
    pub fn update_if_greater(&self, txn_id: TxnId) -> Result<(), TimestampError> {
        if txn_id.raw() == TxnId::MAX {
            return Err(TimestampError::TxnIdOverflow(txn_id.raw()));
        }
        self.counter.fetch_max(txn_id.raw() + 1, Ordering::SeqCst);
        Ok(())
    }
}

impl Default for TxnIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

// --------- Global singletons ---------

static GLOBAL_COMMIT_TS_GENERATOR: OnceLock<Arc<CommitTsGenerator>> = OnceLock::new();
static GLOBAL_TXN_ID_GENERATOR: OnceLock<Arc<TxnIdGenerator>> = OnceLock::new();

/// Returns the global commit timestamp generator.
pub fn global_commit_ts_generator() -> Arc<CommitTsGenerator> {
    GLOBAL_COMMIT_TS_GENERATOR
        .get_or_init(|| Arc::new(CommitTsGenerator::new()))
        .clone()
}

/// Returns the global transaction ID generator.
pub fn global_txn_id_generator() -> Arc<TxnIdGenerator> {
    GLOBAL_TXN_ID_GENERATOR
        .get_or_init(|| Arc::new(TxnIdGenerator::new()))
        .clone()
}

/// Initializes the global commit timestamp generator.
pub fn init_global_commit_ts_generator(start: CommitTs) -> Result<(), &'static str> {
    GLOBAL_COMMIT_TS_GENERATOR
        .set(Arc::new(CommitTsGenerator::with_start(start)))
        .map_err(|_| "Global commit-ts generator already initialized")
}

/// Initializes the global transaction ID generator.
pub fn init_global_txn_id_generator(start: TxnId) -> Result<(), &'static str> {
    GLOBAL_TXN_ID_GENERATOR
        .set(Arc::new(TxnIdGenerator::with_start(start)))
        .map_err(|_| "Global txn-id generator already initialized")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_ts_range_and_next() {
        let generator = CommitTsGenerator::new();
        let ts1 = generator.next().unwrap();
        assert_eq!(ts1.raw(), 1);
        assert!(CommitTs::try_from_raw(CommitTs::MAX).is_ok());
    }

    #[test]
    fn test_txn_id_range_and_next() {
        let generator = TxnIdGenerator::new();
        let txn1 = generator.next().unwrap();
        assert!(txn1.raw() > TxnId::START);
        assert!(TxnId::try_from_raw(TxnId::START).is_ok());
    }
}
