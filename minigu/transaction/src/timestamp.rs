//! Timestamp / TxnId management for MVCC transactions
//!
//! 本模块将事务 ID 与提交时间戳拆分为独立类型，避免跨域比较和误用。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

use crate::error::TimestampError;

/// 提交时间戳类型（最高位必须为 0）
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct CommitTs(u64);

impl CommitTs {
    /// 最大可用提交时间戳，最高位 0。
    pub const MAX: u64 = TxnId::START - 1;

    /// 使用原始值构造提交时间戳，带域校验。
    pub fn try_from_raw(raw: u64) -> Result<Self, TimestampError> {
        if raw > Self::MAX {
            return Err(TimestampError::CommitTsOverflow(raw));
        }
        Ok(Self(raw))
    }

    /// 直接返回内部值。
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

/// 事务 ID 类型（最高位为 1）
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxnId(u64);

impl TxnId {
    /// 事务 ID 最大值。
    pub const MAX: u64 = u64::MAX;
    /// 事务 ID 起始位置（最高位为 1）
    pub const START: u64 = 1 << 63;

    /// 使用原始值构造事务 ID，带域校验。
    pub fn try_from_raw(raw: u64) -> Result<Self, TimestampError> {
        if raw < Self::START {
            return Err(TimestampError::WrongDomainTxnId(raw));
        }
        Ok(Self(raw))
    }

    /// 直接返回内部值。
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

/// 提交时间戳生成器
pub struct CommitTsGenerator {
    counter: AtomicU64,
}

impl CommitTsGenerator {
    /// 默认从 1 开始递增（保留 0 作为初始占位）。
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }

    /// 指定起始值初始化生成器。
    pub fn with_start(start: CommitTs) -> Self {
        Self {
            counter: AtomicU64::new(start.raw()),
        }
    }

    /// 获取下一个提交时间戳。
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

    /// 获取当前计数（不自增）。
    pub fn current(&self) -> CommitTs {
        // 安全：内部只会写入有效提交时间戳（或溢出前的 MAX+1）
        CommitTs(self.counter.load(Ordering::SeqCst))
    }

    /// 如果传入值更大则推进计数器。
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

/// 事务 ID 生成器
pub struct TxnIdGenerator {
    counter: AtomicU64,
}

impl TxnIdGenerator {
    /// 默认从 `TxnId::START + 1` 开始（保留 START）。
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(TxnId::START + 1),
        }
    }

    /// 指定起始值初始化生成器。
    pub fn with_start(start: TxnId) -> Self {
        Self {
            counter: AtomicU64::new(start.raw()),
        }
    }

    /// 获取下一个事务 ID。
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

    /// 如果传入值更大则推进计数器。
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

// --------- 全局单例 ---------

static GLOBAL_COMMIT_TS_GENERATOR: OnceLock<Arc<CommitTsGenerator>> = OnceLock::new();
static GLOBAL_TXN_ID_GENERATOR: OnceLock<Arc<TxnIdGenerator>> = OnceLock::new();

/// 获取全局提交时间戳生成器。
pub fn global_commit_ts_generator() -> Arc<CommitTsGenerator> {
    GLOBAL_COMMIT_TS_GENERATOR
        .get_or_init(|| Arc::new(CommitTsGenerator::new()))
        .clone()
}

/// 获取全局事务 ID 生成器。
pub fn global_txn_id_generator() -> Arc<TxnIdGenerator> {
    GLOBAL_TXN_ID_GENERATOR
        .get_or_init(|| Arc::new(TxnIdGenerator::new()))
        .clone()
}

/// 初始化全局提交时间戳生成器。
pub fn init_global_commit_ts_generator(start: CommitTs) -> Result<(), &'static str> {
    GLOBAL_COMMIT_TS_GENERATOR
        .set(Arc::new(CommitTsGenerator::with_start(start)))
        .map_err(|_| "Global commit-ts generator already initialized")
}

/// 初始化全局事务 ID 生成器。
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
