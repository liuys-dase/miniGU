use std::sync::{Arc, Mutex, OnceLock, RwLock};
use dashmap::{DashMap, DashSet};
use minigu_storage::{
    common::transaction::{IsolationLevel, Timestamp},
    tp::{MemTransaction, transaction::MemTxnManager},
};

use crate::error::CatalogError;
use super::{
    metadata::{MetadataKey, MetadataValue},
    timestamp::{TimestampManager, TransactionId},
    version_chain::{CatalogUndoEntry, MetadataVersionChain, MetadataUndoEntry},
};

/// System-level transaction manager that coordinates catalog and storage transactions
pub struct SystemTransactionManager {
    /// Unified timestamp manager
    pub timestamp_manager: Arc<TimestampManager>,
    /// Version manager for metadata
    pub version_manager: Arc<VersionManager>,
    /// Catalog transaction manager
    pub catalog_txn_manager: Arc<CatalogTxnManager>,
    /// Storage transaction manager
    pub storage_txn_manager: Arc<MemTxnManager>,
    /// Active system transactions
    pub active_transactions: DashMap<TransactionId, Arc<SystemTransaction>>,
    /// Commit lock to ensure serial commit order
    pub commit_lock: Mutex<()>,
}

impl SystemTransactionManager {
    /// Create a new system transaction manager
    pub fn new(storage_txn_manager: Arc<MemTxnManager>) -> Self {
        let timestamp_manager = Arc::new(TimestampManager::new());
        let version_manager = Arc::new(VersionManager::new());
        let catalog_txn_manager = Arc::new(CatalogTxnManager::new(
            timestamp_manager.clone(),
            version_manager.clone(),
        ));

        Self {
            timestamp_manager,
            version_manager,
            catalog_txn_manager,
            storage_txn_manager,
            active_transactions: DashMap::new(),
            commit_lock: Mutex::new(()),
        }
    }

    /// Begin a new system transaction
    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> Arc<SystemTransaction> {
        let txn_id = self.timestamp_manager.new_transaction_id();
        let start_ts = self.timestamp_manager.get_snapshot_timestamp();

        let system_txn = Arc::new(SystemTransaction {
            txn_id,
            start_ts,
            commit_ts: OnceLock::new(),
            isolation_level,
            catalog_txn: RwLock::new(None),
            storage_txn: RwLock::new(None),
            system_undo_buffer: RwLock::new(Vec::new()),
        });

        self.active_transactions.insert(txn_id, system_txn.clone());
        system_txn
    }

    /// Calculate watermark based on active transactions
    pub fn calculate_watermark(&self) -> Timestamp {
        let active_start_timestamps: Vec<Timestamp> = self
            .active_transactions
            .iter()
            .map(|entry| entry.value().start_ts)
            .collect();

        if active_start_timestamps.is_empty() {
            self.timestamp_manager.latest_commit_timestamp()
        } else {
            *active_start_timestamps.iter().min().unwrap()
        }
    }

    /// Update watermark
    pub fn update_watermark(&self) {
        let active_start_timestamps: Vec<Timestamp> = self
            .active_transactions
            .iter()
            .map(|entry| entry.value().start_ts)
            .collect();

        self.timestamp_manager.update_watermark(&active_start_timestamps);
    }
}

/// System-level transaction that can span catalog and storage operations
pub struct SystemTransaction {
    /// Transaction ID
    pub txn_id: TransactionId,
    /// Start timestamp
    pub start_ts: Timestamp,
    /// Commit timestamp (set when transaction commits)
    pub commit_ts: OnceLock<Timestamp>,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Catalog sub-transaction (lazy initialization)
    pub catalog_txn: RwLock<Option<Arc<CatalogTransaction>>>,
    /// Storage sub-transaction (lazy initialization)
    pub storage_txn: RwLock<Option<Arc<MemTransaction>>>,
    /// System-level undo buffer
    pub system_undo_buffer: RwLock<Vec<SystemUndoEntry>>,
}

impl SystemTransaction {
    /// Get transaction ID
    pub fn txn_id(&self) -> TransactionId {
        self.txn_id
    }

    /// Get start timestamp
    pub fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    /// Get commit timestamp (if committed)
    pub fn commit_ts(&self) -> Option<Timestamp> {
        self.commit_ts.get().copied()
    }

    /// Get or create catalog transaction
    pub fn get_or_create_catalog_txn(
        &self,
        _catalog_manager: Arc<CatalogTxnManager>,
    ) -> Arc<CatalogTransaction> {
        let mut catalog_txn_guard = self.catalog_txn.write().unwrap();
        if let Some(ref txn) = *catalog_txn_guard {
            return txn.clone();
        }

        let catalog_txn = Arc::new(CatalogTransaction {
            txn_id: self.txn_id,
            start_ts: self.start_ts,
            catalog_undo_buffer: RwLock::new(Vec::new()),
            read_set: DashSet::new(),
        });

        *catalog_txn_guard = Some(catalog_txn.clone());
        catalog_txn
    }

    /// Get or create storage transaction
    pub fn get_or_create_storage_txn(
        &self,
        storage_manager: Arc<MemTxnManager>,
    ) -> Result<Arc<MemTransaction>, CatalogError> {
        let storage_txn_guard = self.storage_txn.write().unwrap();
        if let Some(ref txn) = *storage_txn_guard {
            return Ok(txn.clone());
        }

        // Create new storage transaction using the existing MemTxnManager interface
        let _storage_txn_id = storage_manager.new_txn_id(Some(self.txn_id));
        // Note: We need to adapt this to the actual MemTransaction creation interface
        // This is a placeholder that will need to be adjusted based on the actual API
        
        // For now, return an error indicating this needs to be implemented
        Err(CatalogError::Other("Storage transaction creation not yet implemented in system transaction".to_string()))
    }

    /// Basic commit implementation (will be expanded)
    pub fn commit(&self, system_manager: &SystemTransactionManager) -> Result<Timestamp, SystemTransactionError> {
        let _commit_guard = system_manager.commit_lock.lock().unwrap();

        // Allocate commit timestamp
        let commit_ts = system_manager.timestamp_manager.new_commit_timestamp();
        self.commit_ts.set(commit_ts).map_err(|_| SystemTransactionError::AlreadyCommitted)?;

        // TODO: Implement two-phase commit for catalog and storage

        // Update system state
        system_manager.active_transactions.remove(&self.txn_id);
        system_manager.timestamp_manager.update_latest_commit_timestamp(commit_ts);
        system_manager.update_watermark();

        Ok(commit_ts)
    }

    /// Basic abort implementation (will be expanded)
    pub fn abort(&self, system_manager: &SystemTransactionManager) -> Result<(), SystemTransactionError> {
        // TODO: Implement rollback for catalog and storage sub-transactions

        // Clean up transaction state
        system_manager.active_transactions.remove(&self.txn_id);
        system_manager.update_watermark();

        Ok(())
    }
}

/// Catalog transaction for metadata operations
pub struct CatalogTransaction {
    /// Transaction ID
    pub txn_id: TransactionId,
    /// Start timestamp
    pub start_ts: Timestamp,
    /// Catalog-specific undo buffer
    pub catalog_undo_buffer: RwLock<Vec<CatalogUndoEntry>>,
    /// Read set for serializable isolation
    pub read_set: DashSet<MetadataKey>,
}

impl CatalogTransaction {
    /// Read metadata with MVCC visibility
    pub fn read_metadata(
        &self,
        key: &MetadataKey,
        version_manager: &VersionManager,
    ) -> Result<Option<MetadataValue>, CatalogError> {
        // Record in read set
        self.read_set.insert(key.clone());

        if let Some(version_chain) = version_manager.version_chains.get(key) {
            let current = version_chain.current.read().unwrap();

            if current.is_visible_to(self.start_ts) {
                if current.is_tombstone {
                    return Ok(None);
                }

                // TODO: Apply undo chain to reconstruct historical version
                let mut data = current.data.clone();
                self.apply_undo_chain(&version_chain.undo_ptr, &mut data)?;

                return Ok(Some(data));
            }
        }

        Ok(None)
    }

    /// Write metadata with conflict detection
    pub fn write_metadata(
        &self,
        key: MetadataKey,
        value: MetadataValue,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let version_chain = version_manager
            .version_chains
            .entry(key.clone())
            .or_insert_with(|| Arc::new(MetadataVersionChain::new_tombstone()));

        let mut current = version_chain.current.write().unwrap();

        // Write-write conflict detection
        if current.commit_ts > self.start_ts && !current.commit_ts.is_txn_id() {
            return Err(CatalogError::WriteWriteConflict);
        }

        // Create undo entry
        let old_data = current.data.clone();
        let undo_entry = Arc::new(MetadataUndoEntry::new_update(
            old_data,
            current.commit_ts,
            self.txn_id,
            version_chain.undo_ptr.read().unwrap().clone(),
        ));

        // Update version chain
        *version_chain.undo_ptr.write().unwrap() = Some(undo_entry.clone());
        current.data = value;
        current.commit_ts = self.txn_id; // Use txn_id as temporary timestamp
        current.is_tombstone = false;

        // Record undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::Write {
                key,
                old_value: undo_entry,
            });

        Ok(())
    }

    /// Apply undo chain to reconstruct historical version
    fn apply_undo_chain(
        &self,
        undo_ptr: &RwLock<Option<Arc<MetadataUndoEntry>>>,
        data: &mut MetadataValue,
    ) -> Result<(), CatalogError> {
        let mut current_undo = undo_ptr.read().unwrap().clone();

        while let Some(undo_entry) = current_undo {
            if undo_entry.timestamp > self.start_ts {
                undo_entry.apply_reverse(data);
                current_undo = undo_entry.next.clone();
            } else {
                break;
            }
        }

        Ok(())
    }
}

/// Catalog transaction manager
pub struct CatalogTxnManager {
    /// Timestamp manager
    pub timestamp_manager: Arc<TimestampManager>,
    /// Version manager
    pub version_manager: Arc<VersionManager>,
    /// Transaction read sets for conflict detection
    pub read_sets: DashMap<TransactionId, DashSet<MetadataKey>>,
}

impl CatalogTxnManager {
    /// Create a new catalog transaction manager
    pub fn new(
        timestamp_manager: Arc<TimestampManager>,
        version_manager: Arc<VersionManager>,
    ) -> Self {
        Self {
            timestamp_manager,
            version_manager,
            read_sets: DashMap::new(),
        }
    }
}

/// Version manager for metadata MVCC
pub struct VersionManager {
    /// Metadata version chains
    pub version_chains: DashMap<MetadataKey, Arc<MetadataVersionChain>>,
    /// Garbage collection threshold
    pub gc_threshold: u64,
}

impl VersionManager {
    /// Create a new version manager
    pub fn new() -> Self {
        Self {
            version_chains: DashMap::new(),
            gc_threshold: 100, // Default GC threshold
        }
    }

    /// Set garbage collection threshold
    pub fn set_gc_threshold(&mut self, threshold: u64) {
        self.gc_threshold = threshold;
    }
}

/// System-level undo entry
#[derive(Debug, Clone)]
pub enum SystemUndoEntry {
    CatalogOperation(CatalogUndoEntry),
    StorageOperation(String), // Placeholder for storage operations
}

/// System transaction errors
#[derive(Debug, thiserror::Error)]
pub enum SystemTransactionError {
    #[error("Transaction already committed")]
    AlreadyCommitted,
    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),
    #[error("Catalog error: {0}")]
    CatalogError(#[from] CatalogError),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Write-write conflict detected")]
    WriteWriteConflict,
    #[error("Serialization failure")]
    SerializationFailure,
}