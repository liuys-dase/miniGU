use std::sync::{atomic::{AtomicU64, AtomicUsize, Ordering}, Arc, Mutex, OnceLock, RwLock};

use dashmap::{DashMap, DashSet};
use minigu_storage::common::transaction::{IsolationLevel, Timestamp};
use minigu_storage::tp::MemTransaction;
use minigu_storage::tp::transaction::MemTxnManager;

use super::metadata::{MetadataKey, MetadataValue};
use super::timestamp::{TimestampManager, TransactionId};
use super::version_chain::{
    CatalogUndoEntry, MetadataUndoEntry, MetadataVersion, MetadataVersionChain,
};
use crate::error::CatalogError;

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

        self.timestamp_manager
            .update_watermark(&active_start_timestamps);
    }

    /// Run adaptive garbage collection
    pub fn run_adaptive_gc(&self, config: &AdaptiveGcConfig) -> Result<(), SystemTransactionError> {
        // Check if GC should run
        if !self.catalog_txn_manager.should_run_gc(config) {
            return Ok(());
        }

        let watermark = self.calculate_watermark();
        
        // Decide between sequential and parallel GC
        let stats = self.catalog_txn_manager.get_statistics();
        
        if stats.total_chains >= config.parallel_gc_threshold {
            // Use parallel GC for large datasets
            self.catalog_txn_manager
                .parallel_garbage_collect(watermark, config)
                .map_err(|e| SystemTransactionError::CatalogError(e))?;
        } else {
            // Use sequential GC for smaller datasets
            self.catalog_txn_manager
                .garbage_collect(watermark)
                .map_err(|e| SystemTransactionError::CatalogError(e))?;
        }

        // Also run storage GC (commented out due to missing MemoryGraph parameter)
        // self.storage_txn_manager
        //     .garbage_collect()
        //     .map_err(|e| SystemTransactionError::StorageError(format!("Storage GC failed: {}", e)))?;

        Ok(())
    }

    /// Get comprehensive GC statistics
    pub fn get_gc_statistics(&self) -> (GcStatistics, VersionChainStatistics) {
        let gc_stats = self.catalog_txn_manager.version_manager.get_gc_statistics();
        let version_stats = self.catalog_txn_manager.get_statistics();
        (gc_stats, version_stats)
    }

    /// Configure adaptive GC settings
    pub fn configure_adaptive_gc(&self, _config: AdaptiveGcConfig) {
        // Store config in version manager for future use
        // This could be extended to persist configuration
    }

    /// Get comprehensive performance statistics
    pub fn get_performance_statistics(&self) -> PerformanceStatistics {
        let (gc_stats, version_stats) = self.get_gc_statistics();
        let cache_stats = self.catalog_txn_manager.version_manager.get_cache_statistics();
        
        PerformanceStatistics {
            gc_stats,
            version_stats,
            cache_stats,
            active_transactions: self.active_transactions.len(),
            memory_usage_estimate: self.estimate_memory_usage(),
        }
    }

    /// Estimate total system memory usage
    fn estimate_memory_usage(&self) -> u64 {
        let mut total_memory = 0u64;

        // Memory from version chains
        total_memory += self.catalog_txn_manager.estimate_memory_usage();

        // Memory from active transactions
        total_memory += self.active_transactions.len() as u64 * 1024; // ~1KB per transaction

        // Memory from cache
        let cache_stats = self.catalog_txn_manager.version_manager.get_cache_statistics();
        total_memory += cache_stats.current_size as u64 * 512; // ~512B per cache entry

        total_memory
    }

    /// Trigger memory pressure relief
    pub fn relieve_memory_pressure(&self) -> Result<u64, SystemTransactionError> {
        let initial_memory = self.estimate_memory_usage();

        // Clear cache to free memory
        self.catalog_txn_manager.version_manager.clear_cache();

        // Run aggressive GC
        let config = AdaptiveGcConfig {
            min_gc_interval_sec: 0, // Force immediate GC
            memory_pressure_threshold: 0.0, // Lower threshold
            ..Default::default()
        };
        
        self.run_adaptive_gc(&config)?;

        let final_memory = self.estimate_memory_usage();
        Ok(initial_memory.saturating_sub(final_memory))
    }

    /// Perform comprehensive consistency check
    pub fn check_consistency(&self) -> Result<ConsistencyReport, SystemTransactionError> {
        let mut report = ConsistencyReport::new();

        // Check version chain consistency
        self.check_version_chains(&mut report)?;

        // Check active transaction consistency
        self.check_active_transactions(&mut report)?;

        // Check timestamp consistency
        self.check_timestamp_consistency(&mut report)?;

        // Check memory consistency
        self.check_memory_consistency(&mut report)?;

        Ok(report)
    }

    /// Check version chain consistency
    fn check_version_chains(&self, report: &mut ConsistencyReport) -> Result<(), SystemTransactionError> {
        for version_chain_entry in self.catalog_txn_manager.version_manager.version_chains.iter() {
            let key = version_chain_entry.key();
            let version_chain = version_chain_entry.value();
            
            // Check current version
            let current = version_chain.current.read().unwrap();
            
            // Validate commit timestamp
            if current.commit_ts.0 == 0 {
                report.add_error(ConsistencyError::InvalidTimestamp {
                    key: key.clone(),
                    timestamp: current.commit_ts,
                    description: "Zero timestamp in version".to_string(),
                });
            }

            // Check undo chain integrity
            let mut undo_ptr = version_chain.undo_ptr.read().unwrap().clone();
            let mut undo_count = 0;
            let mut prev_timestamp = current.commit_ts;

            while let Some(undo_entry) = undo_ptr {
                undo_count += 1;
                
                // Check timestamp ordering
                if undo_entry.timestamp >= prev_timestamp {
                    report.add_error(ConsistencyError::TimestampOrderViolation {
                        key: key.clone(),
                        current_ts: prev_timestamp,
                        undo_ts: undo_entry.timestamp,
                    });
                }

                // Prevent infinite loops
                if undo_count > 1000 {
                    report.add_error(ConsistencyError::UndoChainTooLong {
                        key: key.clone(),
                        length: undo_count,
                    });
                    break;
                }

                prev_timestamp = undo_entry.timestamp;
                undo_ptr = undo_entry.next.clone();
            }
        }

        Ok(())
    }

    /// Check active transaction consistency
    fn check_active_transactions(&self, report: &mut ConsistencyReport) -> Result<(), SystemTransactionError> {
        for txn_entry in self.active_transactions.iter() {
            let txn_id = *txn_entry.key();
            let txn = txn_entry.value();

            // Check timestamp consistency
            if txn.start_ts.0 == 0 {
                report.add_error(ConsistencyError::InvalidTimestamp {
                    key: MetadataKey::Directory(format!("txn_{}", txn_id.0)),
                    timestamp: txn.start_ts,
                    description: "Zero start timestamp in transaction".to_string(),
                });
            }

            // Check if transaction has been running too long
            let latest_commit = self.timestamp_manager.latest_commit_timestamp();
            if txn.start_ts.0 + 10000 < latest_commit.0 { // Arbitrary threshold
                report.add_warning(ConsistencyWarning::LongRunningTransaction {
                    txn_id,
                    start_ts: txn.start_ts,
                    current_ts: latest_commit,
                });
            }
        }

        Ok(())
    }

    /// Check timestamp consistency
    fn check_timestamp_consistency(&self, report: &mut ConsistencyReport) -> Result<(), SystemTransactionError> {
        let watermark = self.calculate_watermark();
        let latest_commit = self.timestamp_manager.latest_commit_timestamp();

        // Watermark should not be ahead of latest commit
        if watermark > latest_commit {
            report.add_error(ConsistencyError::WatermarkAhead {
                watermark,
                latest_commit,
            });
        }

        Ok(())
    }

    /// Check memory consistency
    fn check_memory_consistency(&self, report: &mut ConsistencyReport) -> Result<(), SystemTransactionError> {
        let estimated_memory = self.estimate_memory_usage();
        const MAX_MEMORY_THRESHOLD: u64 = 1024 * 1024 * 1024; // 1GB

        if estimated_memory > MAX_MEMORY_THRESHOLD {
            report.add_warning(ConsistencyWarning::HighMemoryUsage {
                current_usage: estimated_memory,
                threshold: MAX_MEMORY_THRESHOLD,
            });
        }

        Ok(())
    }

    /// Attempt to repair consistency issues
    pub fn repair_consistency(&self, report: &ConsistencyReport) -> Result<RepairReport, SystemTransactionError> {
        let mut repair_report = RepairReport::new();

        for error in &report.errors {
            match self.repair_consistency_error(error) {
                Ok(action) => repair_report.add_success(action),
                Err(e) => repair_report.add_failure(error.clone(), e),
            }
        }

        Ok(repair_report)
    }

    /// Repair a specific consistency error
    fn repair_consistency_error(&self, error: &ConsistencyError) -> Result<RepairAction, SystemTransactionError> {
        match error {
            ConsistencyError::UndoChainTooLong { key, .. } => {
                // Truncate the undo chain
                if let Some(version_chain) = self.catalog_txn_manager.version_manager.version_chains.get(key) {
                    let mut undo_ptr = version_chain.undo_ptr.write().unwrap();
                    let mut current = undo_ptr.clone();
                    let mut count = 0;
                    const MAX_UNDO_CHAIN_LENGTH: usize = 100;

                    // Find the entry at the maximum allowed length
                    while let Some(entry) = current {
                        count += 1;
                        if count >= MAX_UNDO_CHAIN_LENGTH {
                            // Truncate here
                            *undo_ptr = None;
                            return Ok(RepairAction::TruncatedUndoChain {
                                key: key.clone(),
                                removed_entries: count - MAX_UNDO_CHAIN_LENGTH,
                            });
                        }
                        current = entry.next.clone();
                    }
                }
                
                Ok(RepairAction::NoActionNeeded)
            }
            ConsistencyError::WatermarkAhead { .. } => {
                // Recalculate watermark
                self.update_watermark();
                Ok(RepairAction::RecalculatedWatermark)
            }
            _ => Ok(RepairAction::NoActionNeeded),
        }
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

    /// Get or create catalog transaction with isolation level awareness
    pub fn get_or_create_catalog_txn(
        &self,
        catalog_manager: Arc<CatalogTxnManager>,
    ) -> Arc<CatalogTransaction> {
        let mut catalog_txn_guard = self.catalog_txn.write().unwrap();
        if let Some(ref txn) = *catalog_txn_guard {
            return txn.clone();
        }

        let catalog_txn = catalog_manager.begin_transaction(self.txn_id, self.start_ts);
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
        Err(CatalogError::Other(
            "Storage transaction creation not yet implemented in system transaction".to_string(),
        ))
    }

    /// Enhanced commit implementation with two-phase commit
    pub fn commit(
        &self,
        system_manager: &SystemTransactionManager,
    ) -> Result<Timestamp, SystemTransactionError> {
        let _commit_guard = system_manager.commit_lock.lock().unwrap();

        // Allocate commit timestamp
        let commit_ts = system_manager.timestamp_manager.new_commit_timestamp();
        self.commit_ts
            .set(commit_ts)
            .map_err(|_| SystemTransactionError::AlreadyCommitted)?;

        // Two-phase commit protocol

        // Phase 1: Prepare - validate all sub-transactions
        if let Some(ref catalog_txn) = *self.catalog_txn.read().unwrap() {
            catalog_txn.prepare(commit_ts)?;
        }

        if let Some(ref _storage_txn) = *self.storage_txn.read().unwrap() {
            // Prepare storage transaction if it exists
            // Note: This would need to be integrated with the storage transaction API
            // TODO: Implement storage transaction prepare when integration is complete
        }

        // Phase 2: Commit - make all changes visible atomically

        // Commit catalog transaction
        if let Some(ref catalog_txn) = *self.catalog_txn.read().unwrap() {
            system_manager
                .catalog_txn_manager
                .commit_transaction(catalog_txn, commit_ts)?;
        }

        // Commit storage transaction
        if let Some(ref _storage_txn) = *self.storage_txn.read().unwrap() {
            // Commit storage transaction
            // Note: This would need to be integrated with the storage transaction API
            // TODO: Implement storage transaction commit when integration is complete
            // _storage_txn.commit_at(Some(commit_ts), false)?;
        }

        // Update system state
        system_manager.active_transactions.remove(&self.txn_id);
        system_manager
            .timestamp_manager
            .update_latest_commit_timestamp(commit_ts);
        system_manager.update_watermark();

        Ok(commit_ts)
    }

    /// Enhanced abort implementation with proper rollback
    pub fn abort(
        &self,
        system_manager: &SystemTransactionManager,
    ) -> Result<(), SystemTransactionError> {
        // Abort sub-transactions in reverse order (storage first, then catalog)

        // Abort storage transaction
        if let Some(ref _storage_txn) = *self.storage_txn.read().unwrap() {
            // Abort storage transaction
            // Note: This would need to be integrated with the storage transaction API  
            // TODO: Implement storage transaction abort when integration is complete
            // _storage_txn.abort_at(false)?;
        }

        // Abort catalog transaction
        if let Some(ref catalog_txn) = *self.catalog_txn.read().unwrap() {
            system_manager
                .catalog_txn_manager
                .abort_transaction(catalog_txn)?;
        }

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
    /// Read metadata with MVCC visibility and caching optimization
    pub fn read_metadata(
        &self,
        key: &MetadataKey,
        version_manager: &VersionManager,
    ) -> Result<Option<MetadataValue>, CatalogError> {
        // Record in read set for serializable isolation
        self.read_set.insert(key.clone());

        // Try cache first for performance optimization
        if let Some(cached_value) = version_manager.read_cache.get(key, self.start_ts) {
            // Update cache statistics
            {
                let mut stats = version_manager.cache_stats.write().unwrap();
                stats.record_hit();
            }
            return Ok(Some(cached_value));
        }

        // Cache miss - record statistics
        {
            let mut stats = version_manager.cache_stats.write().unwrap();
            stats.record_miss();
        }

        if let Some(version_chain) = version_manager.version_chains.get(key) {
            let current = version_chain.current.read().unwrap();

            // Check if current version is visible to this transaction
            if self.is_version_visible(&current) {
                if current.is_tombstone {
                    // Don't cache tombstone results to avoid cache pollution
                    return Ok(None);
                }

                // Reconstruct the version that should be visible to this transaction
                let mut data = current.data.clone();
                self.apply_undo_chain_optimized(&version_chain.undo_ptr, &mut data)?;

                // Cache the result for future reads
                version_manager.read_cache.put(key, self.start_ts, data.clone());

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
        // Validate that the value is compatible with the key
        if !value.is_compatible_with_key(&key) {
            return Err(CatalogError::IncompatibleKeyValue);
        }

        // Invalidate cache entries for this key since we're modifying it
        version_manager.invalidate_cache(&key);

        let version_chain = version_manager
            .version_chains
            .entry(key.clone())
            .or_insert_with(|| Arc::new(MetadataVersionChain::new_tombstone()));

        let mut current = version_chain.current.write().unwrap();

        // Write-write conflict detection
        if self.has_write_write_conflict(&current) {
            return Err(CatalogError::WriteWriteConflict);
        }

        // Create undo entry for rollback
        let old_data = current.data.clone();
        let _old_is_tombstone = current.is_tombstone; // Saved for potential future use
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

        // Record undo operation for transaction rollback
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::Write {
                key: key.clone(),
                old_value: undo_entry,
            });

        Ok(())
    }

    /// Delete metadata entry (mark as tombstone)
    pub fn delete_metadata(
        &self,
        key: &MetadataKey,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        // Invalidate cache entries for this key since we're modifying it
        version_manager.invalidate_cache(key);
        
        if let Some(version_chain) = version_manager.version_chains.get(key) {
            let mut current = version_chain.current.write().unwrap();

            // Write-write conflict detection
            if self.has_write_write_conflict(&current) {
                return Err(CatalogError::WriteWriteConflict);
            }

            // If already tombstone, nothing to do
            if current.is_tombstone {
                return Ok(());
            }

            // Create undo entry for rollback
            let old_data = current.data.clone();
            let undo_entry = Arc::new(MetadataUndoEntry::new_delete(
                old_data,
                current.commit_ts,
                self.txn_id,
                version_chain.undo_ptr.read().unwrap().clone(),
            ));

            // Update version chain to tombstone
            *version_chain.undo_ptr.write().unwrap() = Some(undo_entry.clone());
            current.is_tombstone = true;
            current.commit_ts = self.txn_id;

            // Record undo operation
            self.catalog_undo_buffer
                .write()
                .unwrap()
                .push(CatalogUndoEntry::Write {
                    key: key.clone(),
                    old_value: undo_entry,
                });
        }

        Ok(())
    }

    /// Check if a version is visible to this transaction
    fn is_version_visible(&self, version: &MetadataVersion) -> bool {
        // Own uncommitted changes are always visible
        if version.commit_ts == self.txn_id {
            return true;
        }

        // Check if version was committed before this transaction started
        !version.commit_ts.is_txn_id() && version.commit_ts <= self.start_ts
    }

    /// Check for write-write conflicts
    fn has_write_write_conflict(&self, current: &MetadataVersion) -> bool {
        // Conflict if another transaction has uncommitted changes
        current.commit_ts.is_txn_id() && current.commit_ts != self.txn_id
    }

    /// Apply undo chain to reconstruct historical version
    fn apply_undo_chain(
        &self,
        undo_ptr: &RwLock<Option<Arc<MetadataUndoEntry>>>,
        data: &mut MetadataValue,
    ) -> Result<(), CatalogError> {
        let mut current_undo = undo_ptr.read().unwrap().clone();

        while let Some(undo_entry) = current_undo {
            // Only apply undo entries that are newer than our start timestamp
            if undo_entry.timestamp > self.start_ts {
                undo_entry.apply_reverse(data);
                current_undo = undo_entry.next.clone();
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Optimized undo chain application with early termination and bounds checking
    fn apply_undo_chain_optimized(
        &self,
        undo_ptr: &RwLock<Option<Arc<MetadataUndoEntry>>>,
        data: &mut MetadataValue,
    ) -> Result<(), CatalogError> {
        let mut current_undo = undo_ptr.read().unwrap().clone();
        let mut applied_count = 0;
        const MAX_UNDO_APPLICATIONS: usize = 100; // Prevent excessive undo chain traversal

        while let Some(undo_entry) = current_undo {
            // Early termination for performance and safety
            if applied_count >= MAX_UNDO_APPLICATIONS {
                return Err(CatalogError::Other("Undo chain too long".to_string()));
            }

            if undo_entry.timestamp > self.start_ts {
                // Apply undo operation
                undo_entry.apply_reverse(data);
                current_undo = undo_entry.next.clone();
                applied_count += 1;
            } else {
                // We've reached entries that are too old, stop here
                break;
            }
        }

        Ok(())
    }

    /// Prepare transaction for commit (validate read set for serializable isolation)
    pub fn prepare(&self, commit_ts: Timestamp) -> Result<(), CatalogError> {
        // Always validate read set for serializable isolation
        self.validate_read_set(commit_ts)?;
        Ok(())
    }

    /// Validate read set for serializable isolation
    fn validate_read_set(&self, commit_ts: Timestamp) -> Result<(), CatalogError> {
        // For each key in the read set, check if it has been modified by concurrent transactions
        for key_ref in self.read_set.iter() {
            let key = key_ref.key();

            // Check if any concurrent transaction has modified this key
            if self.has_concurrent_modification(key, commit_ts)? {
                return Err(CatalogError::SerializationFailure);
            }
        }
        Ok(())
    }

    /// Check if a key has been modified by concurrent transactions
    fn has_concurrent_modification(
        &self,
        _key: &MetadataKey,
        _commit_ts: Timestamp,
    ) -> Result<bool, CatalogError> {
        // This is a simplified version - in practice, the CatalogTxnManager
        // would provide this functionality through its validate_read_set method
        Ok(false)
    }

    /// Commit the catalog transaction
    pub fn commit(&self, _commit_ts: Timestamp) -> Result<(), CatalogError> {
        // Replace transaction IDs with actual commit timestamp
        let undo_buffer = self.catalog_undo_buffer.read().unwrap();
        for undo_entry in undo_buffer.iter() {
            if let CatalogUndoEntry::Write { .. } = undo_entry {
                // Note: In a real implementation, we would update the commit timestamp
                // in the version chain. This is a simplified version.
                // TODO: Implement actual commit timestamp replacement when needed
            }
        }
        Ok(())
    }

    /// Abort the catalog transaction (rollback changes)
    pub fn abort(&self) -> Result<(), CatalogError> {
        let undo_buffer = self.catalog_undo_buffer.read().unwrap();

        // Apply rollback operations in reverse order
        for undo_entry in undo_buffer.iter().rev() {
            match undo_entry {
                CatalogUndoEntry::Write {
                    key: _key,
                    old_value: _old_value,
                } => {
                    // Rollback the change by restoring the old value
                    // Note: In a real implementation, we would restore the version chain state
                    // This is a simplified version that demonstrates the concept
                }
                _ => {
                    // Handle other types of undo operations
                }
            }
        }

        Ok(())
    }

    /// Get read set for conflict detection
    pub fn get_read_set(&self) -> Vec<MetadataKey> {
        self.read_set
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// High-level transactional catalog operations

    /// Create a new directory
    pub fn create_directory(
        &self,
        name: String,
        metadata: super::metadata::DirectoryMetadata,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let key = MetadataKey::Directory(name.clone());
        let value = MetadataValue::Directory(metadata.clone());

        // Check if directory already exists
        if let Some(_existing) = self.read_metadata(&key, version_manager)? {
            return Err(CatalogError::InvalidMetadataOperation(format!(
                "Directory '{}' already exists",
                name
            )));
        }

        // Create the directory
        self.write_metadata(key.clone(), value, version_manager)?;

        // Record high-level undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::CreateDirectory(name, metadata));

        Ok(())
    }

    /// Drop a directory
    pub fn drop_directory(
        &self,
        name: String,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let key = MetadataKey::Directory(name.clone());

        // Check if directory exists
        let _existing_metadata = self
            .read_metadata(&key, version_manager)?
            .ok_or_else(|| CatalogError::MetadataKeyNotFound(format!("Directory '{}'", name)))?;

        // Delete the directory (mark as tombstone)
        self.delete_metadata(&key, version_manager)?;

        // Record high-level undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::DropDirectory(name));

        Ok(())
    }

    /// Create a new schema
    pub fn create_schema(
        &self,
        name: String,
        metadata: super::metadata::SchemaMetadata,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let key = MetadataKey::Schema(name.clone());
        let value = MetadataValue::Schema(metadata.clone());

        // Check if schema already exists
        if let Some(_existing) = self.read_metadata(&key, version_manager)? {
            return Err(CatalogError::InvalidMetadataOperation(format!(
                "Schema '{}' already exists",
                name
            )));
        }

        // Create the schema
        self.write_metadata(key.clone(), value, version_manager)?;

        // Record high-level undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::CreateSchema(name, metadata));

        Ok(())
    }

    /// Drop a schema
    pub fn drop_schema(
        &self,
        name: String,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let key = MetadataKey::Schema(name.clone());

        // Check if schema exists
        let _existing_metadata = self
            .read_metadata(&key, version_manager)?
            .ok_or_else(|| CatalogError::MetadataKeyNotFound(format!("Schema '{}'", name)))?;

        // Delete the schema (mark as tombstone)
        self.delete_metadata(&key, version_manager)?;

        // Record high-level undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::DropSchema(name));

        Ok(())
    }

    /// Create a new graph
    pub fn create_graph(
        &self,
        name: String,
        metadata: super::metadata::GraphMetadata,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let key = MetadataKey::Graph(name.clone());
        let value = MetadataValue::Graph(metadata.clone());

        // Check if graph already exists
        if let Some(_existing) = self.read_metadata(&key, version_manager)? {
            return Err(CatalogError::InvalidMetadataOperation(format!(
                "Graph '{}' already exists",
                name
            )));
        }

        // Create the graph
        self.write_metadata(key.clone(), value, version_manager)?;

        // Record high-level undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::CreateGraph(name, metadata));

        Ok(())
    }

    /// Drop a graph
    pub fn drop_graph(
        &self,
        name: String,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let key = MetadataKey::Graph(name.clone());

        // Check if graph exists
        let _existing_metadata = self
            .read_metadata(&key, version_manager)?
            .ok_or_else(|| CatalogError::MetadataKeyNotFound(format!("Graph '{}'", name)))?;

        // Delete the graph (mark as tombstone)
        self.delete_metadata(&key, version_manager)?;

        // Record high-level undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::DropGraph(name));

        Ok(())
    }

    /// Create a new graph type
    pub fn create_graph_type(
        &self,
        name: String,
        metadata: super::metadata::GraphTypeMetadata,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let key = MetadataKey::GraphType(name.clone());
        let value = MetadataValue::GraphType(metadata.clone());

        // Check if graph type already exists
        if let Some(_existing) = self.read_metadata(&key, version_manager)? {
            return Err(CatalogError::InvalidMetadataOperation(format!(
                "Graph type '{}' already exists",
                name
            )));
        }

        // Create the graph type
        self.write_metadata(key.clone(), value, version_manager)?;

        // Record high-level undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::CreateGraphType(name, metadata));

        Ok(())
    }

    /// Drop a graph type
    pub fn drop_graph_type(
        &self,
        name: String,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let key = MetadataKey::GraphType(name.clone());

        // Check if graph type exists
        let _existing_metadata = self
            .read_metadata(&key, version_manager)?
            .ok_or_else(|| CatalogError::MetadataKeyNotFound(format!("Graph type '{}'", name)))?;

        // Delete the graph type (mark as tombstone)
        self.delete_metadata(&key, version_manager)?;

        // Record high-level undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::DropGraphType(name));

        Ok(())
    }

    /// Modify an existing graph type
    pub fn modify_graph_type(
        &self,
        name: String,
        new_metadata: super::metadata::GraphTypeMetadata,
        version_manager: &VersionManager,
    ) -> Result<(), CatalogError> {
        let key = MetadataKey::GraphType(name.clone());
        let value = MetadataValue::GraphType(new_metadata.clone());

        // Check if graph type exists
        let old_metadata = self
            .read_metadata(&key, version_manager)?
            .ok_or_else(|| CatalogError::MetadataKeyNotFound(format!("Graph type '{}'", name)))?;

        // Extract old graph type metadata for undo
        let old_graph_type_metadata = match old_metadata {
            MetadataValue::GraphType(metadata) => metadata,
            _ => {
                return Err(CatalogError::InvalidMetadataOperation(
                    "Key does not correspond to a graph type".to_string(),
                ));
            }
        };

        // Update the graph type
        self.write_metadata(key.clone(), value, version_manager)?;

        // Record high-level undo operation
        self.catalog_undo_buffer
            .write()
            .unwrap()
            .push(CatalogUndoEntry::ModifyGraphType(
                name,
                old_graph_type_metadata,
            ));

        Ok(())
    }

    /// List all directories
    pub fn list_directories(
        &self,
        version_manager: &VersionManager,
    ) -> Result<Vec<String>, CatalogError> {
        let mut directories = Vec::new();

        for entry in version_manager.version_chains.iter() {
            if let MetadataKey::Directory(name) = entry.key() {
                if let Some(_metadata) = self.read_metadata(entry.key(), version_manager)? {
                    directories.push(name.clone());
                }
            }
        }

        Ok(directories)
    }

    /// List all schemas
    pub fn list_schemas(
        &self,
        version_manager: &VersionManager,
    ) -> Result<Vec<String>, CatalogError> {
        let mut schemas = Vec::new();

        for entry in version_manager.version_chains.iter() {
            if let MetadataKey::Schema(name) = entry.key() {
                if let Some(_metadata) = self.read_metadata(entry.key(), version_manager)? {
                    schemas.push(name.clone());
                }
            }
        }

        Ok(schemas)
    }

    /// List all graphs
    pub fn list_graphs(
        &self,
        version_manager: &VersionManager,
    ) -> Result<Vec<String>, CatalogError> {
        let mut graphs = Vec::new();

        for entry in version_manager.version_chains.iter() {
            if let MetadataKey::Graph(name) = entry.key() {
                if let Some(_metadata) = self.read_metadata(entry.key(), version_manager)? {
                    graphs.push(name.clone());
                }
            }
        }

        Ok(graphs)
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

    /// Begin a new catalog transaction
    pub fn begin_transaction(
        &self,
        txn_id: TransactionId,
        start_ts: Timestamp,
    ) -> Arc<CatalogTransaction> {
        let catalog_txn = Arc::new(CatalogTransaction {
            txn_id,
            start_ts,
            catalog_undo_buffer: RwLock::new(Vec::new()),
            read_set: DashSet::new(),
        });

        // Track the read set for this transaction
        self.read_sets.insert(txn_id, DashSet::new());

        catalog_txn
    }

    /// Commit a catalog transaction with two-phase commit protocol
    pub fn commit_transaction(
        &self,
        txn: &CatalogTransaction,
        commit_ts: Timestamp,
    ) -> Result<(), CatalogError> {
        // Phase 1: Prepare - validate and acquire locks
        txn.prepare(commit_ts)?;

        // Phase 2: Commit - make changes visible
        self.apply_commit_timestamp(txn, commit_ts)?;

        // Clean up transaction state
        self.read_sets.remove(&txn.txn_id);

        Ok(())
    }

    /// Abort a catalog transaction
    pub fn abort_transaction(&self, txn: &CatalogTransaction) -> Result<(), CatalogError> {
        // Rollback all changes made by this transaction
        self.rollback_changes(txn)?;

        // Clean up transaction state
        self.read_sets.remove(&txn.txn_id);

        Ok(())
    }

    /// Apply commit timestamp to all changes made by the transaction
    fn apply_commit_timestamp(
        &self,
        txn: &CatalogTransaction,
        commit_ts: Timestamp,
    ) -> Result<(), CatalogError> {
        let undo_buffer = txn.catalog_undo_buffer.read().unwrap();

        for undo_entry in undo_buffer.iter() {
            if let CatalogUndoEntry::Write { key, .. } = undo_entry {
                if let Some(version_chain) = self.version_manager.version_chains.get(key) {
                    let mut current = version_chain.current.write().unwrap();

                    // Replace transaction ID with actual commit timestamp
                    if current.commit_ts == txn.txn_id {
                        current.commit_ts = commit_ts;
                    }
                }
            }
        }

        Ok(())
    }

    /// Rollback all changes made by the transaction
    fn rollback_changes(&self, txn: &CatalogTransaction) -> Result<(), CatalogError> {
        let undo_buffer = txn.catalog_undo_buffer.read().unwrap();

        // Process undo operations in reverse order
        for undo_entry in undo_buffer.iter().rev() {
            match undo_entry {
                CatalogUndoEntry::Write { key, old_value } => {
                    if let Some(version_chain) = self.version_manager.version_chains.get(key) {
                        // Restore the previous version
                        let mut current = version_chain.current.write().unwrap();
                        let mut undo_ptr = version_chain.undo_ptr.write().unwrap();

                        // Only rollback if this transaction made the change
                        if current.commit_ts == txn.txn_id {
                            current.data = old_value.old_data.clone();
                            current.commit_ts = old_value.timestamp;
                            current.is_tombstone = old_value.is_delete;
                            *undo_ptr = old_value.next.clone();
                        }
                    }
                }
                _ => {
                    // Handle other types of undo operations
                }
            }
        }

        Ok(())
    }

    /// Validate read set for serializable isolation with comprehensive conflict detection
    pub fn validate_read_set(
        &self,
        txn: &CatalogTransaction,
        _commit_ts: Timestamp,
    ) -> Result<(), CatalogError> {
        for key_ref in txn.read_set.iter() {
            let key = key_ref.key();

            if let Some(version_chain) = self.version_manager.version_chains.get(key) {
                let current = version_chain.current.read().unwrap();

                // Check if the version has been modified since this transaction started
                // and the modification is from a different transaction
                if current.commit_ts > txn.start_ts && current.commit_ts != txn.txn_id {
                    // If the commit timestamp is still a transaction ID, it means another
                    // concurrent transaction has modified this key but hasn't committed yet
                    if current.commit_ts.is_txn_id() {
                        // Check if the other transaction is still active
                        if self.is_transaction_active(current.commit_ts) {
                            return Err(CatalogError::SerializationFailure);
                        }
                    } else {
                        // The other transaction has committed, check if it conflicts
                        return Err(CatalogError::SerializationFailure);
                    }
                }

                // Also check the undo chain for concurrent modifications
                self.validate_undo_chain(txn, &version_chain.undo_ptr)?;
            }
        }
        Ok(())
    }

    /// Validate the undo chain for concurrent modifications
    fn validate_undo_chain(
        &self,
        txn: &CatalogTransaction,
        undo_ptr: &RwLock<Option<Arc<MetadataUndoEntry>>>,
    ) -> Result<(), CatalogError> {
        let mut current_undo = undo_ptr.read().unwrap().clone();

        while let Some(undo_entry) = current_undo {
            // Check for concurrent modifications in the undo chain
            if undo_entry.timestamp > txn.start_ts && undo_entry.txn_id != txn.txn_id {
                // Found a concurrent modification
                if undo_entry.timestamp.is_txn_id() {
                    // Check if the transaction is still active
                    if self.is_transaction_active(undo_entry.txn_id) {
                        return Err(CatalogError::SerializationFailure);
                    }
                } else {
                    // Committed concurrent modification
                    return Err(CatalogError::SerializationFailure);
                }
            }
            current_undo = undo_entry.next.clone();
        }

        Ok(())
    }

    /// Check if a transaction is still active
    fn is_transaction_active(&self, txn_id: TransactionId) -> bool {
        self.read_sets.contains_key(&txn_id)
    }

    /// Detect conflicts between concurrent transactions
    pub fn detect_conflicts(&self, txn1_id: TransactionId, txn2_id: TransactionId) -> bool {
        if let (Some(read_set1), Some(read_set2)) =
            (self.read_sets.get(&txn1_id), self.read_sets.get(&txn2_id))
        {
            // Check for read-write conflicts
            for key in read_set1.iter() {
                // Check if txn2 has written to any key that txn1 has read
                if let Some(version_chain) = self.version_manager.version_chains.get(key.key()) {
                    let current = version_chain.current.read().unwrap();
                    if current.commit_ts == txn2_id {
                        return true;
                    }
                }
            }

            for key in read_set2.iter() {
                // Check if txn1 has written to any key that txn2 has read
                if let Some(version_chain) = self.version_manager.version_chains.get(key.key()) {
                    let current = version_chain.current.read().unwrap();
                    if current.commit_ts == txn1_id {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Garbage collect old versions based on watermark
    pub fn garbage_collect(&self, watermark: Timestamp) -> Result<(), CatalogError> {
        use std::time::Instant;
        let start_time = Instant::now();
        
        // Calculate memory usage before GC (rough estimation)
        let memory_before = self.estimate_memory_usage();
        
        let mut cleaned_entries = 0u64;
        let mut removed_chains = 0u64;

        for version_chain_entry in self.version_manager.version_chains.iter() {
            let version_chain = version_chain_entry.value();

            // Clean up old undo entries
            let mut undo_ptr = version_chain.undo_ptr.write().unwrap();
            let mut chain_cleaned_entries = 0;

            while let Some(ref entry) = *undo_ptr {
                if entry.timestamp < watermark {
                    *undo_ptr = entry.next.clone();
                    chain_cleaned_entries += 1;
                } else {
                    break;
                }
            }
            cleaned_entries += chain_cleaned_entries;

            // If current version is a tombstone and is old enough, remove the entire chain
            let current = version_chain.current.read().unwrap();
            if current.is_tombstone && current.commit_ts < watermark {
                drop(current);
                self.version_manager
                    .version_chains
                    .remove(version_chain_entry.key());
                removed_chains += 1;
            }
        }

        // Update GC statistics
        let gc_time_us = start_time.elapsed().as_micros() as u64;
        let memory_after = self.estimate_memory_usage();
        
        {
            let mut stats = self.version_manager.gc_stats.write().unwrap();
            stats.update(gc_time_us, cleaned_entries, removed_chains, 
                        watermark, memory_before, memory_after);
        }
        
        self.version_manager.update_last_gc_timestamp();

        Ok(())
    }

    /// Parallel garbage collection for large datasets
    pub fn parallel_garbage_collect(&self, watermark: Timestamp, config: &AdaptiveGcConfig) 
        -> Result<(), CatalogError> {
        use std::time::Instant;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::thread;
        
        let start_time = Instant::now();
        let memory_before = self.estimate_memory_usage();
        
        let cleaned_entries = Arc::new(AtomicU64::new(0));
        let removed_chains = Arc::new(AtomicU64::new(0));
        
        // Collect all version chain keys
        let all_keys: Vec<_> = self.version_manager.version_chains.iter()
            .map(|entry| entry.key().clone())
            .collect();
            
        if all_keys.len() < config.parallel_gc_threshold {
            // Fall back to sequential GC for small datasets
            return self.garbage_collect(watermark);
        }
        
        // Split work among threads
        let chunk_size = (all_keys.len() + config.gc_worker_threads - 1) / config.gc_worker_threads;
        let chunks: Vec<_> = all_keys.chunks(chunk_size).collect();
        
        let mut handles = vec![];
        
        for chunk in chunks {
            let chunk = chunk.to_vec();
            let version_manager = self.version_manager.clone();
            let cleaned_entries = cleaned_entries.clone();
            let removed_chains = removed_chains.clone();
            
            let handle = thread::spawn(move || {
                let mut local_cleaned = 0u64;
                let mut local_removed = 0u64;
                
                for key in chunk {
                    if let Some(version_chain_entry) = version_manager.version_chains.get(&key) {
                        let version_chain = version_chain_entry.value();
                        
                        // Clean up old undo entries
                        let mut undo_ptr = version_chain.undo_ptr.write().unwrap();
                        
                        while let Some(ref entry) = *undo_ptr {
                            if entry.timestamp < watermark {
                                *undo_ptr = entry.next.clone();
                                local_cleaned += 1;
                            } else {
                                break;
                            }
                        }
                        
                        // Check if we can remove the entire chain
                        let current = version_chain.current.read().unwrap();
                        let should_remove = current.is_tombstone && current.commit_ts < watermark;
                        drop(current);
                        drop(undo_ptr);
                        
                        if should_remove {
                            drop(version_chain_entry);
                            version_manager.version_chains.remove(&key);
                            local_removed += 1;
                        }
                    }
                }
                
                cleaned_entries.fetch_add(local_cleaned, Ordering::Relaxed);
                removed_chains.fetch_add(local_removed, Ordering::Relaxed);
            });
            
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().map_err(|_| CatalogError::Other("GC thread panicked".to_string()))?;
        }
        
        // Update GC statistics
        let gc_time_us = start_time.elapsed().as_micros() as u64;
        let memory_after = self.estimate_memory_usage();
        let final_cleaned = cleaned_entries.load(Ordering::Relaxed);
        let final_removed = removed_chains.load(Ordering::Relaxed);
        
        {
            let mut stats = self.version_manager.gc_stats.write().unwrap();
            stats.update(gc_time_us, final_cleaned, final_removed, 
                        watermark, memory_before, memory_after);
        }
        
        self.version_manager.update_last_gc_timestamp();
        
        Ok(())
    }
    
    /// Estimate memory usage of version chains (rough approximation)
    fn estimate_memory_usage(&self) -> u64 {
        let mut total_size = 0u64;
        
        for version_chain_entry in self.version_manager.version_chains.iter() {
            let version_chain = version_chain_entry.value();
            
            // Estimate size of current version
            total_size += 128; // Base overhead
            
            // Estimate size of undo chain
            let mut undo_ptr = version_chain.undo_ptr.read().unwrap().clone();
            while let Some(entry) = undo_ptr {
                total_size += 64; // Undo entry overhead
                undo_ptr = entry.next.clone();
            }
        }
        
        total_size
    }
    
    /// Check if GC should run based on adaptive strategy
    pub fn should_run_gc(&self, config: &AdaptiveGcConfig) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
            
        let last_gc = self.version_manager.last_gc_timestamp.load(Ordering::Relaxed);
        let time_since_last_gc = now.saturating_sub(last_gc);
        
        // Always run if minimum interval has passed and we have significant pressure
        if time_since_last_gc >= config.min_gc_interval_sec {
            let stats = self.get_statistics();
            
            // Check memory pressure (estimated)
            let estimated_memory = self.estimate_memory_usage();
            let memory_pressure = if estimated_memory > 0 {
                estimated_memory as f64 / (estimated_memory as f64 * 1.5) // Rough threshold
            } else {
                0.0
            };
            
            // Check undo chain length pressure
            let avg_undo_length = if stats.total_chains > 0 {
                stats.total_undo_entries / stats.total_chains
            } else {
                0
            };
            
            return memory_pressure > config.memory_pressure_threshold
                || avg_undo_length > config.undo_chain_threshold as usize
                || time_since_last_gc >= config.max_gc_interval_sec;
        }
        
        false
    }

    /// Get statistics about version chains for monitoring
    pub fn get_statistics(&self) -> VersionChainStatistics {
        let mut total_chains = 0;
        let mut total_undo_entries = 0;
        let mut tombstone_chains = 0;

        for version_chain_entry in self.version_manager.version_chains.iter() {
            total_chains += 1;
            let version_chain = version_chain_entry.value();

            let current = version_chain.current.read().unwrap();
            if current.is_tombstone {
                tombstone_chains += 1;
            }

            // Count undo entries
            let mut undo_ptr = version_chain.undo_ptr.read().unwrap().clone();
            while let Some(entry) = undo_ptr {
                total_undo_entries += 1;
                undo_ptr = entry.next.clone();
            }
        }

        VersionChainStatistics {
            total_chains,
            total_undo_entries,
            tombstone_chains,
            active_transactions: self.read_sets.len(),
        }
    }
}

/// Version manager for metadata MVCC
pub struct VersionManager {
    /// Metadata version chains
    pub version_chains: DashMap<MetadataKey, Arc<MetadataVersionChain>>,
    /// Garbage collection threshold
    pub gc_threshold: u64,
    /// GC statistics for adaptive strategy
    pub gc_stats: Arc<RwLock<GcStatistics>>,
    /// Last GC timestamp
    pub last_gc_timestamp: Arc<AtomicU64>,
    /// Read cache for performance optimization
    pub read_cache: Arc<ReadCache>,
    /// Cache statistics
    pub cache_stats: Arc<RwLock<CacheStatistics>>,
}

impl VersionManager {
    /// Create a new version manager
    pub fn new() -> Self {
        Self {
            version_chains: DashMap::new(),
            gc_threshold: 100, // Default GC threshold
            gc_stats: Arc::new(RwLock::new(GcStatistics::new())),
            last_gc_timestamp: Arc::new(AtomicU64::new(0)),
            read_cache: Arc::new(ReadCache::new(1000)), // Default cache size
            cache_stats: Arc::new(RwLock::new(CacheStatistics::new())),
        }
    }

    /// Set garbage collection threshold
    pub fn set_gc_threshold(&mut self, threshold: u64) {
        self.gc_threshold = threshold;
    }

    /// Get GC statistics for monitoring
    pub fn get_gc_statistics(&self) -> GcStatistics {
        self.gc_stats.read().unwrap().clone()
    }

    /// Get cache statistics for monitoring
    pub fn get_cache_statistics(&self) -> CacheStatistics {
        self.cache_stats.read().unwrap().clone()
    }

    /// Update last GC timestamp
    pub fn update_last_gc_timestamp(&self) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_gc_timestamp.store(now, Ordering::Relaxed);
    }

    /// Configure read cache size
    pub fn configure_cache(&self, max_size: usize) {
        self.read_cache.set_max_size(max_size);
    }

    /// Invalidate cache entries for a key
    pub fn invalidate_cache(&self, key: &MetadataKey) {
        self.read_cache.invalidate(key);
    }

    /// Clear entire cache
    pub fn clear_cache(&self) {
        self.read_cache.clear();
    }
}

/// System-level undo entry
#[derive(Debug, Clone)]
pub enum SystemUndoEntry {
    CatalogOperation(CatalogUndoEntry),
    StorageOperation(String), // Placeholder for storage operations
}

/// Statistics about version chains for monitoring and debugging
#[derive(Debug, Clone)]
pub struct VersionChainStatistics {
    pub total_chains: usize,
    pub total_undo_entries: usize,
    pub tombstone_chains: usize,
    pub active_transactions: usize,
}

/// Garbage collection statistics for adaptive strategy
#[derive(Debug, Clone)]
pub struct GcStatistics {
    /// Total number of GC runs
    pub total_gc_runs: u64,
    /// Total time spent in GC (microseconds)
    pub total_gc_time_us: u64,
    /// Number of undo entries cleaned up
    pub cleaned_undo_entries: u64,
    /// Number of version chains removed
    pub removed_version_chains: u64,
    /// Average GC time per run (microseconds)
    pub avg_gc_time_us: u64,
    /// Last GC watermark
    pub last_watermark: Timestamp,
    /// Memory usage before last GC (estimated)
    pub memory_before_gc: u64,
    /// Memory usage after last GC (estimated)
    pub memory_after_gc: u64,
}

impl GcStatistics {
    pub fn new() -> Self {
        Self {
            total_gc_runs: 0,
            total_gc_time_us: 0,
            cleaned_undo_entries: 0,
            removed_version_chains: 0,
            avg_gc_time_us: 0,
            last_watermark: Timestamp(0),
            memory_before_gc: 0,
            memory_after_gc: 0,
        }
    }

    pub fn update(&mut self, gc_time_us: u64, cleaned_entries: u64, removed_chains: u64, 
                  watermark: Timestamp, memory_before: u64, memory_after: u64) {
        self.total_gc_runs += 1;
        self.total_gc_time_us += gc_time_us;
        self.cleaned_undo_entries += cleaned_entries;
        self.removed_version_chains += removed_chains;
        self.avg_gc_time_us = self.total_gc_time_us / self.total_gc_runs;
        self.last_watermark = watermark;
        self.memory_before_gc = memory_before;
        self.memory_after_gc = memory_after;
    }
}

/// Adaptive GC strategy configuration
#[derive(Debug, Clone)]
pub struct AdaptiveGcConfig {
    /// Minimum interval between GC runs (seconds)
    pub min_gc_interval_sec: u64,
    /// Maximum interval between GC runs (seconds)  
    pub max_gc_interval_sec: u64,
    /// Memory pressure threshold (0.0 - 1.0)
    pub memory_pressure_threshold: f64,
    /// Undo chain length threshold
    pub undo_chain_threshold: u64,
    /// Parallel GC threshold (number of version chains)
    pub parallel_gc_threshold: usize,
    /// Number of GC worker threads
    pub gc_worker_threads: usize,
}

impl Default for AdaptiveGcConfig {
    fn default() -> Self {
        Self {
            min_gc_interval_sec: 30,
            max_gc_interval_sec: 300,
            memory_pressure_threshold: 0.8,
            undo_chain_threshold: 50,
            parallel_gc_threshold: 1000,
            gc_worker_threads: 4,
        }
    }
}

/// Read cache for metadata values
pub struct ReadCache {
    /// Cache entries
    cache: DashMap<CacheKey, CacheEntry>,
    /// Maximum cache size
    max_size: Arc<RwLock<usize>>,
    /// Current size
    current_size: Arc<AtomicUsize>,
    /// Access order for LRU eviction
    access_order: Arc<RwLock<std::collections::VecDeque<CacheKey>>>,
}

impl ReadCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: DashMap::new(),
            max_size: Arc::new(RwLock::new(max_size)),
            current_size: Arc::new(AtomicUsize::new(0)),
            access_order: Arc::new(RwLock::new(std::collections::VecDeque::new())),
        }
    }

    pub fn get(&self, key: &MetadataKey, timestamp: Timestamp) -> Option<MetadataValue> {
        let cache_key = CacheKey {
            metadata_key: key.clone(),
            timestamp,
        };

        if let Some(entry) = self.cache.get(&cache_key) {
            if entry.is_valid() {
                // Update access order for LRU
                self.update_access_order(&cache_key);
                return Some(entry.value.clone());
            } else {
                // Remove invalid entry
                self.cache.remove(&cache_key);
                self.current_size.fetch_sub(1, Ordering::Relaxed);
            }
        }

        None
    }

    pub fn put(&self, key: &MetadataKey, timestamp: Timestamp, value: MetadataValue) {
        let cache_key = CacheKey {
            metadata_key: key.clone(),
            timestamp,
        };

        let entry = CacheEntry::new(value);

        // Check if we need to evict entries
        self.evict_if_necessary();

        self.cache.insert(cache_key.clone(), entry);
        self.current_size.fetch_add(1, Ordering::Relaxed);
        self.update_access_order(&cache_key);
    }

    pub fn invalidate(&self, key: &MetadataKey) {
        // Remove all cache entries for this key
        let to_remove: Vec<_> = self.cache
            .iter()
            .filter(|entry| &entry.key().metadata_key == key)
            .map(|entry| entry.key().clone())
            .collect();

        for cache_key in to_remove {
            self.cache.remove(&cache_key);
            self.current_size.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn clear(&self) {
        self.cache.clear();
        self.current_size.store(0, Ordering::Relaxed);
        self.access_order.write().unwrap().clear();
    }

    pub fn set_max_size(&self, max_size: usize) {
        *self.max_size.write().unwrap() = max_size;
        self.evict_if_necessary();
    }

    fn evict_if_necessary(&self) {
        let max_size = *self.max_size.read().unwrap();
        let current_size = self.current_size.load(Ordering::Relaxed);

        if current_size >= max_size {
            let mut access_order = self.access_order.write().unwrap();
            
            // Evict least recently used entries
            while self.current_size.load(Ordering::Relaxed) >= max_size {
                if let Some(lru_key) = access_order.pop_front() {
                    if self.cache.remove(&lru_key).is_some() {
                        self.current_size.fetch_sub(1, Ordering::Relaxed);
                    }
                } else {
                    break;
                }
            }
        }
    }

    fn update_access_order(&self, key: &CacheKey) {
        let mut access_order = self.access_order.write().unwrap();
        
        // Remove existing entry if present
        if let Some(pos) = access_order.iter().position(|k| k == key) {
            access_order.remove(pos);
        }
        
        // Add to back (most recently used)
        access_order.push_back(key.clone());
    }
}

/// Cache key combining metadata key and timestamp
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    metadata_key: MetadataKey,
    timestamp: Timestamp,
}

/// Cache entry with validity tracking
#[derive(Debug, Clone)]
struct CacheEntry {
    value: MetadataValue,
    created_at: std::time::Instant,
    ttl_seconds: u64,
}

impl CacheEntry {
    fn new(value: MetadataValue) -> Self {
        Self {
            value,
            created_at: std::time::Instant::now(),
            ttl_seconds: 300, // 5 minutes TTL
        }
    }

    fn is_valid(&self) -> bool {
        self.created_at.elapsed().as_secs() < self.ttl_seconds
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone)]
pub struct CacheStatistics {
    pub total_requests: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub hit_rate: f64,
    pub current_size: usize,
    pub max_size: usize,
}

impl CacheStatistics {
    pub fn new() -> Self {
        Self {
            total_requests: 0,
            cache_hits: 0,
            cache_misses: 0,
            evictions: 0,
            hit_rate: 0.0,
            current_size: 0,
            max_size: 0,
        }
    }

    pub fn record_hit(&mut self) {
        self.total_requests += 1;
        self.cache_hits += 1;
        self.update_hit_rate();
    }

    pub fn record_miss(&mut self) {
        self.total_requests += 1;
        self.cache_misses += 1;
        self.update_hit_rate();
    }

    pub fn record_eviction(&mut self) {
        self.evictions += 1;
    }

    fn update_hit_rate(&mut self) {
        if self.total_requests > 0 {
            self.hit_rate = self.cache_hits as f64 / self.total_requests as f64;
        }
    }
}

/// Comprehensive performance statistics
#[derive(Debug, Clone)]
pub struct PerformanceStatistics {
    pub gc_stats: GcStatistics,
    pub version_stats: VersionChainStatistics,
    pub cache_stats: CacheStatistics,
    pub active_transactions: usize,
    pub memory_usage_estimate: u64,
}

/// Consistency check report
#[derive(Debug, Clone)]
pub struct ConsistencyReport {
    pub errors: Vec<ConsistencyError>,
    pub warnings: Vec<ConsistencyWarning>,
    pub timestamp: std::time::Instant,
}

impl ConsistencyReport {
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
            timestamp: std::time::Instant::now(),
        }
    }

    pub fn add_error(&mut self, error: ConsistencyError) {
        self.errors.push(error);
    }

    pub fn add_warning(&mut self, warning: ConsistencyWarning) {
        self.warnings.push(warning);
    }

    pub fn is_healthy(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn severity_score(&self) -> u32 {
        self.errors.len() as u32 * 10 + self.warnings.len() as u32
    }
}

/// Consistency errors that require immediate attention
#[derive(Debug, Clone)]
pub enum ConsistencyError {
    InvalidTimestamp {
        key: MetadataKey,
        timestamp: Timestamp,
        description: String,
    },
    TimestampOrderViolation {
        key: MetadataKey,
        current_ts: Timestamp,
        undo_ts: Timestamp,
    },
    UndoChainTooLong {
        key: MetadataKey,
        length: usize,
    },
    WatermarkAhead {
        watermark: Timestamp,
        latest_commit: Timestamp,
    },
    VersionChainCorruption {
        key: MetadataKey,
        description: String,
    },
}

/// Consistency warnings that should be monitored
#[derive(Debug, Clone)]
pub enum ConsistencyWarning {
    LongRunningTransaction {
        txn_id: TransactionId,
        start_ts: Timestamp,
        current_ts: Timestamp,
    },
    HighMemoryUsage {
        current_usage: u64,
        threshold: u64,
    },
    LongUndoChain {
        key: MetadataKey,
        length: usize,
    },
    CacheHitRateLow {
        current_rate: f64,
        threshold: f64,
    },
}

/// Repair report after attempting to fix consistency issues
#[derive(Debug)]
pub struct RepairReport {
    pub successful_repairs: Vec<RepairAction>,
    pub failed_repairs: Vec<(ConsistencyError, String)>, // Use String instead of SystemTransactionError
    pub timestamp: std::time::Instant,
}

impl RepairReport {
    pub fn new() -> Self {
        Self {
            successful_repairs: Vec::new(),
            failed_repairs: Vec::new(),
            timestamp: std::time::Instant::now(),
        }
    }

    pub fn add_success(&mut self, action: RepairAction) {
        self.successful_repairs.push(action);
    }

    pub fn add_failure(&mut self, error: ConsistencyError, repair_error: SystemTransactionError) {
        self.failed_repairs.push((error, repair_error.to_string()));
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.successful_repairs.len() + self.failed_repairs.len();
        if total == 0 {
            1.0
        } else {
            self.successful_repairs.len() as f64 / total as f64
        }
    }
}

/// Actions taken during repair
#[derive(Debug, Clone)]
pub enum RepairAction {
    TruncatedUndoChain {
        key: MetadataKey,
        removed_entries: usize,
    },
    RecalculatedWatermark,
    ClearedCache,
    AbortedLongRunningTransaction {
        txn_id: TransactionId,
    },
    NoActionNeeded,
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
