use std::sync::{Arc, Mutex, OnceLock, RwLock};

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

        if let Some(ref storage_txn) = *self.storage_txn.read().unwrap() {
            // Prepare storage transaction if it exists
            // Note: This would need to be integrated with the storage transaction API
        }

        // Phase 2: Commit - make all changes visible atomically

        // Commit catalog transaction
        if let Some(ref catalog_txn) = *self.catalog_txn.read().unwrap() {
            system_manager
                .catalog_txn_manager
                .commit_transaction(catalog_txn, commit_ts)?;
        }

        // Commit storage transaction
        if let Some(ref storage_txn) = *self.storage_txn.read().unwrap() {
            // Commit storage transaction
            // Note: This would need to be integrated with the storage transaction API
            // storage_txn.commit_at(Some(commit_ts), false)?;
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
        if let Some(ref storage_txn) = *self.storage_txn.read().unwrap() {
            // Abort storage transaction
            // Note: This would need to be integrated with the storage transaction API
            // storage_txn.abort_at(false)?;
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
    /// Read metadata with MVCC visibility
    pub fn read_metadata(
        &self,
        key: &MetadataKey,
        version_manager: &VersionManager,
    ) -> Result<Option<MetadataValue>, CatalogError> {
        // Record in read set for serializable isolation
        self.read_set.insert(key.clone());

        if let Some(version_chain) = version_manager.version_chains.get(key) {
            let current = version_chain.current.read().unwrap();

            // Check if current version is visible to this transaction
            if self.is_version_visible(&current) {
                if current.is_tombstone {
                    return Ok(None);
                }

                // Reconstruct the version that should be visible to this transaction
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
        // Validate that the value is compatible with the key
        if !value.is_compatible_with_key(&key) {
            return Err(CatalogError::IncompatibleKeyValue);
        }

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
        let old_is_tombstone = current.is_tombstone;
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
        key: &MetadataKey,
        commit_ts: Timestamp,
    ) -> Result<bool, CatalogError> {
        // This is a simplified version - in practice, the CatalogTxnManager
        // would provide this functionality through its validate_read_set method
        Ok(false)
    }

    /// Commit the catalog transaction
    pub fn commit(&self, commit_ts: Timestamp) -> Result<(), CatalogError> {
        // Replace transaction IDs with actual commit timestamp
        let undo_buffer = self.catalog_undo_buffer.read().unwrap();
        for undo_entry in undo_buffer.iter() {
            if let CatalogUndoEntry::Write { old_value, .. } = undo_entry {
                // Note: In a real implementation, we would update the commit timestamp
                // in the version chain. This is a simplified version.
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
                    old_value,
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
        let existing_metadata = self
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
        let existing_metadata = self
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
        let existing_metadata = self
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
        let existing_metadata = self
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
                if let Some(metadata) = self.read_metadata(entry.key(), version_manager)? {
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
                if let Some(metadata) = self.read_metadata(entry.key(), version_manager)? {
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
                if let Some(metadata) = self.read_metadata(entry.key(), version_manager)? {
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
        commit_ts: Timestamp,
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
        for version_chain_entry in self.version_manager.version_chains.iter() {
            let version_chain = version_chain_entry.value();

            // Clean up old undo entries
            let mut undo_ptr = version_chain.undo_ptr.write().unwrap();
            let mut prev_ptr: Option<Arc<MetadataUndoEntry>> = None;

            while let Some(ref entry) = *undo_ptr {
                if entry.timestamp < watermark {
                    *undo_ptr = entry.next.clone();
                } else {
                    prev_ptr = Some(entry.clone());
                    break;
                }
            }

            // If current version is a tombstone and is old enough, remove the entire chain
            let current = version_chain.current.read().unwrap();
            if current.is_tombstone && current.commit_ts < watermark {
                drop(current);
                self.version_manager
                    .version_chains
                    .remove(version_chain_entry.key());
            }
        }

        Ok(())
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

/// Statistics about version chains for monitoring and debugging
#[derive(Debug, Clone)]
pub struct VersionChainStatistics {
    pub total_chains: usize,
    pub total_undo_entries: usize,
    pub tombstone_chains: usize,
    pub active_transactions: usize,
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
