use std::sync::{Arc, RwLock, Weak};

use minigu_storage::common::transaction::Timestamp;
use serde::{Deserialize, Serialize};

use super::metadata::MetadataValue;
use super::timestamp::TransactionId;

/// Weak pointer to undo entry for memory efficiency
pub type MetadataUndoPtr = Weak<MetadataUndoEntry>;

/// Version chain for metadata entries supporting MVCC
pub struct MetadataVersionChain {
    /// Current version of the metadata
    pub current: RwLock<MetadataVersion>,
    /// Pointer to the undo chain
    pub undo_ptr: RwLock<Option<Arc<MetadataUndoEntry>>>,
}

impl MetadataVersionChain {
    /// Create a new version chain with initial data
    pub fn new(initial_data: MetadataValue, commit_ts: Timestamp) -> Self {
        Self {
            current: RwLock::new(MetadataVersion {
                data: initial_data,
                commit_ts,
                is_tombstone: false,
            }),
            undo_ptr: RwLock::new(None),
        }
    }

    /// Create an empty version chain (tombstone)
    pub fn new_tombstone() -> Self {
        Self {
            current: RwLock::new(MetadataVersion {
                data: MetadataValue::Directory(super::metadata::DirectoryMetadata {
                    name: String::new(),
                }), // Placeholder, will be replaced
                commit_ts: Timestamp(0),
                is_tombstone: true,
            }),
            undo_ptr: RwLock::new(None),
        }
    }
}

/// A specific version of metadata
#[derive(Debug, Clone)]
pub struct MetadataVersion {
    /// The actual metadata content
    pub data: MetadataValue,
    /// Commit timestamp of this version
    pub commit_ts: Timestamp,
    /// Whether this is a tombstone (deleted entry)
    pub is_tombstone: bool,
}

impl MetadataVersion {
    /// Create a new metadata version
    pub fn new(data: MetadataValue, commit_ts: Timestamp) -> Self {
        Self {
            data,
            commit_ts,
            is_tombstone: false,
        }
    }

    /// Create a tombstone version
    pub fn new_tombstone(commit_ts: Timestamp) -> Self {
        Self {
            data: MetadataValue::Directory(super::metadata::DirectoryMetadata {
                name: String::new(),
            }), // Placeholder
            commit_ts,
            is_tombstone: true,
        }
    }

    /// Check if this version is visible to a transaction with given start timestamp
    pub fn is_visible_to(&self, start_ts: Timestamp) -> bool {
        // A version is visible if it was committed before the transaction started
        self.commit_ts <= start_ts
    }
}

/// Undo entry for maintaining version history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataUndoEntry {
    /// Previous version of the data (before this change)
    pub old_data: MetadataValue,
    /// Timestamp when this undo entry was created
    pub timestamp: Timestamp,
    /// Transaction ID that created this undo entry
    pub txn_id: TransactionId,
    /// Next undo entry in the chain
    pub next: Option<Arc<MetadataUndoEntry>>,
    /// Whether this represents a delete operation
    pub is_delete: bool,
}

impl MetadataUndoEntry {
    /// Create a new undo entry for update operation
    pub fn new_update(
        old_data: MetadataValue,
        timestamp: Timestamp,
        txn_id: TransactionId,
        next: Option<Arc<MetadataUndoEntry>>,
    ) -> Self {
        Self {
            old_data,
            timestamp,
            txn_id,
            next,
            is_delete: false,
        }
    }

    /// Create a new undo entry for delete operation
    pub fn new_delete(
        old_data: MetadataValue,
        timestamp: Timestamp,
        txn_id: TransactionId,
        next: Option<Arc<MetadataUndoEntry>>,
    ) -> Self {
        Self {
            old_data,
            timestamp,
            txn_id,
            next,
            is_delete: true,
        }
    }

    /// Apply this undo entry to reconstruct historical data
    pub fn apply_reverse(&self, current_data: &mut MetadataValue) {
        if self.is_delete {
            // This was originally a delete, so the old state was having data
            *current_data = self.old_data.clone();
        } else {
            // This was originally an update, so revert to old data
            *current_data = self.old_data.clone();
        }
    }
}

/// Enum representing different types of catalog undo operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogUndoEntry {
    CreateDirectory(String, super::metadata::DirectoryMetadata),
    DropDirectory(String),
    CreateSchema(String, super::metadata::SchemaMetadata),
    DropSchema(String),
    CreateGraph(String, super::metadata::GraphMetadata),
    DropGraph(String),
    CreateGraphType(String, super::metadata::GraphTypeMetadata),
    DropGraphType(String),
    ModifyGraphType(String, super::metadata::GraphTypeMetadata),
    Write {
        key: super::metadata::MetadataKey,
        old_value: Arc<MetadataUndoEntry>,
    },
}

impl CatalogUndoEntry {
    /// Get a description of this undo entry
    pub fn description(&self) -> String {
        match self {
            CatalogUndoEntry::CreateDirectory(name, _) => format!("CreateDirectory({})", name),
            CatalogUndoEntry::DropDirectory(name) => format!("DropDirectory({})", name),
            CatalogUndoEntry::CreateSchema(name, _) => format!("CreateSchema({})", name),
            CatalogUndoEntry::DropSchema(name) => format!("DropSchema({})", name),
            CatalogUndoEntry::CreateGraph(name, _) => format!("CreateGraph({})", name),
            CatalogUndoEntry::DropGraph(name) => format!("DropGraph({})", name),
            CatalogUndoEntry::CreateGraphType(name, _) => format!("CreateGraphType({})", name),
            CatalogUndoEntry::DropGraphType(name) => format!("DropGraphType({})", name),
            CatalogUndoEntry::ModifyGraphType(name, _) => format!("ModifyGraphType({})", name),
            CatalogUndoEntry::Write { key, .. } => format!("Write({})", key),
        }
    }
}
