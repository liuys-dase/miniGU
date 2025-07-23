#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use minigu_storage::{
        common::transaction::{IsolationLevel, Timestamp},
        tp::transaction::MemTxnManager,
    };
    use crate::system_txn::{
        metadata::{MetadataKey, MetadataValue, DirectoryMetadata},
        timestamp::TimestampManager,
        transaction::SystemTransactionManager,
        version_chain::{MetadataVersionChain, MetadataVersion},
    };

    #[test]
    fn test_timestamp_manager_basic() {
        let ts_manager = TimestampManager::new();
        
        let txn_id1 = ts_manager.new_transaction_id();
        let txn_id2 = ts_manager.new_transaction_id();
        
        assert!(txn_id1.0 < txn_id2.0);
        assert!(txn_id1.is_txn_id());
        assert!(txn_id2.is_txn_id());
        
        let commit_ts1 = ts_manager.new_commit_timestamp();
        let commit_ts2 = ts_manager.new_commit_timestamp();
        
        assert!(commit_ts1.0 < commit_ts2.0);
        assert!(commit_ts1.is_commit_ts());
        assert!(commit_ts2.is_commit_ts());
    }

    #[test]
    fn test_system_transaction_manager_creation() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);
        
        assert_eq!(system_txn_manager.active_transactions.len(), 0);
        
        let watermark = system_txn_manager.calculate_watermark();
        assert_eq!(watermark.0, 0); // No active transactions
    }

    #[test]
    fn test_system_transaction_creation() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);
        
        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        
        assert!(system_txn.txn_id().is_txn_id());
        assert!(system_txn.start_ts().0 > 0);
        assert!(system_txn.commit_ts().is_none());
        assert_eq!(system_txn_manager.active_transactions.len(), 1);
    }

    #[test]
    fn test_metadata_key_display() {
        let dir_key = MetadataKey::Directory("test_dir".to_string());
        assert_eq!(format!("{}", dir_key), "directory:test_dir");
        
        let schema_key = MetadataKey::Schema("test_schema".to_string());
        assert_eq!(format!("{}", schema_key), "schema:test_schema");
    }

    #[test]
    fn test_metadata_value_compatibility() {
        let dir_metadata = MetadataValue::Directory(DirectoryMetadata {
            name: "test_dir".to_string(),
        });
        let dir_key = MetadataKey::Directory("test_dir".to_string());
        let schema_key = MetadataKey::Schema("test_schema".to_string());
        
        assert!(dir_metadata.is_compatible_with_key(&dir_key));
        assert!(!dir_metadata.is_compatible_with_key(&schema_key));
        assert_eq!(dir_metadata.name(), "test_dir");
    }

    #[test]
    fn test_version_chain_creation() {
        let dir_metadata = MetadataValue::Directory(DirectoryMetadata {
            name: "test_dir".to_string(),
        });
        let commit_ts = Timestamp(1);
        
        let version_chain = MetadataVersionChain::new(dir_metadata.clone(), commit_ts);
        let current = version_chain.current.read().unwrap();
        
        assert_eq!(current.commit_ts, commit_ts);
        assert!(!current.is_tombstone);
        assert_eq!(current.data.name(), "test_dir");
    }

    #[test]
    fn test_metadata_version_visibility() {
        let dir_metadata = MetadataValue::Directory(DirectoryMetadata {
            name: "test_dir".to_string(),
        });
        let version = MetadataVersion::new(dir_metadata, Timestamp(5));
        
        assert!(version.is_visible_to(Timestamp(10))); // Transaction started after commit
        assert!(version.is_visible_to(Timestamp(5)));  // Transaction started at commit time
        assert!(!version.is_visible_to(Timestamp(3))); // Transaction started before commit
    }
}