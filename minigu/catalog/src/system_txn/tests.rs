#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use minigu_storage::common::transaction::{IsolationLevel, Timestamp};
    use minigu_storage::tp::transaction::MemTxnManager;

    use crate::system_txn::metadata::{DirectoryMetadata, MetadataKey, MetadataValue};
    use crate::system_txn::timestamp::TimestampManager;
    use crate::system_txn::transaction::SystemTransactionManager;
    use crate::system_txn::version_chain::{MetadataVersion, MetadataVersionChain};

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
        assert!(version.is_visible_to(Timestamp(5))); // Transaction started at commit time
        assert!(!version.is_visible_to(Timestamp(3))); // Transaction started before commit
    }

    #[test]
    fn test_catalog_transaction_basic_operations() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        let catalog_txn =
            system_txn.get_or_create_catalog_txn(system_txn_manager.catalog_txn_manager.clone());

        // Test directory creation
        let dir_metadata = DirectoryMetadata {
            name: "test_dir".to_string(),
        };

        let result = catalog_txn.create_directory(
            "test_dir".to_string(),
            dir_metadata.clone(),
            &system_txn_manager.version_manager,
        );
        assert!(result.is_ok());

        // Test reading the created directory
        let key = MetadataKey::Directory("test_dir".to_string());
        let read_result = catalog_txn.read_metadata(&key, &system_txn_manager.version_manager);
        assert!(read_result.is_ok());
        assert!(read_result.unwrap().is_some());

        // Test directory listing
        let directories = catalog_txn.list_directories(&system_txn_manager.version_manager);
        assert!(directories.is_ok());
        assert_eq!(directories.unwrap().len(), 1);
    }

    #[test]
    fn test_catalog_transaction_schema_operations() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        let catalog_txn =
            system_txn.get_or_create_catalog_txn(system_txn_manager.catalog_txn_manager.clone());

        // Test schema creation
        let schema_metadata = crate::system_txn::metadata::SchemaMetadata {
            name: "test_schema".to_string(),
        };

        let result = catalog_txn.create_schema(
            "test_schema".to_string(),
            schema_metadata.clone(),
            &system_txn_manager.version_manager,
        );
        assert!(result.is_ok());

        // Test reading the created schema
        let key = MetadataKey::Schema("test_schema".to_string());
        let read_result = catalog_txn.read_metadata(&key, &system_txn_manager.version_manager);
        assert!(read_result.is_ok());
        assert!(read_result.unwrap().is_some());

        // Test schema listing
        let schemas = catalog_txn.list_schemas(&system_txn_manager.version_manager);
        assert!(schemas.is_ok());
        assert_eq!(schemas.unwrap().len(), 1);
    }

    #[test]
    fn test_catalog_transaction_duplicate_creation() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        let catalog_txn =
            system_txn.get_or_create_catalog_txn(system_txn_manager.catalog_txn_manager.clone());

        let dir_metadata = DirectoryMetadata {
            name: "test_dir".to_string(),
        };

        // First creation should succeed
        let result1 = catalog_txn.create_directory(
            "test_dir".to_string(),
            dir_metadata.clone(),
            &system_txn_manager.version_manager,
        );
        assert!(result1.is_ok());

        // Second creation should fail
        let result2 = catalog_txn.create_directory(
            "test_dir".to_string(),
            dir_metadata,
            &system_txn_manager.version_manager,
        );
        assert!(result2.is_err());
    }

    #[test]
    fn test_catalog_transaction_drop_operations() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        let catalog_txn =
            system_txn.get_or_create_catalog_txn(system_txn_manager.catalog_txn_manager.clone());

        // Create a directory first
        let dir_metadata = DirectoryMetadata {
            name: "test_dir".to_string(),
        };
        catalog_txn
            .create_directory(
                "test_dir".to_string(),
                dir_metadata,
                &system_txn_manager.version_manager,
            )
            .unwrap();

        // Drop the directory
        let drop_result =
            catalog_txn.drop_directory("test_dir".to_string(), &system_txn_manager.version_manager);
        assert!(drop_result.is_ok());

        // Try to read the dropped directory
        let key = MetadataKey::Directory("test_dir".to_string());
        let read_result = catalog_txn.read_metadata(&key, &system_txn_manager.version_manager);
        assert!(read_result.is_ok());
        assert!(read_result.unwrap().is_none()); // Should be None (tombstone)
    }

    #[test]
    fn test_catalog_transaction_drop_nonexistent() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        let catalog_txn =
            system_txn.get_or_create_catalog_txn(system_txn_manager.catalog_txn_manager.clone());

        // Try to drop a non-existent directory
        let drop_result = catalog_txn.drop_directory(
            "nonexistent_dir".to_string(),
            &system_txn_manager.version_manager,
        );
        assert!(drop_result.is_err());
    }

    #[test]
    fn test_catalog_transaction_read_set_tracking() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Serializable);
        let catalog_txn =
            system_txn.get_or_create_catalog_txn(system_txn_manager.catalog_txn_manager.clone());

        // Read some keys to populate read set
        let key1 = MetadataKey::Directory("dir1".to_string());
        let key2 = MetadataKey::Schema("schema1".to_string());

        let _ = catalog_txn.read_metadata(&key1, &system_txn_manager.version_manager);
        let _ = catalog_txn.read_metadata(&key2, &system_txn_manager.version_manager);

        let read_set = catalog_txn.get_read_set();
        assert_eq!(read_set.len(), 2);
        assert!(read_set.contains(&key1));
        assert!(read_set.contains(&key2));
    }

    #[test]
    fn test_catalog_transaction_commit() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        let catalog_txn =
            system_txn.get_or_create_catalog_txn(system_txn_manager.catalog_txn_manager.clone());

        // Create a directory
        let dir_metadata = DirectoryMetadata {
            name: "test_dir".to_string(),
        };
        catalog_txn
            .create_directory(
                "test_dir".to_string(),
                dir_metadata,
                &system_txn_manager.version_manager,
            )
            .unwrap();

        // Commit the system transaction
        let commit_result = system_txn.commit(&system_txn_manager);
        assert!(commit_result.is_ok());

        // Verify transaction is no longer active
        assert_eq!(system_txn_manager.active_transactions.len(), 0);
    }

    #[test]
    fn test_catalog_transaction_abort() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        let catalog_txn =
            system_txn.get_or_create_catalog_txn(system_txn_manager.catalog_txn_manager.clone());

        // Create a directory
        let dir_metadata = DirectoryMetadata {
            name: "test_dir".to_string(),
        };
        catalog_txn
            .create_directory(
                "test_dir".to_string(),
                dir_metadata,
                &system_txn_manager.version_manager,
            )
            .unwrap();

        // Abort the system transaction
        let abort_result = system_txn.abort(&system_txn_manager);
        assert!(abort_result.is_ok());

        // Verify transaction is no longer active
        assert_eq!(system_txn_manager.active_transactions.len(), 0);
    }

    #[test]
    fn test_version_manager_statistics() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        let catalog_txn =
            system_txn.get_or_create_catalog_txn(system_txn_manager.catalog_txn_manager.clone());

        // Create some directories
        for i in 0..3 {
            let dir_metadata = DirectoryMetadata {
                name: format!("test_dir_{}", i),
            };
            catalog_txn
                .create_directory(
                    format!("test_dir_{}", i),
                    dir_metadata,
                    &system_txn_manager.version_manager,
                )
                .unwrap();
        }

        let stats = system_txn_manager.catalog_txn_manager.get_statistics();
        assert_eq!(stats.total_chains, 3);
        assert_eq!(stats.active_transactions, 1);
    }

    #[test]
    fn test_watermark_calculation() {
        let storage_txn_manager = Arc::new(MemTxnManager::new());
        let system_txn_manager = SystemTransactionManager::new(storage_txn_manager);

        let system_txn1 = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);
        let system_txn2 = system_txn_manager.begin_transaction(IsolationLevel::Snapshot);

        let watermark = system_txn_manager.calculate_watermark();
        let min_start_ts = std::cmp::min(system_txn1.start_ts(), system_txn2.start_ts());

        assert_eq!(watermark, min_start_ts);

        // Commit one transaction and check watermark updates
        let _ = system_txn1.commit(&system_txn_manager);
        let new_watermark = system_txn_manager.calculate_watermark();
        assert_eq!(new_watermark, system_txn2.start_ts());
    }
}
