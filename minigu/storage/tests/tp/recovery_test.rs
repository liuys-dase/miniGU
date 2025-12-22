use std::fs;
use std::path::PathBuf;

use minigu_common::value::ScalarValue;
use minigu_storage::tp::MemoryGraph;
use minigu_storage::tp::checkpoint::GraphCheckpoint;
use minigu_transaction::{GraphTxnManager, IsolationLevel, Transaction};
use serial_test::serial;

use crate::common::*;

fn test_db_path() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("recovery_test_{}.minigu", std::process::id()));
    path
}

fn cleanup(path: &PathBuf) {
    if path.exists() {
        let _ = fs::remove_file(path);
    }
}

#[test]
#[serial]
fn test_recovery_from_wal() {
    let db_path = test_db_path();
    cleanup(&db_path);
    // 1. Create a graph backed by a file
    {
        let graph = MemoryGraph::with_db_file(&db_path).unwrap();

        let txn = graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();

        // Create Alice
        let alice = create_test_vertex(1, "Alice", 25);
        graph.create_vertex(&txn, alice).unwrap();

        txn.commit().unwrap();

        // Ensure data is synced to disk
        graph.persistence().sync_all().unwrap();
    } // Graph is dropped here

    // 2. Re-open graph from the same file (Recovery)
    {
        let graph = MemoryGraph::with_db_file(&db_path).unwrap();

        let txn = graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();

        // Verify Alice exists
        let alice = graph.get_vertex(&txn, 1).unwrap();
        assert_eq!(alice.vid(), 1);
        assert_eq!(
            alice.properties()[0],
            ScalarValue::String(Some("Alice".to_string()))
        );
    }

    cleanup(&db_path);
}

#[test]
#[serial]
fn test_recovery_from_checkpoint_and_wal() {
    let db_path = test_db_path();
    cleanup(&db_path);

    // 1. Create graph and add some data (Checkpoint part)
    {
        let graph = MemoryGraph::with_db_file(&db_path).unwrap();
        let txn = graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();

        // Create Alice and Bob
        let alice = create_test_vertex(1, "Alice", 25);
        let bob = create_test_vertex(2, "Bob", 30);
        graph.create_vertex(&txn, alice).unwrap();
        graph.create_vertex(&txn, bob).unwrap();

        txn.commit().unwrap();

        // Create a checkpoint
        // Note: Checkpoint creation usually involves locking and truncating WAL
        // We simulate it by manually creating a checkpoint and writing it.
        // In a real scenario, a background thread or a manager would do this.
        let checkpoint = GraphCheckpoint::new(&graph);
        graph.persistence().write_checkpoint(&checkpoint).unwrap();
        graph
            .persistence()
            .truncate_wal_until(checkpoint.metadata.lsn)
            .unwrap();
        graph.persistence().sync_all().unwrap();

        // 2. Add more data (WAL part)
        let txn2 = graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();

        // Create Carol (will be in WAL but not checkpoint)
        let carol = create_test_vertex(3, "Carol", 22);
        graph.create_vertex(&txn2, carol).unwrap();

        txn2.commit().unwrap();
        graph.persistence().sync_all().unwrap();
    } // Graph dropped

    // 3. Recover
    {
        let graph = MemoryGraph::with_db_file(&db_path).unwrap();

        let txn = graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();

        // Verify Alice and Bob (from checkpoint)
        let alice = graph.get_vertex(&txn, 1).unwrap();
        assert_eq!(alice.vid(), 1);

        let bob = graph.get_vertex(&txn, 2).unwrap();
        assert_eq!(bob.vid(), 2);

        // Verify Carol (from WAL)
        let carol = graph.get_vertex(&txn, 3).unwrap();
        assert_eq!(carol.vid(), 3);
    }

    cleanup(&db_path);
}
