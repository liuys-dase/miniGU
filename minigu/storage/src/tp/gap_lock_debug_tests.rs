#[cfg(test)]
mod debug_tests {
    use crate::tp::IsolationLevel;
    use crate::tp::gap_lock::*;
    use crate::tp::memory_graph::tests;

    #[test]
    fn test_simple_gap_lock_debug() {
        let (graph, _cleaner) = tests::mock_empty_graph();

        // Transaction 1: Start and immediately acquire gap lock
        let txn1 = graph.begin_transaction(IsolationLevel::Serializable);

        // Manually acquire a gap lock
        let gap_range = GapRange::IdRange {
            entity_type: EntityType::Vertex,
            lower_bound: std::ops::Bound::Unbounded,
            upper_bound: std::ops::Bound::Unbounded,
        };
        let result1 = txn1.acquire_gap_lock(gap_range.clone());
        println!("Transaction 1 gap lock result: {:?}", result1);
        assert!(result1.is_ok());

        // Transaction 2: Try to acquire the same gap lock
        let txn2 = graph.begin_transaction(IsolationLevel::Serializable);
        let result2 = txn2.acquire_gap_lock(gap_range);
        println!("Transaction 2 gap lock result: {:?}", result2);

        // This should fail due to conflict
        assert!(result2.is_err(), "Second transaction should be blocked");

        txn1.commit().unwrap();
        txn2.abort().unwrap();
    }

    #[test]
    fn test_record_lock_debug() {
        let (graph, _cleaner) = tests::mock_empty_graph();

        let txn1 = graph.begin_transaction(IsolationLevel::Serializable);
        let result1 = txn1.acquire_record_lock(EntityId::Vertex(100));
        println!("Transaction 1 record lock result: {:?}", result1);
        assert!(result1.is_ok());

        let txn2 = graph.begin_transaction(IsolationLevel::Serializable);
        let result2 = txn2.acquire_record_lock(EntityId::Vertex(100));
        println!("Transaction 2 record lock result: {:?}", result2);

        // This should fail due to conflict
        assert!(result2.is_err(), "Second transaction should be blocked");

        txn1.commit().unwrap();
        txn2.abort().unwrap();
    }
}
