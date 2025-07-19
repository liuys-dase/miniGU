use std::num::NonZero;

use minigu_common::types::LabelId;
use minigu_common::value::ScalarValue;

use super::gap_lock::*;
use super::transaction::*;
use crate::common::model::properties::PropertyRecord;
use crate::common::model::vertex::Vertex;

const PERSON_LABEL_ID: LabelId = unsafe { NonZero::new_unchecked(1) };

/// Test that gap locks are correctly acquired during iteration
#[test]
fn test_gap_lock_acquisition_during_iteration() {
    let (graph, _cleaner) = super::memory_graph::tests::mock_empty_graph();

    // Create initial vertices
    let txn_setup = graph.begin_transaction(IsolationLevel::Serializable);

    let vertex1 = Vertex::new(
        1,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![
            ScalarValue::String(Some("Alice".to_string())),
            ScalarValue::Int32(Some(25)),
        ]),
    );

    graph.create_vertex(&txn_setup, vertex1).unwrap();
    txn_setup.commit().unwrap();

    // Start transaction 1 and iterate (should acquire gap lock)
    let txn1 = graph.begin_transaction(IsolationLevel::Serializable);

    println!("Before iteration - checking gap lock acquisition");

    // Force iteration to happen and acquire gap lock
    let mut iter = txn1.iter_vertices();
    let _first = iter.next(); // This should acquire gap lock
    println!("After first next() call");

    // Now start transaction 2 and try to acquire gap lock manually
    let txn2 = graph.begin_transaction(IsolationLevel::Serializable);

    let gap_range = GapRange::IdRange {
        entity_type: EntityType::Vertex,
        lower_bound: std::ops::Bound::Unbounded,
        upper_bound: std::ops::Bound::Unbounded,
    };

    let result = txn2.acquire_gap_lock(gap_range);
    println!("Transaction 2 gap lock result: {:?}", result);

    // This should fail because txn1 should have the gap lock
    assert!(
        result.is_err(),
        "Transaction 2 should be blocked by txn1's gap lock"
    );

    txn1.commit().unwrap();
    txn2.abort().unwrap();
}

/// Test gap lock conflicts during vertex creation
#[test]
fn test_gap_lock_blocks_vertex_creation() {
    let (graph, _cleaner) = super::memory_graph::tests::mock_empty_graph();

    // Transaction 1: Acquire gap lock manually
    let txn1 = graph.begin_transaction(IsolationLevel::Serializable);

    let gap_range = GapRange::IdRange {
        entity_type: EntityType::Vertex,
        lower_bound: std::ops::Bound::Unbounded,
        upper_bound: std::ops::Bound::Unbounded,
    };

    let result1 = txn1.acquire_gap_lock(gap_range);
    println!("Transaction 1 gap lock result: {:?}", result1);
    assert!(result1.is_ok());

    // Transaction 2: Try to create a vertex
    let txn2 = graph.begin_transaction(IsolationLevel::Serializable);

    let vertex = Vertex::new(
        1,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![ScalarValue::String(Some("Test".to_string()))]),
    );

    let result2 = graph.create_vertex(&txn2, vertex);
    println!("Transaction 2 create vertex result: {:?}", result2);

    // This should be blocked by the gap lock
    assert!(
        result2.is_err(),
        "Vertex creation should be blocked by gap lock"
    );

    txn1.commit().unwrap();
    txn2.abort().unwrap();
}
