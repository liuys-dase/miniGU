use std::num::NonZero;

use minigu_common::types::LabelId;
use minigu_common::value::ScalarValue;

use super::transaction::*;
use crate::common::model::edge::Edge;
use crate::common::model::properties::PropertyRecord;
use crate::common::model::vertex::Vertex;

const PERSON_LABEL_ID: LabelId = unsafe { NonZero::new_unchecked(1) };

/// Test phantom read prevention with gap locks during vertex iteration
#[test]
fn test_phantom_read_prevention_vertex_iteration() {
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
    let vertex2 = Vertex::new(
        2,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![
            ScalarValue::String(Some("Bob".to_string())),
            ScalarValue::Int32(Some(30)),
        ]),
    );

    graph.create_vertex(&txn_setup, vertex1).unwrap();
    graph.create_vertex(&txn_setup, vertex2).unwrap();
    txn_setup.commit().unwrap();

    // Start two concurrent transactions
    let txn1 = graph.begin_transaction(IsolationLevel::Serializable);
    let txn2 = graph.begin_transaction(IsolationLevel::Serializable);

    // Transaction 1: Count vertices with age between 20-30
    let count1_before = txn1
        .iter_vertices()
        .filter_map(|v_result| v_result.ok())
        .filter(|v| match &v.properties()[1] {
            ScalarValue::Int32(Some(age)) => (20..=30).contains(age),
            _ => false,
        })
        .count();
    assert_eq!(count1_before, 2);

    // Transaction 2: Try to insert a new vertex with age 27
    let vertex3 = Vertex::new(
        3,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![
            ScalarValue::String(Some("Charlie".to_string())),
            ScalarValue::Int32(Some(27)),
        ]),
    );

    // This should be blocked due to gap lock held by txn1
    let result = graph.create_vertex(&txn2, vertex3);
    assert!(
        result.is_err(),
        "Transaction 2 should be blocked by gap lock"
    );

    // Transaction 1: Count again - should get same result (no phantom read)
    let count1_after = txn1
        .iter_vertices()
        .filter_map(|v_result| v_result.ok())
        .filter(|v| match &v.properties()[1] {
            ScalarValue::Int32(Some(age)) => (20..=30).contains(age),
            _ => false,
        })
        .count();
    assert_eq!(count1_after, count1_before, "No phantom read should occur");

    // Commit transaction 1 and abort transaction 2
    txn1.commit().unwrap();
    txn2.abort().unwrap();

    // Now transaction 3 should be able to insert the vertex
    let txn3 = graph.begin_transaction(IsolationLevel::Serializable);
    let vertex3_new = Vertex::new(
        3,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![
            ScalarValue::String(Some("Charlie".to_string())),
            ScalarValue::Int32(Some(27)),
        ]),
    );

    graph.create_vertex(&txn3, vertex3_new).unwrap();
    txn3.commit().unwrap();

    // Verify the vertex was inserted
    let txn4 = graph.begin_transaction(IsolationLevel::Serializable);
    let final_count = txn4
        .iter_vertices()
        .filter_map(|v_result| v_result.ok())
        .filter(|v| match &v.properties()[1] {
            ScalarValue::Int32(Some(age)) => (20..=30).contains(age),
            _ => false,
        })
        .count();
    assert_eq!(final_count, 3);
    txn4.commit().unwrap();
}

/// Test phantom read prevention with gap locks during edge iteration
#[test]
fn test_phantom_read_prevention_edge_iteration() {
    let (graph, _cleaner) = super::memory_graph::tests::mock_empty_graph();

    // Create initial vertices and edges
    let txn_setup = graph.begin_transaction(IsolationLevel::Serializable);

    let vertex1 = Vertex::new(
        1,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![ScalarValue::String(Some("Alice".to_string()))]),
    );
    let vertex2 = Vertex::new(
        2,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![ScalarValue::String(Some("Bob".to_string()))]),
    );

    graph.create_vertex(&txn_setup, vertex1).unwrap();
    graph.create_vertex(&txn_setup, vertex2).unwrap();

    let edge1 = Edge::new(
        1,
        1,
        2,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![ScalarValue::String(Some("friend".to_string()))]),
    );

    graph.create_edge(&txn_setup, edge1).unwrap();
    txn_setup.commit().unwrap();

    // Start two concurrent transactions
    let txn1 = graph.begin_transaction(IsolationLevel::Serializable);
    let txn2 = graph.begin_transaction(IsolationLevel::Serializable);

    // Transaction 1: Count all edges
    let count1_before = txn1
        .iter_edges()
        .filter_map(|e_result| e_result.ok())
        .count();
    assert_eq!(count1_before, 1);

    // Transaction 2: Try to insert a new edge
    let edge2 = Edge::new(
        2,
        2,
        1,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![ScalarValue::String(Some("friend".to_string()))]),
    );

    // This should be blocked due to gap lock held by txn1
    let result = graph.create_edge(&txn2, edge2);
    assert!(
        result.is_err(),
        "Transaction 2 should be blocked by gap lock"
    );

    // Transaction 1: Count again - should get same result (no phantom read)
    let count1_after = txn1
        .iter_edges()
        .filter_map(|e_result| e_result.ok())
        .count();
    assert_eq!(count1_after, count1_before, "No phantom read should occur");

    // Commit transaction 1 and abort transaction 2
    txn1.commit().unwrap();
    txn2.abort().unwrap();
}

/// Test that phantom read prevention works with complex filtering conditions
#[test]
fn test_phantom_read_with_complex_filtering() {
    let (graph, _cleaner) = super::memory_graph::tests::mock_empty_graph();

    // Create initial data
    let txn_setup = graph.begin_transaction(IsolationLevel::Serializable);

    for i in 1..=5 {
        let vertex = Vertex::new(
            i,
            PERSON_LABEL_ID,
            PropertyRecord::new(vec![
                ScalarValue::String(Some(format!("Person{}", i))),
                ScalarValue::Int32(Some(20 + i as i32)),
            ]),
        );
        graph.create_vertex(&txn_setup, vertex).unwrap();
    }
    txn_setup.commit().unwrap();

    // Transaction 1: Complex query with multiple conditions
    let txn1 = graph.begin_transaction(IsolationLevel::Serializable);
    let count_before = txn1
        .iter_vertices()
        .filter_map(|v_result| v_result.ok())
        .filter(|v| match (&v.properties()[0], &v.properties()[1]) {
            (ScalarValue::String(Some(name)), ScalarValue::Int32(Some(age))) => {
                name.starts_with("Person") && *age >= 22 && *age <= 24
            }
            _ => false,
        })
        .count();
    assert_eq!(count_before, 3); // Person2(age 22), Person3(age 23), Person4(age 24)

    // Transaction 2: Try to insert a vertex that would match the filter
    let txn2 = graph.begin_transaction(IsolationLevel::Serializable);
    let new_vertex = Vertex::new(
        6,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![
            ScalarValue::String(Some("Person6".to_string())),
            ScalarValue::Int32(Some(23)), // This would match the filter
        ]),
    );

    // Should be blocked by gap lock
    let result = graph.create_vertex(&txn2, new_vertex);
    assert!(result.is_err(), "Insert should be blocked by gap lock");

    txn1.commit().unwrap();
    txn2.abort().unwrap();
}

/// Test record locks prevent concurrent modifications
#[test]
fn test_record_lock_prevents_concurrent_modification() {
    let (graph, _cleaner) = super::memory_graph::tests::mock_empty_graph();

    // Create initial vertex
    let txn_setup = graph.begin_transaction(IsolationLevel::Serializable);
    let vertex = Vertex::new(
        1,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![
            ScalarValue::String(Some("Alice".to_string())),
            ScalarValue::Int32(Some(25)),
        ]),
    );
    graph.create_vertex(&txn_setup, vertex).unwrap();
    txn_setup.commit().unwrap();

    // Transaction 1: Read the vertex (acquires record lock)
    let txn1 = graph.begin_transaction(IsolationLevel::Serializable);
    let _vertex1 = graph.get_vertex(&txn1, 1).unwrap();

    // Transaction 2: Try to read the same vertex (should be blocked)
    let txn2 = graph.begin_transaction(IsolationLevel::Serializable);
    let result = graph.get_vertex(&txn2, 1);
    assert!(
        result.is_err(),
        "Second read should be blocked by record lock"
    );

    txn1.commit().unwrap();
    txn2.abort().unwrap();
}

/// Test that locks are properly released after transaction commit/abort
#[test]
fn test_lock_release_after_transaction_end() {
    let (graph, _cleaner) = super::memory_graph::tests::mock_empty_graph();

    // Create initial vertex
    let txn_setup = graph.begin_transaction(IsolationLevel::Serializable);
    let vertex = Vertex::new(
        1,
        PERSON_LABEL_ID,
        PropertyRecord::new(vec![ScalarValue::String(Some("Alice".to_string()))]),
    );
    graph.create_vertex(&txn_setup, vertex).unwrap();
    txn_setup.commit().unwrap();

    // Transaction 1: Acquire locks and then commit
    {
        let txn1 = graph.begin_transaction(IsolationLevel::Serializable);
        let _vertex = graph.get_vertex(&txn1, 1).unwrap();
        let _iter = txn1.iter_vertices();
        txn1.commit().unwrap();
    } // txn1 goes out of scope here

    // Transaction 2: Should be able to acquire locks now
    let txn2 = graph.begin_transaction(IsolationLevel::Serializable);
    let vertex2 = graph.get_vertex(&txn2, 1).unwrap();
    assert_eq!(vertex2.vid(), 1);

    let count = txn2
        .iter_vertices()
        .filter_map(|v_result| v_result.ok())
        .count();
    assert_eq!(count, 1);

    txn2.commit().unwrap();
}
