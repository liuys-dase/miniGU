use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

use minigu_common::types::{EdgeId, VertexId};
use minigu_transaction::global_timestamp_generator;

use super::memory_graph::MemoryGraph;
use super::transaction::UndoEntry;
use super::txn_manager::MemTxnManager;
use crate::common::model::edge::{Edge, Neighbor};
use crate::common::transaction::DeltaOp;
use crate::error::StorageResult;

/// Garbage collection monitoring information
#[derive(Debug, Clone)]
pub struct GcInfo {
    /// Current watermark (the start timestamp of the minimum active transaction)
    pub watermark: u64,
    /// Number of committed transactions
    pub committed_txns_count: usize,
    /// Timestamp of the last garbage collection
    pub last_gc_timestamp: u64,
}

/// Garbage collection configuration parameters
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Check interval in milliseconds
    pub check_interval_ms: u64,
    /// Watermark change threshold, triggers GC when exceeded
    pub watermark_threshold: u64,
    /// Committed transactions count threshold, triggers GC when exceeded
    pub committed_txns_threshold: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 1000,       // Check every 1 second
            watermark_threshold: 50,       // Use previous threshold
            committed_txns_threshold: 100, // 100 committed transactions
        }
    }
}

/// Garbage collection monitoring trait
/// Provides read-only access to transaction manager state
pub trait GcMonitor {
    /// Determine whether garbage collection should be triggered
    fn should_trigger_gc(&self) -> bool;
    /// Get current garbage collection related information
    fn get_gc_info(&self) -> GcInfo;
    /// Get expired transaction information that needs garbage collection
    fn get_expired_entries(&self) -> (Vec<Arc<UndoEntry>>, u64);
    /// Update the timestamp of the last garbage collection
    fn update_last_gc_timestamp(&self, timestamp: u64);
}

/// Graph garbage collection request
#[derive(Debug)]
pub struct GcRequest {
    /// Undo entries of expired transactions that need to be collected
    pub expired_undo_entries: Vec<Arc<UndoEntry>>,
    /// Minimum read timestamp, used to determine which versions can be safely deleted
    pub min_read_timestamp: u64,
}

/// Graph data garbage collector
/// Specifically responsible for handling graph data garbage collection, decoupled from transaction
/// manager
pub struct GraphGarbageCollector {
    /// Reference to the graph (used for actual garbage collection execution)
    graph: Arc<MemoryGraph>,
    /// Handle to the garbage collection thread
    gc_thread_handle: Option<thread::JoinHandle<()>>,
    /// Sender for stop signal
    stop_sender: Option<Sender<()>>,
}

impl GraphGarbageCollector {
    /// Create a new graph garbage collector
    pub fn new(graph: Arc<MemoryGraph>, _txn_manager: Weak<MemTxnManager>) -> Self {
        Self::new_with_config(graph, GcConfig::default())
    }

    /// Create a new graph garbage collector with custom configuration
    pub fn new_with_config(graph: Arc<MemoryGraph>, config: GcConfig) -> Self {
        Self::new_with_thread_control(graph, config, true)
    }

    /// Create a new graph garbage collector with thread control (mainly for testing)
    pub fn new_with_thread_control(
        graph: Arc<MemoryGraph>,
        config: GcConfig,
        start_thread: bool,
    ) -> Self {
        let (stop_sender, stop_receiver) = channel::<()>();

        let handle = if start_thread {
            let graph_clone = graph.clone();
            let config_clone = config.clone();

            Some(thread::spawn(move || {
                Self::gc_monitor_loop(graph_clone, Weak::new(), config_clone, stop_receiver);
            }))
        } else {
            None
        };

        Self {
            graph,
            gc_thread_handle: handle,
            stop_sender: Some(stop_sender),
        }
    }

    /// Stop the garbage collection thread
    pub fn stop_gc_thread(&mut self) {
        // Send stop signal
        if let Some(sender) = self.stop_sender.take() {
            let _ = sender.send(());
        }

        // Wait for thread to finish
        if let Some(handle) = self.gc_thread_handle.take() {
            let _ = handle.join();
        }
    }

    /// Synchronously process garbage collection request (mainly for testing)
    pub fn process_gc_request_sync(graph: &MemoryGraph, request: GcRequest) -> StorageResult<()> {
        Self::process_gc_request(graph, request)
    }

    /// Manually trigger garbage collection once (mainly for testing)
    pub fn trigger_gc_sync(&self) -> StorageResult<()> {
        let txn_manager = &self.graph.txn_manager;
        let (expired_entries, min_read_ts) = txn_manager.get_expired_entries();
        if !expired_entries.is_empty() {
            let request = GcRequest {
                expired_undo_entries: expired_entries,
                min_read_timestamp: min_read_ts,
            };
            Self::process_gc_request(&self.graph, request)?;

            // Update the last GC timestamp in the transaction manager
            let current_ts = global_timestamp_generator().current();
            txn_manager.update_last_gc_timestamp(current_ts.0);
        }
        Ok(())
    }

    /// New GC monitoring loop, passively monitors transaction manager state
    fn gc_monitor_loop(
        graph: Arc<MemoryGraph>,
        _txn_manager: Weak<MemTxnManager>,
        config: GcConfig,
        stop_receiver: Receiver<()>,
    ) {
        let check_interval = Duration::from_millis(config.check_interval_ms);

        loop {
            // Check if stop signal is received
            if stop_receiver.try_recv().is_ok() {
                break;
            }

            // Get txn_manager reference directly from graph
            let txn_manager = &graph.txn_manager;

            // Check if GC needs to be triggered
            if txn_manager.should_trigger_gc() {
                let (expired_entries, min_read_ts) = txn_manager.get_expired_entries();

                if !expired_entries.is_empty() {
                    let request = GcRequest {
                        expired_undo_entries: expired_entries,
                        min_read_timestamp: min_read_ts,
                    };

                    if let Err(e) = Self::process_gc_request(&graph, request) {
                        eprintln!("Graph garbage collection failed: {:?}", e);
                    } else {
                        // Update last GC timestamp
                        let current_ts = global_timestamp_generator().current();
                        txn_manager.update_last_gc_timestamp(current_ts.0);
                    }
                }
            }

            // Wait for some time before next check
            thread::sleep(check_interval);
        }
    }

    /// Process a single garbage collection request
    fn process_gc_request(graph: &MemoryGraph, request: GcRequest) -> StorageResult<()> {
        let mut expired_edges: HashMap<EdgeId, Edge> = HashMap::new();

        // Analyze expired undo entries and collect edges that need to be collected
        for undo_entry in request.expired_undo_entries {
            match undo_entry.delta() {
                // DeltaOp::CreateEdge means the edge was deleted in this transaction
                DeltaOp::CreateEdge(edge) => {
                    expired_edges.insert(edge.eid(), edge.without_properties());
                }
                DeltaOp::DelEdge(eid) => {
                    expired_edges.remove(eid);
                }
                _ => {}
            }
        }

        // Process expired edges and clean up graph data
        for (_, edge) in expired_edges {
            Self::cleanup_expired_edge(graph, &edge)?;
        }

        Ok(())
    }

    /// Clean up a single expired edge and related data
    fn cleanup_expired_edge(graph: &MemoryGraph, edge: &Edge) -> StorageResult<()> {
        // Check if the entity is marked as tombstone
        macro_rules! check_tombstone {
            ($graph:expr, $collection:ident, $id_method:expr) => {
                $graph
                    .$collection
                    .get($id_method)
                    .map(|v| Some(v.value().chain.current.read().unwrap().data.is_tombstone))
                    .unwrap_or(None)
            };
        }

        let src_tombstone = check_tombstone!(graph, vertices, &edge.src_id());
        let dst_tombstone = check_tombstone!(graph, vertices, &edge.dst_id());
        let edge_tombstone = check_tombstone!(graph, edges, &edge.eid());

        // If the source vertex is a tombstone, clean it up and related adjacencies
        if let Some(true) = src_tombstone {
            Self::remove_vertex_and_adjacencies(graph, edge.src_id());
        }

        // If the destination vertex is a tombstone, clean it up and related adjacencies
        if let Some(true) = dst_tombstone {
            Self::remove_vertex_and_adjacencies(graph, edge.dst_id());
        }

        // If the edge is a tombstone, clean up the edge and related adjacencies
        if let Some(true) = edge_tombstone {
            graph.edges.remove(&edge.eid());
            graph
                .adjacency_list
                .entry(edge.src_id())
                .and_modify(|adj_container| {
                    adj_container.outgoing().remove(&Neighbor::new(
                        edge.label_id(),
                        edge.dst_id(),
                        edge.eid(),
                    ));
                });
            graph
                .adjacency_list
                .entry(edge.dst_id())
                .and_modify(|adj_container| {
                    adj_container.incoming().remove(&Neighbor::new(
                        edge.label_id(),
                        edge.src_id(),
                        edge.eid(),
                    ));
                });
        }

        Ok(())
    }

    /// Remove vertex and all its adjacency relationships
    fn remove_vertex_and_adjacencies(graph: &MemoryGraph, vid: VertexId) {
        graph.vertices.remove(&vid);
        let mut incoming_to_remove = Vec::new();
        let mut outgoing_to_remove = Vec::new();

        if let Some(adj_container) = graph.adjacency_list.get(&vid) {
            for adj in adj_container.incoming().iter() {
                outgoing_to_remove.push((
                    adj.neighbor_id(),
                    Neighbor::new(adj.label_id(), vid, adj.eid()),
                ));
            }
            for adj in adj_container.outgoing().iter() {
                incoming_to_remove.push((
                    adj.neighbor_id(),
                    Neighbor::new(adj.label_id(), vid, adj.eid()),
                ));
            }
        }

        for (other_vid, euid) in incoming_to_remove {
            graph.adjacency_list.entry(other_vid).and_modify(|l| {
                l.incoming().remove(&euid);
            });
        }

        for (other_vid, euid) in outgoing_to_remove {
            graph.adjacency_list.entry(other_vid).and_modify(|l| {
                l.outgoing().remove(&euid);
            });
        }

        // Remove the vertex's adjacency list
        graph.adjacency_list.remove(&vid);
    }
}

impl Drop for GraphGarbageCollector {
    fn drop(&mut self) {
        self.stop_gc_thread();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tp::memory_graph;

    #[test]
    fn test_graph_gc_creation() {
        let (graph, _cleaner) = memory_graph::tests::mock_empty_graph();
        // Test creation without starting thread
        let gc = GraphGarbageCollector::new_with_thread_control(graph, GcConfig::default(), false);
        assert!(gc.gc_thread_handle.is_none());
    }

    #[test]
    fn test_gc_process_request_sync() {
        let (graph, _cleaner) = memory_graph::tests::mock_empty_graph();

        // Create an empty GC request
        let request = GcRequest {
            expired_undo_entries: Vec::new(),
            min_read_timestamp: 0,
        };

        // Test synchronous processing functionality
        let result = GraphGarbageCollector::process_gc_request_sync(&graph, request);
        assert!(result.is_ok());
    }
}
