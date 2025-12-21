// checkpoint.rs
// Implementation of checkpoint mechanism for MemoryGraph
//
// This module provides functionality to create and restore checkpoints of a MemoryGraph.
// A checkpoint represents a consistent snapshot of the graph state at a specific point in time.
// It can be used for backup, recovery, or state transfer purposes.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crc32fast::Hasher;
use minigu_common::types::{EdgeId, VertexId};
use minigu_transaction::Timestamp;
use serde::{Deserialize, Serialize};

use super::memory_graph::{AdjacencyContainer, MemoryGraph, VersionedEdge, VersionedVertex};
use crate::common::model::edge::{Edge, Neighbor};
use crate::common::model::vertex::Vertex;
use crate::error::{CheckpointError, StorageError, StorageResult};

/// Represents a checkpoint of a MemoryGraph at a specific point in time.
///
/// A GraphCheckpoint contains:
/// 1. Metadata about the checkpoint (timestamp, LSN, etc.)
/// 2. Serialized vertices and edges
/// 3. Adjacency list information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphCheckpoint {
    /// Metadata about the checkpoint
    pub metadata: CheckpointMetadata,

    /// Serialized vertices (current version only, no history)
    pub vertices: HashMap<VertexId, SerializedVertex>,

    /// Serialized edges (current version only, no history)
    pub edges: HashMap<EdgeId, SerializedEdge>,

    /// Serialized adjacency list
    pub adjacency_list: HashMap<VertexId, SerializedAdjacency>,
}

/// Metadata about a checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Timestamp when the checkpoint was created
    pub timestamp: u64,

    /// Log sequence number (LSN) at the time of checkpoint
    pub lsn: u64,

    /// Latest commit timestamp at the time of checkpoint
    pub latest_commit_ts: u64,

    /// Checkpoint format version
    pub version: u32,
}

/// Serialized representation of a vertex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedVertex {
    /// The vertex data
    pub data: Vertex,

    /// Commit timestamp of the vertex
    pub commit_ts: Timestamp,
}

/// Serialized representation of an edge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedEdge {
    /// The edge data
    pub data: Edge,

    /// Commit timestamp of the edge
    pub commit_ts: Timestamp,
}

/// Serialized representation of adjacency information for a vertex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedAdjacency {
    /// Outgoing edges from this vertex
    pub outgoing: Vec<(EdgeId, VertexId)>,

    /// Incoming edges to this vertex
    pub incoming: Vec<(EdgeId, VertexId)>,
}

impl GraphCheckpoint {
    /// Creates a new `GraphCheckpoint` from the current in-memory state of a [`MemoryGraph`].
    ///
    /// This method captures a consistent snapshot of the graph, including:
    /// - The metadata (timestamp, LSN, latest commit timestamp, etc.)
    /// - All vertices and edges (current version only)
    /// - The full adjacency list (both outgoing and incoming edges)
    ///
    /// It does **not** include historical versions of vertices or edges—only the
    /// latest committed state is serialized.
    ///
    /// This checkpoint can later be saved to disk using [`GraphCheckpoint::save_to_file`],
    /// and used for recovery via [`GraphCheckpoint::restore`] or the checkpoint manager.
    ///
    /// # Arguments
    ///
    /// * `graph` - A reference-counted pointer to the in-memory [`MemoryGraph`] to be checkpointed.
    ///
    /// # Returns
    ///
    /// A fully materialized `GraphCheckpoint` containing the graph's current state.
    ///
    /// # Panics
    ///
    /// This function may panic if:
    /// - System time is earlier than UNIX_EPOCH (highly unlikely)
    /// - Lock poisoning occurs on internal vertex/edge RwLocks (only if previous panic occurred)
    pub fn new(graph: &Arc<MemoryGraph>) -> Self {
        // Get current LSN
        let lsn = graph.persistence.next_lsn();

        // Create metadata
        let metadata = CheckpointMetadata {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            lsn,
            latest_commit_ts: graph
                .txn_manager
                .latest_commit_ts
                .load(std::sync::atomic::Ordering::SeqCst),
            version: 1, // Initial version
        };

        // Serialize vertices
        let mut vertices = HashMap::new();
        for entry in graph.vertices.iter() {
            let versioned_vertex = entry.value();
            let current = versioned_vertex.chain.current.read().unwrap();

            vertices.insert(
                *entry.key(),
                SerializedVertex {
                    data: current.data.clone(),
                    commit_ts: current.commit_ts,
                },
            );
        }

        // Serialize edges
        let mut edges = HashMap::new();
        for entry in graph.edges.iter() {
            let versioned_edge = entry.value();
            let current = versioned_edge.chain.current.read().unwrap();

            edges.insert(
                *entry.key(),
                SerializedEdge {
                    data: current.data.clone(),
                    commit_ts: current.commit_ts,
                },
            );
        }

        // Serialize adjacency list
        let mut adjacency_list = HashMap::new();
        for entry in graph.adjacency_list.iter() {
            let vertex_id = *entry.key();
            let adj_container = entry.value();

            let mut outgoing = Vec::new();
            for neighbor in adj_container.outgoing().iter() {
                outgoing.push((neighbor.value().eid(), neighbor.value().neighbor_id()));
            }

            let mut incoming = Vec::new();
            for neighbor in adj_container.incoming().iter() {
                incoming.push((neighbor.value().eid(), neighbor.value().neighbor_id()));
            }

            adjacency_list.insert(vertex_id, SerializedAdjacency { outgoing, incoming });
        }

        Self {
            metadata,
            vertices,
            edges,
            adjacency_list,
        }
    }

    /// Saves the checkpoint to a file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> StorageResult<()> {
        let file =
            File::create(path).map_err(|e| StorageError::Checkpoint(CheckpointError::Io(e)))?;

        let mut writer = BufWriter::new(file);

        // Serialize the checkpoint
        let serialized = postcard::to_allocvec(self).map_err(|e| {
            StorageError::Checkpoint(CheckpointError::SerializationFailed(e.to_string()))
        })?;

        // Calculate checksum
        let mut hasher = Hasher::new();
        hasher.update(&serialized);
        let checksum = hasher.finalize();

        // Write length and checksum
        let len = serialized.len() as u32;
        writer
            .write_all(&len.to_le_bytes())
            .map_err(|e| StorageError::Checkpoint(CheckpointError::Io(e)))?;
        writer
            .write_all(&checksum.to_le_bytes())
            .map_err(|e| StorageError::Checkpoint(CheckpointError::Io(e)))?;

        // Write serialized data
        writer
            .write_all(&serialized)
            .map_err(|e| StorageError::Checkpoint(CheckpointError::Io(e)))?;

        // Flush to ensure data is written
        writer
            .flush()
            .map_err(|e| StorageError::Checkpoint(CheckpointError::Io(e)))?;

        Ok(())
    }

    /// Loads a checkpoint from a file
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let file =
            File::open(path).map_err(|e| StorageError::Checkpoint(CheckpointError::Io(e)))?;

        let mut reader = BufReader::new(file);

        // Read length and checksum
        let mut len_bytes = [0u8; 4];
        reader
            .read_exact(&mut len_bytes)
            .map_err(|e| StorageError::Checkpoint(CheckpointError::Io(e)))?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut checksum_bytes = [0u8; 4];
        reader
            .read_exact(&mut checksum_bytes)
            .map_err(|e| StorageError::Checkpoint(CheckpointError::Io(e)))?;
        let checksum = u32::from_le_bytes(checksum_bytes);

        // Read serialized data
        let mut serialized = vec![0u8; len];
        reader
            .read_exact(&mut serialized)
            .map_err(|e| StorageError::Checkpoint(CheckpointError::Io(e)))?;

        // Verify checksum
        let mut hasher = Hasher::new();
        hasher.update(&serialized);
        if hasher.finalize() != checksum {
            return Err(StorageError::Checkpoint(CheckpointError::ChecksumMismatch));
        }

        // Deserialize
        postcard::from_bytes(&serialized).map_err(|e| {
            StorageError::Checkpoint(CheckpointError::DeserializationFailed(e.to_string()))
        })
    }

    /// Restores a new [`MemoryGraph`] instance from this checkpoint snapshot.
    ///
    /// This method reconstructs an in-memory graph by replaying the serialized state
    /// stored in the checkpoint, including:
    /// - Metadata (log sequence number and latest commit timestamp)
    /// - All current vertices and edges (no historical versions)
    /// - The full adjacency list (outgoing/incoming connections)
    ///
    /// This method is typically used during system recovery, state rehydration,
    /// or startup bootstrapping from the latest persisted checkpoint.
    ///
    /// # Arguments
    ///
    /// * `checkpoint_config` - Configuration options for the graph's checkpoint behavior.
    /// * `wal_config` - Configuration for initializing the graph's write-ahead log (WAL) system.
    ///
    /// # Returns
    ///
    /// A fully reconstructed [`Arc<MemoryGraph>`] containing the state at the time of checkpoint
    /// creation.
    pub fn restore(&self, graph: &Arc<MemoryGraph>) -> StorageResult<()> {
        // Set the LSN to the checkpoint's LSN
        graph.persistence.set_next_lsn(self.metadata.lsn);

        // Set the latest commit timestamp
        graph.txn_manager.latest_commit_ts.store(
            self.metadata.latest_commit_ts,
            std::sync::atomic::Ordering::SeqCst,
        );

        // Restore vertices
        for (vid, serialized_vertex) in &self.vertices {
            let versioned_vertex = VersionedVertex::new(serialized_vertex.data.clone());
            // Set the commit timestamp
            let mut current = versioned_vertex.chain.current.write().unwrap();
            current.commit_ts = serialized_vertex.commit_ts;
            drop(current);

            graph.vertices.insert(*vid, versioned_vertex);
        }

        // Restore edges
        for (eid, serialized_edge) in &self.edges {
            let versioned_edge = VersionedEdge::new(serialized_edge.data.clone());
            // Set the commit timestamp
            let mut current = versioned_edge.chain.current.write().unwrap();
            current.commit_ts = serialized_edge.commit_ts;
            drop(current);

            graph.edges.insert(*eid, versioned_edge);
        }

        // Restore adjacency list
        for (vid, serialized_adjacency) in &self.adjacency_list {
            let adjacency_container = AdjacencyContainer::new();

            // Restore outgoing edges
            for (edge_id, dst_id) in &serialized_adjacency.outgoing {
                let edge = graph.edges.get(edge_id).unwrap();
                let label_id = edge.chain.current.read().unwrap().data.label_id();
                adjacency_container
                    .outgoing()
                    .insert(Neighbor::new(label_id, *dst_id, *edge_id));
            }

            // Restore incoming edges
            for (edge_id, src_id) in &serialized_adjacency.incoming {
                let edge = graph.edges.get(edge_id).unwrap();
                let label_id = edge.chain.current.read().unwrap().data.label_id();
                adjacency_container
                    .incoming()
                    .insert(Neighbor::new(label_id, *src_id, *edge_id));
            }

            graph.adjacency_list.insert(*vid, adjacency_container);
        }

        Ok(())
    }
}

impl MemoryGraph {
    // Recovers a [`MemoryGraph`] by loading the latest checkpoint and replaying WAL entries.
    //
    // This method implements a two-phase recovery process:
    //
    // 1. **Checkpoint-based Recovery**   If a valid checkpoint exists in the configured directory,
    //    the graph is restored from it, and all WAL entries with LSN ≥ checkpoint LSN are applied
    //    to reach the latest consistent state.
    //
    // 2. **WAL-only Recovery**   If no checkpoint is found, the graph is initialized empty and
    //    recovered solely from WAL entries.
    //
    // # Returns
    //
    // A fully recovered [`Arc<MemoryGraph>`] containing the most recent state reconstructed
    // from persisted checkpoints and logs.
    // TODO: Reimplement recover_from_checkpoint_and_wal using new persistence architecture
    // This function needs to be rewritten to work with the new PersistenceProvider
    // pub fn recover_from_checkpoint_and_wal(
    // checkpoint_config: CheckpointManagerConfig,
    // wal_config: WalManagerConfig,
    // ) -> StorageResult<Arc<MemoryGraph>> {
    // let checkpoint_path = Self::find_most_recent_checkpoint(&checkpoint_config)?;
    //
    // if checkpoint_path.is_none() {
    // let graph = Self::with_config_fresh(checkpoint_config.clone(), wal_config.clone());
    // graph.recover_from_wal()?;
    // return Ok(graph);
    // }
    //
    // Restore from checkpoint
    // let checkpoint = GraphCheckpoint::load_from_file(checkpoint_path.unwrap())?;
    // let checkpoint_lsn = checkpoint.metadata.lsn;
    // let graph = checkpoint.restore(checkpoint_config, wal_config)?;
    //
    // Read WAL entries with LSN >= checkpoint_lsn
    // let all_entries = graph.persistence.read_wal_entries()?;
    //
    // let new_entries: Vec<_> = all_entries
    // .into_iter()
    // .filter(|entry| entry.lsn >= checkpoint_lsn)
    // .collect();
    //
    // Apply new WAL entries
    // if !new_entries.is_empty() {
    // graph.apply_wal_entries(new_entries)?;
    // }
    //
    // Ok(graph)
    // }

    // Unused helper - commented out until recovery logic is reimplemented
    // Finds the most recent checkpoint in the checkpoint directory
    // fn find_most_recent_checkpoint(
    // config: &CheckpointManagerConfig,
    // ) -> StorageResult<Option<PathBuf>> {
    // let entries = match fs::read_dir(&config.checkpoint_dir) {
    // Ok(entries) => entries,
    // Err(e) => return Err(StorageError::Checkpoint(CheckpointError::Io(e))),
    // };
    // ... (implementation omitted)
    // Ok(None) // Placeholder
    // }
    // Ok(latest_checkpoint.map(|(path, _)| path))
    // }

    // TODO: Re-implement create_managed_checkpoint using persistence provider
    // Temporarily disabled due to CheckpointManager refactoring
    // pub fn create_managed_checkpoint(&self, description: Option<String>) -> StorageResult<String>
    // { match &self.checkpoint_manager {
    // Some(manager) => {
    // let manager_ptr = manager as *const CheckpointManager as *mut CheckpointManager;
    // unsafe { (*manager_ptr).create_checkpoint(description) }
    // }
    // None => Err(StorageError::Checkpoint(
    // crate::error::CheckpointError::DirectoryError(
    // "No checkpoint manager configured".to_string(),
    // ),
    // )),
    // }
    // }

    // TODO: Re-implement check_auto_checkpoint using persistence provider
    // Temporarily disabled due to CheckpointManager refactoring
    // pub fn check_auto_checkpoint(&self) -> StorageResult<Option<String>> {
    // match &self.checkpoint_manager {
    // Some(manager) => {
    // Need to get a mutable reference to the manager
    // This is safe because we're only modifying the manager's internal state
    // let manager_ptr = manager as *const CheckpointManager as *mut CheckpointManager;
    // unsafe { (*manager_ptr).check_auto_checkpoint() }
    // }
    // None => Ok(None), // No checkpoint manager, so no auto checkpoint
    // }
    // }
}

#[cfg(test)]
mod tests {
    use std::io::Seek;
    use std::{env, fs};

    use minigu_common::types::VertexId;
    use minigu_common::value::ScalarValue;
    use minigu_transaction::{GraphTxnManager, IsolationLevel};

    use super::*;
    use crate::error::CheckpointError;
    use crate::tp::memory_graph;

    fn get_temp_file_path(prefix: &str) -> std::path::PathBuf {
        let mut path = env::temp_dir();
        path.push(format!("{}_{}.bin", prefix, uuid::Uuid::new_v4()));
        path
    }

    #[test]
    fn test_checkpoint_creation() {
        // Create a graph with mock data
        let (graph, _cleaner) = memory_graph::tests::mock_graph();

        // Create checkpoint
        let checkpoint = GraphCheckpoint::new(&graph);

        // Verify checkpoint contents
        assert!(checkpoint.vertices.len() == 4);
        assert!(checkpoint.edges.len() == 4);

        let alice_vid: VertexId = VertexId::from(1u64);
        // Verify vertex data
        let alice_serialized = checkpoint.vertices.get(&alice_vid).unwrap();
        assert_eq!(alice_serialized.data.vid(), alice_vid);
        assert_eq!(
            alice_serialized.data.properties()[0],
            ScalarValue::String(Some("Alice".to_string()))
        );

        // Verify adjacency list
        let alice_adj = checkpoint.adjacency_list.get(&alice_vid).unwrap();
        assert!(alice_adj.outgoing.len() == 2);
        assert!(alice_adj.incoming.len() == 1);
    }

    #[test]
    fn test_checkpoint_save_and_load() {
        // Create a graph with mock data
        let (graph, _cleaner) = memory_graph::tests::mock_graph();

        // Create and save checkpoint
        let checkpoint_path = get_temp_file_path("checkpoint_save_load");
        let checkpoint = GraphCheckpoint::new(&graph);
        checkpoint.save_to_file(&checkpoint_path).unwrap();

        // Load checkpoint
        let loaded_checkpoint = GraphCheckpoint::load_from_file(&checkpoint_path).unwrap();

        // Verify loaded checkpoint has same number of elements
        assert_eq!(loaded_checkpoint.vertices.len(), checkpoint.vertices.len());
        assert_eq!(loaded_checkpoint.edges.len(), checkpoint.edges.len());
        assert_eq!(
            loaded_checkpoint.adjacency_list.len(),
            checkpoint.adjacency_list.len()
        );

        // Clean up
        if checkpoint_path.exists() {
            fs::remove_file(checkpoint_path).unwrap();
        }
    }

    #[test]
    fn test_checkpoint_restore() {
        // Create a graph with mock data
        let (original_graph, _cleaner) = memory_graph::tests::mock_graph();

        // Create checkpoint
        let checkpoint = GraphCheckpoint::new(&original_graph);

        // Create a new empty graph to restore into
        let (restored_graph, _) = memory_graph::tests::mock_empty_graph();

        // Restore graph from checkpoint
        checkpoint.restore(&restored_graph).unwrap();

        let origin_txn = original_graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();
        let restore_txn = restored_graph
            .txn_manager()
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();

        // Check vertices
        let original_alice = original_graph
            .get_vertex(&origin_txn, VertexId::from(1u64))
            .unwrap();
        let restored_alice = restored_graph
            .get_vertex(&restore_txn, VertexId::from(1u64))
            .unwrap();
        assert_eq!(original_alice.vid(), restored_alice.vid());
        assert_eq!(original_alice.properties(), restored_alice.properties());

        let original_bob = original_graph
            .get_vertex(&origin_txn, VertexId::from(2u64))
            .unwrap();
        let restored_bob = restored_graph
            .get_vertex(&restore_txn, VertexId::from(2u64))
            .unwrap();
        assert_eq!(original_bob.vid(), restored_bob.vid());
        assert_eq!(original_bob.properties(), restored_bob.properties());

        // Check edges
        let original_friend_edge = original_graph
            .get_edge(&origin_txn, VertexId::from(1u64))
            .unwrap();
        let restored_friend_edge = restored_graph
            .get_edge(&restore_txn, VertexId::from(1u64))
            .unwrap();
        assert_eq!(original_friend_edge.eid(), restored_friend_edge.eid());
        assert_eq!(
            original_friend_edge.properties(),
            restored_friend_edge.properties()
        );

        let original_follow_edge = original_graph
            .get_edge(&origin_txn, VertexId::from(3u64))
            .unwrap();
        let restored_follow_edge = restored_graph
            .get_edge(&restore_txn, VertexId::from(3u64))
            .unwrap();
        assert_eq!(original_follow_edge.eid(), restored_follow_edge.eid());
        assert_eq!(
            original_follow_edge.properties(),
            restored_follow_edge.properties()
        );

        // Check adjacency list
        let original_alice_adj = original_graph
            .adjacency_list
            .get(&VertexId::from(1u64))
            .unwrap();
        let restored_alice_adj = restored_graph
            .adjacency_list
            .get(&VertexId::from(1u64))
            .unwrap();
        assert_eq!(
            original_alice_adj.outgoing.len(),
            restored_alice_adj.outgoing.len()
        );
        assert_eq!(
            original_alice_adj.incoming.len(),
            restored_alice_adj.incoming.len()
        );
    }

    #[test]
    fn test_checkpoint_with_corrupted_file() {
        // Create a graph with mock data
        let (graph, _cleaner) = memory_graph::tests::mock_graph();

        // Create and save checkpoint
        let checkpoint_path = get_temp_file_path("checkpoint_corrupted");
        let checkpoint = GraphCheckpoint::new(&graph);
        checkpoint.save_to_file(&checkpoint_path).unwrap();

        // Corrupt the file
        {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .open(&checkpoint_path)
                .unwrap();
            file.seek(std::io::SeekFrom::Start(8)).unwrap(); // Skip length and checksum
            file.write_all(&[0, 0, 0, 0]).unwrap(); // Write some garbage
        }

        // Try to load the corrupted checkpoint
        let result = GraphCheckpoint::load_from_file(&checkpoint_path);
        assert!(result.is_err());

        // Verify it's a checksum error
        match result {
            Err(StorageError::Checkpoint(CheckpointError::ChecksumMismatch)) => (),
            _ => panic!("Expected ChecksumMismatch error"),
        }

        // Clean up
        if checkpoint_path.exists() {
            fs::remove_file(checkpoint_path).unwrap();
        }
    }
}
