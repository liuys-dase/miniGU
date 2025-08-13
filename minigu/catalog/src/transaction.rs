//! Transaction support for catalog operations.
//!
//! This module implements transaction support for catalog operations,
//! enabling DDL operations to be performed within transactions.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use minigu_transaction::{
    GraphTxnManager, IsolationLevel, Timestamp, Transaction, global_timestamp_generator,
    global_transaction_id_generator,
};

use crate::error::{CatalogError, CatalogResult};
use crate::provider::{CatalogProvider, DirectoryOrSchema, GraphRef, SchemaRef};

/// Create a placeholder graph reference for testing purposes
/// In a real implementation, this would create an actual graph object
fn create_placeholder_graph() -> GraphRef {
    use std::any::Any;

    use crate::memory::graph_type::MemoryGraphTypeCatalog;
    use crate::provider::GraphProvider;

    #[derive(Debug)]
    struct PlaceholderGraph;

    impl GraphProvider for PlaceholderGraph {
        fn graph_type(&self) -> crate::provider::GraphTypeRef {
            Arc::new(MemoryGraphTypeCatalog::new())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    Arc::new(PlaceholderGraph)
}

/// Catalog operation types for undo logging
#[derive(Debug, Clone)]
pub enum CatalogOp {
    /// Create schema operation
    CreateSchema { name: String, parent_dir: String },
    /// Drop schema operation
    DropSchema {
        name: String,
        parent_dir: String,
        schema: SchemaRef,
    },
    /// Create graph operation
    CreateGraph {
        schema_name: String,
        graph_name: String,
    },
    /// Drop graph operation  
    DropGraph {
        schema_name: String,
        graph_name: String,
        graph: GraphRef,
    },
}

/// Catalog transaction implementation
#[derive(Debug)]
pub struct CatalogTransaction {
    txn_id: Timestamp,
    start_ts: Timestamp,
    commit_ts: Mutex<Option<Timestamp>>,
    isolation_level: IsolationLevel,
    /// Reference to the catalog for applying operations
    catalog: Arc<dyn CatalogProvider>,
    /// Weak reference to the transaction manager for conflict detection
    txn_manager: std::sync::Weak<CatalogTxnManager>,
    /// Operations performed in this transaction (for undo)
    operations: Mutex<Vec<CatalogOp>>,
    /// Modified state during transaction
    local_changes: Mutex<HashMap<String, DirectoryOrSchema>>,
    /// Read set: records what catalog objects were read
    read_set: Mutex<HashMap<String, Timestamp>>,
    /// Write set: records what catalog objects were written
    write_set: Mutex<HashMap<String, CatalogOp>>,
}

impl CatalogTransaction {
    /// Create a new catalog transaction
    pub fn new(
        isolation_level: IsolationLevel,
        catalog: Arc<dyn CatalogProvider>,
        txn_manager: std::sync::Weak<CatalogTxnManager>,
    ) -> Self {
        let txn_id = global_transaction_id_generator().next();
        let start_ts = global_timestamp_generator().next();

        Self {
            txn_id,
            start_ts,
            commit_ts: Mutex::new(None),
            isolation_level,
            catalog,
            txn_manager,
            operations: Mutex::new(Vec::new()),
            local_changes: Mutex::new(HashMap::new()),
            read_set: Mutex::new(HashMap::new()),
            write_set: Mutex::new(HashMap::new()),
        }
    }

    /// Add an operation to the undo log
    pub fn add_operation(&self, op: CatalogOp) {
        self.operations.lock().unwrap().push(op);
    }

    /// Get local changes made in this transaction
    pub fn get_local_changes(&self) -> HashMap<String, DirectoryOrSchema> {
        self.local_changes.lock().unwrap().clone()
    }

    /// Add a local change
    pub fn add_local_change(&self, key: String, value: DirectoryOrSchema) {
        self.local_changes.lock().unwrap().insert(key, value);
    }

    /// Record a read operation in the read set
    pub fn add_to_read_set(&self, key: String, read_timestamp: Timestamp) {
        self.read_set.lock().unwrap().insert(key, read_timestamp);
    }

    /// Record a write operation in the write set
    pub fn add_to_write_set(&self, key: String, operation: CatalogOp) {
        self.write_set.lock().unwrap().insert(key, operation);
    }

    /// Get the read set
    pub fn get_read_set(&self) -> HashMap<String, Timestamp> {
        self.read_set.lock().unwrap().clone()
    }

    /// Get the write set
    pub fn get_write_set(&self) -> HashMap<String, CatalogOp> {
        self.write_set.lock().unwrap().clone()
    }

    /// Validate the read set against other transactions for serializability
    pub fn validate_read_set(
        &self,
        active_transactions: &HashMap<Timestamp, Arc<CatalogTransaction>>,
    ) -> bool {
        let read_set = self.read_set.lock().unwrap();

        // Check for read-write conflicts with other transactions
        for (other_txn_id, other_txn) in active_transactions {
            // Skip self
            if *other_txn_id == self.txn_id {
                continue;
            }

            // Only check transactions that started after this one
            if other_txn.start_ts > self.start_ts {
                let other_write_set = other_txn.get_write_set();

                // Check if any object we read was written by the other transaction
                for read_key in read_set.keys() {
                    if other_write_set.contains_key(read_key) {
                        return false; // Read-write conflict detected
                    }
                }
            }
        }

        true // No conflicts detected
    }

    /// Rollback operations in reverse order
    pub fn rollback_operations(&self, catalog: &dyn CatalogProvider) -> CatalogResult<()> {
        let operations = self.operations.lock().unwrap();

        // Apply operations in reverse order for rollback
        for op in operations.iter().rev() {
            match op {
                CatalogOp::CreateSchema { name, parent_dir } => {
                    // Rollback create schema by removing it from the catalog
                    self.rollback_create_schema(catalog, name, parent_dir)?;
                }
                CatalogOp::DropSchema {
                    name,
                    parent_dir,
                    schema,
                } => {
                    // Rollback drop schema by restoring it to the catalog
                    self.rollback_drop_schema(catalog, name, parent_dir, schema)?;
                }
                CatalogOp::CreateGraph {
                    schema_name,
                    graph_name,
                } => {
                    // Rollback create graph by removing it from the schema
                    self.rollback_create_graph(catalog, schema_name, graph_name)?;
                }
                CatalogOp::DropGraph {
                    schema_name,
                    graph_name,
                    graph,
                } => {
                    // Rollback drop graph by restoring it to the schema
                    self.rollback_drop_graph(catalog, schema_name, graph_name, graph)?;
                }
            }
        }

        Ok(())
    }

    /// Rollback a create schema operation
    fn rollback_create_schema(
        &self,
        catalog: &dyn CatalogProvider,
        name: &str,
        parent_dir: &str,
    ) -> CatalogResult<()> {
        // Remove from local changes
        let mut changes = self.local_changes.lock().unwrap();
        changes.remove(&format!("{}/{}", parent_dir, name));

        // Remove the schema from the actual catalog if it was created
        if let Ok(root) = catalog.get_root() {
            if let Some(root_dir) = root.as_directory() {
                if let Ok(Some(parent_directory)) = root_dir.get_child(parent_dir) {
                    if let Some(parent_dir_ref) = parent_directory.as_directory() {
                        use crate::memory::directory::MemoryDirectoryCatalog;
                        if let Some(memory_dir) = parent_dir_ref
                            .as_any()
                            .downcast_ref::<MemoryDirectoryCatalog>()
                        {
                            memory_dir.remove_child(name);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Rollback a drop schema operation  
    fn rollback_drop_schema(
        &self,
        catalog: &dyn CatalogProvider,
        name: &str,
        parent_dir: &str,
        schema: &SchemaRef,
    ) -> CatalogResult<()> {
        // Add back to local changes
        let mut changes = self.local_changes.lock().unwrap();
        changes.insert(
            format!("{}/{}", parent_dir, name),
            DirectoryOrSchema::Schema(schema.clone()),
        );
        // Remove the DROP marker if it exists
        changes.remove(&format!("{}/{}/DROP", parent_dir, name));

        // Restore the schema to the actual catalog
        if let Ok(root) = catalog.get_root() {
            if let Some(root_dir) = root.as_directory() {
                if let Ok(Some(parent_directory)) = root_dir.get_child(parent_dir) {
                    if let Some(parent_dir_ref) = parent_directory.as_directory() {
                        use crate::memory::directory::MemoryDirectoryCatalog;
                        if let Some(memory_dir) = parent_dir_ref
                            .as_any()
                            .downcast_ref::<MemoryDirectoryCatalog>()
                        {
                            memory_dir.add_child(
                                name.to_string(),
                                DirectoryOrSchema::Schema(schema.clone()),
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Rollback a create graph operation
    fn rollback_create_graph(
        &self,
        catalog: &dyn CatalogProvider,
        schema_name: &str,
        graph_name: &str,
    ) -> CatalogResult<()> {
        // Remove from local changes
        let mut changes = self.local_changes.lock().unwrap();
        changes.remove(&format!("{}/{}", schema_name, graph_name));

        // Remove the graph from the actual schema
        if let Ok(root) = catalog.get_root() {
            if let Some(root_dir) = root.as_directory() {
                if let Ok(Some(schema_entry)) = root_dir.get_child(schema_name) {
                    if let Some(schema_ref) = schema_entry.as_schema() {
                        use crate::memory::schema::MemorySchemaCatalog;
                        if let Some(memory_schema) =
                            schema_ref.as_any().downcast_ref::<MemorySchemaCatalog>()
                        {
                            memory_schema.remove_graph(graph_name);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Rollback a drop graph operation
    fn rollback_drop_graph(
        &self,
        catalog: &dyn CatalogProvider,
        schema_name: &str,
        graph_name: &str,
        graph: &GraphRef,
    ) -> CatalogResult<()> {
        // Remove the DROP marker from local changes
        let mut changes = self.local_changes.lock().unwrap();
        changes.remove(&format!("{}/{}/DROP", schema_name, graph_name));

        // Restore the graph to the actual schema
        if let Ok(root) = catalog.get_root() {
            if let Some(root_dir) = root.as_directory() {
                if let Ok(Some(schema_entry)) = root_dir.get_child(schema_name) {
                    if let Some(schema_ref) = schema_entry.as_schema() {
                        use crate::memory::schema::MemorySchemaCatalog;
                        if let Some(memory_schema) =
                            schema_ref.as_any().downcast_ref::<MemorySchemaCatalog>()
                        {
                            memory_schema.add_graph(graph_name.to_string(), graph.clone());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Transaction for CatalogTransaction {
    type Error = CatalogError;

    fn txn_id(&self) -> Timestamp {
        self.txn_id
    }

    fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    fn commit_ts(&self) -> Option<Timestamp> {
        *self.commit_ts.lock().unwrap()
    }

    fn isolation_level(&self) -> &IsolationLevel {
        &self.isolation_level
    }

    fn commit(&self) -> Result<Timestamp, Self::Error> {
        // Perform conflict detection for serializable isolation level
        if let IsolationLevel::Serializable = self.isolation_level {
            // Try to get the transaction manager for conflict detection
            if let Some(txn_manager) = self.txn_manager.upgrade() {
                let active_txns = txn_manager.active_txns.lock().unwrap();
                if !self.validate_read_set(&active_txns) {
                    // Conflict detected, abort the transaction
                    let _ = self.abort();
                    return Err(CatalogError::General(
                        "Transaction aborted due to serialization conflict".to_string(),
                    ));
                }
            }
            // If txn_manager can't be upgraded, we proceed without conflict detection
            // This should be rare and only happen during shutdown
        }

        let commit_ts = global_timestamp_generator().next();

        // Mark as committed
        *self.commit_ts.lock().unwrap() = Some(commit_ts);

        // Apply all transaction operations to the underlying catalog
        self.apply_operations_to_catalog()?;

        Ok(commit_ts)
    }

    fn abort(&self) -> Result<(), Self::Error> {
        // Note: To properly rollback, we need access to the catalog, but the
        // Transaction trait doesn't provide it. In practice, rollback should be
        // called through the CatalogTxnManager.finish_transaction method when
        // the transaction is aborted.

        // Clear any local changes, operations, and read/write sets
        self.local_changes.lock().unwrap().clear();
        self.operations.lock().unwrap().clear();
        self.read_set.lock().unwrap().clear();
        self.write_set.lock().unwrap().clear();

        Ok(())
    }
}

impl CatalogTransaction {
    /// Apply all transaction operations to the underlying catalog
    fn apply_operations_to_catalog(&self) -> CatalogResult<()> {
        let operations = self.operations.lock().unwrap();

        // Apply all operations to the underlying catalog in order
        for op in operations.iter() {
            match op {
                CatalogOp::CreateSchema { name, parent_dir } => {
                    self.apply_create_schema(name, parent_dir)?;
                }
                CatalogOp::DropSchema {
                    name, parent_dir, ..
                } => {
                    self.apply_drop_schema(name, parent_dir)?;
                }
                CatalogOp::CreateGraph {
                    schema_name,
                    graph_name,
                } => {
                    self.apply_create_graph(schema_name, graph_name)?;
                }
                CatalogOp::DropGraph {
                    schema_name,
                    graph_name,
                    ..
                } => {
                    self.apply_drop_graph(schema_name, graph_name)?;
                }
            }
        }

        Ok(())
    }

    /// Apply create schema operation to the underlying catalog
    fn apply_create_schema(&self, name: &str, parent_dir: &str) -> CatalogResult<()> {
        // Get the root directory
        let root = self.catalog.get_root()?;

        if let Some(root_dir) = root.as_directory() {
            // If parent_dir is "root", use the root directory itself
            let parent_directory = if parent_dir == "root" {
                Some(root_dir.clone())
            } else {
                // Find the parent directory
                root_dir
                    .get_child(parent_dir)?
                    .and_then(|child| child.into_directory())
            };

            if let Some(parent_dir_ref) = parent_directory {
                // Check if we have a memory directory catalog to modify
                use crate::memory::directory::MemoryDirectoryCatalog;
                if let Some(memory_dir) = parent_dir_ref
                    .as_any()
                    .downcast_ref::<MemoryDirectoryCatalog>()
                {
                    // Create the new schema
                    use crate::memory::schema::MemorySchemaCatalog;
                    let new_schema = Arc::new(MemorySchemaCatalog::new(Some(Arc::downgrade(
                        &parent_dir_ref,
                    ))));
                    let schema_ref: SchemaRef = new_schema;

                    // Add the schema to the parent directory
                    if !memory_dir
                        .add_child(name.to_string(), DirectoryOrSchema::Schema(schema_ref))
                    {
                        return Err(CatalogError::General(format!(
                            "Failed to create schema '{}': already exists",
                            name
                        )));
                    }

                    return Ok(());
                }
            }
        }

        Err(CatalogError::General(format!(
            "Cannot apply CreateSchema operation for '{}/{}'",
            parent_dir, name
        )))
    }

    /// Apply drop schema operation to the underlying catalog
    fn apply_drop_schema(&self, name: &str, parent_dir: &str) -> CatalogResult<()> {
        // Get the root directory
        let root = self.catalog.get_root()?;

        if let Some(root_dir) = root.as_directory() {
            // If parent_dir is "root", use the root directory itself
            let parent_directory = if parent_dir == "root" {
                Some(root_dir.clone())
            } else {
                // Find the parent directory
                root_dir
                    .get_child(parent_dir)?
                    .and_then(|child| child.into_directory())
            };

            if let Some(parent_dir_ref) = parent_directory {
                // Check if we have a memory directory catalog to modify
                use crate::memory::directory::MemoryDirectoryCatalog;
                if let Some(memory_dir) = parent_dir_ref
                    .as_any()
                    .downcast_ref::<MemoryDirectoryCatalog>()
                {
                    // Remove the schema from the parent directory
                    if !memory_dir.remove_child(name) {
                        return Err(CatalogError::General(format!(
                            "Failed to drop schema '{}': not found",
                            name
                        )));
                    }

                    return Ok(());
                }
            }
        }

        Err(CatalogError::General(format!(
            "Cannot apply DropSchema operation for '{}/{}'",
            parent_dir, name
        )))
    }

    /// Apply create graph operation to the underlying catalog
    fn apply_create_graph(&self, schema_name: &str, graph_name: &str) -> CatalogResult<()> {
        // Get the root directory
        let root = self.catalog.get_root()?;

        if let Some(root_dir) = root.as_directory() {
            // Find the schema in the root directory
            if let Some(schema) = root_dir
                .get_child(schema_name)?
                .and_then(|child| child.into_schema())
            {
                // Check if we have a memory schema catalog to modify
                use crate::memory::schema::MemorySchemaCatalog;
                if let Some(memory_schema) = schema.as_any().downcast_ref::<MemorySchemaCatalog>() {
                    // Create the graph
                    let new_graph = create_placeholder_graph();

                    // Add the graph to the schema
                    if !memory_schema.add_graph(graph_name.to_string(), new_graph) {
                        return Err(CatalogError::General(format!(
                            "Failed to create graph '{}': already exists",
                            graph_name
                        )));
                    }

                    return Ok(());
                }
            }
        }

        Err(CatalogError::General(format!(
            "Cannot apply CreateGraph operation for '{}/{}'",
            schema_name, graph_name
        )))
    }

    /// Apply drop graph operation to the underlying catalog
    fn apply_drop_graph(&self, schema_name: &str, graph_name: &str) -> CatalogResult<()> {
        // Get the root directory
        let root = self.catalog.get_root()?;

        if let Some(root_dir) = root.as_directory() {
            // Find the schema in the root directory
            if let Some(schema) = root_dir
                .get_child(schema_name)?
                .and_then(|child| child.into_schema())
            {
                // Check if we have a memory schema catalog to modify
                use crate::memory::schema::MemorySchemaCatalog;
                if let Some(memory_schema) = schema.as_any().downcast_ref::<MemorySchemaCatalog>() {
                    // Remove the graph from the schema
                    if !memory_schema.remove_graph(graph_name) {
                        return Err(CatalogError::General(format!(
                            "Failed to drop graph '{}': not found",
                            graph_name
                        )));
                    }

                    return Ok(());
                }
            }
        }

        Err(CatalogError::General(format!(
            "Cannot apply DropGraph operation for '{}/{}'",
            schema_name, graph_name
        )))
    }
}

/// Garbage collection configuration
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Maximum number of completed transactions to keep in history
    pub max_txn_history: usize,
    /// Maximum age of completed transactions before cleanup (in milliseconds)  
    pub max_txn_age_ms: u64,
    /// Maximum number of undo operations to keep per transaction
    pub max_undo_operations: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            max_txn_history: 1000,
            max_txn_age_ms: 300_000, // 5 minutes
            max_undo_operations: 100,
        }
    }
}

/// Information about completed transactions for garbage collection
#[derive(Debug, Clone)]
struct CompletedTxnInfo {
    #[allow(dead_code)]
    txn_id: Timestamp,
    completion_time: std::time::Instant,
    #[allow(dead_code)]
    was_committed: bool,
}

/// Garbage collection statistics
#[derive(Debug, Clone)]
pub struct GcStats {
    /// Number of active transactions
    pub active_txn_count: usize,
    /// Number of completed transactions in history
    pub completed_txn_count: usize,
    /// Last garbage collection time
    pub last_gc_time: std::time::Instant,
}

/// Result of garbage collection operation
#[derive(Debug, Clone)]
pub struct GcResult {
    /// Number of completed transactions cleaned up
    pub cleaned_completed_txns: usize,
    /// Number of undo operations cleaned up
    pub cleaned_undo_operations: usize,
    /// Number of local changes cleaned up
    pub cleaned_local_changes: usize,
    /// Total time taken for garbage collection
    pub gc_duration: std::time::Duration,
}

/// Transaction manager for catalog operations
pub struct CatalogTxnManager {
    /// Active transactions
    active_txns: Mutex<HashMap<Timestamp, Arc<CatalogTransaction>>>,
    /// Completed transaction history for garbage collection
    completed_txns: Mutex<Vec<CompletedTxnInfo>>,
    /// Reference to the catalog
    #[allow(dead_code)]
    catalog: Arc<dyn CatalogProvider>,
    /// Garbage collection configuration
    gc_config: GcConfig,
    /// Last garbage collection time
    last_gc_time: Mutex<std::time::Instant>,
}

impl CatalogTxnManager {
    /// Create a new catalog transaction manager
    pub fn new(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self {
            active_txns: Mutex::new(HashMap::new()),
            completed_txns: Mutex::new(Vec::new()),
            catalog,
            gc_config: GcConfig::default(),
            last_gc_time: Mutex::new(std::time::Instant::now()),
        }
    }

    /// Create a new catalog transaction manager wrapped in Arc
    pub fn new_arc(catalog: Arc<dyn CatalogProvider>) -> Arc<Self> {
        Arc::new(Self::new(catalog))
    }

    /// Create a new catalog transaction manager with custom GC configuration
    pub fn new_with_gc_config(catalog: Arc<dyn CatalogProvider>, gc_config: GcConfig) -> Self {
        Self {
            active_txns: Mutex::new(HashMap::new()),
            completed_txns: Mutex::new(Vec::new()),
            catalog,
            gc_config,
            last_gc_time: Mutex::new(std::time::Instant::now()),
        }
    }

    /// Get garbage collection statistics
    pub fn gc_stats(&self) -> GcStats {
        let active_count = self.active_txns.lock().unwrap().len();
        let completed_count = self.completed_txns.lock().unwrap().len();
        let last_gc = *self.last_gc_time.lock().unwrap();

        GcStats {
            active_txn_count: active_count,
            completed_txn_count: completed_count,
            last_gc_time: last_gc,
        }
    }

    /// Trigger garbage collection manually
    pub fn manual_gc(&self, catalog: &Arc<dyn CatalogProvider>) -> Result<GcResult, CatalogError> {
        self.perform_garbage_collection(catalog)
    }

    /// Clean up undo operations in a transaction to prevent memory bloat
    fn cleanup_transaction_undo_ops(&self, txn: &CatalogTransaction) {
        let mut operations = txn.operations.lock().unwrap();
        if operations.len() > self.gc_config.max_undo_operations {
            // Keep only the most recent operations
            let keep_count = self.gc_config.max_undo_operations / 2;
            let current_len = operations.len();
            operations.drain(0..(current_len - keep_count));
        }
    }

    /// Perform the actual garbage collection work
    fn perform_garbage_collection(
        &self,
        _catalog: &Arc<dyn CatalogProvider>,
    ) -> Result<GcResult, CatalogError> {
        let start_time = std::time::Instant::now();

        let cleaned_completed_txns;
        let mut cleaned_undo_operations = 0;
        let mut cleaned_local_changes = 0;

        // 1. Clean up old completed transactions
        {
            let mut completed_txns = self.completed_txns.lock().unwrap();
            let cutoff_time = std::time::Instant::now()
                - std::time::Duration::from_millis(self.gc_config.max_txn_age_ms);

            let original_len = completed_txns.len();

            // Remove transactions older than the cutoff time or keep only max_txn_history
            if completed_txns.len() > self.gc_config.max_txn_history {
                let keep_count = self.gc_config.max_txn_history / 2; // Keep half when cleaning
                let current_len = completed_txns.len();
                completed_txns.drain(0..(current_len - keep_count));
            }

            // Remove transactions older than max age
            completed_txns.retain(|info| info.completion_time > cutoff_time);

            cleaned_completed_txns = original_len - completed_txns.len();
        }

        // 2. Clean up undo operations in active transactions
        {
            let active_txns = self.active_txns.lock().unwrap();
            for txn in active_txns.values() {
                let operations_before = {
                    let ops = txn.operations.lock().unwrap();
                    ops.len()
                };

                self.cleanup_transaction_undo_ops(txn);

                let operations_after = {
                    let ops = txn.operations.lock().unwrap();
                    ops.len()
                };

                cleaned_undo_operations += operations_before.saturating_sub(operations_after);
            }
        }

        // 3. Clean up excessive local changes in active transactions
        {
            let active_txns = self.active_txns.lock().unwrap();
            for txn in active_txns.values() {
                let changes_before = {
                    let changes = txn.local_changes.lock().unwrap();
                    changes.len()
                };

                // Limit the number of local changes to prevent memory bloat
                if changes_before > self.gc_config.max_undo_operations {
                    let mut changes = txn.local_changes.lock().unwrap();
                    let keys_to_remove: Vec<_> = changes
                        .keys()
                        .take(changes_before - self.gc_config.max_undo_operations / 2)
                        .cloned()
                        .collect();

                    for key in keys_to_remove {
                        changes.remove(&key);
                    }
                }

                let changes_after = {
                    let changes = txn.local_changes.lock().unwrap();
                    changes.len()
                };

                cleaned_local_changes += changes_before.saturating_sub(changes_after);
            }
        }

        let gc_duration = start_time.elapsed();

        Ok(GcResult {
            cleaned_completed_txns,
            cleaned_undo_operations,
            cleaned_local_changes,
            gc_duration,
        })
    }
}

impl CatalogTxnManager {
    /// Begin a transaction with Arc self
    pub fn begin_transaction_arc(
        self: &Arc<Self>,
    ) -> Result<Arc<CatalogTransaction>, CatalogError> {
        let txn = Arc::new(CatalogTransaction::new(
            IsolationLevel::Serializable,
            self.catalog.clone(),
            Arc::downgrade(self),
        ));

        // Add to active transactions
        self.active_txns
            .lock()
            .unwrap()
            .insert(txn.txn_id(), txn.clone());

        Ok(txn)
    }
}

impl GraphTxnManager for CatalogTxnManager {
    type Error = CatalogError;
    type GraphContext = Arc<dyn CatalogProvider>;
    type Transaction = CatalogTransaction;

    fn begin_transaction(&self) -> Result<Arc<Self::Transaction>, Self::Error> {
        // Create a temporary Arc to get weak reference
        // This is not ideal but necessary due to trait constraints
        let txn = Arc::new(CatalogTransaction::new(
            IsolationLevel::Serializable,
            self.catalog.clone(),
            std::sync::Weak::new(), // Empty weak ref for now
        ));

        // Add to active transactions
        self.active_txns
            .lock()
            .unwrap()
            .insert(txn.txn_id(), txn.clone());

        Ok(txn)
    }

    fn finish_transaction(&self, txn: &Self::Transaction) -> Result<(), Self::Error> {
        let was_committed = txn.commit_ts().is_some();

        if !was_committed {
            // If the transaction was aborted, rollback any operations that might have
            // been partially applied during the transaction
            txn.rollback_operations(self.catalog.as_ref())?;
        }
        // Note: If the transaction was committed, operations were already applied
        // in the commit() method, so no additional work is needed here.

        // Remove from active transactions
        self.active_txns.lock().unwrap().remove(&txn.txn_id());

        // Add to completed transactions history for potential garbage collection
        let completed_info = CompletedTxnInfo {
            txn_id: txn.txn_id(),
            completion_time: std::time::Instant::now(),
            was_committed,
        };

        self.completed_txns.lock().unwrap().push(completed_info);

        Ok(())
    }

    fn garbage_collect(&self, catalog: &Self::GraphContext) -> Result<(), Self::Error> {
        let _gc_result = self.perform_garbage_collection(catalog)?;

        // Update last GC time
        *self.last_gc_time.lock().unwrap() = std::time::Instant::now();

        Ok(())
    }
}

/// Transactional catalog wrapper that delegates operations to underlying catalog
/// while maintaining transaction isolation
#[derive(Debug)]
pub struct TransactionalCatalog {
    /// The underlying catalog
    inner: Arc<dyn CatalogProvider>,
    /// Current transaction (if any)
    current_txn: Option<Arc<CatalogTransaction>>,
}

impl TransactionalCatalog {
    /// Create a new transactional catalog wrapper
    pub fn new(inner: Arc<dyn CatalogProvider>) -> Self {
        Self {
            inner,
            current_txn: None,
        }
    }

    /// Set the current transaction
    pub fn set_transaction(&mut self, txn: Arc<CatalogTransaction>) {
        self.current_txn = Some(txn);
    }

    /// Clear the current transaction
    pub fn clear_transaction(&mut self) {
        self.current_txn = None;
    }

    /// Get the current transaction
    pub fn current_transaction(&self) -> Option<Arc<CatalogTransaction>> {
        self.current_txn.clone()
    }

    /// Create a schema within a transaction
    pub fn create_schema(&self, name: String, parent_dir: String) -> CatalogResult<()> {
        if let Some(txn) = &self.current_txn {
            // First, check if the schema already exists
            let full_path = format!("{}/{}", parent_dir, name);

            // Check in local changes first
            let local_changes = txn.get_local_changes();
            if local_changes.contains_key(&full_path) {
                return Err(CatalogError::General(format!(
                    "Schema '{}' already exists",
                    name
                )));
            }

            // Check in the underlying catalog and record the read
            if let Ok(root) = self.inner.get_root() {
                if let Some(dir) = root.as_directory() {
                    if let Ok(Some(child)) = dir.get_child(&parent_dir) {
                        if let Some(parent_directory) = child.as_directory() {
                            // Record the read operation
                            txn.add_to_read_set(
                                format!("{}/{}", parent_dir, name),
                                global_timestamp_generator().next(),
                            );

                            if let Ok(Some(_existing)) = parent_directory.get_child(&name) {
                                return Err(CatalogError::General(format!(
                                    "Schema '{}' already exists",
                                    name
                                )));
                            }
                        }
                    }
                }
            }

            // Create the schema object
            use crate::memory::schema::MemorySchemaCatalog;
            let schema = Arc::new(MemorySchemaCatalog::new(None));
            let schema_ref: SchemaRef = schema;

            // Add to local changes (this is the transactional view)
            txn.add_local_change(
                full_path.clone(),
                DirectoryOrSchema::Schema(schema_ref.clone()),
            );

            // Record the operation in the transaction for undo/redo
            let create_op = CatalogOp::CreateSchema {
                name: name.clone(),
                parent_dir: parent_dir.clone(),
            };
            txn.add_operation(create_op.clone());

            // Record the write operation in the write set
            txn.add_to_write_set(full_path, create_op);

            Ok(())
        } else {
            Err(CatalogError::General("No active transaction".to_string()))
        }
    }

    /// Drop a schema within a transaction
    pub fn drop_schema(&self, name: String, parent_dir: String) -> CatalogResult<SchemaRef> {
        if let Some(txn) = &self.current_txn {
            let full_path = format!("{}/{}", parent_dir, name);

            // First, check if the schema exists (considering local changes)
            let local_changes = txn.get_local_changes();

            let schema_to_drop = if let Some(local_schema) = local_changes.get(&full_path) {
                // Schema exists in local changes
                if let Some(schema_ref) = local_schema.as_schema() {
                    schema_ref.clone()
                } else {
                    return Err(CatalogError::General(format!("'{}' is not a schema", name)));
                }
            } else {
                // Look in the underlying catalog and record the read
                if let Ok(root) = self.inner.get_root() {
                    if let Some(dir) = root.as_directory() {
                        if let Ok(Some(child)) = dir.get_child(&parent_dir) {
                            if let Some(parent_directory) = child.as_directory() {
                                // Record the read operation
                                txn.add_to_read_set(
                                    format!("{}/{}", parent_dir, name),
                                    global_timestamp_generator().next(),
                                );

                                if let Ok(Some(existing)) = parent_directory.get_child(&name) {
                                    if let Some(schema_ref) = existing.as_schema() {
                                        schema_ref.clone()
                                    } else {
                                        return Err(CatalogError::General(format!(
                                            "'{}' is not a schema",
                                            name
                                        )));
                                    }
                                } else {
                                    return Err(CatalogError::General(format!(
                                        "Schema '{}' not found",
                                        name
                                    )));
                                }
                            } else {
                                return Err(CatalogError::General(format!(
                                    "Parent directory '{}' not found",
                                    parent_dir
                                )));
                            }
                        } else {
                            return Err(CatalogError::General(format!(
                                "Parent directory '{}' not found",
                                parent_dir
                            )));
                        }
                    } else {
                        return Err(CatalogError::General("Root is not a directory".to_string()));
                    }
                } else {
                    return Err(CatalogError::General(
                        "Cannot access catalog root".to_string(),
                    ));
                }
            };

            // Mark as deleted in local changes (by removing from the map)
            let drop_marker = format!("{}/DROP", full_path);
            txn.add_local_change(
                drop_marker.clone(),
                DirectoryOrSchema::Directory(Arc::new(
                    crate::memory::directory::MemoryDirectoryCatalog::new(None),
                )),
            );

            // Record the operation in the transaction for rollback
            let drop_op = CatalogOp::DropSchema {
                name: name.clone(),
                parent_dir: parent_dir.clone(),
                schema: schema_to_drop.clone(),
            };
            txn.add_operation(drop_op.clone());

            // Record the write operation in the write set
            txn.add_to_write_set(full_path, drop_op);

            Ok(schema_to_drop)
        } else {
            Err(CatalogError::General("No active transaction".to_string()))
        }
    }

    /// Create a graph within a transaction
    pub fn create_graph(&self, schema_name: String, graph_name: String) -> CatalogResult<()> {
        if let Some(txn) = &self.current_txn {
            let schema_path = format!("root/{}", schema_name);
            let graph_path = format!("{}/{}", schema_name, graph_name);

            // Find the schema where we want to create the graph
            let local_changes = txn.get_local_changes();

            let target_schema = if let Some(local_schema) = local_changes.get(&schema_path) {
                // Schema exists in local changes
                if let Some(schema_ref) = local_schema.as_schema() {
                    schema_ref.clone()
                } else {
                    return Err(CatalogError::General(format!(
                        "'{}' is not a schema",
                        schema_name
                    )));
                }
            } else {
                // Look in the underlying catalog
                if let Ok(root) = self.inner.get_root() {
                    if let Some(dir) = root.as_directory() {
                        if let Ok(Some(schema_entry)) = dir.get_child(&schema_name) {
                            if let Some(schema_ref) = schema_entry.as_schema() {
                                schema_ref.clone()
                            } else {
                                return Err(CatalogError::General(format!(
                                    "'{}' is not a schema",
                                    schema_name
                                )));
                            }
                        } else {
                            return Err(CatalogError::General(format!(
                                "Schema '{}' not found",
                                schema_name
                            )));
                        }
                    } else {
                        return Err(CatalogError::General("Root is not a directory".to_string()));
                    }
                } else {
                    return Err(CatalogError::General(
                        "Cannot access catalog root".to_string(),
                    ));
                }
            };

            // Check if graph already exists
            if let Ok(Some(_existing_graph)) = target_schema.get_graph(&graph_name) {
                return Err(CatalogError::General(format!(
                    "Graph '{}' already exists in schema '{}'",
                    graph_name, schema_name
                )));
            }

            // Create the graph object
            let _graph_ref = create_placeholder_graph();

            // Store the graph creation in local changes for transactional view
            txn.add_local_change(
                graph_path,
                DirectoryOrSchema::Directory(Arc::new(
                    crate::memory::directory::MemoryDirectoryCatalog::new(None),
                )),
            );

            // Record the operation in the transaction
            txn.add_operation(CatalogOp::CreateGraph {
                schema_name: schema_name.clone(),
                graph_name: graph_name.clone(),
            });

            Ok(())
        } else {
            Err(CatalogError::General("No active transaction".to_string()))
        }
    }

    /// Drop a graph within a transaction  
    pub fn drop_graph(&self, schema_name: String, graph_name: String) -> CatalogResult<GraphRef> {
        if let Some(txn) = &self.current_txn {
            let schema_path = format!("root/{}", schema_name);
            let graph_path = format!("{}/{}", schema_name, graph_name);

            // Find the schema containing the graph
            let local_changes = txn.get_local_changes();

            let target_schema = if let Some(local_schema) = local_changes.get(&schema_path) {
                // Schema exists in local changes
                if let Some(schema_ref) = local_schema.as_schema() {
                    schema_ref.clone()
                } else {
                    return Err(CatalogError::General(format!(
                        "'{}' is not a schema",
                        schema_name
                    )));
                }
            } else {
                // Look in the underlying catalog
                if let Ok(root) = self.inner.get_root() {
                    if let Some(dir) = root.as_directory() {
                        if let Ok(Some(schema_entry)) = dir.get_child(&schema_name) {
                            if let Some(schema_ref) = schema_entry.as_schema() {
                                schema_ref.clone()
                            } else {
                                return Err(CatalogError::General(format!(
                                    "'{}' is not a schema",
                                    schema_name
                                )));
                            }
                        } else {
                            return Err(CatalogError::General(format!(
                                "Schema '{}' not found",
                                schema_name
                            )));
                        }
                    } else {
                        return Err(CatalogError::General("Root is not a directory".to_string()));
                    }
                } else {
                    return Err(CatalogError::General(
                        "Cannot access catalog root".to_string(),
                    ));
                }
            };

            // Check if the graph was created in this transaction
            let graph_to_drop = if local_changes.contains_key(&graph_path) {
                // Graph was created in this transaction, use a placeholder
                create_placeholder_graph()
            } else if let Ok(Some(existing_graph)) = target_schema.get_graph(&graph_name) {
                // Graph exists in the underlying catalog
                existing_graph
            } else {
                return Err(CatalogError::General(format!(
                    "Graph '{}' not found in schema '{}'",
                    graph_name, schema_name
                )));
            };

            // Mark as deleted in local changes
            txn.add_local_change(
                format!("{}/DROP", graph_path),
                DirectoryOrSchema::Directory(Arc::new(
                    crate::memory::directory::MemoryDirectoryCatalog::new(None),
                )),
            );

            // Record the operation in the transaction for rollback
            txn.add_operation(CatalogOp::DropGraph {
                schema_name: schema_name.clone(),
                graph_name: graph_name.clone(),
                graph: graph_to_drop.clone(),
            });

            Ok(graph_to_drop)
        } else {
            Err(CatalogError::General("No active transaction".to_string()))
        }
    }

    /// Apply all transaction operations to the underlying catalog
    pub fn apply_transaction(&self, txn: &CatalogTransaction) -> CatalogResult<()> {
        let operations = txn.operations.lock().unwrap();

        // Apply all operations to the underlying catalog in order
        for op in operations.iter() {
            match op {
                CatalogOp::CreateSchema { name, parent_dir } => {
                    self.apply_create_schema(name, parent_dir)?;
                }
                CatalogOp::DropSchema {
                    name, parent_dir, ..
                } => {
                    self.apply_drop_schema(name, parent_dir)?;
                }
                CatalogOp::CreateGraph {
                    schema_name,
                    graph_name,
                } => {
                    self.apply_create_graph(schema_name, graph_name)?;
                }
                CatalogOp::DropGraph {
                    schema_name,
                    graph_name,
                    ..
                } => {
                    self.apply_drop_graph(schema_name, graph_name)?;
                }
            }
        }

        Ok(())
    }

    /// Apply create schema operation to the underlying catalog
    fn apply_create_schema(&self, name: &str, parent_dir: &str) -> CatalogResult<()> {
        // Get the root directory
        let root = self.inner.get_root()?;

        if let Some(root_dir) = root.as_directory() {
            // If parent_dir is "root", use the root directory itself
            let parent_directory = if parent_dir == "root" {
                Some(root_dir.clone())
            } else {
                // Find the parent directory
                root_dir
                    .get_child(parent_dir)?
                    .and_then(|child| child.into_directory())
            };

            if let Some(parent_dir_ref) = parent_directory {
                // Check if we have a memory directory catalog to modify
                use crate::memory::directory::MemoryDirectoryCatalog;
                if let Some(memory_dir) = parent_dir_ref
                    .as_any()
                    .downcast_ref::<MemoryDirectoryCatalog>()
                {
                    // Create the new schema
                    use crate::memory::schema::MemorySchemaCatalog;
                    let new_schema = Arc::new(MemorySchemaCatalog::new(Some(Arc::downgrade(
                        &parent_dir_ref,
                    ))));
                    let schema_ref: SchemaRef = new_schema;

                    // Add the schema to the parent directory
                    if !memory_dir
                        .add_child(name.to_string(), DirectoryOrSchema::Schema(schema_ref))
                    {
                        return Err(CatalogError::General(format!(
                            "Failed to create schema '{}': already exists",
                            name
                        )));
                    }

                    return Ok(());
                }
            }
        }

        Err(CatalogError::General(format!(
            "Cannot apply CreateSchema operation for '{}/{}'",
            parent_dir, name
        )))
    }

    /// Apply drop schema operation to the underlying catalog
    fn apply_drop_schema(&self, name: &str, parent_dir: &str) -> CatalogResult<()> {
        // Get the root directory
        let root = self.inner.get_root()?;

        if let Some(root_dir) = root.as_directory() {
            // If parent_dir is "root", use the root directory itself
            let parent_directory = if parent_dir == "root" {
                Some(root_dir.clone())
            } else {
                // Find the parent directory
                root_dir
                    .get_child(parent_dir)?
                    .and_then(|child| child.into_directory())
            };

            if let Some(parent_dir_ref) = parent_directory {
                // Check if we have a memory directory catalog to modify
                use crate::memory::directory::MemoryDirectoryCatalog;
                if let Some(memory_dir) = parent_dir_ref
                    .as_any()
                    .downcast_ref::<MemoryDirectoryCatalog>()
                {
                    // Remove the schema from the parent directory
                    if !memory_dir.remove_child(name) {
                        return Err(CatalogError::General(format!(
                            "Failed to drop schema '{}': not found",
                            name
                        )));
                    }

                    return Ok(());
                }
            }
        }

        Err(CatalogError::General(format!(
            "Cannot apply DropSchema operation for '{}/{}'",
            parent_dir, name
        )))
    }

    /// Apply create graph operation to the underlying catalog
    fn apply_create_graph(&self, schema_name: &str, graph_name: &str) -> CatalogResult<()> {
        // Get the root directory
        let root = self.inner.get_root()?;

        if let Some(root_dir) = root.as_directory() {
            // Find the schema in the root directory
            if let Some(schema) = root_dir
                .get_child(schema_name)?
                .and_then(|child| child.into_schema())
            {
                // Check if we have a memory schema catalog to modify
                use crate::memory::schema::MemorySchemaCatalog;
                if let Some(memory_schema) = schema.as_any().downcast_ref::<MemorySchemaCatalog>() {
                    // Create the graph
                    let new_graph = create_placeholder_graph();

                    // Add the graph to the schema
                    if !memory_schema.add_graph(graph_name.to_string(), new_graph) {
                        return Err(CatalogError::General(format!(
                            "Failed to create graph '{}': already exists",
                            graph_name
                        )));
                    }

                    return Ok(());
                }
            }
        }

        Err(CatalogError::General(format!(
            "Cannot apply CreateGraph operation for '{}/{}'",
            schema_name, graph_name
        )))
    }

    /// Apply drop graph operation to the underlying catalog
    fn apply_drop_graph(&self, schema_name: &str, graph_name: &str) -> CatalogResult<()> {
        // Get the root directory
        let root = self.inner.get_root()?;

        if let Some(root_dir) = root.as_directory() {
            // Find the schema in the root directory
            if let Some(schema) = root_dir
                .get_child(schema_name)?
                .and_then(|child| child.into_schema())
            {
                // Check if we have a memory schema catalog to modify
                use crate::memory::schema::MemorySchemaCatalog;
                if let Some(memory_schema) = schema.as_any().downcast_ref::<MemorySchemaCatalog>() {
                    // Remove the graph from the schema
                    if !memory_schema.remove_graph(graph_name) {
                        return Err(CatalogError::General(format!(
                            "Failed to drop graph '{}': not found",
                            graph_name
                        )));
                    }

                    return Ok(());
                }
            }
        }

        Err(CatalogError::General(format!(
            "Cannot apply DropGraph operation for '{}/{}'",
            schema_name, graph_name
        )))
    }
}

impl CatalogProvider for TransactionalCatalog {
    fn get_root(&self) -> CatalogResult<DirectoryOrSchema> {
        // Check for local changes first if in a transaction
        if let Some(txn) = &self.current_txn {
            let changes = txn.get_local_changes();
            if let Some(root) = changes.get("root") {
                return Ok(root.clone());
            }
        }

        // Delegate to underlying catalog
        self.inner.get_root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryCatalog;
    use crate::memory::directory::MemoryDirectoryCatalog;

    /// Mock catalog for testing
    fn create_mock_catalog() -> Arc<dyn CatalogProvider> {
        let root_dir = Arc::new(MemoryDirectoryCatalog::new(None));
        let root = DirectoryOrSchema::Directory(root_dir);
        Arc::new(MemoryCatalog::new(root))
    }

    #[test]
    fn test_catalog_transaction_creation() {
        let mock_catalog = create_mock_catalog();
        let txn = CatalogTransaction::new(
            IsolationLevel::Serializable,
            mock_catalog,
            std::sync::Weak::new(),
        );

        assert_eq!(txn.isolation_level(), &IsolationLevel::Serializable);
        assert!(txn.commit_ts().is_none());
        assert!(txn.txn_id() > Timestamp(0));
        assert!(txn.start_ts() > Timestamp(0));
    }

    #[test]
    fn test_catalog_transaction_operations() {
        let mock_catalog = create_mock_catalog();
        let txn = CatalogTransaction::new(
            IsolationLevel::Serializable,
            mock_catalog.clone(),
            std::sync::Weak::new(),
        );

        // Test adding operations
        let create_op = CatalogOp::CreateSchema {
            name: "test_schema".to_string(),
            parent_dir: "root".to_string(),
        };

        txn.add_operation(create_op);

        // Test local changes
        let root = mock_catalog.get_root().unwrap();
        txn.add_local_change("test_key".to_string(), root);

        let changes = txn.get_local_changes();
        assert!(changes.contains_key("test_key"));
    }

    #[test]
    fn test_catalog_transaction_manager() {
        let mock_catalog = create_mock_catalog();
        let txn_manager = CatalogTxnManager::new(mock_catalog.clone());

        // Test begin transaction
        let txn = txn_manager.begin_transaction().unwrap();
        assert_eq!(txn.isolation_level(), &IsolationLevel::Serializable);

        // Test finish transaction
        let result = txn_manager.finish_transaction(&txn);
        assert!(result.is_ok());

        // Test garbage collection
        let gc_result = txn_manager.garbage_collect(&mock_catalog);
        assert!(gc_result.is_ok());
    }

    #[test]
    fn test_transactional_catalog() {
        let mock_catalog = create_mock_catalog();
        let mut txn_catalog = TransactionalCatalog::new(mock_catalog.clone());

        // Test without transaction
        let root = txn_catalog.get_root().unwrap();
        assert!(root.is_directory());

        // Test with transaction
        let txn = Arc::new(CatalogTransaction::new(
            IsolationLevel::Serializable,
            mock_catalog,
            std::sync::Weak::new(),
        ));
        txn_catalog.set_transaction(txn.clone());

        assert!(txn_catalog.current_transaction().is_some());

        // Clear transaction
        txn_catalog.clear_transaction();
        assert!(txn_catalog.current_transaction().is_none());
    }

    #[test]
    fn test_catalog_transaction_commit_abort() {
        let mock_catalog = create_mock_catalog();
        let txn = CatalogTransaction::new(
            IsolationLevel::Serializable,
            mock_catalog,
            std::sync::Weak::new(),
        );

        // Test commit
        let commit_result = txn.commit();
        assert!(commit_result.is_ok());
        let commit_ts = commit_result.unwrap();
        assert!(commit_ts > Timestamp(0));

        // Test abort
        let abort_result = txn.abort();
        assert!(abort_result.is_ok());

        // After abort, local changes should be cleared
        let changes = txn.get_local_changes();
        assert!(changes.is_empty());
    }

    #[test]
    fn test_catalog_rollback_operations() {
        let mock_catalog = create_mock_catalog();
        let txn = CatalogTransaction::new(
            IsolationLevel::Serializable,
            mock_catalog.clone(),
            std::sync::Weak::new(),
        );

        // Add some operations
        let create_schema_op = CatalogOp::CreateSchema {
            name: "test_schema".to_string(),
            parent_dir: "root".to_string(),
        };

        let create_graph_op = CatalogOp::CreateGraph {
            schema_name: "test_schema".to_string(),
            graph_name: "test_graph".to_string(),
        };

        txn.add_operation(create_schema_op);
        txn.add_operation(create_graph_op);

        // Test rollback - should not fail
        let rollback_result = txn.rollback_operations(mock_catalog.as_ref());
        assert!(rollback_result.is_ok());
    }

    #[test]
    fn test_transactional_catalog_ddl_operations() {
        let mock_catalog = create_mock_catalog();
        let mut txn_catalog = TransactionalCatalog::new(mock_catalog.clone());

        // Start a transaction
        let txn = Arc::new(CatalogTransaction::new(
            IsolationLevel::Serializable,
            mock_catalog,
            std::sync::Weak::new(),
        ));
        txn_catalog.set_transaction(txn.clone());

        // Test create schema
        let create_result =
            txn_catalog.create_schema("test_schema".to_string(), "root".to_string());
        assert!(create_result.is_ok());

        // Test create graph in the newly created schema
        let create_graph_result =
            txn_catalog.create_graph("test_schema".to_string(), "test_graph".to_string());
        assert!(create_graph_result.is_ok());

        // Test drop graph
        let drop_graph_result =
            txn_catalog.drop_graph("test_schema".to_string(), "test_graph".to_string());
        assert!(drop_graph_result.is_ok());

        // Test drop schema after graph operations
        let drop_result = txn_catalog.drop_schema("test_schema".to_string(), "root".to_string());
        assert!(drop_result.is_ok());

        // Test without transaction (should fail)
        txn_catalog.clear_transaction();
        let no_txn_result =
            txn_catalog.create_schema("fail_schema".to_string(), "root".to_string());
        assert!(no_txn_result.is_err());
    }

    #[test]
    fn test_transaction_commit_with_operations() {
        let mock_catalog = create_mock_catalog();
        let txn = CatalogTransaction::new(
            IsolationLevel::Serializable,
            mock_catalog,
            std::sync::Weak::new(),
        );

        // Add operations
        txn.add_operation(CatalogOp::CreateSchema {
            name: "test_schema".to_string(),
            parent_dir: "root".to_string(),
        });

        // Check initial state
        assert!(txn.commit_ts().is_none());

        // Commit transaction
        let commit_result = txn.commit();
        assert!(commit_result.is_ok());

        // Check commit timestamp is set
        let commit_ts = commit_result.unwrap();
        assert!(commit_ts > Timestamp(0));
        assert_eq!(txn.commit_ts(), Some(commit_ts));
    }

    #[test]
    fn test_apply_transaction_operations() {
        let mock_catalog = create_mock_catalog();
        let txn_catalog = TransactionalCatalog::new(mock_catalog.clone());
        let txn = CatalogTransaction::new(
            IsolationLevel::Serializable,
            mock_catalog,
            std::sync::Weak::new(),
        );

        // Add various operations
        txn.add_operation(CatalogOp::CreateSchema {
            name: "schema1".to_string(),
            parent_dir: "root".to_string(),
        });

        txn.add_operation(CatalogOp::CreateGraph {
            schema_name: "schema1".to_string(),
            graph_name: "graph1".to_string(),
        });

        // Apply transaction should not fail
        let apply_result = txn_catalog.apply_transaction(&txn);
        assert!(apply_result.is_ok());
    }

    #[test]
    fn test_garbage_collection_basics() {
        let mock_catalog = create_mock_catalog();
        let txn_manager = CatalogTxnManager::new(mock_catalog.clone());

        // Check initial stats
        let initial_stats = txn_manager.gc_stats();
        assert_eq!(initial_stats.active_txn_count, 0);
        assert_eq!(initial_stats.completed_txn_count, 0);

        // Create some transactions
        let txn1 = txn_manager.begin_transaction().unwrap();
        let txn2 = txn_manager.begin_transaction().unwrap();

        // Check active transaction count
        let stats_with_active = txn_manager.gc_stats();
        assert_eq!(stats_with_active.active_txn_count, 2);

        // Commit one transaction
        let _commit_result = txn1.commit();
        txn_manager.finish_transaction(&txn1).unwrap();

        // Abort the other transaction
        let _abort_result = txn2.abort();
        txn_manager.finish_transaction(&txn2).unwrap();

        // Check completed transaction count
        let final_stats = txn_manager.gc_stats();
        assert_eq!(final_stats.active_txn_count, 0);
        assert_eq!(final_stats.completed_txn_count, 2);

        // Run garbage collection
        let gc_result = txn_manager.manual_gc(&mock_catalog);
        assert!(gc_result.is_ok());
    }

    #[test]
    fn test_garbage_collection_with_custom_config() {
        let mock_catalog = create_mock_catalog();

        // Create a custom GC config with very low limits
        let gc_config = GcConfig {
            max_txn_history: 2,
            max_txn_age_ms: 100, // Very short age
            max_undo_operations: 5,
        };

        let txn_manager = CatalogTxnManager::new_with_gc_config(mock_catalog.clone(), gc_config);

        // Create and finish several transactions
        for i in 0..5 {
            let txn = txn_manager.begin_transaction().unwrap();

            // Add some operations to the transaction
            txn.add_operation(CatalogOp::CreateSchema {
                name: format!("schema_{}", i),
                parent_dir: "root".to_string(),
            });

            // Commit the transaction
            let _commit_result = txn.commit();
            txn_manager.finish_transaction(&txn).unwrap();
        }

        // Check that we have many completed transactions
        let stats_before_gc = txn_manager.gc_stats();
        assert_eq!(stats_before_gc.completed_txn_count, 5);

        // Wait a bit to exceed the age limit
        std::thread::sleep(std::time::Duration::from_millis(150));

        // Run garbage collection
        let gc_result = txn_manager.manual_gc(&mock_catalog).unwrap();

        // Should have cleaned up some transactions
        assert!(gc_result.cleaned_completed_txns > 0);

        // Check that completed transaction count is reduced
        let stats_after_gc = txn_manager.gc_stats();
        assert!(stats_after_gc.completed_txn_count < stats_before_gc.completed_txn_count);
    }

    #[test]
    fn test_transaction_undo_operations_cleanup() {
        let mock_catalog = create_mock_catalog();
        let gc_config = GcConfig {
            max_txn_history: 100,
            max_txn_age_ms: 300_000,
            max_undo_operations: 3, // Very low limit
        };

        let txn_manager = CatalogTxnManager::new_with_gc_config(mock_catalog.clone(), gc_config);
        let txn = txn_manager.begin_transaction().unwrap();

        // Add many operations to exceed the limit
        for i in 0..10 {
            txn.add_operation(CatalogOp::CreateSchema {
                name: format!("schema_{}", i),
                parent_dir: "root".to_string(),
            });
        }

        // Check initial operation count
        let initial_ops_count = {
            let ops = txn.operations.lock().unwrap();
            ops.len()
        };
        assert_eq!(initial_ops_count, 10);

        // Run garbage collection
        let gc_result = txn_manager.manual_gc(&mock_catalog).unwrap();

        // Should have cleaned up some undo operations
        assert!(gc_result.cleaned_undo_operations > 0);

        // Check that operation count is reduced
        let final_ops_count = {
            let ops = txn.operations.lock().unwrap();
            ops.len()
        };
        assert!(final_ops_count < initial_ops_count);

        // Clean up transaction
        txn_manager.finish_transaction(&txn).unwrap();
    }

    #[test]
    fn test_gc_result_fields() {
        let mock_catalog = create_mock_catalog();
        let txn_manager = CatalogTxnManager::new(mock_catalog.clone());

        // Create and finish a transaction
        let txn = txn_manager.begin_transaction().unwrap();
        txn.add_operation(CatalogOp::CreateSchema {
            name: "test_schema".to_string(),
            parent_dir: "root".to_string(),
        });
        let _commit_result = txn.commit();
        txn_manager.finish_transaction(&txn).unwrap();

        // Run GC and check result fields
        let gc_result = txn_manager.manual_gc(&mock_catalog).unwrap();

        // All fields should be valid (no need to check non-negative for usize)
        assert!(gc_result.cleaned_completed_txns < 1000); // Reasonable upper bound
        assert!(gc_result.cleaned_undo_operations < 1000); // Reasonable upper bound
        assert!(gc_result.cleaned_local_changes < 1000); // Reasonable upper bound

        // Duration should be measurable
        assert!(gc_result.gc_duration.as_nanos() > 0);
    }
}
