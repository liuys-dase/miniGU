use std::path::PathBuf;

const DEFAULT_WAL_THRESHOLD: usize = 1000;

/// Global database configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Number of concurrent threads
    pub num_threads: usize,

    /// Storage layer configuration
    pub storage: StorageConfig,

    /// Execution layer configuration
    pub execution: ExecutionConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            num_threads: 1,
            storage: StorageConfig::default(),
            execution: ExecutionConfig::default(),
        }
    }
}

/// Storage layer configuration
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    // Path of `.minigu` file
    /// Path to the database file. If None, the database is in-memory only.
    pub db_path: Option<PathBuf>,

    /// Checkpoint configuration
    pub checkpoint: CheckpointConfig,
}

impl StorageConfig {
    fn default() -> Self {
        Self {
            db_path: None,
            checkpoint: CheckpointConfig::default(),
        }
    }
}

/// Checkpoint configuration
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Number of WAL entries before triggering auto checkpoint.
    /// 0 means disabled.
    pub wal_threshold: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            wal_threshold: DEFAULT_WAL_THRESHOLD,
        }
    }
}

/// Execution layer configuration
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Batch size for vertex scanning
    pub vertex_scan_batch_size: usize,

    /// Batch size for edge expansion
    pub expand_batch_size: usize,

    /// Chunk size for sorting output
    pub sort_chunk_size: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            vertex_scan_batch_size: 1024,
            expand_batch_size: 64,
            sort_chunk_size: 2048,
        }
    }
}

#[cfg(feature = "test-utils")]
pub mod test_utils {
    use tempfile::TempDir;

    use super::*;

    pub struct TestConfig {
        pub config: DatabaseConfig,
        pub _temp_dirs: Vec<TempDir>, // Keep TempDirs alive
    }

    pub fn gen_test_config() -> TestConfig {
        let checkpoint_dir = TempDir::new().expect("failed to create temp checkpoint dir");

        let checkpoint_config = CheckpointConfig {
            wal_threshold: 1000,
        };

        let storage_config = StorageConfig {
            db_path: None,
            checkpoint: checkpoint_config,
        };

        let config = DatabaseConfig {
            num_threads: 1,
            storage: storage_config,
            execution: ExecutionConfig::default(),
        };

        TestConfig {
            config,
            _temp_dirs: vec![checkpoint_dir],
        }
    }
}
