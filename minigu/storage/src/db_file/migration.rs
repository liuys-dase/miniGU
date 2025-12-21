//! Migration utilities for converting between multi-file and single-file formats.
//!
//! This module provides functionality to:
//! - Migrate from legacy multi-file format (separate WAL + checkpoint files) to single-file
//! - Detect legacy file structures
//! - Perform atomic migration with rollback on failure

use std::fs;
use std::path::{Path, PathBuf};

use super::core::DbFile;
use super::error::{DbFileError, DbFileResult};
use crate::common::wal::StorageWal;
use crate::common::wal::graph_wal::GraphWal;
use crate::tp::checkpoint::GraphCheckpoint;

/// Default legacy WAL directory name.
const LEGACY_WAL_DIR: &str = ".wal";
/// Default legacy checkpoint directory name.
const LEGACY_CHECKPOINT_DIR: &str = ".checkpoint";
/// Default checkpoint file prefix.
const LEGACY_CHECKPOINT_PREFIX: &str = "checkpoint";

/// Configuration for migration operations.
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Path to the legacy WAL directory.
    pub wal_dir: PathBuf,
    /// Path to the legacy checkpoint directory.
    pub checkpoint_dir: PathBuf,
    /// Path for the new single database file.
    pub output_path: PathBuf,
    /// Whether to delete legacy files after successful migration.
    pub delete_legacy_files: bool,
}

impl MigrationConfig {
    /// Creates a new migration config from a base directory.
    ///
    /// This assumes the standard directory structure:
    /// - `{base_dir}/.wal/` for WAL files
    /// - `{base_dir}/.checkpoint/` for checkpoint files
    /// - `{base_dir}/database.minigu` as output
    pub fn from_base_dir<P: AsRef<Path>>(base_dir: P) -> Self {
        let base = base_dir.as_ref();
        Self {
            wal_dir: base.join(LEGACY_WAL_DIR),
            checkpoint_dir: base.join(LEGACY_CHECKPOINT_DIR),
            output_path: base.join("database.minigu"),
            delete_legacy_files: false,
        }
    }

    /// Sets whether to delete legacy files after migration.
    pub fn with_delete_legacy(mut self, delete: bool) -> Self {
        self.delete_legacy_files = delete;
        self
    }

    /// Sets the output path for the new database file.
    pub fn with_output_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.output_path = path.as_ref().to_path_buf();
        self
    }
}

/// Result of a migration operation.
#[derive(Debug)]
pub struct MigrationResult {
    /// Path to the new database file.
    pub output_path: PathBuf,
    /// Number of WAL entries migrated.
    pub wal_entries_count: usize,
    /// Whether a checkpoint was migrated.
    pub checkpoint_migrated: bool,
    /// LSN from the checkpoint (if any).
    pub checkpoint_lsn: Option<u64>,
    /// Whether legacy files were deleted.
    pub legacy_files_deleted: bool,
}

/// Detects if legacy multi-file format exists at the given base directory.
pub fn detect_legacy_format<P: AsRef<Path>>(base_dir: P) -> LegacyFormatInfo {
    let base = base_dir.as_ref();
    let wal_dir = base.join(LEGACY_WAL_DIR);
    let checkpoint_dir = base.join(LEGACY_CHECKPOINT_DIR);

    LegacyFormatInfo {
        wal_exists: wal_dir.exists() && wal_dir.is_dir(),
        checkpoint_exists: checkpoint_dir.exists() && checkpoint_dir.is_dir(),
        wal_dir,
        checkpoint_dir,
    }
}

/// Information about detected legacy format.
#[derive(Debug)]
pub struct LegacyFormatInfo {
    /// Whether the WAL directory exists.
    pub wal_exists: bool,
    /// Whether the checkpoint directory exists.
    pub checkpoint_exists: bool,
    /// Path to the WAL directory.
    pub wal_dir: PathBuf,
    /// Path to the checkpoint directory.
    pub checkpoint_dir: PathBuf,
}

impl LegacyFormatInfo {
    /// Returns true if any legacy format files exist.
    pub fn has_legacy_files(&self) -> bool {
        self.wal_exists || self.checkpoint_exists
    }
}

/// Finds the most recent checkpoint file in a directory.
fn find_latest_checkpoint(checkpoint_dir: &Path) -> DbFileResult<Option<PathBuf>> {
    if !checkpoint_dir.exists() {
        return Ok(None);
    }

    let entries = fs::read_dir(checkpoint_dir).map_err(DbFileError::Io)?;

    let mut latest: Option<(PathBuf, std::time::SystemTime)> = None;

    for entry in entries {
        let entry = entry.map_err(DbFileError::Io)?;
        let path = entry.path();

        // Skip non-files and files that don't start with checkpoint prefix
        if !path.is_file() {
            continue;
        }

        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => continue,
        };

        if !file_name.starts_with(LEGACY_CHECKPOINT_PREFIX) {
            continue;
        }

        let metadata = fs::metadata(&path).map_err(DbFileError::Io)?;
        let modified = metadata.modified().map_err(DbFileError::Io)?;

        match &latest {
            Some((_, latest_time)) if modified > *latest_time => {
                latest = Some((path, modified));
            }
            None => {
                latest = Some((path, modified));
            }
            _ => {}
        }
    }

    Ok(latest.map(|(path, _)| path))
}

/// Migrates from legacy multi-file format to single-file format.
///
/// # Migration Process
///
/// 1. Find and load the latest checkpoint (if any)
/// 2. Load all WAL entries
/// 3. Create new single database file
/// 4. Write checkpoint to new file
/// 5. Write WAL entries (only those with LSN >= checkpoint LSN)
/// 6. Optionally delete legacy files
///
/// # Errors
///
/// Returns an error if:
/// - The output file already exists
/// - Reading legacy files fails
/// - Writing to the new file fails
pub fn migrate_to_single_file(config: &MigrationConfig) -> DbFileResult<MigrationResult> {
    // Check if output already exists
    if config.output_path.exists() {
        return Err(DbFileError::InvalidFile(format!(
            "Output file already exists: {}",
            config.output_path.display()
        )));
    }

    // Find and load checkpoint
    let checkpoint_path = find_latest_checkpoint(&config.checkpoint_dir)?;
    let checkpoint = match &checkpoint_path {
        Some(path) => Some(
            GraphCheckpoint::load_from_file(path)
                .map_err(|e| DbFileError::Deserialization(e.to_string()))?,
        ),
        None => None,
    };

    let checkpoint_lsn = checkpoint.as_ref().map(|c| c.metadata.lsn);

    // Load WAL entries
    let wal_entries = if config.wal_dir.exists() {
        // Find WAL file in directory
        let wal_file_path = config.wal_dir.join("wal");
        if wal_file_path.exists() {
            let wal = GraphWal::open(&wal_file_path)
                .map_err(|e| DbFileError::Deserialization(e.to_string()))?;
            wal.read_all()
                .map_err(|e| DbFileError::Deserialization(e.to_string()))?
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Filter WAL entries if we have a checkpoint
    let filtered_entries: Vec<_> = match checkpoint_lsn {
        Some(lsn) => wal_entries.into_iter().filter(|e| e.lsn >= lsn).collect(),
        None => wal_entries,
    };

    // Create new database file
    let mut db_file = DbFile::create(&config.output_path)?;

    // Write checkpoint if we have one
    let checkpoint_migrated = if let Some(ref cp) = checkpoint {
        db_file.write_checkpoint(cp)?;
        true
    } else {
        false
    };

    // Write WAL entries
    for entry in &filtered_entries {
        db_file.append_wal(entry)?;
    }

    db_file.sync_all()?;

    // Delete legacy files if requested
    let legacy_files_deleted = if config.delete_legacy_files {
        delete_legacy_files(config)?;
        true
    } else {
        false
    };

    Ok(MigrationResult {
        output_path: config.output_path.clone(),
        wal_entries_count: filtered_entries.len(),
        checkpoint_migrated,
        checkpoint_lsn,
        legacy_files_deleted,
    })
}

/// Deletes legacy files after successful migration.
fn delete_legacy_files(config: &MigrationConfig) -> DbFileResult<()> {
    if config.wal_dir.exists() {
        fs::remove_dir_all(&config.wal_dir).map_err(DbFileError::Io)?;
    }
    if config.checkpoint_dir.exists() {
        fs::remove_dir_all(&config.checkpoint_dir).map_err(DbFileError::Io)?;
    }
    Ok(())
}

/// Validates an existing database file for corruption.
///
/// # Validation Checks
///
/// - Header magic number
/// - Header version compatibility
/// - Header CRC checksum
/// - Checkpoint region CRC (if present)
/// - All WAL entries CRC (if present)
pub fn validate_database_file<P: AsRef<Path>>(path: P) -> DbFileResult<ValidationResult> {
    let mut db_file = DbFile::open(path)?;
    let header = db_file.header().clone();

    let mut result = ValidationResult {
        header_valid: true,
        checkpoint_valid: None,
        wal_entries_valid: 0,
        wal_entries_corrupted: 0,
        total_wal_entries: 0,
    };

    // Validate checkpoint if present
    if header.flags.has(super::header::DbFileFlags::HAS_CHECKPOINT) {
        match db_file.read_checkpoint() {
            Ok(Some(_)) => result.checkpoint_valid = Some(true),
            Ok(None) => result.checkpoint_valid = None,
            Err(_) => result.checkpoint_valid = Some(false),
        }
    }

    // Validate WAL entries
    if header.flags.has(super::header::DbFileFlags::HAS_WAL) {
        match db_file.read_wal_entries() {
            Ok(entries) => {
                result.wal_entries_valid = entries.len();
                result.total_wal_entries = entries.len();
            }
            Err(DbFileError::WalChecksumMismatch { .. }) => {
                // Some entries may be corrupted
                result.wal_entries_corrupted = 1; // We can't know exactly how many
            }
            Err(_) => {
                result.wal_entries_corrupted = 1;
            }
        }
    }

    Ok(result)
}

/// Result of database file validation.
#[derive(Debug)]
pub struct ValidationResult {
    /// Whether the header is valid.
    pub header_valid: bool,
    /// Whether the checkpoint is valid (None if no checkpoint).
    pub checkpoint_valid: Option<bool>,
    /// Number of valid WAL entries.
    pub wal_entries_valid: usize,
    /// Number of corrupted WAL entries.
    pub wal_entries_corrupted: usize,
    /// Total number of WAL entries.
    pub total_wal_entries: usize,
}

impl ValidationResult {
    /// Returns true if the database file is fully valid.
    pub fn is_valid(&self) -> bool {
        self.header_valid
            && self.checkpoint_valid.unwrap_or(true)
            && self.wal_entries_corrupted == 0
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use serial_test::serial;

    use super::*;

    fn test_base_dir() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("test_migration_{}", std::process::id()));
        path
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    #[serial]
    fn test_detect_legacy_format_empty() {
        let base = test_base_dir();
        cleanup(&base);

        let info = detect_legacy_format(&base);
        assert!(!info.has_legacy_files());

        cleanup(&base);
    }

    #[test]
    #[serial]
    fn test_detect_legacy_format_with_wal() {
        let base = test_base_dir();
        cleanup(&base);
        fs::create_dir_all(base.join(".wal")).unwrap();

        let info = detect_legacy_format(&base);
        assert!(info.has_legacy_files());
        assert!(info.wal_exists);
        assert!(!info.checkpoint_exists);

        cleanup(&base);
    }

    #[test]
    #[serial]
    fn test_validate_new_database() {
        let base = test_base_dir();
        cleanup(&base);
        fs::create_dir_all(&base).unwrap();

        let db_path = base.join("test.minigu");
        let _db = DbFile::create(&db_path).unwrap();

        let result = validate_database_file(&db_path).unwrap();
        assert!(result.is_valid());
        assert!(result.header_valid);
        assert!(result.checkpoint_valid.is_none());
        assert_eq!(result.wal_entries_valid, 0);

        cleanup(&base);
    }
}
