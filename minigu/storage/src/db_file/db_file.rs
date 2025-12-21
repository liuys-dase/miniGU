//! Core database file implementation.
//!
//! This module provides the main `DbFile` struct for managing a single
//! database file containing both checkpoint and WAL regions.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use super::error::{DbFileError, DbFileResult};
use super::header::{DbFileFlags, DbFileHeader};
use crate::common::wal::LogRecord;
use crate::common::wal::graph_wal::RedoEntry;
use crate::tp::checkpoint::GraphCheckpoint;

/// Record header size: 4 bytes length + 4 bytes CRC.
const RECORD_HEADER_SIZE: usize = 8;

/// A single database file containing checkpoint and WAL regions.
///
/// This struct manages all I/O operations for the unified database file format,
/// including:
/// - Creating new database files
/// - Opening existing database files
/// - Reading/writing checkpoint data
/// - Appending/reading WAL entries
/// - File compaction after checkpoint
pub struct DbFile {
    /// The underlying file handle.
    file: File,
    /// Path to the database file.
    path: PathBuf,
    /// Cached header.
    header: DbFileHeader,
}

impl DbFile {
    /// Creates a new database file at the specified path.
    ///
    /// This will fail if the file already exists.
    pub fn create<P: AsRef<Path>>(path: P) -> DbFileResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;

        let mut header = DbFileHeader::new();
        header.update_crc();

        let mut db_file = Self { file, path, header };
        db_file.write_header()?;
        db_file.file.sync_all()?;

        Ok(db_file)
    }

    /// Opens an existing database file.
    pub fn open<P: AsRef<Path>>(path: P) -> DbFileResult<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let mut db_file = Self {
            file,
            path,
            header: DbFileHeader::new(),
        };

        db_file.header = db_file.read_header()?;

        Ok(db_file)
    }

    /// Opens an existing database file or creates a new one if it doesn't exist.
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> DbFileResult<Self> {
        let path = path.as_ref();
        if path.exists() {
            Self::open(path)
        } else {
            Self::create(path)
        }
    }

    /// Returns the path to the database file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns a reference to the current header.
    pub fn header(&self) -> &DbFileHeader {
        &self.header
    }

    /// Writes the header to the file.
    fn write_header(&mut self) -> DbFileResult<()> {
        self.header.write_to(&mut self.file)?;
        Ok(())
    }

    /// Reads the header from the file.
    fn read_header(&mut self) -> DbFileResult<DbFileHeader> {
        DbFileHeader::read_from(&mut self.file)
    }

    /// Reads the checkpoint from the file, if one exists.
    pub fn read_checkpoint(&mut self) -> DbFileResult<Option<GraphCheckpoint>> {
        if !self.header.flags.has(DbFileFlags::HAS_CHECKPOINT) {
            return Ok(None);
        }

        if self.header.checkpoint_length == 0 {
            return Ok(None);
        }

        self.file
            .seek(SeekFrom::Start(self.header.checkpoint_offset))?;

        // Read length and checksum
        let mut len_bytes = [0u8; 4];
        self.file.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut crc_bytes = [0u8; 4];
        self.file.read_exact(&mut crc_bytes)?;
        let stored_crc = u32::from_le_bytes(crc_bytes);

        // Read payload
        let mut payload = vec![0u8; len];
        self.file.read_exact(&mut payload)?;

        // Verify checksum
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let computed_crc = hasher.finalize();

        if computed_crc != stored_crc {
            return Err(DbFileError::CheckpointChecksumMismatch);
        }

        // Deserialize
        let checkpoint: GraphCheckpoint = postcard::from_bytes(&payload)
            .map_err(|e| DbFileError::Deserialization(e.to_string()))?;

        Ok(Some(checkpoint))
    }

    /// Writes a checkpoint to the file.
    ///
    /// This will overwrite any existing checkpoint. After writing a checkpoint,
    /// the WAL region is reset (old entries are discarded).
    pub fn write_checkpoint(&mut self, checkpoint: &GraphCheckpoint) -> DbFileResult<()> {
        // Serialize the checkpoint
        let payload = postcard::to_allocvec(checkpoint)
            .map_err(|e| DbFileError::Serialization(e.to_string()))?;

        // Compute checksum
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();

        let len = payload.len() as u32;
        let total_size = RECORD_HEADER_SIZE + payload.len();

        // Write checkpoint at the designated offset
        self.file
            .seek(SeekFrom::Start(self.header.checkpoint_offset))?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&payload)?;

        // Update header
        self.header.checkpoint_length = total_size as u64;
        self.header.flags.set(DbFileFlags::HAS_CHECKPOINT);

        // WAL region starts right after checkpoint
        self.header.wal_offset = self.header.checkpoint_offset + total_size as u64;
        self.header.wal_length = 0; // Reset WAL after checkpoint
        self.header.flags.clear(DbFileFlags::HAS_WAL);

        self.header.last_lsn = checkpoint.metadata.lsn;
        self.header.last_commit_ts = checkpoint.metadata.latest_commit_ts;

        self.header.update_crc();
        self.write_header()?;

        // Sync to ensure durability
        self.file.sync_all()?;

        Ok(())
    }

    /// Appends a WAL entry to the file.
    pub fn append_wal(&mut self, entry: &RedoEntry) -> DbFileResult<()> {
        // Serialize the entry
        let payload = entry
            .to_bytes()
            .map_err(|e| DbFileError::Serialization(e.to_string()))?;

        // Compute checksum
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();

        let len = payload.len() as u32;
        let entry_size = RECORD_HEADER_SIZE + payload.len();

        // Seek to end of WAL region
        let write_offset = self.header.wal_offset + self.header.wal_length;
        self.file.seek(SeekFrom::Start(write_offset))?;

        // Write entry
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&payload)?;

        // Update header
        self.header.wal_length += entry_size as u64;
        self.header.flags.set(DbFileFlags::HAS_WAL);
        self.header.last_lsn = entry.lsn;

        self.header.update_crc();
        self.write_header()?;

        Ok(())
    }

    /// Flushes buffered data and syncs to disk.
    pub fn flush(&mut self) -> DbFileResult<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Syncs all data and metadata to disk.
    pub fn sync_all(&mut self) -> DbFileResult<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Reads all WAL entries from the file.
    pub fn read_wal_entries(&mut self) -> DbFileResult<Vec<RedoEntry>> {
        if !self.header.flags.has(DbFileFlags::HAS_WAL) {
            return Ok(Vec::new());
        }

        if self.header.wal_length == 0 {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        let wal_end = self.header.wal_offset + self.header.wal_length;
        let mut current_offset = self.header.wal_offset;

        while current_offset < wal_end {
            self.file.seek(SeekFrom::Start(current_offset))?;

            // Read length and checksum
            let mut len_bytes = [0u8; 4];
            if self.file.read_exact(&mut len_bytes).is_err() {
                break; // EOF
            }
            let len = u32::from_le_bytes(len_bytes) as usize;

            let mut crc_bytes = [0u8; 4];
            self.file.read_exact(&mut crc_bytes)?;
            let stored_crc = u32::from_le_bytes(crc_bytes);

            // Read payload
            let mut payload = vec![0u8; len];
            self.file.read_exact(&mut payload)?;

            // Verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let computed_crc = hasher.finalize();

            if computed_crc != stored_crc {
                return Err(DbFileError::WalChecksumMismatch {
                    offset: current_offset,
                });
            }

            // Deserialize
            let entry = RedoEntry::from_bytes(payload)
                .map_err(|e| DbFileError::Deserialization(e.to_string()))?;

            entries.push(entry);
            current_offset += (RECORD_HEADER_SIZE + len) as u64;
        }

        // Sort by LSN
        entries.sort_by_key(|e| e.lsn);

        Ok(entries)
    }

    /// Returns the last LSN in the WAL.
    pub fn last_lsn(&self) -> u64 {
        self.header.last_lsn
    }

    /// Returns the last commit timestamp.
    pub fn last_commit_ts(&self) -> u64 {
        self.header.last_commit_ts
    }

    /// Updates the last commit timestamp in the header.
    pub fn set_last_commit_ts(&mut self, ts: u64) -> DbFileResult<()> {
        self.header.last_commit_ts = ts;
        self.header.update_crc();
        self.write_header()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use minigu_transaction::{IsolationLevel, Timestamp};
    use serial_test::serial;

    use super::*;
    use crate::common::DeltaOp;
    use crate::common::wal::graph_wal::Operation;

    fn temp_db_path() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("test_db_{}.minigu", std::process::id()));
        path
    }

    fn cleanup(path: &std::path::Path) {
        let _ = fs::remove_file(path);
    }

    #[test]
    #[serial]
    fn test_create_and_open() {
        let path = temp_db_path();
        cleanup(&path);

        // Create
        {
            let db = DbFile::create(&path).unwrap();
            assert_eq!(db.header().version, super::super::header::CURRENT_VERSION);
        }

        // Open
        {
            let db = DbFile::open(&path).unwrap();
            assert_eq!(db.header().version, super::super::header::CURRENT_VERSION);
        }

        cleanup(&path);
    }

    #[test]
    #[serial]
    fn test_wal_append_and_read() {
        let path = temp_db_path();
        cleanup(&path);

        {
            let mut db = DbFile::create(&path).unwrap();

            // Append entries
            for i in 1..=5 {
                let entry = RedoEntry {
                    lsn: i,
                    txn_id: Timestamp::with_ts(100 + i),
                    iso_level: IsolationLevel::Serializable,
                    op: Operation::Delta(DeltaOp::DelVertex(i)),
                };
                db.append_wal(&entry).unwrap();
            }
            db.flush().unwrap();

            // Read entries
            let entries = db.read_wal_entries().unwrap();
            assert_eq!(entries.len(), 5);
            for (i, entry) in entries.iter().enumerate() {
                assert_eq!(entry.lsn, (i + 1) as u64);
            }
        }

        cleanup(&path);
    }

    #[test]
    #[serial]
    fn test_open_or_create() {
        let path = temp_db_path();
        cleanup(&path);

        // Should create
        {
            let _db = DbFile::open_or_create(&path).unwrap();
            assert!(path.exists());
        }

        // Should open
        {
            let _db = DbFile::open_or_create(&path).unwrap();
        }

        cleanup(&path);
    }
}
