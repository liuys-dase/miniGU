//! Single database file module for miniGU.
//!
//! This module provides functionality to integrate WAL and Checkpoint data
//! into a single database file (`.minigu`), supporting:
//! - Fixed file header with magic number, version, and region offsets
//! - Checkpoint region for graph state snapshots
//! - WAL region for write-ahead log entries
//! - Crash recovery from a single file
//!
//! # File Layout
//!
//! ```text
//! ┌────────────────────────────────────────────────────────┐
//! │                  FILE HEADER                           │
//! │  (self-describing length, minimum 256 bytes)           │
//! ├────────────────────────────────────────────────────────┤
//! │                 CHECKPOINT REGION                      │
//! │  [len:4B][crc:4B][GraphCheckpoint payload...]          │
//! ├────────────────────────────────────────────────────────┤
//! │                    WAL REGION                          │
//! │  [len:4B][crc:4B][RedoEntry 1...]                      │
//! │  [len:4B][crc:4B][RedoEntry 2...]                      │
//! │  ...                                                   │
//! └────────────────────────────────────────────────────────┘
//! ```

pub mod db_file;
pub mod error;
pub mod header;

pub use db_file::DbFile;
pub use error::DbFileError;
pub use header::{CURRENT_VERSION, DbFileHeader, HEADER_SIZE, MAGIC};
