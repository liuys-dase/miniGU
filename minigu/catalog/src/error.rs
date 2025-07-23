use std::error::Error;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error(transparent)]
    External(#[from] Box<dyn Error + Send + Sync + 'static>),
    #[error("Write-write conflict detected")]
    WriteWriteConflict,
    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),
    #[error("Metadata key not found: {0}")]
    MetadataKeyNotFound(String),
    #[error("Invalid metadata operation: {0}")]
    InvalidMetadataOperation(String),
    #[error("Serialization failure")]
    SerializationFailure,
    #[error("Other error: {0}")]
    Other(String),
}

pub type CatalogResult<T> = Result<T, CatalogError>;
