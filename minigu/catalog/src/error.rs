use std::error::Error;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("{0}")]
    General(String),
    #[error(transparent)]
    External(#[from] Box<dyn Error + Send + Sync + 'static>),
}

pub type CatalogResult<T> = Result<T, CatalogError>;
