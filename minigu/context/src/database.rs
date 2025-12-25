use std::sync::Arc;

use minigu_catalog::memory::MemoryCatalog;
use minigu_common::config::DatabaseConfig;
use rayon::ThreadPool;

#[derive(Debug)]
pub struct DatabaseContext {
    catalog: MemoryCatalog,
    runtime: ThreadPool,
    config: Arc<DatabaseConfig>,
}

impl DatabaseContext {
    pub fn new(catalog: MemoryCatalog, runtime: ThreadPool, config: Arc<DatabaseConfig>) -> Self {
        Self {
            catalog,
            runtime,
            config,
        }
    }

    #[inline]
    pub fn catalog(&self) -> &MemoryCatalog {
        &self.catalog
    }

    #[inline]
    pub fn runtime(&self) -> &ThreadPool {
        &self.runtime
    }

    #[inline]
    pub fn config(&self) -> &Arc<DatabaseConfig> {
        &self.config
    }
}
