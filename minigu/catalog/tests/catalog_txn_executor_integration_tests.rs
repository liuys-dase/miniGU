use std::sync::Arc;

use minigu_catalog::memory::MemoryCatalog;
use minigu_catalog::memory::directory::MemoryDirectoryCatalog;
use minigu_catalog::memory::schema::MemorySchemaCatalog;
use minigu_catalog::provider::{
    CatalogProvider, DirectoryOrSchema, DirectoryProvider, SchemaProvider,
};
use minigu_context::database::DatabaseContext;
use minigu_context::session::SessionContext;
use minigu_transaction::Transaction;
use rayon::ThreadPoolBuilder;
use smol_str::SmolStr;

fn make_db() -> Arc<DatabaseContext> {
    // Root directory catalog
    let root = Arc::new(MemoryDirectoryCatalog::new(None)) as Arc<dyn DirectoryProvider>;
    let catalog = MemoryCatalog::new(DirectoryOrSchema::Directory(root));
    let pool = ThreadPoolBuilder::new().num_threads(2).build().unwrap();
    Arc::new(DatabaseContext::new(catalog, pool))
}

// Note: Executor for DDL currently returns an empty DataChunk which panics in DataChunk::new.
// These tests focus on end-to-end behavior using SessionContext + provider APIs directly.

#[test]
fn two_sessions_auto_commit_visibility() {
    let db = make_db();
    let mut s1 = SessionContext::new(db.clone());
    let mut s2 = SessionContext::new(db.clone());

    // s2 starts a txn before s1 creates schema
    let t2_old = s2.get_or_begin_txn().unwrap();

    // s1: create schema `s` via provider APIs under a session-managed txn
    let t1 = s1.get_or_begin_txn().unwrap();
    let root = db.catalog().get_root().unwrap();
    let dir = root.as_directory().unwrap();
    let mem_dir = dir
        .as_any()
        .downcast_ref::<MemoryDirectoryCatalog>()
        .expect("memory dir");
    let new_schema = Arc::new(MemorySchemaCatalog::new(Some(Arc::downgrade(dir))));
    mem_dir
        .add_child(
            SmolStr::new("s").to_string(),
            DirectoryOrSchema::Schema(new_schema as _),
            &t1,
        )
        .unwrap();
    t1.commit().unwrap();
    s1.clear_current_txn();

    // s2 old txn should not see it (SI)
    {
        let root = db.catalog().get_root().unwrap();
        let dir = root.as_directory().unwrap();
        assert!(dir.get_child("s", &t2_old).unwrap().is_none());
        // end old txn
        t2_old.abort().ok();
        s2.clear_current_txn();
    }

    // s2 new txn should see it
    let t2_new = s2.get_or_begin_txn().unwrap();
    {
        let root = db.catalog().get_root().unwrap();
        let dir = root.as_directory().unwrap();
        assert!(dir.get_child("s", &t2_new).unwrap().is_some());
    }
}

#[test]
fn two_sessions_graph_type_visibility_and_current_schema() {
    let db = make_db();
    let mut s1 = SessionContext::new(db.clone());
    let mut s2 = SessionContext::new(db.clone());

    // Prepare a standalone MemorySchemaCatalog and set as current for both sessions
    let schema = Arc::new(MemorySchemaCatalog::new(None));
    s1.current_schema = Some(schema.clone());
    s2.current_schema = Some(schema.clone());

    // s2 begins a txn before s1 creates graph type
    let t2_old = s2.get_or_begin_txn().unwrap();

    // s1: create a graph type `gt` by provider APIs under a txn
    let t1 = s1.get_or_begin_txn().unwrap();
    use minigu_catalog::memory::graph_type::MemoryGraphTypeCatalog;
    let gt = Arc::new(MemoryGraphTypeCatalog::new());
    schema
        .add_graph_type_txn("gt".to_string(), gt as _, &t1)
        .unwrap();
    t1.commit().unwrap();
    s1.clear_current_txn();

    // s2 old txn should not see it
    assert!(schema.get_graph_type("gt", &t2_old).unwrap().is_none());
    t2_old.abort().ok();
    s2.clear_current_txn();

    // s2 new txn should see it
    let t2_new = s2.get_or_begin_txn().unwrap();
    assert!(schema.get_graph_type("gt", &t2_new).unwrap().is_some());
}
