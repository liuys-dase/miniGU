use std::sync::Arc;

use arrow::array::StringArray;
use minigu_catalog::provider::SchemaProvider;
use minigu_common::data_chunk::DataChunk;
use minigu_common::data_type::{DataField, DataSchema, LogicalType};
use minigu_context::procedure::Procedure;
use minigu_transaction::Transaction;

// Show graph names in current schema.

pub fn build_procedure() -> Procedure {
    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "graph_name".into(),
        LogicalType::String,
        false,
    )]));

    Procedure::new(vec![], Some(schema.clone()), move |mut context, args| {
        assert!(args.is_empty());
        let chunk = if let Some(current_schema) = context.current_schema.clone() {
            let txn = context.get_or_begin_txn().unwrap();
            let names = current_schema.graph_names(txn.as_ref());
            txn.commit().unwrap();
            context.clear_current_txn();
            let names_out = Arc::new(StringArray::from_iter_values(names));
            DataChunk::new(vec![names_out])
        } else {
            DataChunk::new_empty(&schema)
        };
        Ok(vec![chunk])
    })
}
