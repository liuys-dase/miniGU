use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::create_array;
use gql_parser::ast::{
    EndTransaction, Procedure, Program, ProgramActivity, SessionActivity, TransactionActivity,
};
use gql_parser::parse_gql;
use itertools::Itertools;
use minigu_catalog::memory::MemoryCatalog;
use minigu_catalog::memory::schema::MemorySchemaCatalog;
use minigu_catalog::provider::SchemaRef;
use minigu_common::data_chunk::DataChunk;
use minigu_common::data_type::{DataField, DataSchema, LogicalType};
use minigu_common::error::not_implemented;
use minigu_context::database::DatabaseContext;
use minigu_context::session::SessionContext;
use minigu_execution::builder::ExecutorBuilder;
use minigu_execution::executor::Executor;
use minigu_planner::Planner;
use minigu_planner::plan::PlanData;
use minigu_transaction::{GraphTxnManager, Transaction};

use crate::error::{Error, Result};
use crate::metrics::QueryMetrics;
use crate::result::QueryResult;

pub struct Session {
    context: SessionContext,
    closed: bool,
}

impl Session {
    pub(crate) fn new(
        database: Arc<DatabaseContext>,
        default_schema: Arc<MemorySchemaCatalog>,
    ) -> Result<Self> {
        let mut context = SessionContext::new(database);
        context.home_schema = Some(default_schema.clone());
        context.current_schema = Some(default_schema);
        Ok(Self {
            context,
            closed: false,
        })
    }

    pub fn query(&mut self, query: &str) -> Result<QueryResult> {
        if self.closed {
            return Err(Error::SessionClosed);
        }
        let start = Instant::now();
        let program = parse_gql(query)?;
        let parsing_time = start.elapsed();
        let mut result = program
            .value()
            .activity
            .as_ref()
            .map(|activity| match activity.value() {
                ProgramActivity::Session(activity) => self.handle_session_activity(activity),
                ProgramActivity::Transaction(activity) => {
                    self.handle_transaction_activity(activity)
                }
            })
            .transpose()?
            .unwrap_or_default();
        result.metrics.parsing_time = parsing_time;
        if program.value().session_close {
            self.closed = true;
        }
        Ok(result)
    }

    fn handle_session_activity(&self, activity: &SessionActivity) -> Result<QueryResult> {
        not_implemented("session activity", None)
    }

    fn handle_transaction_activity(
        &mut self,
        activity: &TransactionActivity,
    ) -> Result<QueryResult> {
        if activity.start.is_some() {
            // Begin transaction: default using Snapshot isolation
            use minigu_transaction::IsolationLevel;
            let txn = self
                .context
                .catalog_txn_mgr
                .begin_transaction(IsolationLevel::Snapshot)
                .map_err(|e| {
                    crate::error::Error::Catalog(minigu_catalog::error::CatalogError::External(
                        Box::new(e),
                    ))
                })?;
            self.context.current_txn = Some(txn);
        }
        if let Some(end) = activity.end.as_ref() {
            if let Some(txn) = self.context.current_txn.take() {
                match end.value() {
                    EndTransaction::Commit => {
                        let _cts = txn.commit().map_err(|e| {
                            crate::error::Error::Catalog(
                                minigu_catalog::error::CatalogError::External(Box::new(e)),
                            )
                        })?;
                        self.context
                            .catalog_txn_mgr
                            .finish_transaction(&txn)
                            .map_err(|e| {
                                crate::error::Error::Catalog(
                                    minigu_catalog::error::CatalogError::External(Box::new(e)),
                                )
                            })?;
                    }
                    EndTransaction::Rollback => {
                        txn.abort().map_err(|e| {
                            crate::error::Error::Catalog(
                                minigu_catalog::error::CatalogError::External(Box::new(e)),
                            )
                        })?;
                        self.context
                            .catalog_txn_mgr
                            .finish_transaction(&txn)
                            .map_err(|e| {
                                crate::error::Error::Catalog(
                                    minigu_catalog::error::CatalogError::External(Box::new(e)),
                                )
                            })?;
                    }
                }
            }
        }
        let result = activity
            .procedure
            .as_ref()
            .map(|procedure| self.handle_procedure(procedure.value()))
            .transpose()?
            .unwrap_or_default();
        Ok(result)
    }

    fn handle_procedure(&self, procedure: &Procedure) -> Result<QueryResult> {
        let mut metrics = QueryMetrics::default();

        let start = Instant::now();
        let planner = Planner::new(self.context.clone());
        let physical_plan = planner.plan_query(procedure)?;
        metrics.planning_time = start.elapsed();

        let schema = physical_plan.schema().cloned();
        let start = Instant::now();
        let chunks: Vec<_> = self.context.database().runtime().scope(|_| {
            let mut executor = ExecutorBuilder::new(self.context.clone()).build(&physical_plan);
            executor.into_iter().try_collect()
        })?;
        metrics.execution_time = start.elapsed();

        Ok(QueryResult {
            schema,
            metrics,
            chunks,
        })
    }
}
