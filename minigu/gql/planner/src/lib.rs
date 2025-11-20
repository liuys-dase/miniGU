use gql_parser::ast::Procedure;
use minigu_catalog::txn::catalog_txn::CatalogTxn;
use minigu_catalog::txn::error::CatalogTxnError;
use minigu_context::session::SessionContext;

use crate::binder::Binder;
use crate::error::PlanResult;
use crate::logical_planner::LogicalPlanner;
use crate::optimizer::Optimizer;
use crate::plan::PlanNode;
pub mod binder;
pub mod bound;
pub mod error;
mod logical_planner;
mod optimizer;
pub mod plan;

pub struct Planner {
    context: SessionContext,
}

impl Planner {
    pub fn new(context: SessionContext) -> Self {
        Self { context }
    }

    pub fn plan_query(&mut self, txn: &CatalogTxn, query: &Procedure) -> PlanResult<PlanNode> {
        let catalog = self.context.database().catalog();
        let current_schema = self.context.current_schema.clone().map(|s| s as _);
        let home_schema = self.context.home_schema.clone().map(|s| s as _);
        let current_graph = self.context.current_graph.clone();
        let home_graph = self.context.home_graph.clone();

        let binder = Binder::new(
            catalog,
            current_schema,
            home_schema,
            current_graph,
            home_graph,
            txn,
        );
        let bound = binder
            .bind(query)
            .map_err(|e| CatalogTxnError::External(Box::new(e)))?;
        let logical_plan = LogicalPlanner::new()
            .create_logical_plan(bound)
            .map_err(|e| CatalogTxnError::External(Box::new(e)))?;
        let plan = Optimizer::new()
            .create_physical_plan(&logical_plan)
            .map_err(|e| CatalogTxnError::External(Box::new(e)))?;
        Ok(plan)
    }
}
