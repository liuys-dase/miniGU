//! AST definitions for *procedure specification*.

use super::{
    BindingTableVariableDef, CompositeQueryStatement, GraphVariableDef,
    LinearCatalogModifyingStatement, LinearDataModifyingStatement, SchemaRef, ValueVariableDef,
    Yield,
};
use crate::macros::base;
use crate::span::{OptSpanned, Spanned, VecSpanned};

#[apply(base)]
pub struct Procedure {
    pub at: OptSpanned<SchemaRef>,                      // 模式引用（可选）
    pub binding_variable_defs: BindingVariableDefBlock, // 绑定变量定义块
    pub statement: Spanned<Statement>,                  // 实际的数据操作语句
    pub next_statements: VecSpanned<NextStatement>,     // 后续语句列表
}

#[apply(base)]
pub enum Statement {
    Catalog(LinearCatalogModifyingStatement), // 目录修改语句
    Query(CompositeQueryStatement),           // 复合查询语句
    Data(LinearDataModifyingStatement),       // 数据修改语句
}

#[apply(base)]
pub struct NextStatement {
    pub yield_clause: OptSpanned<Yield>,
    pub statement: Spanned<Statement>,
}

pub type BindingVariableDefBlock = VecSpanned<BindingVariableDef>;

#[apply(base)]
pub enum BindingVariableDef {
    Graph(GraphVariableDef),
    BindingTable(BindingTableVariableDef),
    Value(ValueVariableDef),
}
