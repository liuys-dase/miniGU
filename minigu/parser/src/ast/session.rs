//! AST definitions for *session management*.

use super::{
    GraphExpr, Ident, SchemaRef, StringLiteral, TypedGraphInitializer, TypedValueInitializer,
};
use crate::macros::base;
use crate::span::{OptSpanned, Spanned};

#[apply(base)]
pub enum SessionSet {
    Schema(Spanned<SchemaRef>),              // 设置模式
    Graph(Spanned<GraphExpr>),               // 设置图
    TimeZone(Spanned<StringLiteral>),        // 设置时区
    Parameter(Spanned<SessionSetParameter>), // 设置参数
}

#[apply(base)]
pub struct SessionSetParameter {
    pub name: Spanned<Ident>,
    pub if_not_exists: bool,
    pub kind: SessionSetParameterKind,
}

#[apply(base)]
pub enum SessionSetParameterKind {
    Graph(Spanned<TypedGraphInitializer>),
    Value(Spanned<TypedValueInitializer>),
}

#[apply(base)]
pub struct SessionReset(pub OptSpanned<SessionResetArgs>);

#[apply(base)]
pub enum SessionResetArgs {
    AllCharacteristics,        // 重置所有特征
    AllParameters,             // 重置所有参数
    Schema,                    // 重置模式
    Graph,                     // 重置图
    TimeZone,                  // 重置时区
    Parameter(Spanned<Ident>), // 重置特定参数
}
