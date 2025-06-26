//! AST definitions for *GQL-program*.

use super::{EndTransaction, Procedure, SessionReset, SessionSet, StartTransaction};
use crate::macros::base;
use crate::span::{OptSpanned, VecSpanned};

#[apply(base)]
pub struct Program {
    pub activity: OptSpanned<ProgramActivity>, // 程序活动（可选）
    pub session_close: bool,                   // 是否包含会话关闭
}

#[apply(base)]
pub enum ProgramActivity {
    Session(SessionActivity),         // 会话管理活动
    Transaction(TransactionActivity), // 事务管理活动
}

/// 会话管理活动，用于管理数据库会话的配置和状态
#[apply(base)]
pub struct SessionActivity {
    pub set: VecSpanned<SessionSet>,     // 会话设置命令列表
    pub reset: VecSpanned<SessionReset>, // 会话重置命令列表
}

/// 事务管理活动，管理数据库事务的生命周期
#[apply(base)]
pub struct TransactionActivity {
    pub start: OptSpanned<StartTransaction>, // 启动事务命令（可选）
    pub procedure: OptSpanned<Procedure>,    // 存储过程调用（可选）
    pub end: OptSpanned<EndTransaction>,     // 结束事务命令（可选）
}
