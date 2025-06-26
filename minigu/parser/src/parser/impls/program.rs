use winnow::combinator::{alt, dispatch, empty, fail, opt, peek, repeat};
use winnow::{ModalResult, Parser};

use super::procedure_spec::procedure_specification;
use super::session::{session_close_command, session_reset_command, session_set_command};
use super::transaction::start_transaction_command;
use crate::ast::{EndTransaction, Program, ProgramActivity, SessionActivity, TransactionActivity};
use crate::imports::Vec;
use crate::lexer::TokenKind;
use crate::parser::token::{TokenStream, any};
use crate::parser::utils::{SpannedParserExt, ToSpanned};
use crate::span::Spanned;

/// 解析器函数
///
/// 该解析器使用 winnow 解析器组合子：
/// - `alt()`: 尝试多个替代解析路径
/// - `opt()`: 可选解析
/// - `map()`: 转换解析结果
/// - `spanned()`: 添加位置信息
///
/// Program
///
/// ├── activity: Option<ProgramActivity>
/// │   ├── Session(SessionActivity)           // 会话管理，不涉及数据操作
/// │   └── Transaction(TransactionActivity)   // 🎯 包含数据操作
/// │       ├── start: Option<StartTransaction>
/// │       ├── procedure: Option<Procedure>   // 🎯 数据操作的核心
/// │       └── end: Option<EndTransaction>
/// └── session_close: bool
///
/// # 总结
/// Program 结构体现了 GQL 语言的核心概念：
/// - 会话管理: 配置数据库连接和参数
/// - 事务管理: 控制数据操作的原子性
/// - 位置追踪: 支持精确的错误报告和代码分析

pub fn gql_program(input: &mut TokenStream) -> ModalResult<Spanned<Program>> {
    alt((
        (program_activity, opt(session_close_command)).map(|(activity, session_close)| Program {
            activity: Some(activity),
            session_close: session_close.is_some(),
        }),
        session_close_command.map(|_| Program {
            activity: None,
            session_close: true,
        }),
    ))
    .spanned()
    .parse_next(input)
}

pub fn program_activity(input: &mut TokenStream) -> ModalResult<Spanned<ProgramActivity>> {
    dispatch! {peek(any);
        TokenKind::Session => session_activity.map_inner(ProgramActivity::Session),
        _ => transaction_activity.map_inner(ProgramActivity::Transaction),
    }
    .parse_next(input)
}

pub fn end_transaction_command(input: &mut TokenStream) -> ModalResult<Spanned<EndTransaction>> {
    dispatch! {any;
        TokenKind::Rollback => empty.value(EndTransaction::Rollback),
        TokenKind::Commit => empty.value(EndTransaction::Commit),
        _ => fail,
    }
    .spanned()
    .parse_next(input)
}

pub fn session_activity(input: &mut TokenStream) -> ModalResult<Spanned<SessionActivity>> {
    dispatch! {peek((any, any));
        (TokenKind::Session, TokenKind::Set) => (
            repeat(1.., session_set_command),
            repeat(0.., session_reset_command),
        )
            .map(|(set, reset)| SessionActivity { set, reset }),
        (TokenKind::Session, TokenKind::Reset) => repeat(1.., session_reset_command)
            .map(|reset| SessionActivity {
                set: Vec::new(),
                reset,
            }),
        _ => fail,
    }
    .spanned()
    .parse_next(input)
}

pub fn transaction_activity(input: &mut TokenStream) -> ModalResult<Spanned<TransactionActivity>> {
    dispatch! {peek(any);
        TokenKind::Start => {
            (
                start_transaction_command,
                opt((procedure_specification, opt(end_transaction_command))),
            )
                .map(|(start, follow)| {
                    let start = Some(start);
                    let (procedure, end) = follow.unzip();
                    let end = end.flatten();
                    TransactionActivity {
                        start,
                        procedure,
                        end,
                    }
                })
        },
        TokenKind::Commit | TokenKind::Rollback => {
            end_transaction_command.map(|end| TransactionActivity {
                start: None,
                procedure: None,
                end: Some(end),
            })
        },
        _ => {
            (procedure_specification, opt(end_transaction_command)).map(
                |(procedure, end)| {
                    let procedure = Some(procedure);
                    TransactionActivity {
                        start: None,
                        procedure,
                        end,
                    }
                },
            )
        }
    }
    .spanned()
    .parse_next(input)
}

// SessionActivity: SessionActivity<'a> = {
//     <reset: SessionResetCommand+> => SessionActivity { set: vec![], reset },
//     <set: SessionSetCommand+> <reset: SessionResetCommand*> => SessionActivity { set, reset },
// }

// TransactionActivity: TransactionActivity<'a> = {
//     StartTransactionCommand => TransactionActivity { start: Some(<>), procedure: None, end: None
// },     <start: StartTransactionCommand> <procedure: ProcedureSpecification> <end:
// EndTransactionCommand?> => {         TransactionActivity { start: Some(start), procedure:
// Some(procedure), end }     },
//     <procedure: ProcedureSpecification> <end: EndTransactionCommand?> => {
//         TransactionActivity { start: None, procedure: Some(procedure), end }
//     },
//     EndTransactionCommand => TransactionActivity { start: None, procedure: None, end: Some(<>) },
// }

// EndTransactionCommand: EndTransaction = {
//     "ROLLBACK" => EndTransaction::Rollback,
//     "COMMIT" => EndTransaction::Commit,
// }

// ProgramActivity: ProgramActivity<'a> = {
//     SessionActivity => ProgramActivity::Session(<>), // SESSION
//     TransactionActivity => ProgramActivity::Transaction(<>), // COMMIT, ROLLBACK,
// }

// pub GqlProgram: Program<'a> = {
//     // <activity: ProgramActivity> <session_close: SessionCloseCommand?> => {
//     //     Program { activity, session_close: session_close.is_some() }
//     // },
//     <activity: ProgramActivity> => {
//         Program { activity, session_close: false }
//     },
//     SessionCloseCommand => Program { activity: None, session_close: true }, // SESSION
// }

#[cfg(all(test, feature = "serde"))]
mod tests {
    use insta::assert_yaml_snapshot;

    use super::*;
    use crate::parser::utils::parse;

    #[test]
    fn test_gql_program() {
        let parsed = parse!(gql_program, "session close");
        assert_yaml_snapshot!(parsed);
    }

    #[test]
    fn test_end_transaction_command_1() {
        let parsed = parse!(end_transaction_command, "rollback");
        assert_yaml_snapshot!(parsed);
    }

    #[test]
    fn test_end_transaction_command_2() {
        let parsed = parse!(end_transaction_command, "commit");
        assert_yaml_snapshot!(parsed);
    }
}
