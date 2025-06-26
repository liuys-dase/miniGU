use itertools::Itertools;
use winnow::Parser;

use super::impls::gql_program;
use super::token::{Token, build_token_stream, tokenize};
use crate::ast::Program;
use crate::error::Error;
use crate::span::Spanned;

/// Options which can be used to configure the behavior of the parser.
///
/// # Examples
///
/// Parsing a GQL query with default options:
/// ```
/// # use gql_parser::ParseOptions;
/// let parsed = ParseOptions::new().parse("match (: Person) -> (b: Person) return b");
/// assert!(parsed.is_ok());
/// ```
#[derive(Debug, Clone, Default)]
pub struct ParseOptions(ParseOptionsInner);

impl ParseOptions {
    /// Create a default set of parse options for configuration.  
    ///
    /// # Examples
    ///
    /// ```
    /// # use gql_parser::ParseOptions;
    /// let mut options = ParseOptions::new();
    /// let parsed = options.unescape(true).parse("CREATE GRAPH mygraph ANY");
    /// assert!(parsed.is_ok());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether quoted character sequences should be unescaped by the parser.
    ///
    /// If set to `true` (default), the parser will unescape quoted sequences in the input query.
    /// For example, `""` in GQL is unescaped to `"` in a double quoted character sequence.
    ///
    /// Otherwise, the parser will leave the raw strings unchanged in the output, and the
    /// caller should handle them manually.
    ///
    /// # Examples
    ///
    /// TODO: Fill this part.
    ///
    /// Parsing a GQL query with quoted character sequences unescaped:
    /// ```no_run
    /// use gql_parser::ParseOptions;
    ///
    /// let parsed = ParseOptions::new()
    ///     .unescape(true)
    ///     .parse(r"session set graph /`my\ngraph`");
    /// ```
    pub fn unescape(&mut self, unescape: bool) -> &mut Self {
        self.0.unescape = unescape;
        self
    }

    /// 解析 GQL 查询的主入口，核心调用流程：
    /// 1. 用户输入 GQL 字符串
    /// 2. ParseOptions::parse()
    /// 3. tokenize(input)  ← 这里调用 TokenKind::lexer()
    /// 4. Token 流生成
    /// 5. 语法解析器使用 TokenKind 进行模式匹配
    /// 6. 生成 AST
    ///
    /// Parses a GQL query `gql` into a spanned abstract syntax tree with the options specified by
    /// `self`.
    ///
    /// # Errors
    ///
    /// This function will return an error if `gql` is not a valid GQL query. The error will carry
    /// fancy diagnostics if feature `miette` is enabled.
    ///
    /// Currently, we provide only simple and non-informative errors as defined in [`Error`]. More
    /// specific errors will be introduced in the future.
    ///
    /// # Examples
    ///
    /// ```
    /// # use gql_parser::ParseOptions;
    /// let program = ParseOptions::new().parse("SESSION CLOSE");
    /// assert!(program.is_ok());
    /// assert_eq!(program.unwrap().span(), 0..13);
    /// ```
    pub fn parse(&self, gql: &str) -> Result<Spanned<Program>, Error> {
        // 调用 token.rs 中的 tokenize 函数，将输入字符串转换为 Token 流
        let tokens = tokenize(gql).map_err(|e| Error::from_tokenize_error(gql, e))?;
        // 调用 parse_tokens 函数，将 Token 流转换为抽象语法树。
        self.parse_tokens(gql, &tokens)
    }

    /// `parse_tokens` 方法是 GQL 解析器的核心方法之一，负责将词法分析后的 Token
    /// 数组转换为抽象语法树（AST），作为语法分析阶段的入口点。
    ///
    /// # 参数说明
    /// - `&self: ParseOptions`：实例的不可变引用，包含解析配置选项
    /// - `gql: &str`：原始 GQL 查询字符串，用于错误定位与报告
    /// - `tokens: &[Token]`：词法分析生成的 Token 数组切片
    ///
    /// # 返回值
    /// - `Result<Spanned<Program>, Error>`
    ///   - 成功时：返回带位置信息的 `Program` 根节点 AST
    ///   - 失败时：返回包含详细错误信息的 `Error` 实例
    ///
    /// Parses the tokens into a
    /// spanned abstract syntax tree with the options specified by `self`.
    ///
    /// Since this produces detailed error messages, the caller should provide the original input
    /// string.
    ///
    /// # Errors
    ///
    /// This function will return an error if `tokens` cannot be parsed into a valid abstract syntax
    /// tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use gql_parser::{ParseOptions, tokenize};
    /// let input = "SESSION CLOSE";
    /// let tokens = tokenize(input).unwrap();
    /// let program = ParseOptions::new().parse_tokens(input, &tokens);
    /// assert!(program.is_ok());
    /// ```
    pub fn parse_tokens(&self, gql: &str, tokens: &[Token]) -> Result<Spanned<Program>, Error> {
        // 第 1 步：将 Token 数组转换为 Token 流
        let stream = build_token_stream(tokens, self.0.clone());
        // 第 2 步：语法分析
        gql_program
            .parse(stream)
            .map_err(|e| match tokens.get(e.offset()) {
                Some(token) => Error::unexpected(gql, token.span.clone()),
                None => Error::UnexpectedEof,
            })
    }
}

#[derive(Debug, Clone)]
pub(super) struct ParseOptionsInner {
    unescape: bool,
}

impl Default for ParseOptionsInner {
    fn default() -> Self {
        Self { unescape: true }
    }
}

impl ParseOptionsInner {
    pub(super) fn unescape(&self) -> bool {
        self.unescape
    }
}
