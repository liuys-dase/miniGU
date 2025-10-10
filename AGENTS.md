# Repository Guidelines

## Always Respond in Chinese
- Please always respond in Chinese.

## Project Structure & Module Organization
- Workspace: Rust crates under `minigu/` (core library and internal modules: `catalog/`, `common/`, `context/`, `gql/{parser,planner,execution}/`, `storage/`, `transaction/`).
- CLI: `minigu-cli/` with the `minigu` binary (`src/main.rs`, interactive shell and script executor).
- Tests: `minigu-test/` for system and sqllogictest cases (`tests/`, `gql/**/*.slt`).
- Docs & assets: `docs/`, `resources/`. Config: `rustfmt.toml`, `clippy.toml`, `deny.toml`, `taplo.toml`.

## Build, Test, and Development Commands
- Build: `cargo build` (use `-r` for release).
- Run shell: `cargo run -- shell` or `cargo run --bin minigu -- shell`.
- Execute script: `cargo run -- execute <file.gql>`.
- Unit/integration tests: `cargo test` or package-scoped `cargo test -p minigu-storage`.
- SQL logic tests: `cargo test -p minigu-test --test sqllogictest -- --nocapture`.
- Lint/format: `cargo fmt`, `cargo clippy --tests`.
- TOML format: `taplo fmt` (use `--check --diff` in CI).
- CI locally: `scripts/run_ci.sh` (formats, clippy, build, nextest, docs).

## Coding Style & Naming Conventions
- Rustfmt enforced (4 spaces, reordered imports; see `rustfmt.toml`). Run `cargo fmt` before pushing.
- Prefer Conventional Commits style for messages (e.g., `feat(storage): add LSM layer`).
- Rust naming: types/traits `CamelCase`, modules/files `snake_case`, constants `SCREAMING_SNAKE_CASE`. Prefer full acronyms (e.g., `HTTP` over `Http`).

## Testing Guidelines
- Frameworks: Rust `#[test]`, `libtest-mimic` in `minigu-test`, snapshot tests via `insta`.
- Add unit tests near code; system tests in `minigu-test/`.
- Naming: test files mirror module names; snapshot names describe scenario (e.g., `opengql/create_schema`).
- Run full suite before PR: `scripts/run_ci.sh` or `cargo test && cargo clippy`.

## Commit & Pull Request Guidelines
- Commits: small, focused; message includes scope (e.g., `fix(parser): handle whitespace`).
- PRs: include description, rationale, key changes, and how to test; link issues; add screenshots or sample shell output when UI/CLI behavior changes.
- Requirements: all checks green (fmt, clippy, tests); update docs/examples when behavior changes.

## Security & Configuration Tips
- Toolchain pinned via `rust-toolchain.toml` (Rust 2024 edition).
- License/dep checks: optional `cargo deny check` (configured in `deny.toml`).
- Avoid adding new crates with wildcard versions; prefer caret ranges and workspace deps.
