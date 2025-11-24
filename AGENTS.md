# Repository Guidelines

## Awlways response in Chinese

## Project Structure & Module Organization
- Workspace crates under `minigu/*`:
  - `minigu/core` (library API), `minigu-cli` (binary `minigu`), `minigu-test` (end-to-end tests).
  - Internal crates: `minigu/gql/{parser,planner,execution}`, `minigu/{catalog,common,context,storage,transaction}`.
- Docs and assets: `docs/`, `resources/` (GQL samples).
- CI/help scripts: `scripts/` (e.g., `run_ci.sh`).

## Build, Test, and Development Commands
- Build: `cargo build --features "std,serde,miette"`
- Run CLI (interactive shell): `cargo run --bin minigu -- shell`
- Format (Rust): `cargo fmt --check`  | TOML: `taplo fmt --check --diff`
- Lint: `cargo clippy --tests --features "std,serde,miette" --no-deps`
- Tests: `cargo nextest run --features "std,serde,miette"` and `cargo test --doc`
- Docs: `cargo doc --lib --no-deps --features "std,serde,miette"`
- Full CI locally: `bash scripts/run_ci.sh`
Prereqs: nightly toolchain (see `rust-toolchain.toml`), `cargo-nextest`, `taplo`, optional `cargo-deny`, `typos`.

## Coding Style & Naming Conventions
- Rustfmt enforced (see `rustfmt.toml`): 4 spaces, 2024 style edition, ordered imports (`StdExternalCrate`).
- Clippy lints set at workspace level; fix or allow with clear justification.
- Module/file names: snake_case; types/enums: PascalCase; functions/vars: snake_case; constants: SCREAMING_SNAKE_CASE.

## Testing Guidelines
- Unit tests live alongside code (`#[cfg(test)] mod tests`) and in crate `tests/` dirs.
- Snapshot tests (parser) use `insta`; name tests `test_<area>_...` for clarity.
- End-to-end flows live in `minigu-test`; prefer realistic fixtures in `resources/`.
- Aim to maintain or improve coverage; add tests with new behavior.

## Commit & Pull Request Guidelines
- Use Conventional Commits style when possible: `feat(scope): message`, `fix(scope): message`, `ci:` etc.
- Commits should be small and focused; update docs/tests with code changes.
- PRs: clear description, linked issues, reproduction steps, and screenshots/logs when relevant. Ensure `scripts/run_ci.sh` passes.

## Security & Configuration Tips
- Supply chain checks: `cargo deny check` (see `deny.toml`).
- Spelling: `typos` (config in `typos.toml`).
- Do not commit secrets; prefer env vars and local config files ignored by Git.

## Agent-Specific Instructions
- Follow these guidelines for any automated edits; keep patches minimal and aligned with existing style and workspace lints.
- When in doubt, open an issue/PR describing the approach before broad refactors.

