# Repository Guidelines

## Project Structure & Module Organization
- Workspace root manages Rust crates under `minigu/` with shared configs in `Cargo.toml`, `clippy.toml`, `rustfmt.toml`, and `deny.toml`.
- Core library lives in `minigu/core`; supporting crates include `minigu/catalog`, `minigu/common`, `minigu/context`, `minigu/transaction`, and `minigu/storage` (with `diskann-rs` subcrates).
- GraphQL pipeline crates are in `minigu/gql/` (`parser`, `planner`, `execution`).
- CLI entrypoint is `minigu-cli`, and end-to-end integration tests are in `minigu-test`.
- Documentation assets are under `docs/`; utility scripts live in `scripts/`.

## Build, Test, and Development Commands
- `cargo fmt --check` — enforce Rust formatting using workspace `rustfmt.toml`.
- `cargo clippy --tests --features "${DEFAULT_FEATURES:-std,serde,miette}" --no-deps` with `RUSTFLAGS=-Dwarnings` — lint and reject warnings.
- `cargo build --features "${DEFAULT_FEATURES:-std,serde,miette}"` — compile the full workspace.
- `cargo nextest run --features "${DEFAULT_FEATURES:-std,serde,miette}"` — run the primary test suite quickly.
- `cargo test --features "${DEFAULT_FEATURES:-std,serde,miette}" --doc` — execute doc tests.
- `scripts/run_ci.sh` — runs formatting, linting, build, nextest, and docs in CI order.
- `cargo run -- shell` (or `-r`) — start the interactive MiniGU shell for manual checks.

## Coding Style & Naming Conventions
- Follow Rust 2024 edition defaults: 4-space indentation, `snake_case` for functions/modules, `CamelCase` for types, and `SCREAMING_SNAKE_CASE` for constants.
- Keep public APIs documented with `///` docs; prefer `anyhow`/`thiserror` for errors and `miette` for diagnostics when applicable.
- Maintain Clippy cleanliness; avoid `unsafe` unless necessary and justified inline.
- Preserve existing module boundaries in each crate; place shared utilities in the relevant `common` or crate-local `utils` modules rather than new top-level crates.

## Testing Guidelines
- Prefer `cargo nextest run` during development; keep `cargo test --doc` green for user-facing examples.
- Co-locate unit tests in `#[cfg(test)] mod tests` blocks beside the code; place integration or scenario tests in `minigu-test` or crate-level `tests/` directories.
- When adding fixtures or snapshots (e.g., via `insta`), keep them small and deterministic; update snapshots with intent and review diffs.
- Add tests for new behavior and regression cases before filing PRs; aim for meaningful coverage of parsing, planning, and storage paths impacted by your change.

## Commit & Pull Request Guidelines
- Use concise, present-tense commit messages (e.g., `Add edge range scan API`); group related edits per commit.
- Reference issues in PR descriptions (`Closes #123`) and summarize motivation, design choices, and testing performed (commands and results).
- Include screenshots or logs only when they clarify behavior (CLI transcripts are acceptable).
- Keep PRs focused: prefer smaller, reviewable changes over broad refactors; note any follow-ups explicitly.

## Security & Configuration Notes
- Do not commit credentials or large datasets; prefer generated fixtures and `.gitignore` defaults (e.g., `target/`).
- Respect feature flags: default features are `std,serde,miette`; ensure optional paths compile by testing with and without `DEFAULT_FEATURES` overrides.
- When touching dependency versions or lint settings, align with `deny.toml`, `taplo.toml`, and `typos.toml` rules to keep CI green.
