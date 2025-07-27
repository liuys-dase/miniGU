# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building
```bash
cargo build --features "std,serde,miette"                    # Debug build
cargo build --release --features "std,serde,miette"         # Release build
```

### Running the Interactive Shell
```bash
cargo run -- shell                                          # Debug mode
cargo run -r -- shell                                       # Release mode
```

### Testing
```bash
cargo nextest run --features "std,serde,miette"             # Run unit tests with nextest
cargo test --features "std,serde,miette" --doc              # Run documentation tests
```

### Code Quality
```bash
cargo fmt --check                                           # Check formatting
cargo fmt                                                   # Format code
cargo clippy --tests --features "std,serde,miette" --no-deps # Run clippy lints
taplo fmt --check --diff                                    # Check TOML formatting
taplo fmt                                                    # Format TOML files
```

### Documentation
```bash
cargo doc --lib --no-deps --features "std,serde,miette"     # Build documentation
```

### CI Script
```bash
./scripts/run_ci.sh                                         # Run full CI pipeline locally
```

## Architecture Overview

MiniGU is a graph database implemented in Rust, structured as a Cargo workspace with modular components:

### Core Components
- **minigu/main**: Public API and main database engine
- **minigu/parser**: GQL (Graph Query Language) parser with lexer and AST
- **minigu/storage**: Storage layer with both OLTP and OLAP support
- **minigu/execution**: Query execution engine with operators and evaluators
- **minigu/planner**: Query planner and optimizer
- **minigu/binder**: Binds parsed AST to logical structures
- **minigu/catalog**: Schema and metadata management
- **minigu/context**: Database and session context management
- **minigu/ir**: Intermediate representation for queries
- **minigu/common**: Shared data types and utilities

### Client Tools
- **minigu-cli**: Command-line interface with interactive shell
- **minigu-test**: System-level integration tests

### Key Features
- GQL standard compliance for graph queries
- Interactive shell environment for graph operations
- Modular architecture separating parsing, planning, and execution
- Support for both transactional (OLTP) and analytical (OLAP) workloads
- Comprehensive test suite with snapshot testing

### Storage Architecture
The storage layer supports two modes:
- **TP (Transactional Processing)**: Memory-based graph with transaction support
- **AP (Analytical Processing)**: OLAP graph for analytical queries

### Testing Strategy
- Unit tests in individual modules
- Integration tests in `minigu-test/`
- Snapshot testing with `insta` for parser and execution verification
- Test data in `resources/gql/` covering various GQL scenarios

## Working with the Codebase

### Parser Development
The parser is located in `minigu/parser/` and implements a complete GQL parser with:
- Lexical analysis using the `logos` crate
- Recursive descent parser with precedence handling
- Comprehensive AST definitions in `ast/` module
- Extensive snapshot tests for parser verification

### Adding New GQL Features
1. Update the grammar in `minigu/parser/src/parser/`
2. Add corresponding AST nodes in `minigu/parser/src/ast/`
3. Implement binding logic in `minigu/binder/`
4. Add execution support in `minigu/execution/`
5. Update the planner if needed in `minigu/planner/`

### Running Single Tests
```bash
cargo test -p minigu-test test_name                         # Run specific test  
cargo test -p gql-parser parser_test                        # Run parser tests
cargo test -p minigu-storage isolation_level_test           # Run storage isolation tests
```

### Feature Configuration
Most commands require the default feature set. The project defines:
```bash
DEFAULT_FEATURES="std,serde,miette"                         # Standard feature set used in CI
```

## Technical Details

### Rust Toolchain
- Uses Rust 2024 edition with nightly toolchain (nightly-2025-01-17)
- Requires nightly features including `impl_trait_in_assoc_type`
- Uses `#![feature(duration_millis_float)]` in CLI for performance metrics

### Transaction Processing
The storage layer implements ACID transactions with multiple isolation levels:
- **Serializable**: Full conflict detection with read-write conflict prevention
- **Isolation testing**: Located in `minigu/storage/tests/tp/isolation_level_test.rs`
- **WAL (Write-Ahead Logging)**: Implemented in `minigu/storage/src/common/wal/`
  

