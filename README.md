# OMP Discord Bridge

Rust Discord gateway for persistent, thread-scoped OMP sessions.

A permitted user mentions the bot in an allowed channel, the bot creates a Discord thread, and that thread becomes the durable interface to one OMP session. See `AGENTS.md` for the behavioral, security, and verification invariants.

## Prerequisites

Install Rust with `rustup`:

```bash
rustup toolchain install stable --profile minimal --component rustfmt,clippy
```

The checked-in `rust-toolchain.toml` selects stable Rust and the required components automatically.

## Development checks

Run from the repository root:

```bash
cargo fmt -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --locked
```

Smoke the executable metadata path:

```bash
cargo run -- --version
cargo run -- --help
```

`Cargo.lock` belongs in version control because this repository builds an application. Runtime secrets and SQLite state are local and ignored by Git.
