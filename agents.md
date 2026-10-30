# Agent instructions

Run `cargo bake agent:context:install` to install dependency context and skills. Follow the Base conventions and relevant installed skills under `.agents/`.

Before planning or changing behavior, discover and read applicable `design/*.md`. Use `base-design-review` to assess plans and changes, and `base-design-author` when user requirements are introduced, changed, or removed. Review design compliance before opening or updating a pull request.

Keep runtime dependencies independent of concrete schedulers, `http`, `http-body`, Hyper and third-party protocol engines. Development tooling belongs in the private `bake/` package, which depends on `base-project` for shared tooling.

Before opening a pull request, run `cargo bake test`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings` and `cargo bake test:coverage --all-targets true --all-features true`. The crates declare Rust 1.87.
