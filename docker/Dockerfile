# Multi-stage build with cargo-chef dependency caching → distroless runtime.
# Builds BOTH binaries into one image; compose picks the command per service.

FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin api --bin worker

FROM gcr.io/distroless/cc-debian12 AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/api /usr/local/bin/api
COPY --from=builder /app/target/release/worker /usr/local/bin/worker
# migrations are embedded at compile time; copied only for reference.
COPY --from=builder /app/migrations /app/migrations
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/api"]
