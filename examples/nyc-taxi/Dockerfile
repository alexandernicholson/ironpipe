FROM rust:1.93-bookworm AS builder

WORKDIR /app

# Copy workspace
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
COPY examples/nyc-taxi/ ./examples/nyc-taxi/

# Build
RUN cargo build --release --manifest-path examples/nyc-taxi/Cargo.toml

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/nyc-taxi-demo /usr/local/bin/nyc-taxi-demo

EXPOSE 5000

ENTRYPOINT ["nyc-taxi-demo"]
