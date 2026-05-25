# Stage 1: build
FROM rust:1.95-slim AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests and proto first — Docker caches this layer
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ ./proto/
COPY .sqlx/ ./.sqlx/

# Compile dependencies with a dummy main (cached layer — only rebuilds when Cargo.toml changes)
RUN mkdir -p src/bin \
 && echo "fn main() {}" > src/main.rs \
 && echo "fn main() {}" > src/bin/stress.rs
ENV SQLX_OFFLINE=true
RUN cargo build --release
RUN rm -rf src/

# Build actual source
COPY src/ ./src/
RUN touch src/main.rs && cargo build --release

# Stage 2: minimal runtime
FROM debian:trixie-slim

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/release/turbo_chat_engine /usr/local/bin/turbo_chat_engine

EXPOSE 8080

ENV RUST_LOG=turbo_chat_engine=info

CMD ["turbo_chat_engine"]
