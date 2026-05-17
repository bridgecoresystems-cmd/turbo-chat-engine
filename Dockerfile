# Stage 1: build
FROM rust:1.87-slim AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies — copy manifests first
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ ./proto/
RUN mkdir -p src && echo "fn main() {}" > src/main.rs \
 && echo "fn main() {}" > src/bin/stress.rs
RUN cargo build --release
RUN rm -rf src/

# Build actual source
COPY src/ ./src/
RUN touch src/main.rs && cargo build --release

# Stage 2: minimal runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/turbo_chat_engine /usr/local/bin/turbo_chat_engine

EXPOSE 8080

ENV RUST_LOG=turbo_chat_engine=info

CMD ["turbo_chat_engine"]
