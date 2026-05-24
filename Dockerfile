# Stage 1: build
FROM rust:1.95-slim AS builder

# Use bundled protoc binary — avoids apt-get (no internet in build environment)
COPY protoc /usr/local/bin/protoc

WORKDIR /app

# Vendored dependencies — no internet needed
COPY vendor/ ./vendor/
COPY .cargo/ ./.cargo/

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ ./proto/
COPY .sqlx/ ./.sqlx/

ENV SQLX_OFFLINE=true
RUN mkdir -p src/bin && echo "fn main() {}" > src/main.rs \
 && echo "fn main() {}" > src/bin/stress.rs
RUN cargo build --release --offline
RUN rm -rf src/

# Build actual source
COPY src/ ./src/
RUN touch src/main.rs && cargo build --release --offline

# Stage 2: minimal runtime
FROM debian:bookworm-slim

# Copy CA certs from builder instead of running apt-get (avoids network issues in build)
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/release/turbo_chat_engine /usr/local/bin/turbo_chat_engine

EXPOSE 8080

ENV RUST_LOG=turbo_chat_engine=info

CMD ["turbo_chat_engine"]
