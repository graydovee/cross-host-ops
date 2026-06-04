FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src

RUN cargo build --release --bin xho --bin xhod

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssh-client tar \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/xho /usr/local/bin/xho
COPY --from=builder /app/target/release/xhod /usr/local/bin/xhod
COPY config.example.toml /etc/xho/config.toml

EXPOSE 2222

CMD ["/usr/local/bin/xhod", "--config", "/etc/xho/config.toml", "--origin", "external"]
