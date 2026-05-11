FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src

RUN cargo build --release --bin rhop --bin rhopd

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssh-client tar \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/rhop /usr/local/bin/rhop
COPY --from=builder /app/target/release/rhopd /usr/local/bin/rhopd
COPY config.example.toml /etc/rhop/config.toml

EXPOSE 2222

CMD ["/usr/local/bin/rhopd", "--config", "/etc/rhop/config.toml", "--origin", "external"]
