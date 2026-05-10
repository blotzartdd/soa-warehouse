FROM rust:1.78-slim as builder

RUN apt-get update && apt-get install -y cmake libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml ./
COPY consumer/Cargo.toml ./consumer/
COPY producer/Cargo.toml ./producer/

RUN mkdir -p consumer/src producer/src \
    && echo "fn main() {}" > consumer/src/main.rs \
    && echo "fn main() {}" > producer/src/main.rs

RUN cargo build --release -p producer && rm -rf consumer/src producer/src

COPY producer/src ./producer/src
RUN touch producer/src/main.rs && cargo build --release -p producer

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/producer .
CMD ["./producer"]
