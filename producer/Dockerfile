FROM rust:1.91 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential cmake libssl-dev pkg-config \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir producer
COPY producer/Cargo.toml ./producer

COPY schemas ./schemas
COPY producer/src ./producer/src

RUN cargo build --manifest-path ./producer/Cargo.toml

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates libssl3 \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/producer/target/debug/producer .
CMD ["./producer"]
