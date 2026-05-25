FROM rust:1.91 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential cmake libssl-dev pkg-config \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir consumer
COPY consumer/Cargo.toml ./consumer

COPY schemas ./schemas
COPY migrations ./migrations
COPY consumer/src ./consumer/src

RUN cargo build --manifest-path ./consumer/Cargo.toml


FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates libssl3 \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/consumer/target/debug/consumer .
CMD ["./consumer"]
