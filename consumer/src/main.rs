mod cassandra;
mod config;
mod events;
mod handlers;
mod http;
mod kafka;
mod metrics;
mod schema_registry;

use std::sync::Arc;

use anyhow::Result;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = config::Config::from_env()?;
    info!("Starting warehouse consumer, group={}", config.kafka_group_id);

    let write_cl = cassandra::parse_consistency(&config.cassandra_consistency_write);
    let read_cl = cassandra::parse_consistency(&config.cassandra_consistency_read);
    let db = cassandra::CassandraClient::new(
        &config.cassandra_hosts,
        &config.cassandra_keyspace,
        write_cl,
        read_cl,
    )
    .await?;
    let db = Arc::new(db);

    http::CASSANDRA_HEALTHY.store(true, std::sync::atomic::Ordering::Relaxed);

    let registry = Arc::new(schema_registry::SchemaRegistry::new(&config.schema_registry_url));
    let v1_schema = include_str!("../../schemas/product_received_v1.avsc");
    let v2_schema = include_str!("../../schemas/product_received_v2.avsc");

    if let Err(e) = registry.register("product_received-v1", v1_schema).await {
        warn!("Failed to register v1 schema: {}", e);
    }
    if let Err(e) = registry.register("product_received-v2", v2_schema).await {
        warn!("Failed to register v2 schema: {}", e);
    }

    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .unwrap_or_else(|_| "9090".to_string())
        .parse()
        .unwrap_or(9090);

    tokio::select! {
        _ = http::run_http_server(metrics_port) => {},
        r = kafka::run_consumer(config, db, registry) => { r? },
    }

    Ok(())
}
