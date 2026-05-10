use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::cassandra::CassandraClient;
use crate::config::Config;
use crate::events::{self, WarehouseEvent};
use crate::http::KAFKA_HEALTHY;
use crate::metrics::{CASSANDRA_WRITE_ERRORS, CONSUMER_LAG, EVENT_DURATION, EVENTS_PROCESSED};
use crate::schema_registry::SchemaRegistry;

#[derive(Serialize)]
struct DlqMessage<'a> {
    original_event: &'a WarehouseEvent,
    error_reason: String,
    error_code: &'static str,
    failed_at: String,
    kafka_metadata: DlqMetadata,
}

#[derive(Serialize)]
struct DlqMetadata {
    partition: i32,
    offset: i64,
}

pub async fn run_consumer(
    config: Config,
    db: Arc<CassandraClient>,
    registry: Arc<SchemaRegistry>,
) -> Result<()> {
    let consumer: StreamConsumer = build_consumer_with_retry(&config).await?;
    consumer.subscribe(&[&config.kafka_topic])?;

    let dlq_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()?;

    KAFKA_HEALTHY.store(true, std::sync::atomic::Ordering::Relaxed);

    let committed_offsets: Arc<DashMap<i32, i64>> = Arc::new(DashMap::new());

    {
        let bootstrap = config.kafka_bootstrap_servers.clone();
        let group = format!("{}-lag-monitor", config.kafka_group_id);
        let topic = config.kafka_topic.clone();
        let offsets = committed_offsets.clone();
        tokio::task::spawn_blocking(move || {
            run_lag_monitor(bootstrap, group, topic, offsets);
        });
    }

    info!(
        group_id = %config.kafka_group_id,
        topic = %config.kafka_topic,
        "Kafka consumer started"
    );

    loop {
        match consumer.recv().await {
            Err(e) => {
                error!("Kafka recv error: {}", e);
            }
            Ok(msg) => {
                let partition = msg.partition();
                let offset = msg.offset();

                let payload_bytes = match msg.payload() {
                    Some(b) => b,
                    None => {
                        warn!(partition, offset, "empty message, skipping");
                        consumer.commit_message(&msg, CommitMode::Async)?;
                        committed_offsets.insert(partition, offset);
                        continue;
                    }
                };

                let event: WarehouseEvent =
                    if payload_bytes.first() == Some(&0x00) && payload_bytes.len() > 5 {
                        let schema_id = u32::from_be_bytes([
                            payload_bytes[1],
                            payload_bytes[2],
                            payload_bytes[3],
                            payload_bytes[4],
                        ]);
                        match registry.get_schema(schema_id).await {
                            Ok(schema) => {
                                match events::decode_avro_event(&payload_bytes[5..], &schema) {
                                    Ok(e) => e,
                                    Err(e) => {
                                        error!(partition, offset, "avro decode error: {}", e);
                                        send_to_dlq(
                                            &dlq_producer,
                                            &config.kafka_dlq_topic,
                                            payload_bytes,
                                            format!("avro decode error: {}", e),
                                            "DESERIALIZE_ERROR",
                                            partition,
                                            offset,
                                        )
                                        .await;
                                        consumer.commit_message(&msg, CommitMode::Async)?;
                                        committed_offsets.insert(partition, offset);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    partition,
                                    offset,
                                    schema_id,
                                    "schema not found: {}",
                                    e
                                );
                                send_to_dlq(
                                    &dlq_producer,
                                    &config.kafka_dlq_topic,
                                    payload_bytes,
                                    format!("schema not found {}: {}", schema_id, e),
                                    "DESERIALIZE_ERROR",
                                    partition,
                                    offset,
                                )
                                .await;
                                consumer.commit_message(&msg, CommitMode::Async)?;
                                committed_offsets.insert(partition, offset);
                                continue;
                            }
                        }
                    } else {
                        match serde_json::from_slice(payload_bytes) {
                            Ok(e) => e,
                            Err(e) => {
                                error!(partition, offset, "failed to deserialize event: {}", e);
                                send_to_dlq(
                                    &dlq_producer,
                                    &config.kafka_dlq_topic,
                                    payload_bytes,
                                    format!("deserialize error: {}", e),
                                    "DESERIALIZE_ERROR",
                                    partition,
                                    offset,
                                )
                                .await;
                                consumer.commit_message(&msg, CommitMode::Async)?;
                                committed_offsets.insert(partition, offset);
                                continue;
                            }
                        }
                    };

                info!(
                    event_id = %event.event_id,
                    event_type = %event.event_type,
                    partition,
                    offset,
                    "processing event"
                );

                let event_type = event.event_type.clone();
                let timer = EVENT_DURATION
                    .with_label_values(&[&event_type])
                    .start_timer();

                match crate::handlers::process_event(&event, &db, partition, offset).await {
                    Ok(_) => {
                        timer.observe_duration();
                        EVENTS_PROCESSED.with_label_values(&[&event_type]).inc();
                        consumer.commit_message(&msg, CommitMode::Async)?;
                        committed_offsets.insert(partition, offset);
                    }
                    Err(e) => {
                        timer.observe_duration();
                        let error_str = e.to_string();
                        let error_code = classify_error(&error_str);
                        if error_code == "PROCESSING_ERROR" {
                            CASSANDRA_WRITE_ERRORS.with_label_values(&[&event_type]).inc();
                        }
                        error!(
                            event_id = %event.event_id,
                            event_type = %event_type,
                            partition,
                            offset,
                            error_code,
                            "processing failed: {}",
                            error_str
                        );
                        send_event_to_dlq(
                            &dlq_producer,
                            &config.kafka_dlq_topic,
                            &event,
                            error_str,
                            error_code,
                            partition,
                            offset,
                        )
                        .await;
                        consumer.commit_message(&msg, CommitMode::Async)?;
                        committed_offsets.insert(partition, offset);
                    }
                }
            }
        }
    }
}

fn classify_error(msg: &str) -> &'static str {
    if msg.contains("VALIDATION_ERROR") {
        "VALIDATION_ERROR"
    } else if msg.contains("BUSINESS_ERROR") {
        "BUSINESS_ERROR"
    } else {
        "PROCESSING_ERROR"
    }
}

fn run_lag_monitor(
    bootstrap: String,
    group: String,
    topic: String,
    committed: Arc<DashMap<i32, i64>>,
) {
    let consumer: BaseConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", &group)
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to create lag monitor consumer: {}", e);
            return;
        }
    };

    loop {
        std::thread::sleep(Duration::from_secs(15));
        if let Ok(metadata) = consumer.fetch_metadata(Some(&topic), Duration::from_secs(5)) {
            for t in metadata.topics() {
                for p in t.partitions() {
                    let pid = p.id();
                    if let Ok((_, high)) =
                        consumer.fetch_watermarks(&topic, pid, Duration::from_secs(5))
                    {
                        let current = committed.get(&pid).map(|v| *v).unwrap_or(0);
                        let lag = (high - current - 1).max(0) as f64;
                        CONSUMER_LAG
                            .with_label_values(&[&topic, &pid.to_string()])
                            .set(lag);
                    }
                }
            }
        }
    }
}

async fn send_event_to_dlq(
    producer: &FutureProducer,
    topic: &str,
    event: &WarehouseEvent,
    error_reason: String,
    error_code: &'static str,
    partition: i32,
    offset: i64,
) {
    let msg = DlqMessage {
        original_event: event,
        error_reason,
        error_code,
        failed_at: Utc::now().to_rfc3339(),
        kafka_metadata: DlqMetadata { partition, offset },
    };
    let body = match serde_json::to_string(&msg) {
        Ok(b) => b,
        Err(e) => {
            error!("failed to serialize DLQ message: {}", e);
            return;
        }
    };
    if let Err((e, _)) = producer
        .send(
            FutureRecord::to(topic).key(&event.event_id).payload(&body),
            Duration::from_secs(5),
        )
        .await
    {
        error!("failed to send to DLQ: {}", e);
    } else {
        warn!(event_id = %event.event_id, "event sent to DLQ");
    }
}

async fn send_to_dlq(
    producer: &FutureProducer,
    topic: &str,
    raw_payload: &[u8],
    error_reason: String,
    error_code: &'static str,
    partition: i32,
    offset: i64,
) {
    let body = serde_json::json!({
        "original_event": String::from_utf8_lossy(raw_payload),
        "error_reason": error_reason,
        "error_code": error_code,
        "failed_at": Utc::now().to_rfc3339(),
        "kafka_metadata": { "partition": partition, "offset": offset }
    })
    .to_string();

    if let Err((e, _)) = producer
        .send(
            FutureRecord::to(topic).key("unknown").payload(&body),
            Duration::from_secs(5),
        )
        .await
    {
        error!("failed to send to DLQ: {}", e);
    }
}

async fn build_consumer_with_retry(config: &Config) -> Result<StreamConsumer> {
    let mut attempt = 0u32;
    loop {
        match ClientConfig::new()
            .set("group.id", &config.kafka_group_id)
            .set("bootstrap.servers", &config.kafka_bootstrap_servers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "30000")
            .set("heartbeat.interval.ms", "3000")
            .create::<StreamConsumer>()
        {
            Ok(c) => return Ok(c),
            Err(e) if attempt < 20 => {
                attempt += 1;
                warn!("Kafka consumer creation failed (attempt {}): {}", attempt, e);
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}
