use apache_avro::types::Value as AvroValue;
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use chrono::Utc;
use dotenvy::dotenv;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    producer: Arc<FutureProducer>,
    topic: String,
    v1_schema: Arc<apache_avro::Schema>,
    v1_schema_id: Option<u32>,
    v2_schema: Arc<apache_avro::Schema>,
    v2_schema_id: Option<u32>,
}

#[derive(Deserialize)]
struct PublishRequest {
    event_type: String,
    payload: serde_json::Value,
}

#[derive(Deserialize)]
struct AvroV1Request {
    product_id: String,
    zone_id: String,
    quantity: i64,
}

#[derive(Deserialize)]
struct AvroV2Request {
    product_id: String,
    zone_id: String,
    quantity: i64,
    supplier_id: Option<String>,
}

#[derive(Serialize)]
struct PublishResponse {
    event_id: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

async fn publish_event(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PublishRequest>,
) -> Result<Json<PublishResponse>, (StatusCode, Json<ErrorResponse>)> {
    let event_id = Uuid::new_v4().to_string();
    let event = serde_json::json!({
        "event_id": event_id,
        "event_type": req.event_type,
        "timestamp": Utc::now().to_rfc3339(),
        "sequence_number": null,
        "payload": req.payload,
    });

    let body = serde_json::to_string(&event).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e.to_string() }),
        )
    })?;

    state
        .producer
        .send(
            FutureRecord::to(&state.topic).key(&event_id).payload(&body),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: e.to_string() }),
            )
        })?;

    info!(event_id = %event_id, event_type = %req.event_type, "event published");
    Ok(Json(PublishResponse { event_id }))
}

async fn publish_avro_v1(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AvroV1Request>,
) -> Result<Json<PublishResponse>, (StatusCode, Json<ErrorResponse>)> {
    let schema_id = state.v1_schema_id.ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "schema registry unavailable".to_string(),
            }),
        )
    })?;

    let event_id = Uuid::new_v4().to_string();
    let timestamp = Utc::now().to_rfc3339();

    let value = AvroValue::Record(vec![
        ("event_id".to_string(), AvroValue::String(event_id.clone())),
        (
            "event_type".to_string(),
            AvroValue::String("PRODUCT_RECEIVED".to_string()),
        ),
        (
            "timestamp".to_string(),
            AvroValue::String(timestamp.clone()),
        ),
        (
            "product_id".to_string(),
            AvroValue::String(req.product_id.clone()),
        ),
        (
            "zone_id".to_string(),
            AvroValue::String(req.zone_id.clone()),
        ),
        ("quantity".to_string(), AvroValue::Long(req.quantity)),
    ]);

    let bytes = confluent_encode(schema_id, &state.v1_schema, value).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e.to_string() }),
        )
    })?;

    state
        .producer
        .send(
            FutureRecord::to(&state.topic).key(&event_id).payload(&bytes),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: e.to_string() }),
            )
        })?;

    info!(event_id = %event_id, schema_id, "avro v1 event published");
    Ok(Json(PublishResponse { event_id }))
}

async fn publish_avro_v2(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AvroV2Request>,
) -> Result<Json<PublishResponse>, (StatusCode, Json<ErrorResponse>)> {
    let schema_id = state.v2_schema_id.ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "schema registry unavailable".to_string(),
            }),
        )
    })?;

    let event_id = Uuid::new_v4().to_string();
    let timestamp = Utc::now().to_rfc3339();

    let supplier_val = match req.supplier_id.as_deref() {
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        Some(s) => AvroValue::Union(1, Box::new(AvroValue::String(s.to_string()))),
    };

    let value = AvroValue::Record(vec![
        ("event_id".to_string(), AvroValue::String(event_id.clone())),
        (
            "event_type".to_string(),
            AvroValue::String("PRODUCT_RECEIVED".to_string()),
        ),
        (
            "timestamp".to_string(),
            AvroValue::String(timestamp.clone()),
        ),
        (
            "product_id".to_string(),
            AvroValue::String(req.product_id.clone()),
        ),
        (
            "zone_id".to_string(),
            AvroValue::String(req.zone_id.clone()),
        ),
        ("quantity".to_string(), AvroValue::Long(req.quantity)),
        ("supplier_id".to_string(), supplier_val),
    ]);

    let bytes = confluent_encode(schema_id, &state.v2_schema, value).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e.to_string() }),
        )
    })?;

    state
        .producer
        .send(
            FutureRecord::to(&state.topic).key(&event_id).payload(&bytes),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: e.to_string() }),
            )
        })?;

    info!(event_id = %event_id, schema_id, supplier_id = ?req.supplier_id, "avro v2 event published");
    Ok(Json(PublishResponse { event_id }))
}

fn confluent_encode(
    schema_id: u32,
    schema: &apache_avro::Schema,
    value: AvroValue,
) -> anyhow::Result<Vec<u8>> {
    let avro_bytes = apache_avro::to_avro_datum(schema, value)?;
    let mut buf = Vec::with_capacity(5 + avro_bytes.len());
    buf.push(0x00);
    buf.extend_from_slice(&schema_id.to_be_bytes());
    buf.extend_from_slice(&avro_bytes);
    Ok(buf)
}

async fn register_schema(
    registry_url: &str,
    subject: &str,
    schema_json: &str,
) -> anyhow::Result<u32> {
    let client = reqwest::Client::new();
    let body = serde_json::json!({"schema": schema_json});
    let resp = client
        .post(format!("{}/subjects/{}/versions", registry_url, subject))
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .json(&body)
        .send()
        .await?;
    let json: serde_json::Value = resp.json().await?;
    json["id"]
        .as_u64()
        .map(|v| v as u32)
        .ok_or_else(|| anyhow::anyhow!("no id in registry response: {}", json))
}

async fn register_schema_with_retry(
    registry_url: &str,
    subject: &str,
    schema_json: &str,
) -> Option<u32> {
    for attempt in 1u32..=30 {
        match register_schema(registry_url, subject, schema_json).await {
            Ok(id) => {
                info!(id, subject, "schema registered");
                return Some(id);
            }
            Err(e) => {
                warn!(attempt, subject, "schema registration failed: {}", e);
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
    warn!(subject, "gave up registering schema after 30 attempts");
    None
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let bootstrap = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:9092".to_string());
    let topic = std::env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| "warehouse-events".to_string());
    let registry_url = std::env::var("SCHEMA_REGISTRY_URL")
        .unwrap_or_else(|_| "http://schema-registry:8081".to_string());
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()?;

    let v1_json = include_str!("../../schemas/product_received_v1.avsc");
    let v2_json = include_str!("../../schemas/product_received_v2.avsc");

    let v1_schema = Arc::new(apache_avro::Schema::parse_str(v1_json)?);
    let v2_schema = Arc::new(apache_avro::Schema::parse_str(v2_json)?);

    let v1_schema_id = register_schema_with_retry(&registry_url, "product_received-v1", v1_json).await;
    let v2_schema_id = register_schema_with_retry(&registry_url, "product_received-v2", v2_json).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "5000")
        .create()?;

    let state = Arc::new(AppState {
        producer: Arc::new(producer),
        topic,
        v1_schema,
        v1_schema_id,
        v2_schema,
        v2_schema_id,
    });

    let app = Router::new()
        .route("/api/events", post(publish_event))
        .route("/api/events/avro/v1", post(publish_avro_v1))
        .route("/api/events/avro/v2", post(publish_avro_v2))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!("Producer listening on :{}", port);
    axum::serve(listener, app).await?;
    Ok(())
}
