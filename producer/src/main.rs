use apache_avro::types::Value as AvroValue;
use axum::{
    body::Body,
    extract::{MatchedPath, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use dotenvy::dotenv;
use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec, TextEncoder};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

lazy_static! {
    static ref HTTP_REQUESTS_TOTAL: CounterVec = register_counter_vec!(
        "http_requests_total",
        "Total HTTP requests",
        &["method", "endpoint", "status"]
    ).unwrap();

    static ref HTTP_REQUEST_ERRORS_TOTAL: CounterVec = register_counter_vec!(
        "http_request_errors_total",
        "Total HTTP request errors",
        &["method", "endpoint", "error_type"]
    ).unwrap();

    static ref HTTP_REQUEST_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "http_request_duration_seconds",
        "HTTP request duration in seconds",
        &["method", "endpoint"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
    ).unwrap();
}

async fn metrics_middleware(req: Request<Body>, next: Next) -> Response {
    let method = req.method().to_string();
    let endpoint = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());

    let timer = HTTP_REQUEST_DURATION_SECONDS
        .with_label_values(&[&method, &endpoint])
        .start_timer();

    let response = next.run(req).await;

    timer.observe_duration();

    let status = response.status().as_u16().to_string();
    HTTP_REQUESTS_TOTAL
        .with_label_values(&[&method, &endpoint, &status])
        .inc();

    if response.status().is_server_error() {
        HTTP_REQUEST_ERRORS_TOTAL
            .with_label_values(&[&method, &endpoint, "server_error"])
            .inc();
    } else if response.status().is_client_error() {
        HTTP_REQUEST_ERRORS_TOTAL
            .with_label_values(&[&method, &endpoint, "client_error"])
            .inc();
    }

    response
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, r#"{"status":"ok"}"#)
}

async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    encoder.encode_to_string(&families).unwrap_or_default()
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value as AvroValue;

    const V1_SCHEMA_STR: &str = r#"{
        "type": "record", "name": "ProductReceived", "namespace": "warehouse",
        "fields": [
            {"name": "event_id",   "type": "string"},
            {"name": "event_type", "type": "string"},
            {"name": "timestamp",  "type": "string"},
            {"name": "product_id", "type": "string"},
            {"name": "zone_id",    "type": "string"},
            {"name": "quantity",   "type": "long"}
        ]
    }"#;

    fn sample_record(event_id: &str) -> AvroValue {
        AvroValue::Record(vec![
            ("event_id".to_string(),   AvroValue::String(event_id.to_string())),
            ("event_type".to_string(), AvroValue::String("PRODUCT_RECEIVED".to_string())),
            ("timestamp".to_string(),  AvroValue::String("2024-01-01T00:00:00Z".to_string())),
            ("product_id".to_string(), AvroValue::String("P1".to_string())),
            ("zone_id".to_string(),    AvroValue::String("Z1".to_string())),
            ("quantity".to_string(),   AvroValue::Long(1)),
        ])
    }

    #[test]
    fn confluent_encode_magic_byte_is_zero() {
        let schema = apache_avro::Schema::parse_str(V1_SCHEMA_STR).unwrap();
        let bytes = confluent_encode(1, &schema, sample_record("e1")).unwrap();
        assert_eq!(bytes[0], 0x00);
    }

    #[test]
    fn confluent_encode_schema_id_big_endian() {
        let schema_id: u32 = 0x0102_0304;
        let schema = apache_avro::Schema::parse_str(V1_SCHEMA_STR).unwrap();
        let bytes = confluent_encode(schema_id, &schema, sample_record("e2")).unwrap();
        assert_eq!(u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]), schema_id);
    }

    #[test]
    fn confluent_encode_payload_appended_after_header() {
        let schema = apache_avro::Schema::parse_str(V1_SCHEMA_STR).unwrap();
        let avro_only = apache_avro::to_avro_datum(&schema, sample_record("e3")).unwrap();
        let framed = confluent_encode(1, &schema, sample_record("e3")).unwrap();
        assert_eq!(framed.len(), 5 + avro_only.len());
        assert_eq!(&framed[5..], avro_only.as_slice());
    }

    #[test]
    fn confluent_encode_wrong_value_type_returns_error() {
        let schema = apache_avro::Schema::parse_str(V1_SCHEMA_STR).unwrap();
        let result = confluent_encode(1, &schema, AvroValue::String("not-a-record".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn confluent_encode_header_always_five_bytes() {
        let schema = apache_avro::Schema::parse_str(V1_SCHEMA_STR).unwrap();
        let bytes = confluent_encode(0, &schema, sample_record("e4")).unwrap();
        assert!(bytes.len() >= 5);
        assert_eq!(&bytes[1..5], &[0, 0, 0, 0]);
    }
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
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .route_layer(middleware::from_fn(metrics_middleware))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!("Producer listening on :{}", port);
    axum::serve(listener, app).await?;
    Ok(())
}
