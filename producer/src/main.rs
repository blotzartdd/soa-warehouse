use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use chrono::Utc;
use dotenvy::dotenv;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    producer: Arc<FutureProducer>,
    topic: String,
}

#[derive(Deserialize)]
struct PublishRequest {
    event_type: String,
    payload: serde_json::Value,
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
            FutureRecord::to(&state.topic)
                .key(&event_id)
                .payload(&body),
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
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "5000")
        .create()?;

    let state = Arc::new(AppState {
        producer: Arc::new(producer),
        topic,
    });

    let app = Router::new()
        .route("/api/events", post(publish_event))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!("Producer listening on :{}", port);
    axum::serve(listener, app).await?;
    Ok(())
}
