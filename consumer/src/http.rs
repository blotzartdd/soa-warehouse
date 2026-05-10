use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::info;

use crate::metrics::render_metrics;

pub static KAFKA_HEALTHY: AtomicBool = AtomicBool::new(false);
pub static CASSANDRA_HEALTHY: AtomicBool = AtomicBool::new(false);

async fn health() -> impl IntoResponse {
    let kafka_ok = KAFKA_HEALTHY.load(Ordering::Relaxed);
    let cassandra_ok = CASSANDRA_HEALTHY.load(Ordering::Relaxed);

    if kafka_ok && cassandra_ok {
        (StatusCode::OK, r#"{"status":"ok"}"#)
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"status":"unavailable"}"#,
        )
    }
}

async fn metrics() -> impl IntoResponse {
    render_metrics()
}

pub async fn run_http_server(port: u16) {
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics));

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind HTTP server");

    info!("HTTP server listening on :{}", port);
    axum::serve(listener, app).await.expect("HTTP server failed");
}
