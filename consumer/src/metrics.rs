use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, GaugeVec,
    HistogramVec, TextEncoder,
};

lazy_static! {
    pub static ref EVENTS_PROCESSED: CounterVec = register_counter_vec!(
        "events_processed_total",
        "Total number of processed events",
        &["event_type"]
    )
    .unwrap();

    pub static ref EVENT_DURATION: HistogramVec = register_histogram_vec!(
        "event_processing_duration_seconds",
        "Event processing duration in seconds",
        &["event_type"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
    )
    .unwrap();

    pub static ref CASSANDRA_WRITE_ERRORS: CounterVec = register_counter_vec!(
        "cassandra_write_errors_total",
        "Total Cassandra write errors",
        &["operation"]
    )
    .unwrap();

    pub static ref CONSUMER_LAG: GaugeVec = register_gauge_vec!(
        "consumer_lag",
        "Kafka consumer lag per partition",
        &["topic", "partition"]
    )
    .unwrap();
}

pub fn render_metrics() -> String {
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    encoder.encode_to_string(&families).unwrap_or_default()
}
