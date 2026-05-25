use anyhow::Result;

#[derive(Clone, Debug)]
pub struct Config {
    pub kafka_bootstrap_servers: String,
    pub kafka_group_id: String,
    pub kafka_topic: String,
    pub kafka_dlq_topic: String,
    pub cassandra_hosts: Vec<String>,
    pub cassandra_keyspace: String,
    pub cassandra_consistency_write: String,
    pub cassandra_consistency_read: String,
    pub schema_registry_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        Ok(Self {
            kafka_bootstrap_servers: std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            kafka_group_id: std::env::var("KAFKA_GROUP_ID")
                .unwrap_or_else(|_| "warehouse-state-consumer".to_string()),
            kafka_topic: std::env::var("KAFKA_TOPIC")
                .unwrap_or_else(|_| "warehouse-events".to_string()),
            kafka_dlq_topic: std::env::var("KAFKA_DLQ_TOPIC")
                .unwrap_or_else(|_| "warehouse-events-dlq".to_string()),
            cassandra_hosts: std::env::var("CASSANDRA_HOSTS")
                .unwrap_or_else(|_| "localhost".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            cassandra_keyspace: std::env::var("CASSANDRA_KEYSPACE")
                .unwrap_or_else(|_| "warehouse".to_string()),
            cassandra_consistency_write: std::env::var("CASSANDRA_CONSISTENCY_WRITE")
                .unwrap_or_else(|_| "QUORUM".to_string()),
            cassandra_consistency_read: std::env::var("CASSANDRA_CONSISTENCY_READ")
                .unwrap_or_else(|_| "ONE".to_string()),
            schema_registry_url: std::env::var("SCHEMA_REGISTRY_URL")
                .unwrap_or_else(|_| "http://schema-registry:8081".to_string()),
        })
    }
}
