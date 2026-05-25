use anyhow::Result;
use apache_avro::Schema;
use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;

pub struct SchemaRegistry {
    url: String,
    client: Client,
    cache: DashMap<u32, Schema>,
}

#[derive(Deserialize)]
struct RegisterResponse {
    id: u32,
}

#[derive(Deserialize)]
struct SchemaResponse {
    schema: String,
}

impl SchemaRegistry {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            client: Client::new(),
            cache: DashMap::new(),
        }
    }

    pub async fn register(&self, subject: &str, schema_json: &str) -> Result<u32> {
        let body = serde_json::json!({"schema": schema_json});
        let resp = self
            .client
            .post(format!("{}/subjects/{}/versions", self.url, subject))
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&body)
            .send()
            .await?;
        let reg: RegisterResponse = resp.json().await?;
        Ok(reg.id)
    }

    pub async fn get_schema(&self, schema_id: u32) -> Result<Schema> {
        if let Some(s) = self.cache.get(&schema_id) {
            return Ok(s.clone());
        }
        let resp = self
            .client
            .get(format!("{}/schemas/ids/{}", self.url, schema_id))
            .send()
            .await?;
        let sr: SchemaResponse = resp.json().await?;
        let schema = Schema::parse_str(&sr.schema)?;
        self.cache.insert(schema_id, schema.clone());
        Ok(schema)
    }
}
