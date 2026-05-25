use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WarehouseEvent {
    pub event_id: String,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub sequence_number: Option<i64>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProductReceivedPayload {
    pub product_id: String,
    pub zone_id: String,
    pub quantity: i64,
    pub supplier_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProductShippedPayload {
    pub product_id: String,
    pub zone_id: String,
    pub quantity: i64,
    pub order_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProductMovedPayload {
    pub product_id: String,
    pub from_zone_id: String,
    pub to_zone_id: String,
    pub quantity: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProductReservedPayload {
    pub product_id: String,
    pub zone_id: String,
    pub quantity: i64,
    pub order_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProductReleasedPayload {
    pub product_id: String,
    pub zone_id: String,
    pub quantity: i64,
    pub order_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct InventoryCountedPayload {
    pub product_id: String,
    pub zone_id: String,
    pub counted_quantity: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OrderItem {
    pub product_id: String,
    pub zone_id: String,
    pub quantity: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OrderCreatedPayload {
    pub order_id: String,
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OrderCompletedPayload {
    pub order_id: String,
}

#[derive(Debug, Clone)]
pub enum EventPayload {
    ProductReceived(ProductReceivedPayload),
    ProductShipped(ProductShippedPayload),
    ProductMoved(ProductMovedPayload),
    ProductReserved(ProductReservedPayload),
    ProductReleased(ProductReleasedPayload),
    InventoryCounted(InventoryCountedPayload),
    OrderCreated(OrderCreatedPayload),
    OrderCompleted(OrderCompletedPayload),
}

impl WarehouseEvent {
    pub fn parse_payload(&self) -> anyhow::Result<EventPayload> {
        let p = self.payload.clone();
        Ok(match self.event_type.as_str() {
            "PRODUCT_RECEIVED"  => EventPayload::ProductReceived(serde_json::from_value(p)?),
            "PRODUCT_SHIPPED"   => EventPayload::ProductShipped(serde_json::from_value(p)?),
            "PRODUCT_MOVED"     => EventPayload::ProductMoved(serde_json::from_value(p)?),
            "PRODUCT_RESERVED"  => EventPayload::ProductReserved(serde_json::from_value(p)?),
            "PRODUCT_RELEASED"  => EventPayload::ProductReleased(serde_json::from_value(p)?),
            "INVENTORY_COUNTED" => EventPayload::InventoryCounted(serde_json::from_value(p)?),
            "ORDER_CREATED"     => EventPayload::OrderCreated(serde_json::from_value(p)?),
            "ORDER_COMPLETED"   => EventPayload::OrderCompleted(serde_json::from_value(p)?),
            unknown => anyhow::bail!("Unknown event type: {}", unknown),
        })
    }
}

pub fn decode_avro_event(data: &[u8], schema: &apache_avro::Schema) -> anyhow::Result<WarehouseEvent> {
    use apache_avro::types::Value;

    let value = apache_avro::from_avro_datum(schema, &mut &data[..], None)?;
    let fields = match value {
        Value::Record(f) => f,
        _ => anyhow::bail!("expected Avro record"),
    };
    let mut map: std::collections::HashMap<String, Value> = fields.into_iter().collect();

    let event_id = avro_str(&mut map, "event_id")?;
    let event_type = avro_str(&mut map, "event_type")?;
    let timestamp_str = avro_str(&mut map, "timestamp")?;
    let timestamp = timestamp_str
        .parse::<DateTime<Utc>>()
        .map_err(|e| anyhow::anyhow!("invalid timestamp in avro: {}", e))?;
    let product_id = avro_str(&mut map, "product_id")?;
    let zone_id = avro_str(&mut map, "zone_id")?;
    let quantity = avro_long(&mut map, "quantity")?;
    let supplier_id = avro_optional_str(&mut map, "supplier_id");

    let payload = serde_json::json!({
        "product_id": product_id,
        "zone_id": zone_id,
        "quantity": quantity,
        "supplier_id": supplier_id,
    });
    Ok(WarehouseEvent {
        event_id,
        event_type,
        timestamp,
        sequence_number: None,
        payload,
    })
}

fn avro_str(
    map: &mut std::collections::HashMap<String, apache_avro::types::Value>,
    key: &str,
) -> anyhow::Result<String> {
    match map.remove(key) {
        Some(apache_avro::types::Value::String(s)) => Ok(s),
        other => anyhow::bail!("field '{}': expected string, got {:?}", key, other),
    }
}

fn avro_long(
    map: &mut std::collections::HashMap<String, apache_avro::types::Value>,
    key: &str,
) -> anyhow::Result<i64> {
    match map.remove(key) {
        Some(apache_avro::types::Value::Long(n)) => Ok(n),
        Some(apache_avro::types::Value::Int(n)) => Ok(n as i64),
        other => anyhow::bail!("field '{}': expected long, got {:?}", key, other),
    }
}

fn avro_optional_str(
    map: &mut std::collections::HashMap<String, apache_avro::types::Value>,
    key: &str,
) -> Option<String> {
    match map.remove(key) {
        Some(apache_avro::types::Value::Union(_, boxed)) => match *boxed {
            apache_avro::types::Value::String(s) => Some(s),
            _ => None,
        },
        Some(apache_avro::types::Value::String(s)) => Some(s),
        _ => None,
    }
}
