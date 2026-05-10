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

#[derive(serde::Deserialize)]
struct AvroProductReceived {
    event_id: String,
    event_type: String,
    timestamp: String,
    product_id: String,
    zone_id: String,
    quantity: i64,
    #[serde(default)]
    supplier_id: Option<String>,
}

pub fn decode_avro_event(data: &[u8], schema: &apache_avro::Schema) -> anyhow::Result<WarehouseEvent> {
    let rec: AvroProductReceived = apache_avro::from_avro_datum(schema, &mut &data[..], None)?;
    let timestamp = rec.timestamp.parse::<DateTime<Utc>>()
        .map_err(|e| anyhow::anyhow!("invalid timestamp in avro: {}", e))?;
    let payload = serde_json::json!({
        "product_id": rec.product_id,
        "zone_id": rec.zone_id,
        "quantity": rec.quantity,
        "supplier_id": rec.supplier_id,
    });
    Ok(WarehouseEvent {
        event_id: rec.event_id,
        event_type: rec.event_type,
        timestamp,
        sequence_number: None,
        payload,
    })
}
