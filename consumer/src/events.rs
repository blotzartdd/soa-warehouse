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

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value as AvroValue;
    use chrono::Utc;
    use serde_json::json;

    fn make_event(event_type: &str, payload: serde_json::Value) -> WarehouseEvent {
        WarehouseEvent {
            event_id: "test-id".to_string(),
            event_type: event_type.to_string(),
            timestamp: Utc::now(),
            sequence_number: None,
            payload,
        }
    }

    #[test]
    fn parse_product_received() {
        let e = make_event(
            "PRODUCT_RECEIVED",
            json!({"product_id": "P1", "zone_id": "Z1", "quantity": 10}),
        );
        match e.parse_payload().unwrap() {
            EventPayload::ProductReceived(p) => {
                assert_eq!(p.product_id, "P1");
                assert_eq!(p.zone_id, "Z1");
                assert_eq!(p.quantity, 10);
                assert!(p.supplier_id.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_product_received_with_supplier() {
        let e = make_event(
            "PRODUCT_RECEIVED",
            json!({"product_id": "P1", "zone_id": "Z1", "quantity": 5, "supplier_id": "SUP-1"}),
        );
        match e.parse_payload().unwrap() {
            EventPayload::ProductReceived(p) => assert_eq!(p.supplier_id.as_deref(), Some("SUP-1")),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_product_shipped() {
        let e = make_event(
            "PRODUCT_SHIPPED",
            json!({"product_id": "P1", "zone_id": "Z1", "quantity": 3}),
        );
        match e.parse_payload().unwrap() {
            EventPayload::ProductShipped(p) => assert_eq!(p.quantity, 3),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_product_moved() {
        let e = make_event(
            "PRODUCT_MOVED",
            json!({"product_id": "P1", "from_zone_id": "Z1", "to_zone_id": "Z2", "quantity": 7}),
        );
        match e.parse_payload().unwrap() {
            EventPayload::ProductMoved(p) => {
                assert_eq!(p.from_zone_id, "Z1");
                assert_eq!(p.to_zone_id, "Z2");
                assert_eq!(p.quantity, 7);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_product_reserved() {
        let e = make_event(
            "PRODUCT_RESERVED",
            json!({"product_id": "P1", "zone_id": "Z1", "quantity": 2}),
        );
        match e.parse_payload().unwrap() {
            EventPayload::ProductReserved(p) => assert_eq!(p.quantity, 2),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_product_released() {
        let e = make_event(
            "PRODUCT_RELEASED",
            json!({"product_id": "P1", "zone_id": "Z1", "quantity": 2}),
        );
        match e.parse_payload().unwrap() {
            EventPayload::ProductReleased(p) => assert_eq!(p.quantity, 2),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_inventory_counted() {
        let e = make_event(
            "INVENTORY_COUNTED",
            json!({"product_id": "P1", "zone_id": "Z1", "counted_quantity": 99}),
        );
        match e.parse_payload().unwrap() {
            EventPayload::InventoryCounted(p) => assert_eq!(p.counted_quantity, 99),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_order_created() {
        let e = make_event(
            "ORDER_CREATED",
            json!({"order_id": "O1", "items": [{"product_id": "P1", "zone_id": "Z1", "quantity": 1}]}),
        );
        match e.parse_payload().unwrap() {
            EventPayload::OrderCreated(p) => {
                assert_eq!(p.order_id, "O1");
                assert_eq!(p.items.len(), 1);
                assert_eq!(p.items[0].product_id, "P1");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_order_completed() {
        let e = make_event("ORDER_COMPLETED", json!({"order_id": "O1"}));
        match e.parse_payload().unwrap() {
            EventPayload::OrderCompleted(p) => assert_eq!(p.order_id, "O1"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_unknown_event_type_returns_error() {
        let e = make_event("UNKNOWN_TYPE", json!({}));
        assert!(e.parse_payload().is_err());
    }

    #[test]
    fn parse_missing_required_field_returns_error() {
        let e = make_event(
            "PRODUCT_RECEIVED",
            json!({"zone_id": "Z1", "quantity": 10}),
        );
        assert!(e.parse_payload().is_err());
    }

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

    const V2_SCHEMA_STR: &str = r#"{
        "type": "record", "name": "ProductReceived", "namespace": "warehouse",
        "fields": [
            {"name": "event_id",    "type": "string"},
            {"name": "event_type",  "type": "string"},
            {"name": "timestamp",   "type": "string"},
            {"name": "product_id",  "type": "string"},
            {"name": "zone_id",     "type": "string"},
            {"name": "quantity",    "type": "long"},
            {"name": "supplier_id", "type": ["null", "string"], "default": null}
        ]
    }"#;

    fn v1_avro_bytes(event_id: &str, product_id: &str, zone_id: &str, quantity: i64)
        -> (apache_avro::Schema, Vec<u8>)
    {
        let schema = apache_avro::Schema::parse_str(V1_SCHEMA_STR).unwrap();
        let value = AvroValue::Record(vec![
            ("event_id".to_string(),   AvroValue::String(event_id.to_string())),
            ("event_type".to_string(), AvroValue::String("PRODUCT_RECEIVED".to_string())),
            ("timestamp".to_string(),  AvroValue::String("2024-01-01T00:00:00Z".to_string())),
            ("product_id".to_string(), AvroValue::String(product_id.to_string())),
            ("zone_id".to_string(),    AvroValue::String(zone_id.to_string())),
            ("quantity".to_string(),   AvroValue::Long(quantity)),
        ]);
        let bytes = apache_avro::to_avro_datum(&schema, value).unwrap();
        (schema, bytes)
    }

    #[test]
    fn decode_avro_v1_fields_are_correct() {
        let (schema, bytes) = v1_avro_bytes("evt-1", "PROD-A", "ZONE-1", 42);
        let event = decode_avro_event(&bytes, &schema).unwrap();
        assert_eq!(event.event_id, "evt-1");
        assert_eq!(event.event_type, "PRODUCT_RECEIVED");
        match event.parse_payload().unwrap() {
            EventPayload::ProductReceived(p) => {
                assert_eq!(p.product_id, "PROD-A");
                assert_eq!(p.zone_id, "ZONE-1");
                assert_eq!(p.quantity, 42);
                assert!(p.supplier_id.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn decode_avro_v2_with_supplier() {
        let schema = apache_avro::Schema::parse_str(V2_SCHEMA_STR).unwrap();
        let value = AvroValue::Record(vec![
            ("event_id".to_string(),    AvroValue::String("evt-2".to_string())),
            ("event_type".to_string(),  AvroValue::String("PRODUCT_RECEIVED".to_string())),
            ("timestamp".to_string(),   AvroValue::String("2024-01-01T00:00:00Z".to_string())),
            ("product_id".to_string(),  AvroValue::String("PROD-B".to_string())),
            ("zone_id".to_string(),     AvroValue::String("ZONE-2".to_string())),
            ("quantity".to_string(),    AvroValue::Long(7)),
            ("supplier_id".to_string(), AvroValue::Union(1, Box::new(AvroValue::String("SUP-X".to_string())))),
        ]);
        let bytes = apache_avro::to_avro_datum(&schema, value).unwrap();
        let event = decode_avro_event(&bytes, &schema).unwrap();
        match event.parse_payload().unwrap() {
            EventPayload::ProductReceived(p) => {
                assert_eq!(p.supplier_id.as_deref(), Some("SUP-X"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn decode_avro_v2_with_null_supplier() {
        let schema = apache_avro::Schema::parse_str(V2_SCHEMA_STR).unwrap();
        let value = AvroValue::Record(vec![
            ("event_id".to_string(),    AvroValue::String("evt-3".to_string())),
            ("event_type".to_string(),  AvroValue::String("PRODUCT_RECEIVED".to_string())),
            ("timestamp".to_string(),   AvroValue::String("2024-01-01T00:00:00Z".to_string())),
            ("product_id".to_string(),  AvroValue::String("PROD-C".to_string())),
            ("zone_id".to_string(),     AvroValue::String("ZONE-3".to_string())),
            ("quantity".to_string(),    AvroValue::Long(1)),
            ("supplier_id".to_string(), AvroValue::Union(0, Box::new(AvroValue::Null))),
        ]);
        let bytes = apache_avro::to_avro_datum(&schema, value).unwrap();
        let event = decode_avro_event(&bytes, &schema).unwrap();
        match event.parse_payload().unwrap() {
            EventPayload::ProductReceived(p) => assert!(p.supplier_id.is_none()),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn decode_avro_invalid_bytes_returns_error() {
        let schema = apache_avro::Schema::parse_str(V1_SCHEMA_STR).unwrap();
        assert!(decode_avro_event(&[0xFF, 0xFE, 0xFD], &schema).is_err());
    }
}
