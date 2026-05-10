use anyhow::{bail, Result};
use tracing::{info, warn};

use crate::cassandra::CassandraClient;
use crate::events::*;

pub async fn process_event(
    event: &WarehouseEvent,
    db: &CassandraClient,
    partition: i32,
    offset: i64,
) -> Result<()> {
    if db.is_processed(&event.event_id).await? {
        info!(
            event_id = %event.event_id,
            event_type = %event.event_type,
            "duplicate event skipped"
        );
        return Ok(());
    }

    let payload = event.parse_payload()?;

    match &payload {
        EventPayload::ProductReceived(p) => {
            handle_product_received(event, p, db, partition, offset).await?
        }
        EventPayload::ProductShipped(p) => {
            handle_product_shipped(event, p, db, partition, offset).await?
        }
        EventPayload::ProductMoved(p) => {
            handle_product_moved(event, p, db, partition, offset).await?
        }
        EventPayload::ProductReserved(p) => {
            handle_product_reserved(event, p, db, partition, offset).await?
        }
        EventPayload::ProductReleased(p) => {
            handle_product_released(event, p, db, partition, offset).await?
        }
        EventPayload::InventoryCounted(p) => {
            handle_inventory_counted(event, p, db, partition, offset).await?
        }
        EventPayload::OrderCreated(p) => {
            handle_order_created(event, p, db, partition, offset).await?
        }
        EventPayload::OrderCompleted(p) => {
            handle_order_completed(event, p, db, partition, offset).await?
        }
    }

    info!(
        event_id = %event.event_id,
        event_type = %event.event_type,
        partition,
        offset,
        "event processed"
    );

    Ok(())
}

async fn check_out_of_order(event: &WarehouseEvent, entity_id: &str, db: &CassandraClient) -> Result<bool> {
    if let Some(last_ts) = db.get_entity_timestamp(entity_id).await? {
        if event.timestamp <= last_ts {
            warn!(
                event_id = %event.event_id,
                event_type = %event.event_type,
                entity_id = %entity_id,
                event_ts = %event.timestamp,
                last_ts = %last_ts,
                "out-of-order event ignored"
            );
            return Ok(true);
        }
    }
    Ok(false)
}

async fn handle_product_received(
    event: &WarehouseEvent,
    p: &ProductReceivedPayload,
    db: &CassandraClient,
    partition: i32,
    offset: i64,
) -> Result<()> {
    if p.quantity <= 0 {
        bail!("VALIDATION_ERROR: quantity must be positive, got {}", p.quantity);
    }
    let entity_id = format!("{}:{}", p.product_id, p.zone_id);
    if check_out_of_order(event, &entity_id, db).await? {
        return Ok(());
    }
    db.apply_delta_with_supplier(
        event,
        &p.product_id,
        &p.zone_id,
        p.quantity,
        0,
        partition,
        offset,
        p.supplier_id.as_deref(),
    )
    .await?;
    db.update_entity_timestamp(&entity_id, event.timestamp, &event.event_id).await
}

async fn handle_product_shipped(
    event: &WarehouseEvent,
    p: &ProductShippedPayload,
    db: &CassandraClient,
    partition: i32,
    offset: i64,
) -> Result<()> {
    if p.quantity <= 0 {
        bail!("VALIDATION_ERROR: quantity must be positive, got {}", p.quantity);
    }
    let entity_id = format!("{}:{}", p.product_id, p.zone_id);
    if check_out_of_order(event, &entity_id, db).await? {
        return Ok(());
    }
    let current = db.get_inventory_pz(&p.product_id, &p.zone_id).await?;
    if current.available < p.quantity {
        bail!(
            "BUSINESS_ERROR: insufficient available qty for {} in {}: have {}, need {}",
            p.product_id, p.zone_id, current.available, p.quantity
        );
    }
    db.apply_delta(event, &p.product_id, &p.zone_id, -p.quantity, 0, partition, offset).await?;
    db.update_entity_timestamp(&entity_id, event.timestamp, &event.event_id).await
}

async fn handle_product_moved(
    event: &WarehouseEvent,
    p: &ProductMovedPayload,
    db: &CassandraClient,
    partition: i32,
    offset: i64,
) -> Result<()> {
    if p.quantity <= 0 {
        bail!("VALIDATION_ERROR: quantity must be positive, got {}", p.quantity);
    }
    let entity_id = format!("{}:{}", p.product_id, p.from_zone_id);
    if check_out_of_order(event, &entity_id, db).await? {
        return Ok(());
    }
    let current = db.get_inventory_pz(&p.product_id, &p.from_zone_id).await?;
    if current.available < p.quantity {
        bail!(
            "BUSINESS_ERROR: insufficient available qty for {} in {}: have {}, need {}",
            p.product_id, p.from_zone_id, current.available, p.quantity
        );
    }
    db.apply_move_delta(event, &p.product_id, &p.from_zone_id, &p.to_zone_id, p.quantity, partition, offset).await?;
    db.update_entity_timestamp(&entity_id, event.timestamp, &event.event_id).await?;
    let to_entity_id = format!("{}:{}", p.product_id, p.to_zone_id);
    db.update_entity_timestamp(&to_entity_id, event.timestamp, &event.event_id).await
}

async fn handle_product_reserved(
    event: &WarehouseEvent,
    p: &ProductReservedPayload,
    db: &CassandraClient,
    partition: i32,
    offset: i64,
) -> Result<()> {
    if p.quantity <= 0 {
        bail!("VALIDATION_ERROR: quantity must be positive, got {}", p.quantity);
    }
    let entity_id = format!("{}:{}", p.product_id, p.zone_id);
    if check_out_of_order(event, &entity_id, db).await? {
        return Ok(());
    }
    let current = db.get_inventory_pz(&p.product_id, &p.zone_id).await?;
    if current.available < p.quantity {
        bail!(
            "BUSINESS_ERROR: insufficient available qty for reservation: {} in {}: have {}, need {}",
            p.product_id, p.zone_id, current.available, p.quantity
        );
    }
    db.apply_delta(event, &p.product_id, &p.zone_id, -p.quantity, p.quantity, partition, offset).await?;
    db.update_entity_timestamp(&entity_id, event.timestamp, &event.event_id).await
}

async fn handle_product_released(
    event: &WarehouseEvent,
    p: &ProductReleasedPayload,
    db: &CassandraClient,
    partition: i32,
    offset: i64,
) -> Result<()> {
    if p.quantity <= 0 {
        bail!("VALIDATION_ERROR: quantity must be positive, got {}", p.quantity);
    }
    let entity_id = format!("{}:{}", p.product_id, p.zone_id);
    if check_out_of_order(event, &entity_id, db).await? {
        return Ok(());
    }
    let current = db.get_inventory_pz(&p.product_id, &p.zone_id).await?;
    if current.reserved < p.quantity {
        bail!(
            "BUSINESS_ERROR: insufficient reserved qty for release: {} in {}: have {}, need {}",
            p.product_id, p.zone_id, current.reserved, p.quantity
        );
    }
    db.apply_delta(event, &p.product_id, &p.zone_id, p.quantity, -p.quantity, partition, offset).await?;
    db.update_entity_timestamp(&entity_id, event.timestamp, &event.event_id).await
}

async fn handle_inventory_counted(
    event: &WarehouseEvent,
    p: &InventoryCountedPayload,
    db: &CassandraClient,
    partition: i32,
    offset: i64,
) -> Result<()> {
    if p.counted_quantity < 0 {
        bail!("VALIDATION_ERROR: counted_quantity cannot be negative");
    }
    let entity_id = format!("{}:{}", p.product_id, p.zone_id);
    if check_out_of_order(event, &entity_id, db).await? {
        return Ok(());
    }
    db.set_inventory_count(event, &p.product_id, &p.zone_id, p.counted_quantity, partition, offset).await?;
    db.update_entity_timestamp(&entity_id, event.timestamp, &event.event_id).await
}

async fn handle_order_created(
    event: &WarehouseEvent,
    p: &OrderCreatedPayload,
    db: &CassandraClient,
    partition: i32,
    offset: i64,
) -> Result<()> {
    if p.items.is_empty() {
        bail!("VALIDATION_ERROR: order must have at least one item");
    }
    for item in &p.items {
        if item.quantity <= 0 {
            bail!("VALIDATION_ERROR: item quantity must be positive, got {}", item.quantity);
        }
        let current = db.get_inventory_pz(&item.product_id, &item.zone_id).await?;
        if current.available < item.quantity {
            bail!(
                "BUSINESS_ERROR: insufficient qty for order item {} in {}: have {}, need {}",
                item.product_id, item.zone_id, current.available, item.quantity
            );
        }
    }

    let items_json = serde_json::to_string(&p.items)?;
    db.upsert_order(&p.order_id, "CREATED", &items_json, event.timestamp).await?;

    for item in &p.items {
        db.apply_delta(event, &item.product_id, &item.zone_id, -item.quantity, item.quantity, partition, offset).await?;
        let entity_id = format!("{}:{}", item.product_id, item.zone_id);
        db.update_entity_timestamp(&entity_id, event.timestamp, &event.event_id).await?;
    }

    db.mark_processed(&event.event_id, &event.event_type, partition, offset).await
}

async fn handle_order_completed(
    event: &WarehouseEvent,
    p: &OrderCompletedPayload,
    db: &CassandraClient,
    partition: i32,
    offset: i64,
) -> Result<()> {
    let order = db.get_order(&p.order_id).await?;
    let (_status, items_json) = order.ok_or_else(|| {
        anyhow::anyhow!("BUSINESS_ERROR: order {} not found", p.order_id)
    })?;

    let items: Vec<OrderItem> = serde_json::from_str(&items_json)?;
    db.upsert_order(&p.order_id, "COMPLETED", &items_json, event.timestamp).await?;

    for item in &items {
        let current = db.get_inventory_pz(&item.product_id, &item.zone_id).await?;
        if current.reserved >= item.quantity {
            db.apply_delta(event, &item.product_id, &item.zone_id, 0, -item.quantity, partition, offset).await?;
        }
    }

    db.mark_processed(&event.event_id, &event.event_type, partition, offset).await
}
