use anyhow::Result;
use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::types::Consistency;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use std::time::Duration;
use tracing::{info, warn};

use crate::events::WarehouseEvent;

pub fn parse_consistency(s: &str) -> Consistency {
    match s.to_uppercase().as_str() {
        "ALL"           => Consistency::All,
        "QUORUM"        => Consistency::Quorum,
        "LOCAL_QUORUM"  => Consistency::LocalQuorum,
        "EACH_QUORUM"   => Consistency::EachQuorum,
        "ONE"           => Consistency::One,
        "TWO"           => Consistency::Two,
        "THREE"         => Consistency::Three,
        "LOCAL_ONE"     => Consistency::LocalOne,
        "ANY"           => Consistency::Any,
        _               => Consistency::Quorum,
    }
}

pub struct InventoryRecord {
    pub available: i64,
    pub reserved: i64,
}

impl Default for InventoryRecord {
    fn default() -> Self {
        Self { available: 0, reserved: 0 }
    }
}

struct Stmts {
    check_processed: PreparedStatement,
    insert_processed: PreparedStatement,
    get_inventory_pz: PreparedStatement,
    upsert_inventory_pz: PreparedStatement,
    upsert_inventory_zone: PreparedStatement,
    get_inventory_product: PreparedStatement,
    upsert_inventory_product: PreparedStatement,
    insert_history: PreparedStatement,
    get_order: PreparedStatement,
    upsert_order: PreparedStatement,
    get_entity_ts: PreparedStatement,
    upsert_entity_ts: PreparedStatement,
    update_supplier_id: PreparedStatement,
}

pub struct CassandraClient {
    session: Session,
    stmts: Stmts,
}

impl CassandraClient {
    pub async fn new(
        hosts: &[String],
        keyspace: &str,
        write_consistency: Consistency,
        read_consistency: Consistency,
    ) -> Result<Self> {
        let session = connect_with_retry(hosts).await?;
        apply_migrations(&session).await?;
        let stmts = Stmts::prepare(&session, write_consistency, read_consistency).await?;
        info!(
            keyspace,
            write_cl = ?write_consistency,
            read_cl = ?read_consistency,
            "Cassandra client ready"
        );
        Ok(Self { session, stmts })
    }

    pub async fn is_processed(&self, event_id: &str) -> Result<bool> {
        let result = self
            .session
            .execute_unpaged(&self.stmts.check_processed, (event_id,))
            .await?
            .into_rows_result()?;
        let row = result.maybe_first_row::<(String,)>()?;
        Ok(row.is_some())
    }

    pub async fn get_inventory_pz(&self, product_id: &str, zone_id: &str) -> Result<InventoryRecord> {
        let result = self
            .session
            .execute_unpaged(&self.stmts.get_inventory_pz, (product_id, zone_id))
            .await?
            .into_rows_result()?;
        Ok(match result.maybe_first_row::<(i64, i64)>()? {
            Some((available, reserved)) => InventoryRecord { available, reserved },
            None => InventoryRecord::default(),
        })
    }

    pub async fn get_inventory_product(&self, product_id: &str) -> Result<InventoryRecord> {
        let result = self
            .session
            .execute_unpaged(&self.stmts.get_inventory_product, (product_id,))
            .await?
            .into_rows_result()?;
        Ok(match result.maybe_first_row::<(i64, i64)>()? {
            Some((available, reserved)) => InventoryRecord { available, reserved },
            None => InventoryRecord::default(),
        })
    }

    pub async fn get_order(&self, order_id: &str) -> Result<Option<(String, String)>> {
        let result = self
            .session
            .execute_unpaged(&self.stmts.get_order, (order_id,))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<(String, String)>()?)
    }

    pub async fn apply_delta(
        &self,
        event: &WarehouseEvent,
        product_id: &str,
        zone_id: &str,
        available_delta: i64,
        reserved_delta: i64,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        self.apply_delta_with_supplier(event, product_id, zone_id, available_delta, reserved_delta, partition, offset, None).await
    }

    pub async fn apply_delta_with_supplier(
        &self,
        event: &WarehouseEvent,
        product_id: &str,
        zone_id: &str,
        available_delta: i64,
        reserved_delta: i64,
        partition: i32,
        offset: i64,
        supplier_id: Option<&str>,
    ) -> Result<()> {
        let pz = self.get_inventory_pz(product_id, zone_id).await?;
        let prod = self.get_inventory_product(product_id).await?;

        let new_avail_pz = pz.available + available_delta;
        let new_res_pz = pz.reserved + reserved_delta;
        let new_total_avail = prod.available + available_delta;
        let new_total_res = prod.reserved + reserved_delta;
        let now = Utc::now();
        let payload_json = event.payload.to_string();

        let mut batch = Batch::new(BatchType::Logged);
        batch.append_statement(self.stmts.upsert_inventory_pz.clone());
        batch.append_statement(self.stmts.upsert_inventory_zone.clone());
        batch.append_statement(self.stmts.upsert_inventory_product.clone());
        batch.append_statement(self.stmts.insert_processed.clone());
        batch.append_statement(self.stmts.insert_history.clone());

        self.session
            .batch(
                &batch,
                (
                    (new_avail_pz, new_res_pz, now, event.event_id.as_str(), event.timestamp, product_id, zone_id),
                    (new_avail_pz, new_res_pz, now, zone_id, product_id),
                    (new_total_avail, new_total_res, now, product_id),
                    (event.event_id.as_str(), event.event_type.as_str(), now, partition, offset),
                    (event.event_type.as_str(), event.event_id.as_str(), event.timestamp, product_id, zone_id, payload_json.as_str(), now),
                ),
            )
            .await?;

        if let Some(sid) = supplier_id {
            self.session
                .execute_unpaged(
                    &self.stmts.update_supplier_id,
                    (sid, product_id, zone_id),
                )
                .await?;
        }

        Ok(())
    }

    pub async fn apply_move_delta(
        &self,
        event: &WarehouseEvent,
        product_id: &str,
        from_zone: &str,
        to_zone: &str,
        quantity: i64,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let from = self.get_inventory_pz(product_id, from_zone).await?;
        let to = self.get_inventory_pz(product_id, to_zone).await?;
        let now = Utc::now();
        let payload_json = event.payload.to_string();

        let mut batch = Batch::new(BatchType::Logged);
        batch.append_statement(self.stmts.upsert_inventory_pz.clone());
        batch.append_statement(self.stmts.upsert_inventory_zone.clone());
        batch.append_statement(self.stmts.upsert_inventory_pz.clone());
        batch.append_statement(self.stmts.upsert_inventory_zone.clone());
        batch.append_statement(self.stmts.insert_processed.clone());
        batch.append_statement(self.stmts.insert_history.clone());

        self.session
            .batch(
                &batch,
                (
                    (from.available - quantity, from.reserved, now, event.event_id.as_str(), event.timestamp, product_id, from_zone),
                    (from.available - quantity, from.reserved, now, from_zone, product_id),
                    (to.available + quantity, to.reserved, now, event.event_id.as_str(), event.timestamp, product_id, to_zone),
                    (to.available + quantity, to.reserved, now, to_zone, product_id),
                    (event.event_id.as_str(), event.event_type.as_str(), now, partition, offset),
                    (event.event_type.as_str(), event.event_id.as_str(), event.timestamp, product_id, from_zone, payload_json.as_str(), now),
                ),
            )
            .await?;

        Ok(())
    }

    pub async fn set_inventory_count(
        &self,
        event: &WarehouseEvent,
        product_id: &str,
        zone_id: &str,
        counted: i64,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let pz = self.get_inventory_pz(product_id, zone_id).await?;
        let prod = self.get_inventory_product(product_id).await?;

        let delta = counted - pz.available;
        let new_total_avail = prod.available + delta;
        let now = Utc::now();
        let payload_json = event.payload.to_string();

        let mut batch = Batch::new(BatchType::Logged);
        batch.append_statement(self.stmts.upsert_inventory_pz.clone());
        batch.append_statement(self.stmts.upsert_inventory_zone.clone());
        batch.append_statement(self.stmts.upsert_inventory_product.clone());
        batch.append_statement(self.stmts.insert_processed.clone());
        batch.append_statement(self.stmts.insert_history.clone());

        self.session
            .batch(
                &batch,
                (
                    (counted, pz.reserved, now, event.event_id.as_str(), event.timestamp, product_id, zone_id),
                    (counted, pz.reserved, now, zone_id, product_id),
                    (new_total_avail, prod.reserved, now, product_id),
                    (event.event_id.as_str(), event.event_type.as_str(), now, partition, offset),
                    (event.event_type.as_str(), event.event_id.as_str(), event.timestamp, product_id, zone_id, payload_json.as_str(), now),
                ),
            )
            .await?;

        Ok(())
    }

    pub async fn upsert_order(
        &self,
        order_id: &str,
        status: &str,
        items_json: &str,
        created_at: DateTime<Utc>,
    ) -> Result<()> {
        let now = Utc::now();
        self.session
            .execute_unpaged(
                &self.stmts.upsert_order,
                (order_id, status, items_json, created_at, now),
            )
            .await?;
        Ok(())
    }

    pub async fn mark_processed(&self, event_id: &str, event_type: &str, partition: i32, offset: i64) -> Result<()> {
        let now = Utc::now();
        self.session
            .execute_unpaged(
                &self.stmts.insert_processed,
                (event_id, event_type, now, partition, offset),
            )
            .await?;
        Ok(())
    }

    pub async fn get_entity_timestamp(&self, entity_id: &str) -> Result<Option<DateTime<Utc>>> {
        let result = self
            .session
            .execute_unpaged(&self.stmts.get_entity_ts, (entity_id,))
            .await?
            .into_rows_result()?;
        Ok(result.maybe_first_row::<(DateTime<Utc>,)>()?.map(|(ts,)| ts))
    }

    pub async fn update_entity_timestamp(
        &self,
        entity_id: &str,
        event_timestamp: DateTime<Utc>,
        event_id: &str,
    ) -> Result<()> {
        self.session
            .execute_unpaged(&self.stmts.upsert_entity_ts, (event_timestamp, event_id, entity_id))
            .await?;
        Ok(())
    }
}

impl Stmts {
    async fn prepare(session: &Session, wcl: Consistency, rcl: Consistency) -> Result<Self> {
        macro_rules! ps_write {
            ($sql:expr) => {{
                let mut s = session.prepare($sql).await?;
                s.set_consistency(wcl);
                s
            }};
        }
        macro_rules! ps_read {
            ($sql:expr) => {{
                let mut s = session.prepare($sql).await?;
                s.set_consistency(rcl);
                s
            }};
        }

        Ok(Self {
            check_processed: ps_read!(
                "SELECT event_id FROM warehouse.processed_events WHERE event_id = ?"
            ),
            insert_processed: ps_write!(
                "INSERT INTO warehouse.processed_events \
                 (event_id, event_type, processed_at, partition, kafka_offset) \
                 VALUES (?, ?, ?, ?, ?)"
            ),
            get_inventory_pz: ps_read!(
                "SELECT available_quantity, reserved_quantity \
                 FROM warehouse.inventory_by_product_zone \
                 WHERE product_id = ? AND zone_id = ?"
            ),
            upsert_inventory_pz: ps_write!(
                "UPDATE warehouse.inventory_by_product_zone \
                 SET available_quantity = ?, reserved_quantity = ?, \
                     last_updated = ?, last_event_id = ?, last_event_timestamp = ? \
                 WHERE product_id = ? AND zone_id = ?"
            ),
            upsert_inventory_zone: ps_write!(
                "UPDATE warehouse.inventory_by_zone \
                 SET available_quantity = ?, reserved_quantity = ?, last_updated = ? \
                 WHERE zone_id = ? AND product_id = ?"
            ),
            get_inventory_product: ps_read!(
                "SELECT total_available, total_reserved \
                 FROM warehouse.inventory_by_product \
                 WHERE product_id = ?"
            ),
            upsert_inventory_product: ps_write!(
                "UPDATE warehouse.inventory_by_product \
                 SET total_available = ?, total_reserved = ?, last_updated = ? \
                 WHERE product_id = ?"
            ),
            insert_history: ps_write!(
                "INSERT INTO warehouse.event_history \
                 (event_type, event_id, event_timestamp, product_id, zone_id, payload_json, processed_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)"
            ),
            get_order: ps_read!(
                "SELECT status, items_json FROM warehouse.orders WHERE order_id = ?"
            ),
            upsert_order: ps_write!(
                "INSERT INTO warehouse.orders \
                 (order_id, status, items_json, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?)"
            ),
            get_entity_ts: ps_read!(
                "SELECT last_event_timestamp \
                 FROM warehouse.entity_last_timestamp \
                 WHERE entity_id = ?"
            ),
            upsert_entity_ts: ps_write!(
                "UPDATE warehouse.entity_last_timestamp \
                 SET last_event_timestamp = ?, last_event_id = ? \
                 WHERE entity_id = ?"
            ),
            update_supplier_id: ps_write!(
                "UPDATE warehouse.inventory_by_product_zone \
                 SET supplier_id = ? \
                 WHERE product_id = ? AND zone_id = ?"
            ),
        })
    }
}

async fn connect_with_retry(hosts: &[String]) -> Result<Session> {
    let mut attempt = 0u32;
    loop {
        let mut builder = SessionBuilder::new();
        for host in hosts {
            builder = builder.known_node(host.as_str());
        }
        match builder.build().await {
            Ok(s) => {
                info!("Connected to Cassandra at {:?}", hosts);
                return Ok(s);
            }
            Err(e) if attempt < 30 => {
                attempt += 1;
                warn!("Cassandra not ready (attempt {}): {}", attempt, e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Err(e) => return Err(anyhow::Error::new(e)),
        }
    }
}

async fn apply_migrations(session: &Session) -> Result<()> {
    let cql = include_str!("../../migrations/init.cql");
    for stmt in cql.split(';') {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        session.query_unpaged(trimmed, &[]).await?;
    }
    info!("Migrations applied");
    Ok(())
}
