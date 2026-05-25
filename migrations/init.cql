CREATE KEYSPACE IF NOT EXISTS warehouse
    WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}
    AND durable_writes = true;

USE warehouse;

CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id           TEXT,
    zone_id              TEXT,
    available_quantity   BIGINT,
    reserved_quantity    BIGINT,
    last_event_id        TEXT,
    last_event_timestamp TIMESTAMP,
    last_updated         TIMESTAMP,
    supplier_id          TEXT,
    PRIMARY KEY (product_id, zone_id)
);

CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id            TEXT,
    product_id         TEXT,
    available_quantity BIGINT,
    reserved_quantity  BIGINT,
    last_updated       TIMESTAMP,
    PRIMARY KEY (zone_id, product_id)
);

CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id      TEXT,
    total_available BIGINT,
    total_reserved  BIGINT,
    last_updated    TIMESTAMP,
    PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT,
    event_type   TEXT,
    processed_at TIMESTAMP,
    partition    INT,
    kafka_offset BIGINT,
    PRIMARY KEY (event_id)
) WITH default_time_to_live = 604800;

CREATE TABLE IF NOT EXISTS orders (
    order_id   TEXT,
    status     TEXT,
    items_json TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS entity_last_timestamp (
    entity_id            TEXT,
    last_event_timestamp TIMESTAMP,
    last_event_id        TEXT,
    PRIMARY KEY (entity_id)
);

CREATE TABLE IF NOT EXISTS event_history (
    event_type      TEXT,
    event_id        TEXT,
    event_timestamp TIMESTAMP,
    product_id      TEXT,
    zone_id         TEXT,
    payload_json    TEXT,
    processed_at    TIMESTAMP,
    PRIMARY KEY (event_type, event_id)
);
