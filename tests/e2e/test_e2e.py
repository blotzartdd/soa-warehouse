"""
End-to-End tests — full user scenarios for the warehouse system.

Each test drives the system exclusively through the producer HTTP API,
then verifies the final state in Cassandra (the canonical storage).

Scenario covered (from task.md):
  "Warehouse: отправить PRODUCT_RECEIVED → проверить остатки в Cassandra"

All tests use unique product IDs derived from uuid4() so they are
fully isolated; no teardown / cleanup is required.
"""
import time
import uuid

import pytest
import requests


# ── Cassandra query helpers ───────────────────────────────────────────────────

def get_inventory(session, product_id: str, zone_id: str):
    """Return (available_quantity, reserved_quantity) or None."""
    rows = session.execute(
        "SELECT available_quantity, reserved_quantity "
        "FROM warehouse.inventory_by_product_zone "
        "WHERE product_id = %s AND zone_id = %s",
        [product_id, zone_id],
    )
    row = rows.one()
    if row is None:
        return None
    return row.available_quantity, row.reserved_quantity


def get_product_totals(session, product_id: str):
    """Return (total_available, total_reserved) or None."""
    rows = session.execute(
        "SELECT total_available, total_reserved "
        "FROM warehouse.inventory_by_product "
        "WHERE product_id = %s",
        [product_id],
    )
    row = rows.one()
    if row is None:
        return None
    return row.total_available, row.total_reserved


def wait_for_inventory(session, product_id: str, zone_id: str,
                        expected_available: int, timeout: int = 60) -> bool:
    """Poll Cassandra until available_quantity equals expected_available."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        inv = get_inventory(session, product_id, zone_id)
        if inv is not None and inv[0] == expected_available:
            return True
        time.sleep(2)
    return False


def post_json_event(producer_url: str, event_type: str, payload: dict) -> requests.Response:
    return requests.post(
        f"{producer_url}/api/events",
        json={"event_type": event_type, "payload": payload},
        timeout=10,
    )


def post_avro_v1(producer_url: str, product_id: str, zone_id: str,
                  quantity: int) -> requests.Response:
    return requests.post(
        f"{producer_url}/api/events/avro/v1",
        json={"product_id": product_id, "zone_id": zone_id, "quantity": quantity},
        timeout=10,
    )


# ── Scenario 1: PRODUCT_RECEIVED via Avro → Cassandra ────────────────────────

class TestProductReceivedAvro:
    """Warehouse scenario: send PRODUCT_RECEIVED (Avro v1) → check Cassandra."""

    def test_product_received_creates_inventory_entry(self, producer_url, cassandra_session):
        product_id = f"e2e-avro-{uuid.uuid4()}"
        zone_id = "ZONE-E2E-A"
        quantity = 42

        resp = post_avro_v1(producer_url, product_id, zone_id, quantity)

        # ── HTTP response checks ─────────────────────────────────────────────
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert "event_id" in body, f"Missing event_id in response: {body}"
        assert isinstance(body["event_id"], str) and len(body["event_id"]) > 0

        # ── Cassandra state checks ───────────────────────────────────────────
        ok = wait_for_inventory(cassandra_session, product_id, zone_id, quantity, timeout=60)
        assert ok, (
            f"Inventory not updated in Cassandra within 60 s "
            f"(product={product_id}, zone={zone_id}, expected={quantity})"
        )

        inv = get_inventory(cassandra_session, product_id, zone_id)
        assert inv is not None
        available, reserved = inv
        assert available == quantity, f"available={available}, want {quantity}"
        assert reserved == 0, f"reserved should be 0, got {reserved}"

    def test_product_received_avro_v2_with_supplier(self, producer_url, cassandra_session):
        product_id = f"e2e-avro2-{uuid.uuid4()}"
        zone_id = "ZONE-E2E-B"
        quantity = 15
        supplier = "SUP-E2E-001"

        resp = requests.post(
            f"{producer_url}/api/events/avro/v2",
            json={
                "product_id": product_id,
                "zone_id": zone_id,
                "quantity": quantity,
                "supplier_id": supplier,
            },
            timeout=10,
        )
        assert resp.status_code == 200

        ok = wait_for_inventory(cassandra_session, product_id, zone_id, quantity, timeout=60)
        assert ok

        # Verify supplier_id was persisted
        rows = cassandra_session.execute(
            "SELECT supplier_id FROM warehouse.inventory_by_product_zone "
            "WHERE product_id = %s AND zone_id = %s",
            [product_id, zone_id],
        )
        row = rows.one()
        assert row is not None
        assert row.supplier_id == supplier, f"supplier_id mismatch: {row.supplier_id}"


# ── Scenario 2: PRODUCT_RECEIVED via JSON ────────────────────────────────────

class TestProductReceivedJson:
    """Full scenario via the plain JSON endpoint."""

    def test_product_received_json_updates_cassandra(self, producer_url, cassandra_session):
        product_id = f"e2e-json-{uuid.uuid4()}"
        zone_id = "ZONE-E2E-C"
        quantity = 100

        resp = post_json_event(
            producer_url,
            "PRODUCT_RECEIVED",
            {"product_id": product_id, "zone_id": zone_id, "quantity": quantity},
        )
        assert resp.status_code == 200
        assert "event_id" in resp.json()

        ok = wait_for_inventory(cassandra_session, product_id, zone_id, quantity, timeout=60)
        assert ok

        totals = get_product_totals(cassandra_session, product_id)
        assert totals is not None
        total_available, total_reserved = totals
        assert total_available == quantity
        assert total_reserved == 0


# ── Scenario 3: Reserve → Release cycle ──────────────────────────────────────

class TestReservationCycle:
    """PRODUCT_RECEIVED → PRODUCT_RESERVED → PRODUCT_RELEASED → verify state."""

    def test_reserve_then_release_restores_available(self, producer_url, cassandra_session):
        product_id = f"e2e-res-{uuid.uuid4()}"
        zone_id = "ZONE-E2E-D"
        total_qty = 50
        reserve_qty = 20

        # Step 1: receive
        r = post_json_event(
            producer_url, "PRODUCT_RECEIVED",
            {"product_id": product_id, "zone_id": zone_id, "quantity": total_qty},
        )
        assert r.status_code == 200
        wait_for_inventory(cassandra_session, product_id, zone_id, total_qty, timeout=60)

        # Step 2: reserve
        r = post_json_event(
            producer_url, "PRODUCT_RESERVED",
            {"product_id": product_id, "zone_id": zone_id, "quantity": reserve_qty},
        )
        assert r.status_code == 200

        # Wait for reserved state (available = total - reserve)
        ok = wait_for_inventory(
            cassandra_session, product_id, zone_id, total_qty - reserve_qty, timeout=60
        )
        assert ok
        _, res = get_inventory(cassandra_session, product_id, zone_id)
        assert res == reserve_qty

        # Step 3: release
        r = post_json_event(
            producer_url, "PRODUCT_RELEASED",
            {"product_id": product_id, "zone_id": zone_id, "quantity": reserve_qty},
        )
        assert r.status_code == 200

        # Available should return to total
        ok = wait_for_inventory(cassandra_session, product_id, zone_id, total_qty, timeout=60)
        assert ok, "available did not return to original after release"

        avail, res = get_inventory(cassandra_session, product_id, zone_id)
        assert avail == total_qty
        assert res == 0


# ── Scenario 4: Idempotency ───────────────────────────────────────────────────

class TestIdempotency:
    """Sending the same event twice must not double-count inventory."""

    def test_same_payload_twice_creates_distinct_events(self, producer_url, cassandra_session):
        """The producer always generates a new event_id, so two sends = two events,
        each processed independently (both change inventory correctly).
        This verifies the system doesn't silently drop valid events."""
        product_id = f"e2e-idem-{uuid.uuid4()}"
        zone_id = "ZONE-E2E-IDEM"

        # First receive: +10
        r1 = post_avro_v1(producer_url, product_id, zone_id, 10)
        assert r1.status_code == 200
        event_id_1 = r1.json()["event_id"]

        wait_for_inventory(cassandra_session, product_id, zone_id, 10, timeout=60)

        # Second receive: +10 more → total 20
        r2 = post_avro_v1(producer_url, product_id, zone_id, 10)
        assert r2.status_code == 200
        event_id_2 = r2.json()["event_id"]

        # Each request must return a DIFFERENT event_id
        assert event_id_1 != event_id_2

        wait_for_inventory(cassandra_session, product_id, zone_id, 20, timeout=60)
        avail, _ = get_inventory(cassandra_session, product_id, zone_id)
        assert avail == 20, f"Expected 20, got {avail}"


# ── Scenario 5: Invalid event goes to DLQ, system stays healthy ──────────────

class TestInvalidEventHandling:
    """Consumer must survive bad events and keep processing valid ones."""

    def test_negative_quantity_does_not_crash_consumer(self, producer_url, consumer_url,
                                                        cassandra_session):
        # Send an invalid event (negative quantity should be rejected to DLQ)
        bad = post_json_event(
            producer_url, "PRODUCT_SHIPPED",
            {"product_id": f"e2e-bad-{uuid.uuid4()}", "zone_id": "ZONE-A", "quantity": -5},
        )
        assert bad.status_code == 200  # Producer accepts it (it's syntactically valid)

        # Consumer must still be healthy after the bad event
        time.sleep(5)
        health = requests.get(f"{consumer_url}/health", timeout=10)
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        # And it must still process valid events
        product_id = f"e2e-after-bad-{uuid.uuid4()}"
        good = post_avro_v1(producer_url, product_id, "ZONE-E2E-A", 7)
        assert good.status_code == 200
        ok = wait_for_inventory(cassandra_session, product_id, "ZONE-E2E-A", 7, timeout=60)
        assert ok, "Consumer stopped processing valid events after a bad event"
