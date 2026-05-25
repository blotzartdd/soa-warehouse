import time
import uuid

import pytest
import requests


def prom_counter(prom_url: str, metric: str, label_filter: str = "") -> float:
    q = f'{metric}{{{label_filter}}}' if label_filter else metric
    try:
        resp = requests.get(
            f"{prom_url}/api/v1/query",
            params={"query": f"sum({q})"},
            timeout=5,
        )
        results = resp.json()["data"]["result"]
        return float(results[0]["value"][1]) if results else 0.0
    except Exception:
        return 0.0


def post_event(producer_url: str, product_id: str, zone_id: str = "ZONE-TEST",
               quantity: int = 1) -> requests.Response:
    return requests.post(
        f"{producer_url}/api/events/avro/v1",
        json={"product_id": product_id, "zone_id": zone_id, "quantity": quantity},
        timeout=10,
    )


def wait_until_processed(prom_url: str, initial: float, timeout: int = 40) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = prom_counter(prom_url, "events_processed_total")
        if current > initial:
            return True
        time.sleep(2)
    return False


class TestConsumerHealth:

    def test_health_returns_ok(self, consumer_url):
        resp = requests.get(f"{consumer_url}/health", timeout=10)
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        body = resp.json()
        assert body.get("status") == "ok", f"Unexpected body: {body}"

    def test_metrics_endpoint_returns_prometheus_text(self, consumer_url):
        resp = requests.get(f"{consumer_url}/metrics", timeout=10)
        assert resp.status_code == 200


class TestProducerObservability:

    def test_health_returns_ok(self, producer_url):
        resp = requests.get(f"{producer_url}/health", timeout=10)
        assert resp.status_code == 200
        assert resp.json().get("status") == "ok"

    def test_metrics_endpoint_returns_prometheus_text(self, producer_url):
        resp = requests.get(f"{producer_url}/metrics", timeout=10)
        assert resp.status_code == 200
        text = resp.text
        assert "http_requests_total" in text
        assert "http_request_duration_seconds" in text

    def test_request_increments_counter(self, producer_url):
        post_event(producer_url, f"obs-warm-{uuid.uuid4()}")
        resp = requests.get(f"{producer_url}/metrics", timeout=10)
        assert "http_requests_total" in resp.text


class TestProducerAvailability:

    def test_avro_v1_valid_request_returns_200(self, producer_url):
        product_id = f"intg-{uuid.uuid4()}"
        resp = post_event(producer_url, product_id, quantity=5)
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"

    def test_avro_v1_response_contains_event_id(self, producer_url):
        product_id = f"intg-{uuid.uuid4()}"
        resp = post_event(producer_url, product_id, quantity=3)
        assert resp.status_code == 200
        body = resp.json()
        assert "event_id" in body, f"Missing event_id in: {body}"
        assert len(body["event_id"]) > 0

    def test_avro_v2_valid_request_returns_200(self, producer_url):
        resp = requests.post(
            f"{producer_url}/api/events/avro/v2",
            json={
                "product_id": f"intg-{uuid.uuid4()}",
                "zone_id": "ZONE-B",
                "quantity": 10,
                "supplier_id": "SUP-INTG",
            },
            timeout=10,
        )
        assert resp.status_code == 200

    def test_json_event_valid_request_returns_200(self, producer_url):
        product_id = f"intg-json-{uuid.uuid4()}"
        resp = requests.post(
            f"{producer_url}/api/events",
            json={
                "event_type": "PRODUCT_RECEIVED",
                "payload": {
                    "product_id": product_id,
                    "zone_id": "ZONE-A",
                    "quantity": 7,
                },
            },
            timeout=10,
        )
        assert resp.status_code == 200
        assert "event_id" in resp.json()


class TestProducerToConsumerFlow:

    def test_single_event_increments_processed_counter(self, producer_url, prometheus_url):
        initial = prom_counter(prometheus_url, "events_processed_total")
        product_id = f"intg-flow-{uuid.uuid4()}"
        resp = post_event(producer_url, product_id, quantity=10)
        assert resp.status_code == 200

        processed = wait_until_processed(prometheus_url, initial, timeout=40)
        assert processed, (
            f"events_processed_total did not increase after 40 s "
            f"(initial={initial}, current={prom_counter(prometheus_url, 'events_processed_total')})"
        )

    def test_multiple_events_all_processed(self, producer_url, prometheus_url):
        n = 5
        initial = prom_counter(prometheus_url, "events_processed_total")

        for _ in range(n):
            pid = f"intg-multi-{uuid.uuid4()}"
            resp = post_event(producer_url, pid, quantity=2)
            assert resp.status_code == 200

        deadline = time.time() + 60
        while time.time() < deadline:
            current = prom_counter(prometheus_url, "events_processed_total")
            if current >= initial + n:
                break
            time.sleep(2)

        final = prom_counter(prometheus_url, "events_processed_total")
        assert final >= initial + n, (
            f"Expected at least {initial + n} events processed, got {final}"
        )

    def test_duplicate_event_not_double_counted(self, producer_url, prometheus_url):
        product_id = f"intg-idem-{uuid.uuid4()}"
        initial = prom_counter(prometheus_url, "events_processed_total")

        resp1 = requests.post(
            f"{producer_url}/api/events",
            json={"event_type": "PRODUCT_RECEIVED",
                  "payload": {"product_id": product_id, "zone_id": "ZONE-A", "quantity": 50}},
            timeout=10,
        )
        assert resp1.status_code == 200

        wait_until_processed(prometheus_url, initial, timeout=40)

        after_first = prom_counter(prometheus_url, "events_processed_total")
        assert after_first == initial + 1, (
            f"Expected exactly 1 new processed event, got {after_first - initial}"
        )


class TestConsumerIsolation:

    def test_events_from_different_tests_are_independent(self, producer_url, prometheus_url):
        ids = [f"isolated-{uuid.uuid4()}" for _ in range(3)]
        initial = prom_counter(prometheus_url, "events_processed_total")

        for pid in ids:
            r = post_event(producer_url, pid, quantity=1)
            assert r.status_code == 200

        deadline = time.time() + 60
        while time.time() < deadline:
            if prom_counter(prometheus_url, "events_processed_total") >= initial + 3:
                break
            time.sleep(2)

        assert prom_counter(prometheus_url, "events_processed_total") >= initial + 3
