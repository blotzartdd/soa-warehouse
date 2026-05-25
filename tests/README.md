# Testing & Observability — soa-warehouse

## Running locally

```bash
# Start all services
docker compose up -d --build

# Wait until ready
python tests/wait_for_services.py

# Unit tests (Rust)
cargo test --workspace

# Integration tests
pytest tests/integration/ -v

# E2E tests
pytest tests/e2e/ -v

# Load test (requires k6)
k6 run --env BASE_URL=http://localhost:8080 tests/load/load_test.js

# Prometheus metrics validation
python tests/metrics/check_metrics.py
```

---

## CI pipeline structure

```
push / PR
  └─ build-and-unit-tests  [matrix: consumer, producer]
       ├─ cargo build
       └─ cargo test
  └─ system-tests  (needs: build-and-unit-tests)
       ├─ docker compose build
       ├─ docker compose up -d
       ├─ wait_for_services.py
       ├─ pytest tests/integration/
       ├─ pytest tests/e2e/
       ├─ k6 run tests/load/load_test.js
       ├─ python tests/metrics/check_metrics.py
       ├─ consumer health-check assertion
       └─ upload artifacts (k6 results, service logs on failure)
```

---

## Test overview

| Suite | File | What it tests |
|-------|------|---------------|
| Integration | `tests/integration/test_integration.py` | Service-to-service: producer HTTP → Kafka → consumer; counter increment via Prometheus |
| E2E | `tests/e2e/test_e2e.py` | Full user scenarios: POST event → check Cassandra inventory |
| Load | `tests/load/load_test.js` | 10 VUs × 30 s against `/api/events/avro/v1`; fails CI if thresholds exceeded |
| Metrics | `tests/metrics/check_metrics.py` | Post-load Prometheus check: error rate + p95 latency |

---

## SLI / SLO definitions

Metrics are calculated from Prometheus data; no hard-coded values.

| SLI | Description | PromQL | SLO (target) | Failure threshold |
|-----|-------------|--------|-------------|-------------------|
| **Event processing success rate** | Fraction of events that reach Cassandra without error | `1 - sum(rate(cassandra_write_errors_total[5m])) / (sum(rate(events_processed_total[5m])) + sum(rate(cassandra_write_errors_total[5m])) + 0.0001)` | **> 99 %** | < 95 % |
| **Event processing latency p95** | 95th-percentile time from Kafka receive to Cassandra commit | `histogram_quantile(0.95, sum(rate(event_processing_duration_seconds_bucket[5m])) by (le))` | **< 500 ms** | > 1 000 ms |
| **Kafka consumer lag** | Number of unprocessed messages waiting in the topic | `sum(consumer_lag)` | **< 100** | > 1 000 |

### Threshold rationale

**Success rate (> 99 % SLO, < 95 % failure)**

Cassandra write failures are non-transient by default (network partition or
node outage).  A 1 % error rate means roughly 1 in 100 inventory updates are
lost, which causes silent data inconsistency.  The 95 % failure boundary
triggers an alert when a systemic problem (e.g., Cassandra quorum loss) begins.

**p95 latency (< 500 ms SLO, > 1 s failure)**

The consumer is a Kafka-to-Cassandra bridge; clients are asynchronous producers
who do not wait for the consumer.  500 ms p95 is generous enough for a
3-replica QUORUM write, yet tight enough to detect pathological Cassandra slow
queries.  1 s is chosen as the "something is badly wrong" boundary.

**Consumer lag (< 100 SLO, > 1 000 failure)**

A lag of 100 events at normal throughput is < 1 second of backlog.  Crossing
1 000 means the consumer has fallen significantly behind, which may indicate
it is restarting repeatedly, Cassandra is rejecting writes, or throughput has
spiked well above capacity.

### SLIs in alerts and CI

* `check_metrics.py` enforces the **success rate** and **p95 latency** SLOs
  after every load test — CI fails if violated.
* `monitoring/alerts.yml` fires `HighCassandraErrorRate` (> 5 % error rate)
  and `HighEventProcessingLatency` (p95 > 1 s) as runtime alerts.
* `HighKafkaConsumerLag` fires when lag > 1 000 (the failure threshold).

---

## Demonstrating alerts fire

1. Start the full stack: `docker compose up -d --build`
2. Open Alertmanager UI: http://localhost:9093
3. Open Prometheus alerts: http://localhost:9091/alerts

To trigger **HighKafkaConsumerLag** manually:

```bash
# Stop the consumer so messages accumulate
docker compose stop consumer

# Flood the topic
for i in $(seq 1 1200); do
  curl -s -X POST http://localhost:8080/api/events/avro/v1 \
    -H 'Content-Type: application/json' \
    -d '{"product_id":"lag-demo","zone_id":"Z","quantity":1}' > /dev/null
done

# Check Prometheus — consumer_lag should be > 1000
curl -s 'http://localhost:9091/api/v1/query?query=consumer_lag'

# After 5 min the alert transitions to "firing" in Alertmanager
open http://localhost:9093

# Restart consumer to drain the lag
docker compose start consumer
```

To trigger **ServiceDown**:

```bash
docker compose stop consumer
# Wait ~1 min → "ServiceDown" fires in Prometheus / Alertmanager
```
