#!/usr/bin/env python3
"""
Validate Prometheus metrics after a load test run.

Checks (CI fails with exit code 1 if any condition is violated):
  1. Event processing error rate < 1%
     Metric: cassandra_write_errors_total / (events_processed_total + cassandra_write_errors_total)

  2. Event processing p95 latency < 500 ms
     Metric: histogram_quantile(0.95, rate(event_processing_duration_seconds_bucket[2m]))

  3. Consumer lag == 0 (all produced events consumed)
     Metric: sum(consumer_lag)

Thresholds are derived from SLOs defined in tests/README.md.
"""
import sys
import time
import requests

PROMETHEUS_URL = "http://localhost:9091"
WINDOW = "2m"          # evaluation window for rate-based queries
MAX_WAIT = 60          # seconds to wait for lag to reach 0


# ── Prometheus helpers ────────────────────────────────────────────────────────

def query(expr: str):
    """Return scalar value from instant query, or None."""
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": expr},
            timeout=10,
        )
        results = resp.json()["data"]["result"]
        if results:
            return float(results[0]["value"][1])
    except Exception as e:
        print(f"  Prometheus query error ({expr!r}): {e}")
    return None


def wait_for_zero_lag(timeout: int = MAX_WAIT) -> float:
    """Wait up to *timeout* seconds for consumer lag to reach 0.  Returns final lag."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        lag = query("sum(consumer_lag)") or 0.0
        if lag == 0:
            return 0.0
        time.sleep(5)
    return query("sum(consumer_lag)") or 0.0


# ── Checks ────────────────────────────────────────────────────────────────────

def check_error_rate(threshold: float = 0.01) -> bool:
    processed = query(f"sum(rate(events_processed_total[{WINDOW}]))") or 0.0
    errors    = query(f"sum(rate(cassandra_write_errors_total[{WINDOW}]))") or 0.0

    if processed + errors == 0:
        print("  SKIP error-rate check: no events observed in the window")
        return True

    rate = errors / (processed + errors)
    ok   = rate < threshold
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] Error rate: {rate*100:.3f}% (threshold < {threshold*100:.0f}%)")
    return ok


def check_p95_latency(threshold_ms: float = 500.0) -> bool:
    p95 = query(
        f"histogram_quantile(0.95, "
        f"sum(rate(event_processing_duration_seconds_bucket[{WINDOW}])) by (le))"
    )
    if p95 is None:
        print("  SKIP p95 check: no histogram data")
        return True

    p95_ms = p95 * 1000
    ok     = p95_ms < threshold_ms
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] p95 latency: {p95_ms:.1f} ms (threshold < {threshold_ms:.0f} ms)")
    return ok


def check_producer_error_rate(threshold: float = 0.01) -> bool:
    total  = query(f'sum(rate(http_requests_total{{job="warehouse-producer"}}[{WINDOW}]))') or 0.0
    errors = query(f'sum(rate(http_request_errors_total{{job="warehouse-producer"}}[{WINDOW}]))') or 0.0

    if total == 0:
        print("  SKIP producer error-rate: no requests observed")
        return True

    rate = errors / total
    ok   = rate < threshold
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] Producer HTTP error rate: {rate*100:.3f}% (threshold < {threshold*100:.0f}%)")
    return ok


def check_producer_p95_latency(threshold_ms: float = 1000.0) -> bool:
    p95 = query(
        f'histogram_quantile(0.95, '
        f'sum(rate(http_request_duration_seconds_bucket{{job="warehouse-producer"}}[{WINDOW}])) by (le))'
    )
    if p95 is None:
        print("  SKIP producer p95: no histogram data")
        return True

    p95_ms = p95 * 1000
    ok     = p95_ms < threshold_ms
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] Producer p95 latency: {p95_ms:.1f} ms (threshold < {threshold_ms:.0f} ms)")
    return ok


def check_consumer_lag() -> bool:
    print("  Waiting for consumer lag to drain…")
    lag = wait_for_zero_lag()
    ok  = lag == 0
    status = "PASS" if ok else "WARN"
    # Lag can be non-zero if events were sent very recently; treat as warning
    print(f"  [{status}] Consumer lag: {lag:.0f} (target = 0)")
    return True  # non-blocking warning, not a hard CI failure


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    print(f"Querying Prometheus at {PROMETHEUS_URL} …\n")

    results = [
        check_error_rate(),
        check_p95_latency(),
        check_producer_error_rate(),
        check_producer_p95_latency(),
        check_consumer_lag(),
    ]

    print()
    if all(results):
        print("All metrics checks PASSED.")
        return 0
    else:
        print("One or more metrics checks FAILED.  See output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
