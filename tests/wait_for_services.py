#!/usr/bin/env python3
import sys
import time
import requests

TIMEOUT = 600
INTERVAL = 5

CHECKS = [
    {
        "name": "consumer (health)",
        "fn": lambda: requests.get("http://localhost:9090/health", timeout=5).status_code == 200,
    },
    {
        "name": "Prometheus",
        "fn": lambda: requests.get("http://localhost:9091/-/ready", timeout=5).status_code == 200,
    },
    {
        "name": "producer (health)",
        "fn": lambda: requests.get("http://localhost:8080/health", timeout=5).status_code == 200,
    },
]


def wait_for(check, timeout=TIMEOUT):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if check["fn"]():
                print(f"  ✓ {check['name']}")
                return True
        except Exception:
            pass
        time.sleep(INTERVAL)
    print(f"  ✗ Timed out waiting for {check['name']}", file=sys.stderr)
    return False


def main():
    print("Waiting for services…")
    all_ok = all(wait_for(c) for c in CHECKS)
    if not all_ok:
        print("One or more services failed to start in time.", file=sys.stderr)
        sys.exit(1)
    print("Services up — waiting 30 s extra for Cassandra cluster quorum…")
    time.sleep(30)
    print("All services ready.")


if __name__ == "__main__":
    main()
