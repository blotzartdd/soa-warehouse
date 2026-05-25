import time

import pytest
import requests

PRODUCER_URL = "http://localhost:8080"
CONSUMER_URL = "http://localhost:9090"
PROMETHEUS_URL = "http://localhost:9091"


@pytest.fixture(scope="session")
def producer_url():
    return PRODUCER_URL


@pytest.fixture(scope="session")
def consumer_url():
    return CONSUMER_URL


@pytest.fixture(scope="session")
def prometheus_url():
    return PROMETHEUS_URL


@pytest.fixture(scope="session")
def cassandra_session():
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy

    cluster = Cluster(
        ["localhost"],
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="dc1"),
        connect_timeout=60,
        control_connection_timeout=60,
    )
    session = cluster.connect("warehouse")
    yield session
    cluster.shutdown()


def wait_for_metric_increment(prom_url: str, query: str, initial: float,
                               timeout: int = 30, interval: int = 2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = _prom_scalar(prom_url, query)
        if val is not None and val > initial:
            return True
        time.sleep(interval)
    return False


def _prom_scalar(prom_url: str, query: str):
    try:
        resp = requests.get(f"{prom_url}/api/v1/query", params={"query": query}, timeout=5)
        data = resp.json()["data"]["result"]
        if data:
            return float(data[0]["value"][1])
    except Exception:
        pass
    return None
