import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    warehouse_load: {
      executor: 'constant-vus',
      vus: 10,
      duration: '30s',
    },
  },
  thresholds: {
    http_req_failed:   ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
    checks:            ['rate>0.99'],
  },
};

const successfulEvents = new Counter('successful_events');
const failedEvents     = new Counter('failed_events');

export default function () {
  const productId = `load-vu${__VU}-iter${__ITER}`;

  const payload = JSON.stringify({
    product_id: productId,
    zone_id:    'ZONE-LOAD',
    quantity:   1,
  });

  const res = http.post(`${BASE_URL}/api/events/avro/v1`, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '5s',
  });

  const ok = check(res, {
    'status is 200':          (r) => r.status === 200,
    'response has event_id':  (r) => {
      try { return JSON.parse(r.body).event_id !== undefined; }
      catch (_) { return false; }
    },
  });

  if (ok) {
    successfulEvents.add(1);
  } else {
    failedEvents.add(1);
  }

  sleep(0.1);
}

export function handleSummary(data) {
  return {
    'stdout': JSON.stringify(data, null, 2),
  };
}
