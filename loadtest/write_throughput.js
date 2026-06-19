import http from "k6/http";
import { check } from "k6";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

// NFR: ≥ 2000 event writes/s. Ingestion just persists PENDING (the worker processes async),
// so the API write path stays cheap; the worker is scaled separately.
const TENANT = __ENV.TENANT;
const BASE = __ENV.BASE || "http://localhost:8080";

export const options = {
  scenarios: {
    writes: {
      executor: "constant-arrival-rate",
      rate: 2000,
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 200,
      maxVUs: 800,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_reqs: ["rate>2000"],
  },
};

export default function () {
  const body = JSON.stringify({
    customer_external_id: `c${__VU}`,
    event_type: "purchase",
    payload: { amount_cents: 500 },
  });
  const res = http.post(`${BASE}/v1/events`, body, {
    headers: {
      "X-Tenant-Id": TENANT,
      "Idempotency-Key": uuidv4(),
      "content-type": "application/json",
    },
  });
  check(res, { "200": (r) => r.status === 200 });
}
