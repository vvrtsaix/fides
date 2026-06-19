import http from "k6/http";
import { check } from "k6";

// NFR: cached balance reads p99 < 50ms (FR-1.2).
const TENANT = __ENV.TENANT;
const BASE = __ENV.BASE || "http://localhost:8080";

export const options = {
  scenarios: {
    reads: { executor: "constant-vus", vus: 50, duration: "30s" },
  },
  thresholds: {
    http_req_duration: ["p(99)<50"],
    checks: ["rate>0.99"],
  },
};

export default function () {
  const res = http.get(`${BASE}/v1/customers/c1/balance`, {
    headers: { "X-Tenant-Id": TENANT },
  });
  check(res, { "200": (r) => r.status === 200 });
}
