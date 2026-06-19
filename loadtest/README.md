# Load tests (P6)

Validates the NFR targets: **`<50ms` cached reads** and **2,000 event writes/s** without
table-lock cascades. Run against a deployed stack (`docker compose up`), not in CI.

## Prereqs

```sh
# create a tenant + a customer with balance, export IDs
TENANT=$(curl -s -XPOST localhost:8081/platform/tenants \
  -H 'content-type: application/json' -d '{"name":"load","currency":"USD"}' | jq -r .id)
# seed an earning rule so events mint
curl -s -XPOST localhost:8081/admin/earning-rules -H "X-Tenant-Id: $TENANT" \
  -H 'content-type: application/json' \
  -d '{"event_type":"purchase","condition":{},"base_points":10}'
```

## Read latency — target p99 < 50ms

```sh
TENANT=$TENANT k6 run read_latency.js
```

## Write throughput — target ≥ 2000 events/s

```sh
TENANT=$TENANT k6 run write_throughput.js
```

Both scripts assert their thresholds and exit non-zero on miss, so they can gate a release.
Watch Postgres `pg_stat_activity` / lock waits during the write run to confirm no cascading
table locks (the SKIP LOCKED queue + per-row `FOR UPDATE` design should keep waits flat).
