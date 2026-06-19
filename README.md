# Open Loyalty Engine (`fides`)

Headless, multi-tenant, immutable-ledger loyalty engine. Rust · PostgreSQL 18 · Docker.

See [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md) for architecture and roadmap. This is **P0** — the scaffold.

## Layout

| Crate | Role |
|---|---|
| `crates/core` | Pure domain logic, no I/O (ledger, FEFO, rules) — unit-testable |
| `crates/db` | sqlx pools, migrations, tenant-scoped txn helper (RLS) |
| `crates/api` | axum binary — `:8080` runtime + `:8081` admin |
| `crates/worker` | background binary — event processor, sweeper, webhooks (P1+) |
| `crates/shared` | config, errors, telemetry, tenant types |

## Run locally

```sh
docker compose up --build        # db + api + worker + jaeger
curl localhost:8080/healthz
# traces UI (OTel spans): http://localhost:16686  (pick service fides-api / fides-worker)
# create a tenant (platform-admin, no X-Tenant-Id)
curl -XPOST localhost:8081/platform/tenants -H 'content-type: application/json' \
  -d '{"name":"acme","currency":"usd"}'
# use it (runtime, tenant-scoped)
curl localhost:8080/v1/whoami -H "X-Tenant-Id: <id-from-above>"
```

## Develop

```sh
cargo build --workspace
cargo test  --workspace          # integration tests need a running Docker daemon
cargo fmt --all && cargo clippy --all-targets
```

## Tenancy & RLS (important)

The runtime pool connects then runs `SET ROLE fides_app` so queries execute as a **non-superuser**
— Postgres superusers *bypass* Row-Level Security. Every tenant-scoped table enables `FORCE ROW
LEVEL SECURITY` with a `tenant_id = current_tenant()` policy. `current_tenant()` reads the
`app.tenant_id` GUC set per transaction by `fides_db::set_tenant`. Migrations run via a separate
admin pool. Never point the runtime at a superuser role without the `SET ROLE`.
