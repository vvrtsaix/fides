# Open Loyalty Engine — Implementation Plan & Technology Decisions

**Status:** Draft v1.7 · **Date:** 2026-06-19 · **Built:** P0–P6 + gaps + OTel tracing (spans in Jaeger)
**Source:** `PRD.html` (Open Loyalty Engine, headless, immutable-ledger, schema-driven)
**Stack decision:** Rust (latest stable) · PostgreSQL 18 · Docker (test + prod)
**Deployment context:** Multi-tenant SaaS · core engine on **private network**, called by other internal services · **containerized** (Docker for test *and* prod; orchestration out of scope).

> **Version note:** target Rust *latest stable* pinned via `rust-toolchain.toml` (≈1.88 as of mid-2026; "1.96" does not exist yet — do not hardcode a future number). PostgreSQL 18 confirmed.

---

## 1. Guiding constraints (from PRD)

These four non-negotiables shape every decision below:

1. **Immutable ledger** — balances only change via append-only rows (`EARN`, `REDEEM`, `EXPIRE`, `ADJUSTMENT`, `UNLOCK`). No `UPDATE balance = balance + x`.
2. **ACID + same-transaction cache** — every ledger write updates `customer_balances` inside the *same* transaction, serialized per-customer by a `FOR UPDATE` row lock (READ COMMITTED — see §5 isolation note).
3. **Velocity SLA** — `<50ms` cached reads, `2,000` async event writes/s without table-lock cascades.
4. **Data minimization** — no PII beyond `external_id` + contact strings; `ANONYMIZED` toggle zeroes identity but keeps financial history.
5. **Multi-tenant isolation** — every row scoped by `tenant_id`; each tenant has **one fixed currency** (no FX, no per-txn currency). Engine sits on a private network — no end-user auth, but `tenant_id` is mandatory on every request.
6. **Idempotent writes** — every money-mutating request carries an `Idempotency-Key`. A retry must never double-mint, double-redeem, or double-adjust. Replays return the original result.

---

## 2. Technology decisions

| Concern | Decision | Why | Rejected alternatives |
|---|---|---|---|
| Language | **Rust (latest stable, edition 2021)** — pinned in `rust-toolchain.toml` | Type-safe money math, no GC pauses at 2k/s, fearless concurrency for workers | Go (less type leverage for ledger invariants), TS/Java/Python (throughput/latency risk) |
| Async runtime | **Tokio** | De-facto standard, powers axum/sqlx | async-std (declining) |
| HTTP framework | **axum** | Tower middleware, tokio-native, minimal overhead, mature | actix-web (more complex actor model), warp (ergonomics) |
| DB | **PostgreSQL 18** | JSONB, `FOR UPDATE` / `SKIP LOCKED`, repeatable-read, partial indexes, **Row-Level Security** for tenant isolation — every PRD primitive maps to native PG | MySQL (weaker JSONB + locking), any NoSQL (no ACID ledger) |
| DB access | **sqlx** | Async, **compile-time-checked raw SQL**, explicit control over txn/isolation/locking — exactly what a ledger needs | SeaORM/Diesel (ORM hides the locking & SQL we must control) |
| Money / points | **`i64` for points**, **`rust_decimal::Decimal` for campaign budgets/currency** | Points are whole units → `i64` is exact, atomic in SQL, smallest index, fewest failure modes; budgets are real fractional money → `Decimal`. Never mix the two types, never `f64` | floating point (forbidden for money); `Decimal` for points (needless cost when points are integers) |
| Tenant scoping | **Mandatory `X-Tenant-Id` request header** + axum middleware → request context → query filter; **Postgres RLS** as backstop | No auth (private network, trusted callers) but isolation is non-negotiable. RLS means a forgotten `WHERE tenant_id` can't leak data | JWT/auth tokens (unneeded on private net), trusting callers to filter (no defense-in-depth) |
| Queue (events + webhooks) | **Postgres-backed queue** via `SELECT … FOR UPDATE SKIP LOCKED` + **transactional outbox** | Keeps single source of truth, transactional enqueue (no dual-write), no extra infra to start | Redis/RabbitMQ/Kafka (adds infra + dual-write inconsistency; revisit only if PG queue caps out) |
| Migrations | **sqlx-cli migrations** | Versioned SQL in-repo, runs in CI + container entrypoint | refinery (redundant with sqlx) |
| Config | **`figment`** (env + file layers) | 12-factor, k8s ConfigMap/Secret friendly | hand-rolled env parsing |
| Observability | **`tracing` + OpenTelemetry**, **Prometheus** metrics | Distributed tracing across api↔worker, SLA dashboards | log-only (insufficient for latency SLA) |
| Test DB | **`testcontainers-rs`** (ephemeral Postgres) | Real PG per test run in Docker — matches prod engine exactly | sqlite/mocks (won't exercise locking/isolation) |
| Container | **Distroless multi-stage** (`cargo-chef` cached build) | Small, CVE-light images for k8s | full debian base (bloat) |

**Optional later:** Redis read cache in front of `customer_balances` *only if* PG indexed reads miss the `<50ms` SLA under load. PRD already specifies an in-DB cache table — start there, measure, add Redis if needed.

---

## 2b. Architecture shape — modular monolith (not microservices)

**Decision: standalone modular monolith. One codebase, one Postgres, two deployable processes.**

The ledger's defining guarantee is a *single ACID transaction* spanning ledger write → cache recompute → tier eval (FR-1.2, NFR-3.1). Splitting ledger/rules/rewards into separate services forces distributed transactions (2PC / sagas), which **breaks that guarantee**. Microservices would sacrifice the core invariant for organizational tidiness — rejected.

| Process | Role | Scales on |
|---|---|---|
| `api` (axum bin) | sync path — balance reads, locks, redemptions, admin CRUD | read latency (`<50ms`) |
| `worker` (bin) | async event processor + webhook dispatcher + lock sweeper + expiration job | write throughput (`2000/s`) |

Two **processes**, not two services: both link `core` + `db`, share migrations + types, and talk to the same DB — no network hop in the ledger path. Split exists only so the latency-bound and throughput-bound workloads scale containers independently. **Extract a real service later only if** a bounded context (likely webhook delivery first) proves independent — earn the split, don't pre-pay the distributed-systems tax.

---

## 3. Workspace layout (cargo workspace)

```
fides/
├─ Cargo.toml                  # workspace
├─ crates/
│  ├─ core/                    # PURE domain — no I/O. Ledger math, FEFO, rule eval, lock state machine.
│  ├─ db/                      # sqlx pool, repositories, migrations, txn/isolation helpers
│  ├─ api/                     # axum REST + webhook subscription endpoints (bin)
│  ├─ worker/                  # event processor, webhook dispatcher, expiration sweeper (bin)
│  └─ shared/                  # error types, config (figment), telemetry, DTOs
├─ migrations/                 # sqlx versioned SQL
├─ docker/                     # Dockerfile (distroless), compose for local dev + test
└─ tests/                      # integration (testcontainers)
```

**Why split `core` as pure logic:** ledger invariants (FEFO consumption, lock transitions, tier breach) are the highest-risk code. Keeping them I/O-free makes them exhaustively unit-testable without a DB. The `db` crate wires pure decisions into transactions.

---

## 4. Data model (maps every FR)

Append-only core + denormalized cache. **Every business table carries `tenant_id`** (NOT NULL, FK → `tenants`), and RLS policies filter on the session's tenant. Sketch (not final DDL):

```sql
-- Tenancy — one fixed currency per tenant, no FX (constraint #5)
tenants(id PK, name, currency CHAR(3),               -- ISO-4217, immutable after first txn
        status, created_at)

-- Identity (FR-1.1, NFR ANONYMIZED)
customers(id PK, tenant_id FK, external_id, email, phone,
          status ENUM('ACTIVE','ANONYMIZED'),
          current_tier_id FK, created_at,
          UNIQUE(tenant_id, external_id))             -- external_id unique PER tenant

-- Denormalized cache (FR-1.2) — recomputed in same txn as ledger write
customer_balances(customer_id PK/FK, spendable_balance BIGINT, locked_balance BIGINT,
                  lifetime_earned BIGINT, lifetime_redeemed BIGINT, updated_at)

tiers(id PK, name, threshold_points BIGINT)           -- FR-1.3

-- Immutable ledger (FR-3.1, FR-3.2) — APPEND ONLY
points_ledger(id PK, customer_id FK,
              txn_type ENUM('EARN','REDEEM','EXPIRE','ADJUSTMENT','UNLOCK'),
              amount BIGINT,                            -- signed
              available_amount BIGINT,                  -- FEFO remaining on EARN rows
              expires_at TIMESTAMPTZ,                   -- FEFO key
              source_event_id FK NULL, created_at)
-- indexes: (customer_id, expires_at) WHERE available_amount > 0   -- FEFO scan
--          (customer_id, created_at)                              -- history

points_locks(id PK, tenant_id FK, customer_id FK, amount BIGINT,
             status ENUM('HELD','FULFILLED','RELEASED'),
             expires_at, created_at)                    -- FR-3.3; sweeper scans HELD WHERE expires_at < now()

-- Events + rules (FR-2.*)
loyalty_events(id PK, customer_id FK, event_type,
               payload JSONB,                           -- schema-less intake
               status ENUM('PENDING','PROCESSED','FAILED'),
               locked_at NULL, created_at)              -- SKIP LOCKED queue column
earning_rules(id PK, event_type, condition JSONB, base_points BIGINT,
              modifiers JSONB, active BOOL)

-- Rewards (FR-4.*)
campaigns(id PK, budget_cap NUMERIC, current_spend NUMERIC, active)
rewards(id PK, campaign_id FK, cost_points BIGINT, available_stock INT)
vouchers(id PK, reward_id FK, customer_id FK, code UNIQUE,
         status ENUM('ISSUED','USED'), valid_until)

-- Idempotency (constraint #6) — dedupe across ALL money-mutating endpoints
idempotency_keys(tenant_id, key,                     -- caller-supplied Idempotency-Key
                 request_fingerprint,                 -- hash of route+body; mismatch on same key => 409
                 response_status, response_body JSONB, -- cached result, replayed verbatim
                 created_at,
                 PRIMARY KEY(tenant_id, key))

-- Webhooks + audit (FR-5.*)
webhook_subscriptions(id PK, url, event_filter, secret, active)
webhook_logs(id PK, subscription_id FK, payload JSONB, attempt INT,
             status, next_retry_at)                     -- outbox + backoff
audit_logs(id PK, actor, entity, old_values JSONB, new_values JSONB, created_at)  -- read-only
```

---

## 5. Concurrency & integrity strategy (the hard part)

| Mechanism | How | Covers |
|---|---|---|
| **Write transaction** | `READ COMMITTED` + per-customer `SELECT … FOR UPDATE` on `customer_balances` → append ledger row → **incrementally** update cache → evaluate tier → `COMMIT`. (Refined from REPEATABLE READ in P1 — see note.) | FR-1.2, FR-1.3, NFR-3.1 |
| **FEFO redemption** | In txn, `SELECT … WHERE available_amount>0 ORDER BY expires_at FOR UPDATE`, drain oldest-first, decrement `available_amount`, write `REDEEM` rows | FR-3.2 |
| **Point locks** | `HELD` row deducts spendable / adds locked (same txn). **Caller** drives happy path → `FULFILLED` (write `REDEEM`) on order success, `RELEASED` (write `UNLOCK`) on cancel. **Sweeper** is the backstop: scans `HELD WHERE expires_at < now()` → `RELEASED`, covering crashed/forgetful callers | FR-3.3 |
| **Inventory race** | `SELECT … FOR UPDATE` on `rewards` row before decrementing `available_stock` (pessimistic) | FR-4.2 |
| **Budget overdraft** | In same txn as reward issue: check `current_spend + cost <= budget_cap`, then `UPDATE current_spend` | FR-4.1 |
| **Event queue** | Worker pulls `SELECT … WHERE status='PENDING' FOR UPDATE SKIP LOCKED LIMIT n` — no lock cascades at 2k/s | FR-2.2, NFR write SLA |
| **Webhook outbox** | Tier/balance changes write `webhook_logs` row in the *business* txn; dispatcher reads + POSTs + exponential backoff on `next_retry_at` | FR-5.1, FR-5.2 |
| **Audit** | Config/adjustment mutations write `audit_logs` snapshot in same txn; table revoked from `UPDATE/DELETE` | FR-5.3 |
| **Anonymization** | `UPDATE customers SET email=NULL, phone=NULL, status='ANONYMIZED'` — ledger untouched | NFR-3.3 |
| **Idempotency** | In the write txn, `INSERT INTO idempotency_keys … ON CONFLICT DO NOTHING`. 0 rows → replay: return cached `response_body`. Fingerprint mismatch on same key → `409`. Insert + ledger write share one txn, so the key commits iff the mint did | Constraint #6 |
| **Tenant isolation** | Middleware sets `SET LOCAL app.tenant_id = $1` from `X-Tenant-Id` per txn; **RLS** policy `tenant_id = current_setting('app.tenant_id')::uuid` on every table — leak-proof even if a query forgets the filter | Constraint #5 |

**Isolation note (refined in P1).** Original plan said REPEATABLE READ. In practice, RR + a per-row `FOR UPDATE` lock turns concurrent writes to the *same* customer into a 40001 serialization-failure storm (proven by the concurrent-EARN test). The cache is updated **incrementally** (`spendable += delta`), not re-aggregated via `SUM`, so there is no phantom-read exposure that RR would protect against — the row lock alone serializes correctly. Decision: **READ COMMITTED + `FOR UPDATE`**. The bounded retry wrapper (3 attempts) is kept for the rare deadlock (40P01). Aggregate/reporting reads that scan many ledger rows will use RR or a snapshot when added.

---

## 6. Phased delivery

| Phase | Goal | Key deliverables | Exit criteria |
|---|---|---|---|
| **P0 — Scaffold + tenancy** ✅ | Buildable skeleton + isolation foundation | Workspace, sqlx+migrations, `tenants` table, `X-Tenant-Id` middleware, **RLS policy template** (+ `fides_app` non-superuser role), Dockerfile (cargo-chef/distroless), compose, testcontainers harness, CI, tracing/metrics boot | ✅ `cargo test` green; cross-tenant read **denied by RLS** |
| **P1 — Financial core** ✅ | The ledger | `customers`, `points_ledger`, `customer_balances`, `tiers`, `idempotency_keys`; EARN/ADJUSTMENT txns; same-txn incremental cache; tier evaluation; retry wrapper; idempotency dedup in-txn; runtime `transactions`/`balance` + admin `tiers` endpoints | ✅ Concurrent EARN holds `cache == SUM(ledger)`; tier breach fires; duplicate `Idempotency-Key` mints once; reused key w/ changed payload → 409 |
| **P2 — Events & rules** ✅ | Async intake | `loyalty_events` JSONB intake (`PENDING`), `earning_rules`, SKIP LOCKED worker (`fides_worker` BYPASSRLS role), pure JSONB condition eval, PENDING→PROCESSED/FAILED; mint idempotency keyed on event id | ✅ Matching event mints end-to-end; non-match is no-op; duplicate ingestion dedupes |
| **P3 — Locks, FEFO, expiry** ✅ | Spend safety | `points_locks` HELD/FULFILLED/RELEASED, pure FEFO planner + DB consume, expiration + lock sweepers | ✅ Lifecycle holds `spendable+locked == SUM(ledger)`; FEFO drains soonest-expiring first; sweeper expires due points |
| **P4 — Rewards** ✅ | Redemption | `campaigns`/`rewards`/`vouchers`, budget cap (FR-4.1), `FOR UPDATE` stock lock (FR-4.2), unique voucher mint, idempotent redemption | ✅ 2 concurrent claims on 1 stock → exactly 1 wins; out-of-stock/over-budget rejected; replay no double-issue |
| **P5 — Webhooks & audit** ✅ | Integration + compliance | Outbox `emit` + generic dispatcher w/ HMAC-SHA256 + exp backoff, `webhook_subscriptions`, append-only `audit_logs` (no UPDATE/DELETE grant), `ANONYMIZED` toggle | ✅ Emit fans out; 2xx→DELIVERED, fail→backoff→FAILED after MAX; anonymize keeps ledger + audited; audit UPDATE denied |
| **P6 — NFR hardening** ✅* | Hit the SLAs | k6 scripts (read p99<50ms, ≥2k writes/s) under `loadtest/`, request TraceLayer, index-usage regression test, prod Docker image | ✅ Index-usage test green; tracing wired; *load SLAs require running k6 against a deployed stack (not CI) |

All phases built and green: **28 tests** (11 core unit + 17 Postgres integration via testcontainers), `clippy -D warnings` clean, `cargo fmt` clean.

**Post-phase gap closure (v1.6):**
- **Webhook trigger wired** — `customer.tier_upgraded` is enqueued (`emit_in_tx`) inside the ledger txn whenever the tier changes, atomic with the upgrade.
- **Voucher `ISSUED→USED`** — `use_voucher` + `POST /v1/vouchers/{code}/use`, rejects already-used/expired.
- **Audit coverage broadened** — every config create (tier, earning-rule, campaign, reward, webhook-subscription) and every manual `ADJUSTMENT` writes an `audit_logs` snapshot (FR-5.3). Secrets excluded from snapshots.
- **Lock creation idempotent** — `create_lock` takes an `Idempotency-Key`; a retry returns the original lock instead of double-reserving.

**Observability (v1.7):** OpenTelemetry OTLP/HTTP export wired in `telemetry.rs` (enabled by `FIDES_OTEL_ENDPOINT`); `docker-compose` runs **Jaeger** (UI `:16686`). Verified end-to-end: `fides-api` emits nested `request`→`post_ledger_txn` spans, `fides-worker` emits `process_one` — visible in the Jaeger UI.
**Bug found + fixed during this step:** API used axum-0.8 `{param}` route syntax on axum 0.7 → all parameterized routes 404'd. Switched to `:param`. Escaped the test suite because integration tests call the `db` layer directly, not over HTTP — **follow-up: add an HTTP-level route smoke test.**
\* P6 SLA numbers are validated by running the provided k6 scripts against a deployed stack; the harness + index guard are in place, the throughput run is an ops step.

---

## 7. API surface (headless, REST)

One `api` binary, **three logical surfaces on two ports**. No auth tokens (private network); isolation via network policy + mandatory `X-Tenant-Id` + RLS. Splitting admin onto its own port lets the firewall restrict who can mutate money-minting rules — without building auth.

### Port `:8080` — Runtime (tenant-scoped, `X-Tenant-Id` required)
High-volume, latency-bound. Middleware rejects missing/unknown tenant `400`/`404`, then pins the txn's RLS tenant. **Mutating routes require `Idempotency-Key` header** (constraint #6) — replays return the original result.

- `POST /v1/events` — ingest event, returns `PENDING` immediately (FR-2.1/2.2) · idempotent
- `GET /v1/customers/{external_id}/balance` — cached read, `<50ms` (FR-1.2)
- `POST /v1/customers/{external_id}/locks` · `…/locks/{id}:fulfill` · `:release` (FR-3.3)
- `POST /v1/redemptions` — reward claim (FR-4.*)
- `POST /v1/customers/{external_id}:anonymize` (NFR-3.3)

### Port `:8081` — Admin (low-volume, **all routes audited**, FR-5.3)

**Tenant-admin** (`X-Tenant-Id` required):
- CRUD `/admin/earning-rules`, `/admin/tiers`, `/admin/campaigns`, `/admin/rewards`
- `/admin/webhook-subscriptions` (FR-5.1) · `POST /admin/adjustments` — manual `ADJUSTMENT`

**Platform-admin** (**no** `X-Tenant-Id` — operates across/above tenants):
- CRUD `/platform/tenants` — tenant lifecycle, sets immutable `currency`

---

## 8. Key risks

1. **Repeatable-read serialization failures at 2k/s** → bounded jittered retries + keep balance txns short; benchmark early in P6 (validate by end of P1).
2. **PG queue throughput ceiling** → SKIP LOCKED scales far; if it caps, swap worker queue for Redis/NATS behind the same `db`-layer trait. Outbox stays in PG.
3. **FEFO scan cost** → partial index `(customer_id, expires_at) WHERE available_amount>0`; monitor plan.
4. **Hot-customer lock contention** → acceptable for correctness; flag if a single customer becomes a throughput hotspot.

---

## 9. Decisions resolved (v1.2)

- ✅ **Points = `i64`** (whole); **budgets/currency = `Decimal`**.
- ✅ **Lock release = caller + sweeper** (caller happy path, sweeper backstop).
- ✅ **Multi-tenant**, one fixed currency per tenant, **no FX**.
- ✅ **Read cache** — Postgres-only first; add Redis only if P6 load test misses `<50ms`.
- ✅ **No auth** (private network); **`X-Tenant-Id` header mandatory** + Postgres RLS.
- ✅ **Lock TTL = 15 min** default, **sweeper poll = 30 s** — both config-driven, per-tenant override later.
- ✅ **Webhook signing = HMAC-SHA256**, per-subscription secret. Headers `X-Loyalty-Signature` + `X-Loyalty-Timestamp`; timestamp is part of signed payload → replay-resistant.
- ✅ **Currency immutable** after first transaction — DB-enforced.
- ✅ **Tenant provisioning** — engine owns `tenants` + exposes admin `POST /tenants`; onboarding *orchestration* lives upstream.

All open questions resolved for v1.2. No blockers remaining to start P0.
