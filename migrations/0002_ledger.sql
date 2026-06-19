-- P1: the financial core. Immutable ledger + denormalized cache + tiers + idempotency.
-- Every table is tenant-scoped and follows the RLS template from 0001.

-- ---------------------------------------------------------------------------
-- tiers — milestone thresholds on lifetime_earned (FR-1.3).
-- ---------------------------------------------------------------------------
CREATE TABLE tiers (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        uuid   NOT NULL REFERENCES tenants(id),
    name             text   NOT NULL,
    threshold_points bigint NOT NULL CHECK (threshold_points >= 0),
    UNIQUE (tenant_id, name)
);
CREATE INDEX tiers_tenant_threshold_idx ON tiers (tenant_id, threshold_points);

ALTER TABLE customers ADD COLUMN current_tier_id uuid REFERENCES tiers(id);

-- ---------------------------------------------------------------------------
-- points_ledger — APPEND ONLY. The five txn types are the only ways points move (FR-3.1).
-- available_amount tracks FEFO remaining on EARN rows (consumed in P3).
-- ---------------------------------------------------------------------------
CREATE TABLE points_ledger (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        uuid   NOT NULL REFERENCES tenants(id),
    customer_id      uuid   NOT NULL REFERENCES customers(id),
    txn_type         text   NOT NULL CHECK (txn_type IN ('EARN','REDEEM','EXPIRE','ADJUSTMENT','UNLOCK')),
    amount           bigint NOT NULL,            -- signed
    available_amount bigint,                     -- NULL except on EARN rows
    expires_at       timestamptz,                -- FEFO key (P3)
    source_event_id  uuid,                        -- FK added in P2
    created_at       timestamptz NOT NULL DEFAULT now()
);
-- FEFO scan: oldest-expiring EARN rows with points left.
CREATE INDEX ledger_fefo_idx ON points_ledger (customer_id, expires_at)
    WHERE available_amount > 0;
CREATE INDEX ledger_history_idx ON points_ledger (customer_id, created_at);

-- ---------------------------------------------------------------------------
-- customer_balances — denormalized cache, recomputed in the SAME txn as the
-- ledger write (FR-1.2). One row per customer.
-- ---------------------------------------------------------------------------
CREATE TABLE customer_balances (
    customer_id       uuid PRIMARY KEY REFERENCES customers(id),
    tenant_id         uuid   NOT NULL REFERENCES tenants(id),
    spendable_balance bigint NOT NULL DEFAULT 0 CHECK (spendable_balance >= 0),
    locked_balance    bigint NOT NULL DEFAULT 0 CHECK (locked_balance >= 0),
    lifetime_earned   bigint NOT NULL DEFAULT 0,
    lifetime_redeemed bigint NOT NULL DEFAULT 0,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- idempotency_keys — dedupe money-mutating requests (constraint #6).
-- Inserted + finalized inside the write txn, so the key commits iff the mint did.
-- ---------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    tenant_id           uuid   NOT NULL REFERENCES tenants(id),
    key                 text   NOT NULL,
    request_fingerprint text   NOT NULL,
    response_status     int,
    response_body       jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, key)
);

-- ===== RLS (same template as 0001) =========================================
ALTER TABLE tiers ENABLE ROW LEVEL SECURITY;
ALTER TABLE tiers FORCE ROW LEVEL SECURITY;
CREATE POLICY tiers_isolation ON tiers
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

ALTER TABLE points_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE points_ledger FORCE ROW LEVEL SECURITY;
CREATE POLICY ledger_isolation ON points_ledger
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

ALTER TABLE customer_balances ENABLE ROW LEVEL SECURITY;
ALTER TABLE customer_balances FORCE ROW LEVEL SECURITY;
CREATE POLICY balances_isolation ON customer_balances
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

ALTER TABLE idempotency_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY idempotency_isolation ON idempotency_keys
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());
-- ===========================================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON tiers TO fides_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON points_ledger TO fides_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON customer_balances TO fides_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON idempotency_keys TO fides_app;
