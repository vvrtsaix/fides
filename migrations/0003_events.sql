-- P2: async event ingestion + rules engine (FR-2.1, FR-2.2, FR-2.3).

-- ---------------------------------------------------------------------------
-- Worker role. The background worker processes events ACROSS all tenants, so it
-- cannot set a single app.tenant_id. It connects as fides_worker, which inherits
-- fides_app's table grants but has BYPASSRLS. Safe because every worker query
-- still binds tenant_id explicitly; RLS is the API's defense-in-depth, not the
-- worker's isolation mechanism.
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'fides_worker') THEN
        CREATE ROLE fides_worker NOLOGIN BYPASSRLS;
    END IF;
END $$;
GRANT fides_app TO fides_worker;

-- ---------------------------------------------------------------------------
-- loyalty_events — schema-less intake. JSONB payload, async status machine.
-- idempotency_key dedupes ingestion so a retried POST /events does not create a
-- second event (which would mint twice downstream).
-- ---------------------------------------------------------------------------
CREATE TABLE loyalty_events (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            uuid   NOT NULL REFERENCES tenants(id),
    customer_external_id text   NOT NULL,
    event_type           text   NOT NULL,
    payload              jsonb  NOT NULL DEFAULT '{}'::jsonb,
    status               text   NOT NULL DEFAULT 'PENDING'
                                CHECK (status IN ('PENDING','PROCESSED','FAILED')),
    error                text,
    idempotency_key      text   NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    processed_at         timestamptz,
    UNIQUE (tenant_id, idempotency_key)
);
-- Queue scan: oldest PENDING first. Worker pulls with FOR UPDATE SKIP LOCKED.
CREATE INDEX events_pending_idx ON loyalty_events (created_at)
    WHERE status = 'PENDING';

-- ---------------------------------------------------------------------------
-- earning_rules — match event_type, evaluate JSONB condition, mint base_points.
-- points_expire_days seeds the ledger row's expires_at (FEFO, consumed in P3).
-- ---------------------------------------------------------------------------
CREATE TABLE earning_rules (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id          uuid    NOT NULL REFERENCES tenants(id),
    event_type         text    NOT NULL,
    condition          jsonb   NOT NULL DEFAULT '{}'::jsonb,
    base_points        bigint  NOT NULL CHECK (base_points >= 0),
    points_expire_days int,
    active             boolean NOT NULL DEFAULT true,
    created_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX earning_rules_lookup_idx ON earning_rules (tenant_id, event_type) WHERE active;

-- ===== RLS =================================================================
ALTER TABLE loyalty_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE loyalty_events FORCE ROW LEVEL SECURITY;
CREATE POLICY events_isolation ON loyalty_events
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

ALTER TABLE earning_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE earning_rules FORCE ROW LEVEL SECURITY;
CREATE POLICY rules_isolation ON earning_rules
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());
-- ===========================================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON loyalty_events TO fides_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON earning_rules TO fides_app;

-- points_ledger.source_event_id can now reference the originating event.
ALTER TABLE points_ledger
    ADD CONSTRAINT points_ledger_source_event_fk
    FOREIGN KEY (source_event_id) REFERENCES loyalty_events(id);
