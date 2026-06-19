-- Customer segments — STATIC (explicit-membership) audiences for targeting.
-- Distinct from tiers: many-per-customer, unordered, manually managed.
--
-- ponytail: static membership only. Dynamic/predicate segments ("everyone with
-- lifetime_points > 1000") would add a `definition` JSONB + a re-evaluation job —
-- not built. Add when rule-based audiences are actually needed.

CREATE TABLE segments (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid        NOT NULL REFERENCES tenants(id),
    name        text        NOT NULL,
    description text,
    active      boolean     NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

-- M2M membership. PK doubles as the dedupe + the segment-side lookup index.
CREATE TABLE customer_segments (
    tenant_id   uuid        NOT NULL REFERENCES tenants(id),
    segment_id  uuid        NOT NULL REFERENCES segments(id),
    customer_id uuid        NOT NULL REFERENCES customers(id),
    added_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (segment_id, customer_id)
);
CREATE INDEX customer_segments_customer_idx ON customer_segments (customer_id);

-- RLS template (both tenant-scoped) — same pattern as customers.
ALTER TABLE segments ENABLE ROW LEVEL SECURITY;
ALTER TABLE segments FORCE ROW LEVEL SECURITY;
CREATE POLICY segments_tenant_isolation ON segments
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

ALTER TABLE customer_segments ENABLE ROW LEVEL SECURITY;
ALTER TABLE customer_segments FORCE ROW LEVEL SECURITY;
CREATE POLICY customer_segments_tenant_isolation ON customer_segments
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

GRANT SELECT, INSERT, UPDATE, DELETE ON segments TO fides_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON customer_segments TO fides_app;
