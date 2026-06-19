-- P3: transactional fraud holds (FR-3.3). FEFO consumption + expiration use the
-- points_ledger.available_amount track added in 0002.

CREATE TABLE points_locks (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid   NOT NULL REFERENCES tenants(id),
    customer_id  uuid   NOT NULL REFERENCES customers(id),
    amount       bigint NOT NULL CHECK (amount > 0),
    status       text   NOT NULL DEFAULT 'HELD'
                        CHECK (status IN ('HELD','FULFILLED','RELEASED')),
    expires_at   timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    resolved_at  timestamptz
);
-- Sweeper scan: HELD locks past their TTL.
CREATE INDEX locks_due_idx ON points_locks (expires_at) WHERE status = 'HELD';

ALTER TABLE points_locks ENABLE ROW LEVEL SECURITY;
ALTER TABLE points_locks FORCE ROW LEVEL SECURITY;
CREATE POLICY locks_isolation ON points_locks
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

GRANT SELECT, INSERT, UPDATE, DELETE ON points_locks TO fides_app;
