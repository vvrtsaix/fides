-- P5: outbound webhooks (FR-5.1, FR-5.2), security audit (FR-5.3), anonymization (NFR-3.3).

CREATE TABLE webhook_subscriptions (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  uuid    NOT NULL REFERENCES tenants(id),
    url        text    NOT NULL,
    event_type text    NOT NULL,            -- exact match, or '*' for all
    secret     text    NOT NULL,            -- per-subscription HMAC key
    active     boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX webhook_subs_lookup_idx
    ON webhook_subscriptions (tenant_id, event_type) WHERE active;

-- Outbox: rows are written inside the business txn that triggered the event, then a dispatcher
-- delivers them with exponential backoff.
CREATE TABLE webhook_logs (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid    NOT NULL REFERENCES tenants(id),
    subscription_id uuid    NOT NULL REFERENCES webhook_subscriptions(id),
    event_type      text    NOT NULL,
    payload         jsonb   NOT NULL,
    status          text    NOT NULL DEFAULT 'PENDING'
                            CHECK (status IN ('PENDING','DELIVERED','FAILED')),
    attempt         int     NOT NULL DEFAULT 0,
    next_retry_at   timestamptz NOT NULL DEFAULT now(),
    response_status int,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX webhook_logs_due_idx ON webhook_logs (next_retry_at) WHERE status = 'PENDING';

-- Append-only audit trail (FR-5.3). Read-only is enforced by the grant below (no UPDATE/DELETE).
CREATE TABLE audit_logs (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  uuid    NOT NULL REFERENCES tenants(id),
    actor      text    NOT NULL,
    entity     text    NOT NULL,
    entity_id  text,
    action     text    NOT NULL,
    old_values jsonb,
    new_values jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_logs_entity_idx ON audit_logs (tenant_id, entity, created_at);

-- ===== RLS =================================================================
ALTER TABLE webhook_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_subscriptions FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_subs_isolation ON webhook_subscriptions
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

ALTER TABLE webhook_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_logs FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_logs_isolation ON webhook_logs
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_logs FORCE ROW LEVEL SECURITY;
CREATE POLICY audit_logs_isolation ON audit_logs
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());
-- ===========================================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON webhook_subscriptions TO fides_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON webhook_logs TO fides_app;
-- audit_logs: INSERT + SELECT only — no UPDATE/DELETE, so the trail is immutable (FR-5.3).
GRANT SELECT, INSERT ON audit_logs TO fides_app;
