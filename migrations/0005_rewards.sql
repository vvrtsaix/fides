-- P4: campaigns, rewards, vouchers (FR-4.*). Money columns are NUMERIC (Decimal), never float.

CREATE TABLE campaigns (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     uuid    NOT NULL REFERENCES tenants(id),
    name          text    NOT NULL,
    budget_cap    numeric NOT NULL CHECK (budget_cap >= 0),
    current_spend numeric NOT NULL DEFAULT 0 CHECK (current_spend >= 0),
    active        boolean NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE rewards (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid    NOT NULL REFERENCES tenants(id),
    campaign_id     uuid    NOT NULL REFERENCES campaigns(id),
    name            text    NOT NULL,
    cost_points     bigint  NOT NULL CHECK (cost_points > 0),
    reward_value    numeric NOT NULL DEFAULT 0 CHECK (reward_value >= 0),
    available_stock int     NOT NULL CHECK (available_stock >= 0),
    valid_days      int     NOT NULL DEFAULT 365,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE vouchers (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid   NOT NULL REFERENCES tenants(id),
    reward_id   uuid   NOT NULL REFERENCES rewards(id),
    customer_id uuid   NOT NULL REFERENCES customers(id),
    code        text   NOT NULL,
    status      text   NOT NULL DEFAULT 'ISSUED' CHECK (status IN ('ISSUED','USED')),
    valid_until timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    used_at     timestamptz,
    UNIQUE (tenant_id, code)
);

-- ===== RLS =================================================================
ALTER TABLE campaigns ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaigns FORCE ROW LEVEL SECURITY;
CREATE POLICY campaigns_isolation ON campaigns
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

ALTER TABLE rewards ENABLE ROW LEVEL SECURITY;
ALTER TABLE rewards FORCE ROW LEVEL SECURITY;
CREATE POLICY rewards_isolation ON rewards
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());

ALTER TABLE vouchers ENABLE ROW LEVEL SECURITY;
ALTER TABLE vouchers FORCE ROW LEVEL SECURITY;
CREATE POLICY vouchers_isolation ON vouchers
    USING (tenant_id = current_tenant()) WITH CHECK (tenant_id = current_tenant());
-- ===========================================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON campaigns TO fides_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON rewards TO fides_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON vouchers TO fides_app;
