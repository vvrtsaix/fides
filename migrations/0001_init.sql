-- P0 foundation: tenancy + RLS isolation primitive.
-- PostgreSQL 18. gen_random_uuid() is built in.

-- ---------------------------------------------------------------------------
-- Application role. The runtime pool does `SET ROLE fides_app` on every
-- connection (fides_db::connect_app) so queries run as a NON-superuser and RLS
-- actually applies — superusers bypass RLS. NOLOGIN: it's a privilege group the
-- login user assumes, never a direct login.
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'fides_app') THEN
        CREATE ROLE fides_app NOLOGIN;
    END IF;
END $$;
GRANT USAGE ON SCHEMA public TO fides_app;

-- ---------------------------------------------------------------------------
-- Tenant context helper. Reads the LOCAL GUC set by fides_db::set_tenant().
-- STABLE + the `missing_ok = true` arg means it returns NULL when unset
-- (e.g. on the platform-admin path) rather than erroring.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION current_tenant() RETURNS uuid
    LANGUAGE sql STABLE
    AS $$ SELECT NULLIF(current_setting('app.tenant_id', true), '')::uuid $$;

-- ---------------------------------------------------------------------------
-- tenants — PLATFORM-level table. NOT tenant-scoped, so NO RLS:
-- platform-admin (POST /platform/tenants) manages all rows.
-- currency is immutable after first transaction (enforced in P1 once the
-- ledger exists; column-level for now).
-- ---------------------------------------------------------------------------
CREATE TABLE tenants (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text        NOT NULL,
    currency    char(3)     NOT NULL,          -- ISO-4217, one per tenant, no FX
    status      text        NOT NULL DEFAULT 'ACTIVE'
                            CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- customers — first TENANT-scoped table; brought in early so RLS is provable
-- in P0. Full identity columns land in P1. This row is the RLS template:
-- every tenant-scoped table created later copies this exact pattern.
-- ---------------------------------------------------------------------------
CREATE TABLE customers (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid        NOT NULL REFERENCES tenants(id),
    external_id     text        NOT NULL,
    email           text,
    phone           text,
    status          text        NOT NULL DEFAULT 'ACTIVE'
                                CHECK (status IN ('ACTIVE', 'ANONYMIZED')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, external_id)             -- external_id unique PER tenant
);

CREATE INDEX customers_tenant_idx ON customers (tenant_id);

-- ===== RLS TEMPLATE (apply to every tenant-scoped table) ===================
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
-- FORCE so the table owner is also subject to the policy (the app role is the
-- owner in dev/CI; without FORCE, owners bypass RLS and the isolation test lies).
ALTER TABLE customers FORCE ROW LEVEL SECURITY;

CREATE POLICY customers_tenant_isolation ON customers
    USING (tenant_id = current_tenant())
    WITH CHECK (tenant_id = current_tenant());
-- ===========================================================================

-- Grant data privileges to the runtime role. RLS still constrains every row.
GRANT SELECT, INSERT, UPDATE, DELETE ON tenants TO fides_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON customers TO fides_app;
