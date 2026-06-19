//! Database access: pools, migrations, and the tenant-scoped transaction helper that powers
//! Row-Level Security (constraint #5).
//!
//! Two pools by design:
//! - `connect_admin` — logs in as the (super)user; used ONLY to run migrations.
//! - `connect_app` — same login, but `SET ROLE fides_app` on every connection so it runs as a
//!   NON-superuser. Mandatory: superusers BYPASS RLS, so without the role switch tenant
//!   isolation would silently not apply.

use fides_shared::TenantId;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Executor, Postgres, Transaction};

pub mod admin;
pub mod audit;
pub mod events;
pub mod ledger;
pub mod locks;
pub mod rewards;
pub mod segments;
pub mod webhooks;
pub use admin::{
    add_member, create_customer, create_segment, delete_campaign, delete_reward, delete_rule,
    delete_segment, delete_subscription, delete_tenant, delete_tier, get_campaign, get_customer,
    get_reward, get_rule, get_segment, get_subscription, get_tenant, get_tier, list_campaigns,
    list_customer_segments, list_customers, list_members, list_rewards, list_rules, list_segments,
    list_subscriptions, list_tenants, list_tiers, remove_member, update_campaign, update_customer,
    update_reward, update_rule, update_segment, update_subscription, update_tenant, update_tier,
};
pub use audit::{anonymize, audit_in_tx};
pub use events::{create_rule, ingest, process_pending};
pub use ledger::{
    apply_in_tx, create_tier, get_balance, post_ledger_txn, Balance, PostTxn, TxnOutcome,
};
pub use locks::{
    create_lock, expire_due, fulfill_lock, release_expired, release_lock, LOCK_TTL_MINUTES,
};
pub use rewards::{
    create_campaign, create_reward, redeem_reward, use_voucher, NewReward, RedeemOutcome,
};
pub use segments::reconcile_segments;
pub use webhooks::{create_subscription, dispatch_pending, emit, SendReq};

pub use sqlx;

/// Admin pool for migrations only. Runs as the login user (table owner / superuser).
pub async fn connect_admin(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await
}

/// Runtime pool. Drops to the non-superuser `fides_app` role on each connection so RLS applies.
pub async fn connect_app(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    connect_as(database_url, max_connections, "SET ROLE fides_app").await
}

/// Worker pool. Runs as `fides_worker` (BYPASSRLS) so it can process events across all tenants.
/// Every worker query still binds tenant_id explicitly.
pub async fn connect_worker(
    database_url: &str,
    max_connections: u32,
) -> Result<PgPool, sqlx::Error> {
    connect_as(database_url, max_connections, "SET ROLE fides_worker").await
}

async fn connect_as(
    database_url: &str,
    max_connections: u32,
    set_role: &'static str,
) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                conn.execute(set_role).await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
}

/// Run pending migrations. Migrations are embedded from `/migrations` at compile time.
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}

/// Pin the tenant for the lifetime of a transaction.
///
/// Sets `app.tenant_id` as a LOCAL GUC so every RLS policy (`tenant_id = current_tenant()`)
/// filters automatically — even if a query forgets its `WHERE tenant_id`. MUST be the first
/// statement in any tenant-scoped transaction.
pub async fn set_tenant(
    tx: &mut Transaction<'_, Postgres>,
    tenant: TenantId,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}
