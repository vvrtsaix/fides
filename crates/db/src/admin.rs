//! Admin CRUD: list / get / update / soft-delete for the management entities.
//!
//! Creates live in their domain modules (ledger/events/rewards/webhooks) and the
//! API's `create_tenant`. This module adds the read/update/delete arms so the
//! admin surface is full CRUD.
//!
//! Every tenant-scoped op runs inside a `set_tenant` transaction so RLS applies.
//! `tenants` is platform-level (no RLS) so its ops use the pool directly.
//!
//! Delete = SOFT delete everywhere: `active = false` (entities) or
//! `status = 'SUSPENDED'` (tenant). A loyalty ledger never hard-deletes config
//! that historical rows reference.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use fides_shared::{AppError, AppResult, TenantId};

use crate::set_tenant;

fn internal(e: sqlx::Error) -> AppError {
    AppError::Internal(e.to_string())
}

/// Begin a transaction with the tenant GUC pinned, so RLS filters every query.
async fn scoped(pool: &PgPool, tenant: TenantId) -> AppResult<Transaction<'_, Postgres>> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    Ok(tx)
}

/// Soft-delete a tenant-scoped row by flipping `active`. table/entity are
/// `&'static` literals from the callers below — never user input, so the
/// `format!` into SQL is safe.
async fn soft_delete(
    pool: &PgPool,
    tenant: TenantId,
    table: &'static str,
    entity: &'static str,
    id: Uuid,
) -> AppResult<()> {
    let mut tx = scoped(pool, tenant).await?;
    let n = sqlx::query(&format!("UPDATE {table} SET active = false WHERE id = $1"))
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?
        .rows_affected();
    if n == 0 {
        return Err(AppError::NotFound);
    }
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        entity,
        Some(&id.to_string()),
        "delete",
        None,
        None,
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(())
}

// ---- tenants (platform-level, no RLS) -------------------------------------

#[derive(sqlx::FromRow, Serialize)]
pub struct TenantRow {
    pub id: Uuid,
    pub name: String,
    pub currency: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

const TENANT_COLS: &str = "id, name, currency, status, created_at";

pub async fn list_tenants(pool: &PgPool) -> AppResult<Vec<TenantRow>> {
    sqlx::query_as::<_, TenantRow>(&format!(
        "SELECT {TENANT_COLS} FROM tenants ORDER BY created_at"
    ))
    .fetch_all(pool)
    .await
    .map_err(internal)
}

pub async fn get_tenant(pool: &PgPool, id: Uuid) -> AppResult<TenantRow> {
    sqlx::query_as::<_, TenantRow>(&format!(
        "SELECT {TENANT_COLS} FROM tenants WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)
}

/// `currency` is immutable (one currency per tenant, no FX). name/status only.
pub async fn update_tenant(
    pool: &PgPool,
    id: Uuid,
    name: Option<&str>,
    status: Option<&str>,
) -> AppResult<TenantRow> {
    if let Some(s) = status {
        if !matches!(s, "ACTIVE" | "SUSPENDED") {
            return Err(AppError::Validation(
                "status must be ACTIVE or SUSPENDED".into(),
            ));
        }
    }
    sqlx::query_as::<_, TenantRow>(&format!(
        "UPDATE tenants SET name = COALESCE($2, name), status = COALESCE($3, status)
         WHERE id = $1 RETURNING {TENANT_COLS}"
    ))
    .bind(id)
    .bind(name)
    .bind(status)
    .fetch_optional(pool)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)
}

/// Soft-delete = suspend. Hard delete would orphan every tenant-scoped FK.
pub async fn delete_tenant(pool: &PgPool, id: Uuid) -> AppResult<()> {
    let n = sqlx::query("UPDATE tenants SET status = 'SUSPENDED' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .map_err(internal)?
        .rows_affected();
    if n == 0 {
        return Err(AppError::NotFound);
    }
    Ok(())
}

// ---- customers ------------------------------------------------------------

#[derive(sqlx::FromRow, Serialize)]
pub struct CustomerRow {
    pub id: Uuid,
    pub external_id: String,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub status: String,
    pub current_tier_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

const CUSTOMER_COLS: &str = "id, external_id, email, phone, status, current_tier_id, created_at";

/// Register a customer explicitly. Idempotent: re-registering an existing
/// `external_id` updates email/phone rather than erroring (matches the implicit
/// upsert the ledger does on first transaction).
pub async fn create_customer(
    pool: &PgPool,
    tenant: TenantId,
    external_id: &str,
    email: Option<&str>,
    phone: Option<&str>,
) -> AppResult<CustomerRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, CustomerRow>(&format!(
        "INSERT INTO customers (tenant_id, external_id, email, phone)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (tenant_id, external_id) DO UPDATE
           SET email = COALESCE(EXCLUDED.email, customers.email),
               phone = COALESCE(EXCLUDED.phone, customers.phone)
         RETURNING {CUSTOMER_COLS}"
    ))
    .bind(tenant.as_uuid())
    .bind(external_id)
    .bind(email)
    .bind(phone)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "customer",
        Some(external_id),
        "register",
        None,
        Some(&serde_json::json!({ "external_id": external_id, "email": email, "phone": phone })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn list_customers(pool: &PgPool, tenant: TenantId) -> AppResult<Vec<CustomerRow>> {
    let mut tx = scoped(pool, tenant).await?;
    let rows = sqlx::query_as::<_, CustomerRow>(&format!(
        "SELECT {CUSTOMER_COLS} FROM customers ORDER BY created_at"
    ))
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(rows)
}

pub async fn get_customer(
    pool: &PgPool,
    tenant: TenantId,
    external_id: &str,
) -> AppResult<CustomerRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, CustomerRow>(&format!(
        "SELECT {CUSTOMER_COLS} FROM customers WHERE external_id = $1"
    ))
    .bind(external_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

/// `external_id` and `status` are not editable here (status is owned by the
/// anonymize path). `current_tier_id` lets an admin override the auto-assigned tier.
pub async fn update_customer(
    pool: &PgPool,
    tenant: TenantId,
    external_id: &str,
    email: Option<&str>,
    phone: Option<&str>,
    current_tier_id: Option<Uuid>,
) -> AppResult<CustomerRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, CustomerRow>(&format!(
        "UPDATE customers SET
            email = COALESCE($2, email),
            phone = COALESCE($3, phone),
            current_tier_id = COALESCE($4, current_tier_id)
         WHERE external_id = $1 RETURNING {CUSTOMER_COLS}"
    ))
    .bind(external_id)
    .bind(email)
    .bind(phone)
    .bind(current_tier_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "customer",
        Some(external_id),
        "update",
        None,
        Some(&serde_json::json!({ "email": email, "phone": phone, "current_tier_id": current_tier_id })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

// Delete = anonymize (GDPR scrub). Customers are never hard-deleted — the ledger,
// balances, and vouchers FK them. Reuse `crate::audit::anonymize`.

// ---- tiers ----------------------------------------------------------------

#[derive(sqlx::FromRow, Serialize)]
pub struct TierRow {
    pub id: Uuid,
    pub name: String,
    pub threshold_points: i64,
    pub active: bool,
}

pub async fn list_tiers(pool: &PgPool, tenant: TenantId) -> AppResult<Vec<TierRow>> {
    let mut tx = scoped(pool, tenant).await?;
    let rows = sqlx::query_as::<_, TierRow>(
        "SELECT id, name, threshold_points, active FROM tiers ORDER BY threshold_points",
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(rows)
}

pub async fn get_tier(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<TierRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, TierRow>(
        "SELECT id, name, threshold_points, active FROM tiers WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn update_tier(
    pool: &PgPool,
    tenant: TenantId,
    id: Uuid,
    name: Option<&str>,
    threshold_points: Option<i64>,
) -> AppResult<TierRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, TierRow>(
        "UPDATE tiers SET name = COALESCE($2, name),
            threshold_points = COALESCE($3, threshold_points)
         WHERE id = $1 RETURNING id, name, threshold_points, active",
    )
    .bind(id)
    .bind(name)
    .bind(threshold_points)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "tier",
        Some(&id.to_string()),
        "update",
        None,
        Some(&serde_json::json!({ "name": name, "threshold_points": threshold_points })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn delete_tier(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<()> {
    soft_delete(pool, tenant, "tiers", "tier", id).await
}

// ---- earning_rules --------------------------------------------------------

#[derive(sqlx::FromRow, Serialize)]
pub struct RuleRow {
    pub id: Uuid,
    pub event_type: String,
    pub condition: Value,
    pub base_points: i64,
    pub points_expire_days: Option<i32>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

const RULE_COLS: &str =
    "id, event_type, condition, base_points, points_expire_days, active, created_at";

pub async fn list_rules(pool: &PgPool, tenant: TenantId) -> AppResult<Vec<RuleRow>> {
    let mut tx = scoped(pool, tenant).await?;
    let rows = sqlx::query_as::<_, RuleRow>(&format!(
        "SELECT {RULE_COLS} FROM earning_rules ORDER BY created_at"
    ))
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(rows)
}

pub async fn get_rule(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<RuleRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, RuleRow>(&format!(
        "SELECT {RULE_COLS} FROM earning_rules WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

/// COALESCE partial update. `points_expire_days` can be set but not nulled here
/// (NULL bind means "leave unchanged"). ponytail: add an explicit clear if needed.
pub async fn update_rule(
    pool: &PgPool,
    tenant: TenantId,
    id: Uuid,
    event_type: Option<&str>,
    condition: Option<&Value>,
    base_points: Option<i64>,
    points_expire_days: Option<i32>,
    active: Option<bool>,
) -> AppResult<RuleRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, RuleRow>(&format!(
        "UPDATE earning_rules SET
            event_type = COALESCE($2, event_type),
            condition = COALESCE($3, condition),
            base_points = COALESCE($4, base_points),
            points_expire_days = COALESCE($5, points_expire_days),
            active = COALESCE($6, active)
         WHERE id = $1 RETURNING {RULE_COLS}"
    ))
    .bind(id)
    .bind(event_type)
    .bind(condition)
    .bind(base_points)
    .bind(points_expire_days)
    .bind(active)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "earning_rule",
        Some(&id.to_string()),
        "update",
        None,
        Some(&serde_json::json!({ "event_type": event_type, "base_points": base_points, "active": active })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn delete_rule(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<()> {
    soft_delete(pool, tenant, "earning_rules", "earning_rule", id).await
}

// ---- campaigns ------------------------------------------------------------

#[derive(sqlx::FromRow, Serialize)]
pub struct CampaignRow {
    pub id: Uuid,
    pub name: String,
    pub budget_cap: Decimal,
    pub current_spend: Decimal,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

const CAMPAIGN_COLS: &str = "id, name, budget_cap, current_spend, active, created_at";

pub async fn list_campaigns(pool: &PgPool, tenant: TenantId) -> AppResult<Vec<CampaignRow>> {
    let mut tx = scoped(pool, tenant).await?;
    let rows = sqlx::query_as::<_, CampaignRow>(&format!(
        "SELECT {CAMPAIGN_COLS} FROM campaigns ORDER BY created_at"
    ))
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(rows)
}

pub async fn get_campaign(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<CampaignRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, CampaignRow>(&format!(
        "SELECT {CAMPAIGN_COLS} FROM campaigns WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

/// `current_spend` is ledger-derived, never set here.
pub async fn update_campaign(
    pool: &PgPool,
    tenant: TenantId,
    id: Uuid,
    name: Option<&str>,
    budget_cap: Option<Decimal>,
    active: Option<bool>,
) -> AppResult<CampaignRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, CampaignRow>(&format!(
        "UPDATE campaigns SET name = COALESCE($2, name),
            budget_cap = COALESCE($3, budget_cap), active = COALESCE($4, active)
         WHERE id = $1 RETURNING {CAMPAIGN_COLS}"
    ))
    .bind(id)
    .bind(name)
    .bind(budget_cap)
    .bind(active)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "campaign",
        Some(&id.to_string()),
        "update",
        None,
        Some(&serde_json::json!({ "name": name, "budget_cap": budget_cap.map(|d| d.to_string()), "active": active })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn delete_campaign(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<()> {
    soft_delete(pool, tenant, "campaigns", "campaign", id).await
}

// ---- rewards --------------------------------------------------------------

#[derive(sqlx::FromRow, Serialize)]
pub struct RewardRow {
    pub id: Uuid,
    pub campaign_id: Uuid,
    pub name: String,
    pub cost_points: i64,
    pub reward_value: Decimal,
    pub available_stock: i32,
    pub valid_days: i32,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

const REWARD_COLS: &str =
    "id, campaign_id, name, cost_points, reward_value, available_stock, valid_days, active, created_at";

pub async fn list_rewards(pool: &PgPool, tenant: TenantId) -> AppResult<Vec<RewardRow>> {
    let mut tx = scoped(pool, tenant).await?;
    let rows = sqlx::query_as::<_, RewardRow>(&format!(
        "SELECT {REWARD_COLS} FROM rewards ORDER BY created_at"
    ))
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(rows)
}

pub async fn get_reward(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<RewardRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, RewardRow>(&format!(
        "SELECT {REWARD_COLS} FROM rewards WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

/// `campaign_id` is fixed at creation. Everything else is editable.
#[allow(clippy::too_many_arguments)]
pub async fn update_reward(
    pool: &PgPool,
    tenant: TenantId,
    id: Uuid,
    name: Option<&str>,
    cost_points: Option<i64>,
    reward_value: Option<Decimal>,
    available_stock: Option<i32>,
    valid_days: Option<i32>,
    active: Option<bool>,
) -> AppResult<RewardRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, RewardRow>(&format!(
        "UPDATE rewards SET
            name = COALESCE($2, name),
            cost_points = COALESCE($3, cost_points),
            reward_value = COALESCE($4, reward_value),
            available_stock = COALESCE($5, available_stock),
            valid_days = COALESCE($6, valid_days),
            active = COALESCE($7, active)
         WHERE id = $1 RETURNING {REWARD_COLS}"
    ))
    .bind(id)
    .bind(name)
    .bind(cost_points)
    .bind(reward_value)
    .bind(available_stock)
    .bind(valid_days)
    .bind(active)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "reward",
        Some(&id.to_string()),
        "update",
        None,
        Some(&serde_json::json!({ "name": name, "cost_points": cost_points, "available_stock": available_stock, "active": active })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn delete_reward(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<()> {
    soft_delete(pool, tenant, "rewards", "reward", id).await
}

// ---- webhook_subscriptions -------------------------------------------------

/// Note: `secret` is intentionally omitted from reads.
#[derive(sqlx::FromRow, Serialize)]
pub struct SubscriptionRow {
    pub id: Uuid,
    pub url: String,
    pub event_type: String,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

const SUB_COLS: &str = "id, url, event_type, active, created_at";

pub async fn list_subscriptions(pool: &PgPool, tenant: TenantId) -> AppResult<Vec<SubscriptionRow>> {
    let mut tx = scoped(pool, tenant).await?;
    let rows = sqlx::query_as::<_, SubscriptionRow>(&format!(
        "SELECT {SUB_COLS} FROM webhook_subscriptions ORDER BY created_at"
    ))
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(rows)
}

pub async fn get_subscription(
    pool: &PgPool,
    tenant: TenantId,
    id: Uuid,
) -> AppResult<SubscriptionRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, SubscriptionRow>(&format!(
        "SELECT {SUB_COLS} FROM webhook_subscriptions WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn update_subscription(
    pool: &PgPool,
    tenant: TenantId,
    id: Uuid,
    url: Option<&str>,
    event_type: Option<&str>,
    secret: Option<&str>,
    active: Option<bool>,
) -> AppResult<SubscriptionRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, SubscriptionRow>(&format!(
        "UPDATE webhook_subscriptions SET
            url = COALESCE($2, url),
            event_type = COALESCE($3, event_type),
            secret = COALESCE($4, secret),
            active = COALESCE($5, active)
         WHERE id = $1 RETURNING {SUB_COLS}"
    ))
    .bind(id)
    .bind(url)
    .bind(event_type)
    .bind(secret)
    .bind(active)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    // Secret intentionally omitted from the audit snapshot.
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "webhook_subscription",
        Some(&id.to_string()),
        "update",
        None,
        Some(&serde_json::json!({ "url": url, "event_type": event_type, "active": active })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn delete_subscription(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<()> {
    soft_delete(pool, tenant, "webhook_subscriptions", "webhook_subscription", id).await
}

// ---- segments (static audiences) + membership ------------------------------

#[derive(sqlx::FromRow, Serialize)]
pub struct SegmentRow {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    /// Rule DSL for dynamic segments; null = static (manual membership).
    pub definition: Option<Value>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

const SEGMENT_COLS: &str = "id, name, description, definition, active, created_at";

/// `definition` set → dynamic segment (worker manages membership from the rule).
pub async fn create_segment(
    pool: &PgPool,
    tenant: TenantId,
    name: &str,
    description: Option<&str>,
    definition: Option<&Value>,
) -> AppResult<SegmentRow> {
    if let Some(d) = definition {
        crate::segments::validate(d).map_err(AppError::Validation)?;
    }
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, SegmentRow>(&format!(
        "INSERT INTO segments (tenant_id, name, description, definition) VALUES ($1, $2, $3, $4)
         RETURNING {SEGMENT_COLS}"
    ))
    .bind(tenant.as_uuid())
    .bind(name)
    .bind(description)
    .bind(definition)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "segment",
        Some(&row.id.to_string()),
        "create",
        None,
        Some(&serde_json::json!({ "name": name, "description": description })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn list_segments(pool: &PgPool, tenant: TenantId) -> AppResult<Vec<SegmentRow>> {
    let mut tx = scoped(pool, tenant).await?;
    let rows = sqlx::query_as::<_, SegmentRow>(&format!(
        "SELECT {SEGMENT_COLS} FROM segments ORDER BY created_at"
    ))
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(rows)
}

pub async fn get_segment(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<SegmentRow> {
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, SegmentRow>(&format!(
        "SELECT {SEGMENT_COLS} FROM segments WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

/// `definition` can be set/changed but not cleared here (COALESCE keeps the old
/// value on null). To turn a dynamic segment back into a static one, that'd need
/// an explicit clear — not supported yet.
pub async fn update_segment(
    pool: &PgPool,
    tenant: TenantId,
    id: Uuid,
    name: Option<&str>,
    description: Option<&str>,
    definition: Option<&Value>,
    active: Option<bool>,
) -> AppResult<SegmentRow> {
    if let Some(d) = definition {
        crate::segments::validate(d).map_err(AppError::Validation)?;
    }
    let mut tx = scoped(pool, tenant).await?;
    let row = sqlx::query_as::<_, SegmentRow>(&format!(
        "UPDATE segments SET name = COALESCE($2, name),
            description = COALESCE($3, description), definition = COALESCE($4, definition),
            active = COALESCE($5, active)
         WHERE id = $1 RETURNING {SEGMENT_COLS}"
    ))
    .bind(id)
    .bind(name)
    .bind(description)
    .bind(definition)
    .bind(active)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "segment",
        Some(&id.to_string()),
        "update",
        None,
        Some(&serde_json::json!({ "name": name, "description": description, "active": active })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(row)
}

pub async fn delete_segment(pool: &PgPool, tenant: TenantId, id: Uuid) -> AppResult<()> {
    soft_delete(pool, tenant, "segments", "segment", id).await
}

/// Resolve a customer's UUID from its external_id within the current tenant tx.
async fn customer_id_by_external(
    tx: &mut Transaction<'_, Postgres>,
    external_id: &str,
) -> AppResult<Uuid> {
    sqlx::query_scalar::<_, Uuid>("SELECT id FROM customers WHERE external_id = $1")
        .bind(external_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(internal)?
        .ok_or(AppError::NotFound)
}

/// Fetch a segment's rule definition (NotFound if the segment doesn't exist).
/// `Some` = dynamic segment (membership is rule-managed, not manually editable).
async fn segment_definition(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> AppResult<Option<Value>> {
    let row: Option<(Option<Value>,)> =
        sqlx::query_as("SELECT definition FROM segments WHERE id = $1")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(internal)?;
    match row {
        Some((def,)) => Ok(def),
        None => Err(AppError::NotFound),
    }
}

fn reject_if_dynamic(def: &Option<Value>) -> AppResult<()> {
    if def.is_some() {
        return Err(AppError::Validation(
            "dynamic segment: membership is managed by its rule, not manually".into(),
        ));
    }
    Ok(())
}

/// Add a customer (by external_id) to a segment. Idempotent.
pub async fn add_member(
    pool: &PgPool,
    tenant: TenantId,
    segment_id: Uuid,
    external_id: &str,
) -> AppResult<()> {
    let mut tx = scoped(pool, tenant).await?;
    reject_if_dynamic(&segment_definition(&mut tx, segment_id).await?)?;
    let customer_id = customer_id_by_external(&mut tx, external_id).await?;
    sqlx::query(
        "INSERT INTO customer_segments (tenant_id, segment_id, customer_id)
         VALUES ($1, $2, $3) ON CONFLICT (segment_id, customer_id) DO NOTHING",
    )
    .bind(tenant.as_uuid())
    .bind(segment_id)
    .bind(customer_id)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "segment_member",
        Some(&segment_id.to_string()),
        "add",
        None,
        Some(&serde_json::json!({ "external_id": external_id })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(())
}

/// Remove a customer (by external_id) from a segment. 404 if not a member.
pub async fn remove_member(
    pool: &PgPool,
    tenant: TenantId,
    segment_id: Uuid,
    external_id: &str,
) -> AppResult<()> {
    let mut tx = scoped(pool, tenant).await?;
    reject_if_dynamic(&segment_definition(&mut tx, segment_id).await?)?;
    let customer_id = customer_id_by_external(&mut tx, external_id).await?;
    let n = sqlx::query("DELETE FROM customer_segments WHERE segment_id = $1 AND customer_id = $2")
        .bind(segment_id)
        .bind(customer_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?
        .rows_affected();
    if n == 0 {
        return Err(AppError::NotFound);
    }
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "segment_member",
        Some(&segment_id.to_string()),
        "remove",
        None,
        Some(&serde_json::json!({ "external_id": external_id })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(())
}

/// Customers in a segment.
pub async fn list_members(
    pool: &PgPool,
    tenant: TenantId,
    segment_id: Uuid,
) -> AppResult<Vec<CustomerRow>> {
    let mut tx = scoped(pool, tenant).await?;
    segment_definition(&mut tx, segment_id).await?; // existence check (404 if missing)
    let rows = sqlx::query_as::<_, CustomerRow>(
        "SELECT c.id, c.external_id, c.email, c.phone, c.status, c.current_tier_id, c.created_at
         FROM customers c JOIN customer_segments cs ON cs.customer_id = c.id
         WHERE cs.segment_id = $1 ORDER BY c.created_at",
    )
    .bind(segment_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(rows)
}

/// Segments a customer (by external_id) belongs to.
pub async fn list_customer_segments(
    pool: &PgPool,
    tenant: TenantId,
    external_id: &str,
) -> AppResult<Vec<SegmentRow>> {
    let mut tx = scoped(pool, tenant).await?;
    let customer_id = customer_id_by_external(&mut tx, external_id).await?;
    let rows = sqlx::query_as::<_, SegmentRow>(
        "SELECT s.id, s.name, s.description, s.definition, s.active, s.created_at
         FROM segments s JOIN customer_segments cs ON cs.segment_id = s.id
         WHERE cs.customer_id = $1 ORDER BY s.created_at",
    )
    .bind(customer_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(rows)
}
