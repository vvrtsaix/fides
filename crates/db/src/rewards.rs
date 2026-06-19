//! Campaigns, rewards, voucher minting (FR-4.*). The redemption is the transactional heart:
//! pessimistic stock lock (FR-4.2) + budget overdraft check (FR-4.1) + FEFO point burn + voucher
//! mint, all atomic and idempotent (constraint #6).

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use fides_shared::{AppError, AppResult, TenantId};

use crate::locks::consume_fefo;
use crate::set_tenant;

fn internal(e: sqlx::Error) -> AppError {
    AppError::Internal(e.to_string())
}

pub async fn create_campaign(
    pool: &PgPool,
    tenant: TenantId,
    name: &str,
    budget_cap: Decimal,
) -> AppResult<Uuid> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO campaigns (tenant_id, name, budget_cap) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(tenant.as_uuid())
    .bind(name)
    .bind(budget_cap)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "campaign",
        Some(&id.to_string()),
        "create",
        None,
        Some(&serde_json::json!({ "name": name, "budget_cap": budget_cap.to_string() })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(id)
}

pub struct NewReward<'a> {
    pub campaign_id: Uuid,
    pub name: &'a str,
    pub cost_points: i64,
    pub reward_value: Decimal,
    pub available_stock: i32,
    pub valid_days: i32,
}

pub async fn create_reward(pool: &PgPool, tenant: TenantId, r: NewReward<'_>) -> AppResult<Uuid> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO rewards
           (tenant_id, campaign_id, name, cost_points, reward_value, available_stock, valid_days)
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
    )
    .bind(tenant.as_uuid())
    .bind(r.campaign_id)
    .bind(r.name)
    .bind(r.cost_points)
    .bind(r.reward_value)
    .bind(r.available_stock)
    .bind(r.valid_days)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "reward",
        Some(&id.to_string()),
        "create",
        None,
        Some(&serde_json::json!({
            "campaign_id": r.campaign_id, "name": r.name, "cost_points": r.cost_points,
            "reward_value": r.reward_value.to_string(), "available_stock": r.available_stock,
        })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(id)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RedeemOutcome {
    pub voucher_code: String,
    pub spendable_balance: i64,
    #[serde(default)]
    pub replayed: bool,
}

/// Redeem a reward for points → mint a voucher. One atomic, idempotent transaction.
#[tracing::instrument(skip_all, fields(tenant = %tenant, %reward_id, external_id))]
pub async fn redeem_reward(
    pool: &PgPool,
    tenant: TenantId,
    external_id: &str,
    reward_id: Uuid,
    idempotency_key: &str,
) -> AppResult<RedeemOutcome> {
    let tid = tenant.as_uuid();
    let mut tx = pool.begin().await.map_err(internal)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;

    // Idempotency: claim or replay (constraint #6).
    let claimed: Option<String> = sqlx::query_scalar(
        "INSERT INTO idempotency_keys (tenant_id, key, request_fingerprint)
         VALUES ($1, $2, $3) ON CONFLICT (tenant_id, key) DO NOTHING RETURNING key",
    )
    .bind(tid)
    .bind(idempotency_key)
    .bind(format!("redeem:{reward_id}:{external_id}"))
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?;
    if claimed.is_none() {
        let (fp, body): (String, Option<serde_json::Value>) = sqlx::query_as(
            "SELECT request_fingerprint, response_body FROM idempotency_keys
             WHERE tenant_id = $1 AND key = $2",
        )
        .bind(tid)
        .bind(idempotency_key)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal)?;
        tx.commit().await.map_err(internal)?;
        if fp != format!("redeem:{reward_id}:{external_id}") {
            return Err(AppError::IdempotencyConflict);
        }
        let mut out: RedeemOutcome = body
            .and_then(|v| serde_json::from_value(v).ok())
            .ok_or_else(|| AppError::Internal("idempotency record missing body".into()))?;
        out.replayed = true;
        return Ok(out);
    }

    let customer_id: Uuid =
        sqlx::query_scalar("SELECT id FROM customers WHERE tenant_id = $1 AND external_id = $2")
            .bind(tid)
            .bind(external_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(internal)?
            .ok_or(AppError::NotFound)?;

    // Pessimistic stock lock (FR-4.2): lock the reward row first.
    let (cost_points, stock, reward_value, campaign_id, valid_days): (
        i64,
        i32,
        Decimal,
        Uuid,
        i32,
    ) = sqlx::query_as(
        "SELECT cost_points, available_stock, reward_value, campaign_id, valid_days
             FROM rewards WHERE id = $1 FOR UPDATE",
    )
    .bind(reward_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    if stock <= 0 {
        return Err(AppError::Validation("reward out of stock".into()));
    }

    // Budget overdraft protection (FR-4.1): lock the campaign, check cap.
    let (budget_cap, current_spend, active): (Decimal, Decimal, bool) = sqlx::query_as(
        "SELECT budget_cap, current_spend, active FROM campaigns WHERE id = $1 FOR UPDATE",
    )
    .bind(campaign_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;
    if !active {
        return Err(AppError::Validation("campaign is inactive".into()));
    }
    if current_spend + reward_value > budget_cap {
        return Err(AppError::Validation("campaign budget exceeded".into()));
    }

    // Burn points via FEFO (locks balance row, writes REDEEM).
    let spendable: i64 = sqlx::query_scalar(
        "SELECT spendable_balance FROM customer_balances WHERE customer_id = $1 FOR UPDATE",
    )
    .bind(customer_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;
    if spendable < cost_points {
        return Err(AppError::Validation(
            "insufficient spendable balance".into(),
        ));
    }
    consume_fefo(&mut tx, tid, customer_id, cost_points).await?;
    let new_spendable = spendable - cost_points;
    sqlx::query(
        "UPDATE customer_balances
         SET spendable_balance = $2, lifetime_redeemed = lifetime_redeemed + $3, updated_at = now()
         WHERE customer_id = $1",
    )
    .bind(customer_id)
    .bind(new_spendable)
    .bind(cost_points)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;

    // Decrement stock + charge the budget.
    sqlx::query("UPDATE rewards SET available_stock = available_stock - 1 WHERE id = $1")
        .bind(reward_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    sqlx::query("UPDATE campaigns SET current_spend = current_spend + $2 WHERE id = $1")
        .bind(campaign_id)
        .bind(reward_value)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;

    // Mint a unique voucher (FR-4.3). The UNIQUE(tenant_id, code) constraint is the collision net.
    let code = new_voucher_code();
    sqlx::query(
        "INSERT INTO vouchers (tenant_id, reward_id, customer_id, code, valid_until)
         VALUES ($1, $2, $3, $4, now() + ($5 || ' days')::interval)",
    )
    .bind(tid)
    .bind(reward_id)
    .bind(customer_id)
    .bind(&code)
    .bind(valid_days.to_string())
    .execute(&mut *tx)
    .await
    .map_err(internal)?;

    let outcome = RedeemOutcome {
        voucher_code: code,
        spendable_balance: new_spendable,
        replayed: false,
    };
    let body = serde_json::to_value(&outcome).map_err(|e| AppError::Internal(e.to_string()))?;
    sqlx::query(
        "UPDATE idempotency_keys SET response_status = 200, response_body = $3
         WHERE tenant_id = $1 AND key = $2",
    )
    .bind(tid)
    .bind(idempotency_key)
    .bind(body)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;

    tx.commit().await.map_err(internal)?;
    Ok(outcome)
}

/// Mark a voucher USED (FR-4.3). Idempotent-ish: errors if unknown, already used, or expired.
pub async fn use_voucher(pool: &PgPool, tenant: TenantId, code: &str) -> AppResult<()> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;

    let status: Option<(String, bool)> = sqlx::query_as(
        "SELECT status, (valid_until < now()) AS expired FROM vouchers
         WHERE tenant_id = $1 AND code = $2 FOR UPDATE",
    )
    .bind(tenant.as_uuid())
    .bind(code)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?;

    match status {
        None => return Err(AppError::NotFound),
        Some((s, _)) if s == "USED" => {
            return Err(AppError::Validation("voucher already used".into()))
        }
        Some((_, expired)) if expired => {
            return Err(AppError::Validation("voucher expired".into()))
        }
        Some(_) => {}
    }

    sqlx::query(
        "UPDATE vouchers SET status = 'USED', used_at = now() WHERE tenant_id = $1 AND code = $2",
    )
    .bind(tenant.as_uuid())
    .bind(code)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(())
}

/// Compact, unique-enough alphanumeric code derived from a random UUID.
fn new_voucher_code() -> String {
    Uuid::new_v4()
        .simple()
        .to_string()
        .to_uppercase()
        .chars()
        .take(12)
        .collect()
}
