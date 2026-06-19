//! The financial core write path (FR-1.2, FR-1.3, FR-3.1, constraint #6).
//!
//! `post_ledger_txn` runs one REPEATABLE READ transaction that:
//!   1. dedupes on the idempotency key,
//!   2. appends an immutable ledger row,
//!   3. recomputes `customer_balances` (locked FOR UPDATE) in the SAME txn,
//!   4. re-evaluates the tier.
//!
//! Serialization failures (40001) / deadlocks (40P01) are retried with bounded backoff.

use std::time::Duration;

use chrono::{DateTime, Utc};
use fides_core::{select_tier, Tier, TxnType};
use fides_shared::{AppError, AppResult, TenantId};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::set_tenant;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Balance {
    pub spendable_balance: i64,
    pub locked_balance: i64,
    pub lifetime_earned: i64,
    pub lifetime_redeemed: i64,
}

/// A money-mutating request. `request_fingerprint` is the canonical request identity used to
/// detect a key reused with a different payload.
pub struct PostTxn {
    pub external_id: String,
    pub txn_type: TxnType,
    pub amount: i64,
    pub expires_at: Option<DateTime<Utc>>,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    /// Originating event, when minted by the worker (FR-2.3 traceability).
    pub source_event_id: Option<Uuid>,
}

#[derive(Debug)]
pub struct TxnOutcome {
    pub balance: Balance,
    pub replayed: bool,
}

const MAX_ATTEMPTS: u32 = 3;

/// Internal error that distinguishes retryable DB conflicts from terminal app errors.
enum TxnError {
    Retryable(sqlx::Error),
    App(AppError),
}

impl From<sqlx::Error> for TxnError {
    fn from(e: sqlx::Error) -> Self {
        if let sqlx::Error::Database(db) = &e {
            if matches!(db.code().as_deref(), Some("40001") | Some("40P01")) {
                return TxnError::Retryable(e);
            }
        }
        TxnError::App(AppError::Internal(e.to_string()))
    }
}

impl From<AppError> for TxnError {
    fn from(e: AppError) -> Self {
        TxnError::App(e)
    }
}

#[tracing::instrument(
    skip_all,
    fields(tenant = %tenant, txn_type = req.txn_type.as_str(), external_id = req.external_id)
)]
pub async fn post_ledger_txn(
    pool: &PgPool,
    tenant: TenantId,
    req: PostTxn,
) -> AppResult<TxnOutcome> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match try_post(pool, tenant, &req).await {
            Ok(out) => return Ok(out),
            Err(TxnError::App(e)) => return Err(e),
            Err(TxnError::Retryable(e)) => {
                if attempt >= MAX_ATTEMPTS {
                    return Err(AppError::Internal(format!(
                        "serialization conflict after {attempt} attempts: {e}"
                    )));
                }
                tracing::warn!(attempt, "retrying after serialization conflict");
                tokio::time::sleep(Duration::from_millis(5 * attempt as u64)).await;
            }
        }
    }
}

async fn try_post(pool: &PgPool, tenant: TenantId, req: &PostTxn) -> Result<TxnOutcome, TxnError> {
    let mut tx = pool.begin().await?;

    // READ COMMITTED + a per-customer `FOR UPDATE` lock (in apply_inner) serializes all writes
    // to one customer's balance. The cache is updated INCREMENTALLY (not re-aggregated via SUM),
    // so there is no phantom-read exposure that would require REPEATABLE READ — and RR here would
    // turn concurrent writes to the same customer into a 40001 retry storm. The retry wrapper
    // still covers the rare deadlock (40P01). MUST be set before any query in the txn.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *tx)
        .await?;
    set_tenant(&mut tx, tenant).await?;

    let outcome = apply_inner(&mut tx, tenant, req).await?;
    tx.commit().await?;
    Ok(outcome)
}

/// Apply a ledger transaction inside a caller-managed transaction (no begin/commit here).
/// The caller owns isolation level + `set_tenant`. Used by the worker's event processor so a
/// mint and the event's status update commit atomically. Serialization conflicts surface as
/// `AppError::Internal` (the caller decides whether to retry the surrounding unit of work).
pub async fn apply_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: TenantId,
    req: &PostTxn,
) -> AppResult<TxnOutcome> {
    apply_inner(tx, tenant, req).await.map_err(|e| match e {
        TxnError::App(a) => a,
        TxnError::Retryable(s) => AppError::Internal(s.to_string()),
    })
}

async fn apply_inner(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: TenantId,
    req: &PostTxn,
) -> Result<TxnOutcome, TxnError> {
    let tid = tenant.as_uuid();

    // 1. Idempotency: claim the key, or replay the prior result (constraint #6).
    let claimed: Option<String> = sqlx::query_scalar(
        "INSERT INTO idempotency_keys (tenant_id, key, request_fingerprint)
         VALUES ($1, $2, $3)
         ON CONFLICT (tenant_id, key) DO NOTHING
         RETURNING key",
    )
    .bind(tid)
    .bind(&req.idempotency_key)
    .bind(&req.request_fingerprint)
    .fetch_optional(&mut **tx)
    .await?;

    if claimed.is_none() {
        let (fingerprint, body): (String, Option<serde_json::Value>) = sqlx::query_as(
            "SELECT request_fingerprint, response_body FROM idempotency_keys
             WHERE tenant_id = $1 AND key = $2",
        )
        .bind(tid)
        .bind(&req.idempotency_key)
        .fetch_one(&mut **tx)
        .await?;

        if fingerprint != req.request_fingerprint {
            return Err(AppError::IdempotencyConflict.into());
        }
        let balance: Balance = body
            .and_then(|v| serde_json::from_value(v).ok())
            .ok_or_else(|| AppError::Internal("idempotency record missing body".into()))?;
        return Ok(TxnOutcome {
            balance,
            replayed: true,
        });
    }

    // 2. Get-or-create the customer.
    sqlx::query(
        "INSERT INTO customers (tenant_id, external_id) VALUES ($1, $2)
         ON CONFLICT (tenant_id, external_id) DO NOTHING",
    )
    .bind(tid)
    .bind(&req.external_id)
    .execute(&mut **tx)
    .await?;
    let customer_id: Uuid =
        sqlx::query_scalar("SELECT id FROM customers WHERE tenant_id = $1 AND external_id = $2")
            .bind(tid)
            .bind(&req.external_id)
            .fetch_one(&mut **tx)
            .await?;

    // 3. Lock the balance row (creating it on first touch).
    sqlx::query(
        "INSERT INTO customer_balances (tenant_id, customer_id) VALUES ($1, $2)
         ON CONFLICT (customer_id) DO NOTHING",
    )
    .bind(tid)
    .bind(customer_id)
    .execute(&mut **tx)
    .await?;
    let current: Balance = sqlx::query_as(
        "SELECT spendable_balance, locked_balance, lifetime_earned, lifetime_redeemed
         FROM customer_balances WHERE customer_id = $1 FOR UPDATE",
    )
    .bind(customer_id)
    .fetch_one(&mut **tx)
    .await?;

    // 4. Compute new cache values. delta is signed; lifetime_earned tracks EARN only
    //    (adjustments must not let callers game tier thresholds).
    let new_spendable = current.spendable_balance + req.amount;
    if new_spendable < 0 {
        return Err(AppError::Validation("insufficient spendable balance".into()).into());
    }
    let new_lifetime_earned = current.lifetime_earned
        + if req.txn_type == TxnType::Earn {
            req.amount
        } else {
            0
        };

    // 5. Append the immutable ledger row. available_amount seeds FEFO on EARN only.
    let available = (req.txn_type == TxnType::Earn).then_some(req.amount);
    sqlx::query(
        "INSERT INTO points_ledger
           (tenant_id, customer_id, txn_type, amount, available_amount, expires_at, source_event_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(tid)
    .bind(customer_id)
    .bind(req.txn_type.as_str())
    .bind(req.amount)
    .bind(available)
    .bind(req.expires_at)
    .bind(req.source_event_id)
    .execute(&mut **tx)
    .await?;

    // 6. Update the cache in the same txn (FR-1.2).
    sqlx::query(
        "UPDATE customer_balances
         SET spendable_balance = $2, lifetime_earned = $3, updated_at = now()
         WHERE customer_id = $1",
    )
    .bind(customer_id)
    .bind(new_spendable)
    .bind(new_lifetime_earned)
    .execute(&mut **tx)
    .await?;

    // 7. Re-evaluate tier on the new lifetime_earned (FR-1.3).
    let tier_rows: Vec<(Uuid, i64)> =
        sqlx::query_as("SELECT id, threshold_points FROM tiers WHERE tenant_id = $1")
            .bind(tid)
            .fetch_all(&mut **tx)
            .await?;
    let tiers: Vec<Tier> = tier_rows
        .into_iter()
        .map(|(id, threshold)| Tier { id, threshold })
        .collect();
    let new_tier = select_tier(new_lifetime_earned, &tiers);
    let old_tier: Option<Uuid> =
        sqlx::query_scalar("SELECT current_tier_id FROM customers WHERE id = $1")
            .bind(customer_id)
            .fetch_one(&mut **tx)
            .await?;
    sqlx::query("UPDATE customers SET current_tier_id = $2 WHERE id = $1")
        .bind(customer_id)
        .bind(new_tier)
        .execute(&mut **tx)
        .await?;

    // On a tier upgrade, enqueue the outbound webhook in the SAME txn (FR-5.1) — delivery is
    // atomic with the state change, so a committed upgrade can never lose its notification.
    if let Some(tier_id) = new_tier {
        if new_tier != old_tier {
            let payload = serde_json::json!({
                "customer_external_id": req.external_id,
                "old_tier_id": old_tier,
                "new_tier_id": tier_id,
            });
            crate::webhooks::emit_in_tx(tx, tid, "customer.tier_upgraded", &payload).await?;
        }
    }

    // 8. Persist the idempotent response (caller commits).
    let balance = Balance {
        spendable_balance: new_spendable,
        locked_balance: current.locked_balance,
        lifetime_earned: new_lifetime_earned,
        lifetime_redeemed: current.lifetime_redeemed,
    };
    let body = serde_json::to_value(&balance).map_err(|e| AppError::Internal(e.to_string()))?;
    sqlx::query(
        "UPDATE idempotency_keys SET response_status = $3, response_body = $4
         WHERE tenant_id = $1 AND key = $2",
    )
    .bind(tid)
    .bind(&req.idempotency_key)
    .bind(200_i32)
    .bind(body)
    .execute(&mut **tx)
    .await?;

    // Manual adjustments are operational changes → audited (FR-5.3).
    if req.txn_type == TxnType::Adjustment {
        crate::audit::audit_in_tx(
            tx,
            tid,
            "system",
            "points",
            Some(&req.external_id),
            "adjustment",
            None,
            Some(&serde_json::json!({ "amount": req.amount, "new_spendable": new_spendable })),
        )
        .await?;
    }

    Ok(TxnOutcome {
        balance,
        replayed: false,
    })
}

/// Read the cached balance (FR-1.2) — the `<50ms` runtime read path.
pub async fn get_balance(
    pool: &PgPool,
    tenant: TenantId,
    external_id: &str,
) -> AppResult<Option<Balance>> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    // Start from `customers` and LEFT JOIN the balance cache: a registered customer
    // with no ledger activity yet (no customer_balances row) reads as all-zeros
    // rather than 404. None is returned only for a genuinely unknown external_id.
    let balance = sqlx::query_as::<_, Balance>(
        "SELECT
            COALESCE(b.spendable_balance, 0) AS spendable_balance,
            COALESCE(b.locked_balance, 0)    AS locked_balance,
            COALESCE(b.lifetime_earned, 0)   AS lifetime_earned,
            COALESCE(b.lifetime_redeemed, 0) AS lifetime_redeemed
         FROM customers c
         LEFT JOIN customer_balances b ON b.customer_id = c.id
         WHERE c.external_id = $1",
    )
    .bind(external_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?;
    tx.rollback().await.map_err(internal)?;
    Ok(balance)
}

/// Tenant-admin: create a tier (must run inside a tenant-scoped txn for RLS WITH CHECK).
pub async fn create_tier(
    pool: &PgPool,
    tenant: TenantId,
    name: &str,
    threshold_points: i64,
) -> AppResult<Uuid> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO tiers (tenant_id, name, threshold_points) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(tenant.as_uuid())
    .bind(name)
    .bind(threshold_points)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "tier",
        Some(&id.to_string()),
        "create",
        None,
        Some(&serde_json::json!({ "name": name, "threshold_points": threshold_points })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(id)
}

fn internal(e: sqlx::Error) -> AppError {
    AppError::Internal(e.to_string())
}
