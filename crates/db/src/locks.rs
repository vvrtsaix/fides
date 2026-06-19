//! Point locks (FR-3.3), FEFO consumption (FR-3.2) and expiration (FR-3.2 EXPIRE).
//!
//! Accounting model: locks partition existing points into spendable vs locked WITHOUT writing a
//! ledger row (a hold mints/burns nothing). The ledger's net is the source of truth:
//!     spendable_balance + locked_balance == SUM(points_ledger.amount)
//! HOLD:     spendable -= a, locked += a              (cache only)
//! FULFILL:  FEFO-consume `a`, write REDEEM(-a), locked -= a, lifetime_redeemed += a
//! RELEASE:  spendable += a, locked -= a, write UNLOCK(0) as an audit marker
//! EXPIRE:   zero an EARN row's remaining points, write EXPIRE(-remaining), spendable -= remaining

use chrono::{Duration, Utc};
use fides_core::fefo::{self, FefoRow};
use fides_shared::{AppError, AppResult, TenantId};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::set_tenant;

/// Default hold TTL (§9). Sweeper releases HELD locks past this.
pub const LOCK_TTL_MINUTES: i64 = 15;

fn internal(e: sqlx::Error) -> AppError {
    AppError::Internal(e.to_string())
}

async fn lookup_customer(
    tx: &mut Transaction<'_, Postgres>,
    tid: Uuid,
    external_id: &str,
) -> AppResult<Uuid> {
    sqlx::query_scalar("SELECT id FROM customers WHERE tenant_id = $1 AND external_id = $2")
        .bind(tid)
        .bind(external_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(internal)?
        .ok_or(AppError::NotFound)
}

/// HOLD: reserve `amount` spendable points (FR-3.3). Returns the lock id. Idempotent: a retry with
/// the same key returns the original lock instead of double-reserving (constraint #6).
pub async fn create_lock(
    pool: &PgPool,
    tenant: TenantId,
    external_id: &str,
    amount: i64,
    idempotency_key: &str,
) -> AppResult<Uuid> {
    if amount <= 0 {
        return Err(AppError::Validation("lock amount must be positive".into()));
    }
    let tid = tenant.as_uuid();
    let fingerprint = format!("lock:{external_id}:{amount}");
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;

    // Claim the idempotency key, or replay the prior lock id.
    let claimed: Option<String> = sqlx::query_scalar(
        "INSERT INTO idempotency_keys (tenant_id, key, request_fingerprint)
         VALUES ($1, $2, $3) ON CONFLICT (tenant_id, key) DO NOTHING RETURNING key",
    )
    .bind(tid)
    .bind(idempotency_key)
    .bind(&fingerprint)
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
        if fp != fingerprint {
            return Err(AppError::IdempotencyConflict);
        }
        let id: String = body
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .ok_or_else(|| AppError::Internal("idempotency record missing lock id".into()))?;
        return id
            .parse()
            .map_err(|_| AppError::Internal("bad stored lock id".into()));
    }

    let customer_id = lookup_customer(&mut tx, tid, external_id).await?;

    let spendable: i64 = sqlx::query_scalar(
        "SELECT spendable_balance FROM customer_balances WHERE customer_id = $1 FOR UPDATE",
    )
    .bind(customer_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    .ok_or(AppError::NotFound)?;

    if spendable < amount {
        return Err(AppError::Validation(
            "insufficient spendable balance".into(),
        ));
    }

    sqlx::query(
        "UPDATE customer_balances
         SET spendable_balance = spendable_balance - $2, locked_balance = locked_balance + $2,
             updated_at = now()
         WHERE customer_id = $1",
    )
    .bind(customer_id)
    .bind(amount)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;

    let expires_at = Utc::now() + Duration::minutes(LOCK_TTL_MINUTES);
    let lock_id: Uuid = sqlx::query_scalar(
        "INSERT INTO points_locks (tenant_id, customer_id, amount, expires_at)
         VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(tid)
    .bind(customer_id)
    .bind(amount)
    .bind(expires_at)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;

    // Record the lock id for idempotent replays.
    sqlx::query(
        "UPDATE idempotency_keys SET response_status = 200, response_body = $3
         WHERE tenant_id = $1 AND key = $2",
    )
    .bind(tid)
    .bind(idempotency_key)
    .bind(serde_json::Value::String(lock_id.to_string()))
    .execute(&mut *tx)
    .await
    .map_err(internal)?;

    tx.commit().await.map_err(internal)?;
    Ok(lock_id)
}

/// Fetch and FOR UPDATE-lock a HELD lock, returning (customer_id, amount).
async fn claim_held_lock(
    tx: &mut Transaction<'_, Postgres>,
    lock_id: Uuid,
) -> AppResult<(Uuid, i64)> {
    let row: Option<(Uuid, i64, String)> = sqlx::query_as(
        "SELECT customer_id, amount, status FROM points_locks WHERE id = $1 FOR UPDATE",
    )
    .bind(lock_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(internal)?;
    match row {
        None => Err(AppError::NotFound),
        Some((_, _, status)) if status != "HELD" => {
            Err(AppError::Validation(format!("lock is {status}, not HELD")))
        }
        Some((customer_id, amount, _)) => Ok((customer_id, amount)),
    }
}

/// FULFILL: the checkout completed — convert the hold into a permanent REDEEM (FR-3.3).
pub async fn fulfill_lock(pool: &PgPool, tenant: TenantId, lock_id: Uuid) -> AppResult<()> {
    let tid = tenant.as_uuid();
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    let (customer_id, amount) = claim_held_lock(&mut tx, lock_id).await?;

    // Lock the balance row, then FEFO-consume the points.
    sqlx::query("SELECT 1 FROM customer_balances WHERE customer_id = $1 FOR UPDATE")
        .bind(customer_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal)?;
    consume_fefo(&mut tx, tid, customer_id, amount).await?;

    sqlx::query(
        "UPDATE customer_balances
         SET locked_balance = locked_balance - $2, lifetime_redeemed = lifetime_redeemed + $2,
             updated_at = now()
         WHERE customer_id = $1",
    )
    .bind(customer_id)
    .bind(amount)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;

    sqlx::query("UPDATE points_locks SET status = 'FULFILLED', resolved_at = now() WHERE id = $1")
        .bind(lock_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;

    tx.commit().await.map_err(internal)?;
    Ok(())
}

/// RELEASE: the hold timed out or was cancelled — restore spendable, write UNLOCK (FR-3.3).
pub async fn release_lock(pool: &PgPool, tenant: TenantId, lock_id: Uuid) -> AppResult<()> {
    let tid = tenant.as_uuid();
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    let (customer_id, amount) = claim_held_lock(&mut tx, lock_id).await?;
    release_in_tx(&mut tx, tid, customer_id, lock_id, amount).await?;
    tx.commit().await.map_err(internal)?;
    Ok(())
}

/// Shared release body, usable from the API path and the sweeper.
async fn release_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    tid: Uuid,
    customer_id: Uuid,
    lock_id: Uuid,
    amount: i64,
) -> AppResult<()> {
    sqlx::query(
        "UPDATE customer_balances
         SET spendable_balance = spendable_balance + $2, locked_balance = locked_balance - $2,
             updated_at = now()
         WHERE customer_id = $1",
    )
    .bind(customer_id)
    .bind(amount)
    .execute(&mut **tx)
    .await
    .map_err(internal)?;

    // Audit-only UNLOCK row (amount 0 keeps the net invariant intact).
    sqlx::query(
        "INSERT INTO points_ledger (tenant_id, customer_id, txn_type, amount)
         VALUES ($1, $2, 'UNLOCK', 0)",
    )
    .bind(tid)
    .bind(customer_id)
    .execute(&mut **tx)
    .await
    .map_err(internal)?;

    sqlx::query("UPDATE points_locks SET status = 'RELEASED', resolved_at = now() WHERE id = $1")
        .bind(lock_id)
        .execute(&mut **tx)
        .await
        .map_err(internal)?;
    Ok(())
}

/// FEFO-consume `amount` from a customer's unexpired EARN rows and write the REDEEM (FR-3.2).
/// Caller must already hold the balance row lock and have verified availability/tenant scope.
/// Reused by reward redemption in P4.
pub async fn consume_fefo(
    tx: &mut Transaction<'_, Postgres>,
    tid: Uuid,
    customer_id: Uuid,
    amount: i64,
) -> AppResult<()> {
    let rows: Vec<(Uuid, i64)> = sqlx::query_as(
        "SELECT id, available_amount FROM points_ledger
         WHERE customer_id = $1 AND available_amount > 0
         ORDER BY expires_at NULLS LAST, created_at
         FOR UPDATE",
    )
    .bind(customer_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(internal)?;

    let fefo_rows: Vec<FefoRow> = rows
        .into_iter()
        .map(|(id, available)| FefoRow { id, available })
        .collect();
    let plan = fefo::consume(&fefo_rows, amount)
        .map_err(|short| AppError::Validation(format!("insufficient points: short by {short}")))?;

    for (id, take) in plan {
        sqlx::query(
            "UPDATE points_ledger SET available_amount = available_amount - $2 WHERE id = $1",
        )
        .bind(id)
        .bind(take)
        .execute(&mut **tx)
        .await
        .map_err(internal)?;
    }

    sqlx::query(
        "INSERT INTO points_ledger (tenant_id, customer_id, txn_type, amount)
         VALUES ($1, $2, 'REDEEM', $3)",
    )
    .bind(tid)
    .bind(customer_id)
    .bind(-amount)
    .execute(&mut **tx)
    .await
    .map_err(internal)?;
    Ok(())
}

/// Sweeper: release HELD locks past their TTL (worker pool / BYPASSRLS). Returns count released.
pub async fn release_expired(pool: &PgPool, batch: usize) -> AppResult<usize> {
    let mut released = 0;
    for _ in 0..batch {
        let mut tx = pool.begin().await.map_err(internal)?;
        let row: Option<(Uuid, Uuid, Uuid, i64)> = sqlx::query_as(
            "SELECT id, tenant_id, customer_id, amount FROM points_locks
             WHERE status = 'HELD' AND expires_at < now()
             FOR UPDATE SKIP LOCKED LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;

        let Some((lock_id, tid, customer_id, amount)) = row else {
            tx.rollback().await.map_err(internal)?;
            break;
        };
        set_tenant(&mut tx, TenantId::new(tid))
            .await
            .map_err(internal)?;
        sqlx::query("SELECT 1 FROM customer_balances WHERE customer_id = $1 FOR UPDATE")
            .bind(customer_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(internal)?;
        release_in_tx(&mut tx, tid, customer_id, lock_id, amount).await?;
        tx.commit().await.map_err(internal)?;
        released += 1;
    }
    Ok(released)
}

/// Expiration sweeper: zero out EARN rows past `expires_at`, write EXPIRE, reduce spendable
/// (worker pool / BYPASSRLS). Returns count of expired rows processed.
pub async fn expire_due(pool: &PgPool, batch: usize) -> AppResult<usize> {
    let mut expired = 0;
    for _ in 0..batch {
        let mut tx = pool.begin().await.map_err(internal)?;
        let row: Option<(Uuid, Uuid, Uuid, i64)> = sqlx::query_as(
            "SELECT id, tenant_id, customer_id, available_amount FROM points_ledger
             WHERE available_amount > 0 AND expires_at IS NOT NULL AND expires_at < now()
             FOR UPDATE SKIP LOCKED LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;

        let Some((row_id, tid, customer_id, amount)) = row else {
            tx.rollback().await.map_err(internal)?;
            break;
        };
        set_tenant(&mut tx, TenantId::new(tid))
            .await
            .map_err(internal)?;
        sqlx::query("SELECT 1 FROM customer_balances WHERE customer_id = $1 FOR UPDATE")
            .bind(customer_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(internal)?;

        sqlx::query("UPDATE points_ledger SET available_amount = 0 WHERE id = $1")
            .bind(row_id)
            .execute(&mut *tx)
            .await
            .map_err(internal)?;
        sqlx::query(
            "INSERT INTO points_ledger (tenant_id, customer_id, txn_type, amount)
             VALUES ($1, $2, 'EXPIRE', $3)",
        )
        .bind(tid)
        .bind(customer_id)
        .bind(-amount)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        sqlx::query(
            "UPDATE customer_balances
             SET spendable_balance = spendable_balance - $2, updated_at = now()
             WHERE customer_id = $1",
        )
        .bind(customer_id)
        .bind(amount)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;

        tx.commit().await.map_err(internal)?;
        expired += 1;
    }
    Ok(expired)
}
