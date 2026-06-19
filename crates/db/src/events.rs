//! Event ingestion + the async rules processor (FR-2.*).
//!
//! Ingestion (API path, tenant-scoped) writes a PENDING event, deduped by idempotency key.
//! Processing (worker path, BYPASSRLS) claims PENDING events with `FOR UPDATE SKIP LOCKED`,
//! evaluates earning rules, mints points via `apply_in_tx`, and flips status — all in ONE txn,
//! so the mint and the status change commit atomically. Reprocessing is safe: the mint's
//! idempotency key is derived from the event id.

use chrono::{Duration, Utc};
use fides_shared::{AppError, AppResult, TenantId};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::ledger::{apply_in_tx, PostTxn};
use crate::set_tenant;
use fides_core::{rules, TxnType};

fn internal(e: sqlx::Error) -> AppError {
    AppError::Internal(e.to_string())
}

/// Ingest an event (tenant-scoped). Returns `(event_id, created)`; `created=false` means a prior
/// event with the same idempotency key already exists (FR-2.2 dedup).
pub async fn ingest(
    pool: &PgPool,
    tenant: TenantId,
    customer_external_id: &str,
    event_type: &str,
    payload: &Value,
    idempotency_key: &str,
) -> AppResult<(Uuid, bool)> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;

    let inserted: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO loyalty_events
           (tenant_id, customer_external_id, event_type, payload, idempotency_key)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
         RETURNING id",
    )
    .bind(tenant.as_uuid())
    .bind(customer_external_id)
    .bind(event_type)
    .bind(payload)
    .bind(idempotency_key)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?;

    let result = match inserted {
        Some(id) => (id, true),
        None => {
            let id: Uuid = sqlx::query_scalar(
                "SELECT id FROM loyalty_events WHERE tenant_id = $1 AND idempotency_key = $2",
            )
            .bind(tenant.as_uuid())
            .bind(idempotency_key)
            .fetch_one(&mut *tx)
            .await
            .map_err(internal)?;
            (id, false)
        }
    };
    tx.commit().await.map_err(internal)?;
    Ok(result)
}

/// Tenant-admin: create an earning rule.
#[allow(clippy::too_many_arguments)]
pub async fn create_rule(
    pool: &PgPool,
    tenant: TenantId,
    event_type: &str,
    condition: &Value,
    base_points: i64,
    points_expire_days: Option<i32>,
) -> AppResult<Uuid> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO earning_rules (tenant_id, event_type, condition, base_points, points_expire_days)
         VALUES ($1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(tenant.as_uuid())
    .bind(event_type)
    .bind(condition)
    .bind(base_points)
    .bind(points_expire_days)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "earning_rule",
        Some(&id.to_string()),
        "create",
        None,
        Some(&serde_json::json!({
            "event_type": event_type, "condition": condition,
            "base_points": base_points, "points_expire_days": points_expire_days,
        })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(id)
}

#[derive(sqlx::FromRow)]
struct EventRow {
    id: Uuid,
    tenant_id: Uuid,
    customer_external_id: String,
    event_type: String,
    payload: Value,
}

#[derive(sqlx::FromRow)]
struct RuleRow {
    id: Uuid,
    condition: Value,
    base_points: i64,
    points_expire_days: Option<i32>,
}

/// Process up to `batch` PENDING events (worker pool / BYPASSRLS). Returns how many were handled.
pub async fn process_pending(pool: &PgPool, batch: usize) -> AppResult<usize> {
    let mut handled = 0;
    for _ in 0..batch {
        if process_one(pool).await? {
            handled += 1;
        } else {
            break; // queue drained
        }
    }
    Ok(handled)
}

#[tracing::instrument(skip_all)]
async fn process_one(pool: &PgPool) -> AppResult<bool> {
    let mut tx = pool.begin().await.map_err(internal)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *tx)
        .await
        .map_err(internal)?;

    // Claim one event; other workers skip it.
    let ev: Option<EventRow> = sqlx::query_as(
        "SELECT id, tenant_id, customer_external_id, event_type, payload
         FROM loyalty_events WHERE status = 'PENDING'
         ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1",
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?;

    let Some(ev) = ev else {
        tx.rollback().await.map_err(internal)?;
        return Ok(false);
    };

    let tenant = TenantId::new(ev.tenant_id);
    set_tenant(&mut tx, tenant).await.map_err(internal)?;

    // Evaluate rules + mint inside this same txn. On failure, record FAILED but still COMMIT
    // (so the status change persists) instead of aborting the whole txn.
    match evaluate(&mut tx, tenant, &ev).await {
        Ok(()) => {
            sqlx::query(
                "UPDATE loyalty_events SET status = 'PROCESSED', processed_at = now(), error = NULL
                 WHERE id = $1",
            )
            .bind(ev.id)
            .execute(&mut *tx)
            .await
            .map_err(internal)?;
        }
        Err(e) => {
            tracing::warn!(event_id = %ev.id, error = %e, "event processing failed");
            sqlx::query("UPDATE loyalty_events SET status = 'FAILED', error = $2 WHERE id = $1")
                .bind(ev.id)
                .bind(e.to_string())
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
        }
    }
    tx.commit().await.map_err(internal)?;
    Ok(true)
}

async fn evaluate(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: TenantId,
    ev: &EventRow,
) -> AppResult<()> {
    let rules_rows: Vec<RuleRow> = sqlx::query_as(
        "SELECT id, condition, base_points, points_expire_days FROM earning_rules
         WHERE tenant_id = $1 AND event_type = $2 AND active ORDER BY created_at",
    )
    .bind(ev.tenant_id)
    .bind(&ev.event_type)
    .fetch_all(&mut **tx)
    .await
    .map_err(internal)?;

    // First matching rule wins (FR-2.3).
    for rule in rules_rows {
        if !rules::matches(&rule.condition, &ev.payload) {
            continue;
        }
        if rule.base_points == 0 {
            return Ok(()); // matched, nothing to mint
        }
        let expires_at = rule
            .points_expire_days
            .map(|d| Utc::now() + Duration::days(d as i64));
        let req = PostTxn {
            external_id: ev.customer_external_id.clone(),
            txn_type: TxnType::Earn,
            amount: rule.base_points,
            expires_at,
            idempotency_key: format!("event:{}", ev.id),
            request_fingerprint: format!("event:{}|rule:{}", ev.id, rule.id),
            source_event_id: Some(ev.id),
        };
        apply_in_tx(tx, tenant, &req).await?;
        return Ok(());
    }
    Ok(()) // no rule matched — processed, no mint
}
