//! Outbound webhooks (FR-5.1) with exponential-backoff delivery (FR-5.2).
//!
//! `emit` writes outbox rows (one per matching subscription) — call it inside the business txn
//! that produced the event so enqueue is atomic with the state change. `dispatch_pending` is the
//! worker-side delivery loop; it is generic over the HTTP sender so it can be unit-tested with a
//! mock and driven by reqwest in production.

use fides_core::webhook::{next_backoff_secs, sign, MAX_ATTEMPTS};
use fides_shared::{AppError, AppResult, TenantId};
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::set_tenant;

fn internal(e: sqlx::Error) -> AppError {
    AppError::Internal(e.to_string())
}

pub async fn create_subscription(
    pool: &PgPool,
    tenant: TenantId,
    url: &str,
    event_type: &str,
    secret: &str,
) -> AppResult<Uuid> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO webhook_subscriptions (tenant_id, url, event_type, secret)
         VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(tenant.as_uuid())
    .bind(url)
    .bind(event_type)
    .bind(secret)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;
    // Secret intentionally omitted from the audit snapshot.
    crate::audit::audit_in_tx(
        &mut tx,
        tenant.as_uuid(),
        "system",
        "webhook_subscription",
        Some(&id.to_string()),
        "create",
        None,
        Some(&json!({ "url": url, "event_type": event_type })),
    )
    .await?;
    tx.commit().await.map_err(internal)?;
    Ok(id)
}

/// Enqueue an event to every matching active subscription (outbox). Returns rows enqueued.
/// Tenant-scoped; the `_in_tx` form lets callers enqueue atomically inside a business txn.
pub async fn emit(
    pool: &PgPool,
    tenant: TenantId,
    event_type: &str,
    payload: &Value,
) -> AppResult<u64> {
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;
    let n = emit_in_tx(&mut tx, tenant.as_uuid(), event_type, payload).await?;
    tx.commit().await.map_err(internal)?;
    Ok(n)
}

pub async fn emit_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    tid: Uuid,
    event_type: &str,
    payload: &Value,
) -> AppResult<u64> {
    let res = sqlx::query(
        "INSERT INTO webhook_logs (tenant_id, subscription_id, event_type, payload)
         SELECT $1, s.id, $2, $3 FROM webhook_subscriptions s
         WHERE s.tenant_id = $1 AND s.active AND (s.event_type = $2 OR s.event_type = '*')",
    )
    .bind(tid)
    .bind(event_type)
    .bind(payload)
    .execute(&mut **tx)
    .await
    .map_err(internal)?;
    Ok(res.rows_affected())
}

/// What the injected sender receives. The signature is HMAC-SHA256 over `body` (FR-5.2).
pub struct SendReq {
    pub url: String,
    pub body: String,
    pub signature: String,
}

/// Deliver due webhooks (worker pool / BYPASSRLS). `send` returns the HTTP status on a completed
/// request or `Err` on a transport failure. Non-2xx and transport errors both trigger backoff;
/// after `MAX_ATTEMPTS` the log is flagged FAILED. Returns how many rows were attempted.
pub async fn dispatch_pending<F, Fut>(pool: &PgPool, send: F, batch: usize) -> AppResult<usize>
where
    F: Fn(SendReq) -> Fut,
    Fut: std::future::Future<Output = Result<u16, String>>,
{
    let mut attempted = 0;
    for _ in 0..batch {
        let mut tx = pool.begin().await.map_err(internal)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await
            .map_err(internal)?;

        let row: Option<(Uuid, String, String, String, Value, i32)> = sqlx::query_as(
            "SELECT wl.id, s.url, s.secret, wl.event_type, wl.payload, wl.attempt
             FROM webhook_logs wl JOIN webhook_subscriptions s ON s.id = wl.subscription_id
             WHERE wl.status = 'PENDING' AND wl.next_retry_at <= now()
             ORDER BY wl.next_retry_at FOR UPDATE SKIP LOCKED LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;

        let Some((log_id, url, secret, event_type, payload, attempt)) = row else {
            tx.rollback().await.map_err(internal)?;
            break;
        };

        let body = json!({ "event_type": event_type, "payload": payload }).to_string();
        let signature = sign(&secret, &body);
        let result = send(SendReq {
            url,
            body,
            signature,
        })
        .await;
        let next_attempt = attempt + 1;

        match result {
            Ok(status) if (200..300).contains(&status) => {
                sqlx::query(
                    "UPDATE webhook_logs SET status='DELIVERED', attempt=$2, response_status=$3,
                     last_error=NULL WHERE id=$1",
                )
                .bind(log_id)
                .bind(next_attempt)
                .bind(status as i32)
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
            }
            other => {
                let (resp_status, err_msg) = match other {
                    Ok(status) => (Some(status as i32), format!("HTTP {status}")),
                    Err(e) => (None, e),
                };
                if next_attempt >= MAX_ATTEMPTS {
                    sqlx::query(
                        "UPDATE webhook_logs SET status='FAILED', attempt=$2, response_status=$3,
                         last_error=$4 WHERE id=$1",
                    )
                    .bind(log_id)
                    .bind(next_attempt)
                    .bind(resp_status)
                    .bind(&err_msg)
                    .execute(&mut *tx)
                    .await
                    .map_err(internal)?;
                } else {
                    sqlx::query(
                        "UPDATE webhook_logs SET attempt=$2, response_status=$3, last_error=$4,
                         next_retry_at = now() + ($5 || ' seconds')::interval WHERE id=$1",
                    )
                    .bind(log_id)
                    .bind(next_attempt)
                    .bind(resp_status)
                    .bind(&err_msg)
                    .bind(next_backoff_secs(next_attempt).to_string())
                    .execute(&mut *tx)
                    .await
                    .map_err(internal)?;
                }
            }
        }
        tx.commit().await.map_err(internal)?;
        attempted += 1;
    }
    Ok(attempted)
}
