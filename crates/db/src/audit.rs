//! Security audit trail (FR-5.3) + GDPR/CCPA anonymization (NFR-3.3).
//!
//! `audit_in_tx` writes an append-only snapshot inside the txn making the change, so the record
//! and the change commit together. The `audit_logs` table grants no UPDATE/DELETE to the app role
//! (migration 0006), so the trail is immutable.

use fides_shared::{AppError, AppResult, TenantId};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::set_tenant;

fn internal(e: sqlx::Error) -> AppError {
    AppError::Internal(e.to_string())
}

#[allow(clippy::too_many_arguments)]
pub async fn audit_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    tid: Uuid,
    actor: &str,
    entity: &str,
    entity_id: Option<&str>,
    action: &str,
    old_values: Option<&Value>,
    new_values: Option<&Value>,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO audit_logs
           (tenant_id, actor, entity, entity_id, action, old_values, new_values)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(tid)
    .bind(actor)
    .bind(entity)
    .bind(entity_id)
    .bind(action)
    .bind(old_values)
    .bind(new_values)
    .execute(&mut **tx)
    .await
    .map_err(internal)?;
    Ok(())
}

/// Anonymize a customer (NFR-3.3): zero identity strings, flip status, KEEP the ledger so
/// financial accounting stays intact. Audited. Returns false if the customer doesn't exist.
pub async fn anonymize(
    pool: &sqlx::PgPool,
    tenant: TenantId,
    external_id: &str,
    actor: &str,
) -> AppResult<bool> {
    let tid = tenant.as_uuid();
    let mut tx = pool.begin().await.map_err(internal)?;
    set_tenant(&mut tx, tenant).await.map_err(internal)?;

    let before: Option<(Uuid, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT id, email, phone FROM customers WHERE tenant_id = $1 AND external_id = $2 FOR UPDATE",
    )
    .bind(tid)
    .bind(external_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?;

    let Some((customer_id, old_email, old_phone)) = before else {
        tx.rollback().await.map_err(internal)?;
        return Ok(false);
    };

    sqlx::query(
        "UPDATE customers SET email = NULL, phone = NULL, status = 'ANONYMIZED'
         WHERE id = $1",
    )
    .bind(customer_id)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;

    let old = json!({ "email": old_email, "phone": old_phone, "status": "ACTIVE" });
    let new = json!({ "email": null, "phone": null, "status": "ANONYMIZED" });
    audit_in_tx(
        &mut tx,
        tid,
        actor,
        "customer",
        Some(&customer_id.to_string()),
        "anonymize",
        Some(&old),
        Some(&new),
    )
    .await?;

    tx.commit().await.map_err(internal)?;
    Ok(true)
}
