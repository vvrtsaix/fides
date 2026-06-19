//! `X-Tenant-Id` middleware for tenant-scoped surfaces. Validates the header against the
//! `tenants` table, then stashes `TenantId` in request extensions for handlers to read.
//! Handlers still open a tenant-scoped txn (fides_db::set_tenant) — this just authenticates
//! the tenant exists. No user auth: trust comes from the private network (§7).

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use fides_shared::{AppError, TenantId};
use uuid::Uuid;

use crate::{http_error::ApiError, state::AppState};

pub async fn require_tenant(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let raw = req
        .headers()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .ok_or(AppError::MissingTenant)?;

    let id = Uuid::parse_str(raw).map_err(|_| AppError::MissingTenant)?;

    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = $1)")
        .bind(id)
        .fetch_one(&state.pool)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    if !exists {
        return Err(AppError::UnknownTenant.into());
    }

    req.extensions_mut().insert(TenantId::new(id));
    Ok(next.run(req).await)
}
