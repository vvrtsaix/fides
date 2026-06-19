//! Router assembly for the two surfaces. P1 wires the financial-core endpoints.

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    middleware,
    routing::{delete, get, post},
    Extension, Json, Router,
};
use chrono::{DateTime, Utc};
use fides_core::TxnType;
use fides_db::{get_balance, post_ledger_txn, PostTxn};
use fides_shared::{AppError, TenantId};
use serde::Deserialize;
use serde_json::{json, Value};
use tower_http::trace::TraceLayer;

use crate::{http_error::ApiError, state::AppState, tenant_mw::require_tenant};

async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// :8080 — runtime. `/v1/*` requires a valid `X-Tenant-Id`; health is open.
pub fn runtime_router(state: AppState) -> Router {
    let v1 = Router::new()
        .route("/customers/:external_id/balance", get(get_balance_h))
        .route("/customers/:external_id/transactions", post(post_txn_h))
        .route("/customers/:external_id/locks", post(create_lock_h))
        .route("/locks/:lock_id/fulfill", post(fulfill_lock_h))
        .route("/locks/:lock_id/release", post(release_lock_h))
        .route("/customers/:external_id/redemptions", post(redeem_h))
        .route("/vouchers/:code/use", post(use_voucher_h))
        .route("/customers/:external_id/anonymize", post(anonymize_h))
        .route("/events", post(post_event_h))
        .route("/whoami", get(whoami))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_tenant,
        ));

    Router::new()
        .route("/healthz", get(healthz))
        .nest("/v1", v1)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// :8081 — admin. `/platform/*` is cross-tenant (no X-Tenant-Id); `/admin/*` is tenant-scoped.
pub fn admin_router(state: AppState) -> Router {
    let tenant_admin = Router::new()
        .route("/customers", post(create_customer_h).get(list_customers_h))
        .route(
            "/customers/:external_id",
            get(get_customer_h)
                .patch(update_customer_h)
                .delete(delete_customer_h),
        )
        .route(
            "/customers/:external_id/segments",
            get(list_customer_segments_h),
        )
        .route("/segments", post(create_segment_h).get(list_segments_h))
        .route(
            "/segments/:id",
            get(get_segment_h)
                .patch(update_segment_h)
                .delete(delete_segment_h),
        )
        .route(
            "/segments/:id/members",
            get(list_members_h).post(add_member_h),
        )
        .route(
            "/segments/:id/members/:external_id",
            delete(remove_member_h),
        )
        .route("/tiers", post(create_tier_h).get(list_tiers_h))
        .route("/tiers/:id", get(get_tier_h).patch(update_tier_h).delete(delete_tier_h))
        .route("/earning-rules", post(create_rule_h).get(list_rules_h))
        .route(
            "/earning-rules/:id",
            get(get_rule_h).patch(update_rule_h).delete(delete_rule_h),
        )
        .route("/campaigns", post(create_campaign_h).get(list_campaigns_h))
        .route(
            "/campaigns/:id",
            get(get_campaign_h).patch(update_campaign_h).delete(delete_campaign_h),
        )
        .route("/rewards", post(create_reward_h).get(list_rewards_h))
        .route(
            "/rewards/:id",
            get(get_reward_h).patch(update_reward_h).delete(delete_reward_h),
        )
        .route(
            "/webhook-subscriptions",
            post(create_subscription_h).get(list_subscriptions_h),
        )
        .route(
            "/webhook-subscriptions/:id",
            get(get_subscription_h)
                .patch(update_subscription_h)
                .delete(delete_subscription_h),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_tenant,
        ));

    Router::new()
        .route("/healthz", get(healthz))
        .route("/platform/tenants", post(create_tenant).get(list_tenants_h))
        .route(
            "/platform/tenants/:id",
            get(get_tenant_h).patch(update_tenant_h).delete(delete_tenant_h),
        )
        .nest("/admin", tenant_admin)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn whoami(Extension(tenant): Extension<TenantId>) -> Json<Value> {
    Json(json!({ "tenant_id": tenant.to_string() }))
}

// ---- runtime: balance + transactions -------------------------------------

async fn get_balance_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(external_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    match get_balance(&state.pool, tenant, &external_id).await? {
        Some(b) => Ok(Json(json!(b))),
        None => Err(AppError::NotFound.into()),
    }
}

#[derive(Deserialize)]
struct TxnBody {
    #[serde(rename = "type")]
    txn_type: String,
    amount: i64,
    #[serde(default)]
    expires_at: Option<DateTime<Utc>>,
}

async fn post_txn_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(external_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<TxnBody>,
) -> Result<Json<Value>, ApiError> {
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .ok_or(AppError::MissingIdempotencyKey)?
        .to_string();

    let txn_type = TxnType::parse(&body.txn_type)
        .ok_or_else(|| AppError::Validation(format!("unknown txn type: {}", body.txn_type)))?;
    fides_core::validate_txn(txn_type, body.amount)
        .map_err(|m| AppError::Validation(m.to_string()))?;

    // Canonical request identity: a replay with a different payload under the same key is a 409.
    let request_fingerprint = format!(
        ":external_id|{}|{}|{:?}",
        txn_type.as_str(),
        body.amount,
        body.expires_at
    );

    let outcome = post_ledger_txn(
        &state.pool,
        tenant,
        PostTxn {
            external_id,
            txn_type,
            amount: body.amount,
            expires_at: body.expires_at,
            idempotency_key,
            request_fingerprint,
            source_event_id: None,
        },
    )
    .await?;

    Ok(Json(json!({
        "balance": outcome.balance,
        "replayed": outcome.replayed,
    })))
}

// ---- runtime: point locks (FR-3.3) ---------------------------------------

#[derive(Deserialize)]
struct LockBody {
    amount: i64,
}

async fn create_lock_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(external_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<LockBody>,
) -> Result<Json<Value>, ApiError> {
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .ok_or(AppError::MissingIdempotencyKey)?;
    let id = fides_db::create_lock(
        &state.pool,
        tenant,
        &external_id,
        body.amount,
        idempotency_key,
    )
    .await?;
    Ok(Json(json!({ "id": id.to_string(), "status": "HELD" })))
}

async fn fulfill_lock_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(lock_id): Path<uuid::Uuid>,
) -> Result<Json<Value>, ApiError> {
    fides_db::fulfill_lock(&state.pool, tenant, lock_id).await?;
    Ok(Json(json!({ "status": "FULFILLED" })))
}

async fn release_lock_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(lock_id): Path<uuid::Uuid>,
) -> Result<Json<Value>, ApiError> {
    fides_db::release_lock(&state.pool, tenant, lock_id).await?;
    Ok(Json(json!({ "status": "RELEASED" })))
}

async fn use_voucher_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(code): Path<String>,
) -> Result<Json<Value>, ApiError> {
    fides_db::use_voucher(&state.pool, tenant, &code).await?;
    Ok(Json(json!({ "status": "USED" })))
}

async fn anonymize_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(external_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    // No user identity on the private network; attribute to the internal caller generically.
    let ok = fides_db::anonymize(&state.pool, tenant, &external_id, "system").await?;
    if !ok {
        return Err(AppError::NotFound.into());
    }
    Ok(Json(json!({ "status": "ANONYMIZED" })))
}

// ---- runtime: reward redemption (FR-4.*) ----------------------------------

#[derive(Deserialize)]
struct RedeemBody {
    reward_id: uuid::Uuid,
}

async fn redeem_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(external_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<RedeemBody>,
) -> Result<Json<Value>, ApiError> {
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .ok_or(AppError::MissingIdempotencyKey)?;
    let out = fides_db::redeem_reward(
        &state.pool,
        tenant,
        &external_id,
        body.reward_id,
        idempotency_key,
    )
    .await?;
    Ok(Json(json!({
        "voucher_code": out.voucher_code,
        "spendable_balance": out.spendable_balance,
        "replayed": out.replayed,
    })))
}

#[derive(Deserialize)]
struct EventBody {
    customer_external_id: String,
    event_type: String,
    #[serde(default)]
    payload: Value,
}

/// Ingest an event (FR-2.1/2.2). Returns immediately with PENDING; the worker processes it.
/// `Idempotency-Key` dedupes ingestion so a retried POST does not double-mint downstream.
async fn post_event_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    headers: HeaderMap,
    Json(body): Json<EventBody>,
) -> Result<Json<Value>, ApiError> {
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .ok_or(AppError::MissingIdempotencyKey)?;

    let (id, created) = fides_db::ingest(
        &state.pool,
        tenant,
        &body.customer_external_id,
        &body.event_type,
        &body.payload,
        idempotency_key,
    )
    .await?;
    Ok(Json(
        json!({ "id": id.to_string(), "status": "PENDING", "created": created }),
    ))
}

#[derive(Deserialize)]
struct RuleBody {
    event_type: String,
    #[serde(default)]
    condition: Value,
    base_points: i64,
    #[serde(default)]
    points_expire_days: Option<i32>,
}

async fn create_rule_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Json(body): Json<RuleBody>,
) -> Result<Json<Value>, ApiError> {
    let id = fides_db::create_rule(
        &state.pool,
        tenant,
        &body.event_type,
        &body.condition,
        body.base_points,
        body.points_expire_days,
    )
    .await?;
    Ok(Json(json!({ "id": id.to_string() })))
}

// ---- platform-admin + tenant-admin ----------------------------------------

#[derive(Deserialize)]
struct CreateTenant {
    name: String,
    currency: String,
}

async fn create_tenant(
    State(state): State<AppState>,
    Json(body): Json<CreateTenant>,
) -> Result<Json<Value>, ApiError> {
    if body.currency.len() != 3 {
        return Err(
            AppError::Validation("currency must be a 3-letter ISO-4217 code".into()).into(),
        );
    }
    let id: uuid::Uuid =
        sqlx::query_scalar("INSERT INTO tenants (name, currency) VALUES ($1, $2) RETURNING id")
            .bind(&body.name)
            .bind(body.currency.to_uppercase())
            .fetch_one(&state.pool)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(Json(json!({ "id": id.to_string() })))
}

#[derive(Deserialize)]
struct CreateTier {
    name: String,
    threshold_points: i64,
}

async fn create_tier_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Json(body): Json<CreateTier>,
) -> Result<Json<Value>, ApiError> {
    let id = fides_db::create_tier(&state.pool, tenant, &body.name, body.threshold_points).await?;
    Ok(Json(json!({ "id": id.to_string() })))
}

// Money fields arrive as strings to avoid float rounding.
fn parse_money(s: &str) -> Result<rust_decimal::Decimal, ApiError> {
    rust_decimal::Decimal::from_str_exact(s)
        .map_err(|_| AppError::Validation(format!("invalid decimal: {s}")).into())
}

#[derive(Deserialize)]
struct CreateCampaign {
    name: String,
    budget_cap: String,
}

async fn create_campaign_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Json(body): Json<CreateCampaign>,
) -> Result<Json<Value>, ApiError> {
    let cap = parse_money(&body.budget_cap)?;
    let id = fides_db::create_campaign(&state.pool, tenant, &body.name, cap).await?;
    Ok(Json(json!({ "id": id.to_string() })))
}

#[derive(Deserialize)]
struct CreateReward {
    campaign_id: uuid::Uuid,
    name: String,
    cost_points: i64,
    #[serde(default = "zero_money")]
    reward_value: String,
    available_stock: i32,
    #[serde(default = "default_valid_days")]
    valid_days: i32,
}

fn zero_money() -> String {
    "0".into()
}
fn default_valid_days() -> i32 {
    365
}

async fn create_reward_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Json(body): Json<CreateReward>,
) -> Result<Json<Value>, ApiError> {
    let reward_value = parse_money(&body.reward_value)?;
    let id = fides_db::create_reward(
        &state.pool,
        tenant,
        fides_db::NewReward {
            campaign_id: body.campaign_id,
            name: &body.name,
            cost_points: body.cost_points,
            reward_value,
            available_stock: body.available_stock,
            valid_days: body.valid_days,
        },
    )
    .await?;
    Ok(Json(json!({ "id": id.to_string() })))
}

#[derive(Deserialize)]
struct CreateSubscription {
    url: String,
    event_type: String,
    secret: String,
}

async fn create_subscription_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Json(body): Json<CreateSubscription>,
) -> Result<Json<Value>, ApiError> {
    let id = fides_db::create_subscription(
        &state.pool,
        tenant,
        &body.url,
        &body.event_type,
        &body.secret,
    )
    .await?;
    Ok(Json(json!({ "id": id.to_string() })))
}

// ===========================================================================
// Read / Update / Delete arms (full CRUD for the admin entities).
// Update bodies are partial: omitted fields keep their current value (COALESCE).
// Delete is a SOFT delete (active=false, or tenant status=SUSPENDED).
// ===========================================================================
use uuid::Uuid;

// ---- platform: tenants -----------------------------------------------------

async fn list_tenants_h(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(fides_db::list_tenants(&state.pool).await?)))
}

async fn get_tenant_h(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(fides_db::get_tenant(&state.pool, id).await?)))
}

#[derive(Deserialize)]
struct UpdateTenant {
    name: Option<String>,
    status: Option<String>,
}

async fn update_tenant_h(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateTenant>,
) -> Result<Json<Value>, ApiError> {
    let row =
        fides_db::update_tenant(&state.pool, id, body.name.as_deref(), body.status.as_deref())
            .await?;
    Ok(Json(json!(row)))
}

async fn delete_tenant_h(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    fides_db::delete_tenant(&state.pool, id).await?;
    Ok(Json(json!({ "status": "SUSPENDED" })))
}

// ---- admin: customers ------------------------------------------------------

#[derive(Deserialize)]
struct CreateCustomer {
    external_id: String,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    phone: Option<String>,
}

async fn create_customer_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Json(body): Json<CreateCustomer>,
) -> Result<Json<Value>, ApiError> {
    let row = fides_db::create_customer(
        &state.pool,
        tenant,
        &body.external_id,
        body.email.as_deref(),
        body.phone.as_deref(),
    )
    .await?;
    Ok(Json(json!(row)))
}

async fn list_customers_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::list_customers(&state.pool, tenant).await?
    )))
}

async fn get_customer_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(external_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::get_customer(&state.pool, tenant, &external_id).await?
    )))
}

#[derive(Deserialize)]
struct UpdateCustomer {
    email: Option<String>,
    phone: Option<String>,
    current_tier_id: Option<Uuid>,
}

async fn update_customer_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(external_id): Path<String>,
    Json(body): Json<UpdateCustomer>,
) -> Result<Json<Value>, ApiError> {
    let row = fides_db::update_customer(
        &state.pool,
        tenant,
        &external_id,
        body.email.as_deref(),
        body.phone.as_deref(),
        body.current_tier_id,
    )
    .await?;
    Ok(Json(json!(row)))
}

/// Delete = anonymize (GDPR scrub). Customers are never hard-deleted.
async fn delete_customer_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(external_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let ok = fides_db::anonymize(&state.pool, tenant, &external_id, "system").await?;
    if !ok {
        return Err(AppError::NotFound.into());
    }
    Ok(Json(json!({ "status": "ANONYMIZED" })))
}

// ---- admin: tiers ----------------------------------------------------------

async fn list_tiers_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(fides_db::list_tiers(&state.pool, tenant).await?)))
}

async fn get_tier_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(fides_db::get_tier(&state.pool, tenant, id).await?)))
}

#[derive(Deserialize)]
struct UpdateTier {
    name: Option<String>,
    threshold_points: Option<i64>,
}

async fn update_tier_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateTier>,
) -> Result<Json<Value>, ApiError> {
    let row = fides_db::update_tier(
        &state.pool,
        tenant,
        id,
        body.name.as_deref(),
        body.threshold_points,
    )
    .await?;
    Ok(Json(json!(row)))
}

async fn delete_tier_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    fides_db::delete_tier(&state.pool, tenant, id).await?;
    Ok(Json(json!({ "status": "DELETED" })))
}

// ---- admin: earning-rules --------------------------------------------------

async fn list_rules_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(fides_db::list_rules(&state.pool, tenant).await?)))
}

async fn get_rule_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(fides_db::get_rule(&state.pool, tenant, id).await?)))
}

#[derive(Deserialize)]
struct UpdateRule {
    event_type: Option<String>,
    condition: Option<Value>,
    base_points: Option<i64>,
    points_expire_days: Option<i32>,
    active: Option<bool>,
}

async fn update_rule_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateRule>,
) -> Result<Json<Value>, ApiError> {
    let row = fides_db::update_rule(
        &state.pool,
        tenant,
        id,
        body.event_type.as_deref(),
        body.condition.as_ref(),
        body.base_points,
        body.points_expire_days,
        body.active,
    )
    .await?;
    Ok(Json(json!(row)))
}

async fn delete_rule_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    fides_db::delete_rule(&state.pool, tenant, id).await?;
    Ok(Json(json!({ "status": "DELETED" })))
}

// ---- admin: campaigns ------------------------------------------------------

async fn list_campaigns_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::list_campaigns(&state.pool, tenant).await?
    )))
}

async fn get_campaign_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::get_campaign(&state.pool, tenant, id).await?
    )))
}

#[derive(Deserialize)]
struct UpdateCampaign {
    name: Option<String>,
    budget_cap: Option<String>,
    active: Option<bool>,
}

async fn update_campaign_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateCampaign>,
) -> Result<Json<Value>, ApiError> {
    let budget_cap = body.budget_cap.as_deref().map(parse_money).transpose()?;
    let row =
        fides_db::update_campaign(&state.pool, tenant, id, body.name.as_deref(), budget_cap, body.active)
            .await?;
    Ok(Json(json!(row)))
}

async fn delete_campaign_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    fides_db::delete_campaign(&state.pool, tenant, id).await?;
    Ok(Json(json!({ "status": "DELETED" })))
}

// ---- admin: rewards --------------------------------------------------------

async fn list_rewards_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::list_rewards(&state.pool, tenant).await?
    )))
}

async fn get_reward_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::get_reward(&state.pool, tenant, id).await?
    )))
}

#[derive(Deserialize)]
struct UpdateReward {
    name: Option<String>,
    cost_points: Option<i64>,
    reward_value: Option<String>,
    available_stock: Option<i32>,
    valid_days: Option<i32>,
    active: Option<bool>,
}

async fn update_reward_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateReward>,
) -> Result<Json<Value>, ApiError> {
    let reward_value = body.reward_value.as_deref().map(parse_money).transpose()?;
    let row = fides_db::update_reward(
        &state.pool,
        tenant,
        id,
        body.name.as_deref(),
        body.cost_points,
        reward_value,
        body.available_stock,
        body.valid_days,
        body.active,
    )
    .await?;
    Ok(Json(json!(row)))
}

async fn delete_reward_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    fides_db::delete_reward(&state.pool, tenant, id).await?;
    Ok(Json(json!({ "status": "DELETED" })))
}

// ---- admin: webhook-subscriptions ------------------------------------------

async fn list_subscriptions_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::list_subscriptions(&state.pool, tenant).await?
    )))
}

async fn get_subscription_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::get_subscription(&state.pool, tenant, id).await?
    )))
}

#[derive(Deserialize)]
struct UpdateSubscription {
    url: Option<String>,
    event_type: Option<String>,
    secret: Option<String>,
    active: Option<bool>,
}

async fn update_subscription_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateSubscription>,
) -> Result<Json<Value>, ApiError> {
    let row = fides_db::update_subscription(
        &state.pool,
        tenant,
        id,
        body.url.as_deref(),
        body.event_type.as_deref(),
        body.secret.as_deref(),
        body.active,
    )
    .await?;
    Ok(Json(json!(row)))
}

async fn delete_subscription_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    fides_db::delete_subscription(&state.pool, tenant, id).await?;
    Ok(Json(json!({ "status": "DELETED" })))
}

// ---- admin: segments + membership ------------------------------------------

#[derive(Deserialize)]
struct CreateSegment {
    name: String,
    #[serde(default)]
    description: Option<String>,
    /// Rule DSL → dynamic segment (worker-managed membership). Omit for a static segment.
    #[serde(default)]
    definition: Option<Value>,
}

async fn create_segment_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Json(body): Json<CreateSegment>,
) -> Result<Json<Value>, ApiError> {
    let row = fides_db::create_segment(
        &state.pool,
        tenant,
        &body.name,
        body.description.as_deref(),
        body.definition.as_ref(),
    )
    .await?;
    Ok(Json(json!(row)))
}

async fn list_segments_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::list_segments(&state.pool, tenant).await?
    )))
}

async fn get_segment_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::get_segment(&state.pool, tenant, id).await?
    )))
}

#[derive(Deserialize)]
struct UpdateSegment {
    name: Option<String>,
    description: Option<String>,
    definition: Option<Value>,
    active: Option<bool>,
}

async fn update_segment_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateSegment>,
) -> Result<Json<Value>, ApiError> {
    let row = fides_db::update_segment(
        &state.pool,
        tenant,
        id,
        body.name.as_deref(),
        body.description.as_deref(),
        body.definition.as_ref(),
        body.active,
    )
    .await?;
    Ok(Json(json!(row)))
}

async fn delete_segment_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    fides_db::delete_segment(&state.pool, tenant, id).await?;
    Ok(Json(json!({ "status": "DELETED" })))
}

async fn list_members_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::list_members(&state.pool, tenant, id).await?
    )))
}

#[derive(Deserialize)]
struct AddMember {
    external_id: String,
}

async fn add_member_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<AddMember>,
) -> Result<Json<Value>, ApiError> {
    fides_db::add_member(&state.pool, tenant, id, &body.external_id).await?;
    Ok(Json(json!({ "status": "ADDED" })))
}

async fn remove_member_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path((id, external_id)): Path<(Uuid, String)>,
) -> Result<Json<Value>, ApiError> {
    fides_db::remove_member(&state.pool, tenant, id, &external_id).await?;
    Ok(Json(json!({ "status": "REMOVED" })))
}

async fn list_customer_segments_h(
    State(state): State<AppState>,
    Extension(tenant): Extension<TenantId>,
    Path(external_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        fides_db::list_customer_segments(&state.pool, tenant, &external_id).await?
    )))
}
