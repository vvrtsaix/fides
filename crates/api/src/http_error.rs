//! Maps the transport-agnostic `AppError` onto HTTP responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use fides_shared::AppError;
use serde_json::json;

pub struct ApiError(pub AppError);

impl From<AppError> for ApiError {
    fn from(e: AppError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code) = match &self.0 {
            AppError::MissingTenant => (StatusCode::BAD_REQUEST, "missing_tenant"),
            AppError::UnknownTenant => (StatusCode::NOT_FOUND, "unknown_tenant"),
            AppError::MissingIdempotencyKey => (StatusCode::BAD_REQUEST, "missing_idempotency_key"),
            AppError::IdempotencyConflict => (StatusCode::CONFLICT, "idempotency_conflict"),
            AppError::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            AppError::Validation(_) => (StatusCode::UNPROCESSABLE_ENTITY, "validation"),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self.0, "request failed");
        }
        (
            status,
            Json(json!({ "error": code, "message": self.0.to_string() })),
        )
            .into_response()
    }
}
