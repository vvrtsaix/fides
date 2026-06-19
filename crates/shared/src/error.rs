//! Unified error type. HTTP mapping lives in the `api` crate to keep `shared` transport-agnostic.

use thiserror::Error;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("missing or invalid X-Tenant-Id header")]
    MissingTenant,

    #[error("unknown tenant")]
    UnknownTenant,

    #[error("missing Idempotency-Key header")]
    MissingIdempotencyKey,

    /// Same key replayed with a different request body (constraint #6).
    #[error("idempotency key reused with a different payload")]
    IdempotencyConflict,

    #[error("not found")]
    NotFound,

    #[error("validation: {0}")]
    Validation(String),

    #[error("internal error: {0}")]
    Internal(String),
}
