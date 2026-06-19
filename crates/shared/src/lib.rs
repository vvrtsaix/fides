//! Cross-cutting types shared by every binary: config, errors, telemetry, tenant context.

pub mod config;
pub mod error;
pub mod telemetry;
pub mod tenant;

pub use error::{AppError, AppResult};
pub use tenant::TenantId;
