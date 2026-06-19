//! 12-factor config: TOML file layered under env vars (`FIDES_` prefix).
//! Container/Compose injects everything via env; the TOML is for local dev.

use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub database_url: String,
    #[serde(default = "default_runtime_addr")]
    pub runtime_addr: String, // :8080 — tenant runtime surface
    #[serde(default = "default_admin_addr")]
    pub admin_addr: String, // :8081 — admin surface (network-fenced)
    #[serde(default = "default_max_connections")]
    pub db_max_connections: u32,
    #[serde(default = "default_log_format")]
    pub log_format: String, // "json" | "pretty"
    /// OTLP/HTTP base endpoint (e.g. http://jaeger:4318). None disables span export.
    #[serde(default)]
    pub otel_endpoint: Option<String>,
    #[serde(default = "default_service_name")]
    pub service_name: String,
}

fn default_runtime_addr() -> String {
    "0.0.0.0:8080".into()
}
fn default_admin_addr() -> String {
    "0.0.0.0:8081".into()
}
fn default_max_connections() -> u32 {
    20
}
fn default_log_format() -> String {
    "json".into()
}
fn default_service_name() -> String {
    "fides".into()
}

impl Config {
    /// Load from `config.toml` (optional) then override with `FIDES_*` env vars.
    pub fn load() -> Result<Self, Box<figment::Error>> {
        Figment::new()
            .merge(Toml::file("config.toml"))
            .merge(Env::prefixed("FIDES_"))
            .extract()
            .map_err(Box::new)
    }
}
