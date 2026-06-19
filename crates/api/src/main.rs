//! `api` binary — ONE process, TWO listeners:
//!   :8080 runtime  (tenant-scoped, X-Tenant-Id required)
//!   :8081 admin     (tenant-admin + platform-admin; network-fenced)
//! See IMPLEMENTATION_PLAN.md §2b / §7.

mod http_error;
mod routes;
mod state;
mod tenant_mw;

use fides_shared::{config::Config, telemetry};
use state::AppState;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load()?;
    telemetry::init(
        &cfg.log_format,
        cfg.otel_endpoint.as_deref(),
        &cfg.service_name,
    );

    // Migrate as admin, then drop the admin pool; runtime traffic uses the RLS-bound app role.
    let admin_pool = fides_db::connect_admin(&cfg.database_url).await?;
    fides_db::migrate(&admin_pool).await?;
    admin_pool.close().await;
    tracing::info!("migrations applied");

    let pool = fides_db::connect_app(&cfg.database_url, cfg.db_max_connections).await?;
    let state = AppState { pool };

    let runtime = routes::runtime_router(state.clone());
    let admin = routes::admin_router(state.clone());

    let runtime_listener = TcpListener::bind(&cfg.runtime_addr).await?;
    let admin_listener = TcpListener::bind(&cfg.admin_addr).await?;
    tracing::info!(runtime = %cfg.runtime_addr, admin = %cfg.admin_addr, "api listening");

    // Both servers in one process / one runtime — not two deployments.
    let runtime_srv = axum::serve(runtime_listener, runtime);
    let admin_srv = axum::serve(admin_listener, admin);

    tokio::try_join!(
        async { runtime_srv.await.map_err(anyhow::Error::from) },
        async { admin_srv.await.map_err(anyhow::Error::from) },
    )?;
    Ok(())
}
