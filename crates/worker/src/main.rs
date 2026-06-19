//! `worker` binary — async background processes (separate process, same DB, same image).
//! P1+ fills these in: event processor (SELECT ... FOR UPDATE SKIP LOCKED), webhook
//! dispatcher (outbox + backoff), lock sweeper (15min TTL / 30s poll), expiration job.
//! P0 is a healthy poll loop that proves config/telemetry/DB wiring.

use fides_shared::{config::Config, telemetry};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load()?;
    telemetry::init(
        &cfg.log_format,
        cfg.otel_endpoint.as_deref(),
        &cfg.service_name,
    );

    // sqlx takes an advisory lock during migrate(), so api + worker racing on boot is safe.
    let admin_pool = fides_db::connect_admin(&cfg.database_url).await?;
    fides_db::migrate(&admin_pool).await?;
    admin_pool.close().await;

    // BYPASSRLS role: the worker processes events across all tenants (FR-2.*).
    let pool = fides_db::connect_worker(&cfg.database_url, cfg.db_max_connections).await?;
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    tracing::info!("worker started");

    // Event processing polls fast (500ms); the lock sweeper + expiration run on the 30s tick (§9).
    let mut event_tick = tokio::time::interval(Duration::from_millis(500));
    let mut sweep_tick = tokio::time::interval(Duration::from_secs(30));
    let mut webhook_tick = tokio::time::interval(Duration::from_secs(2));
    // Dynamic-segment membership reconcile (eventually-consistent).
    let mut segment_tick = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = event_tick.tick() => {
                match fides_db::process_pending(&pool, 100).await {
                    Ok(n) if n > 0 => tracing::debug!(processed = n, "events processed"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "event batch failed"),
                }
            }
            _ = sweep_tick.tick() => {
                match fides_db::release_expired(&pool, 500).await {
                    Ok(n) if n > 0 => tracing::info!(released = n, "expired locks released"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "lock sweep failed"),
                }
                match fides_db::expire_due(&pool, 500).await {
                    Ok(n) if n > 0 => tracing::info!(expired = n, "points expired"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "expiration sweep failed"),
                }
            }
            _ = webhook_tick.tick() => {
                let http = http.clone();
                let send = move |req: fides_db::SendReq| {
                    let http = http.clone();
                    async move {
                        http.post(&req.url)
                            .header("X-Loyalty-Signature", req.signature)
                            .header("content-type", "application/json")
                            .body(req.body)
                            .send()
                            .await
                            .map(|r| r.status().as_u16())
                            .map_err(|e| e.to_string())
                    }
                };
                match fides_db::dispatch_pending(&pool, send, 200).await {
                    Ok(n) if n > 0 => tracing::debug!(dispatched = n, "webhooks dispatched"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "webhook dispatch failed"),
                }
            }
            _ = segment_tick.tick() => {
                match fides_db::reconcile_segments(&pool).await {
                    Ok(n) if n > 0 => tracing::info!(changed = n, "segments reconciled"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "segment reconcile failed"),
                }
            }
        }
    }
}
