//! P5 webhook exit criteria: emit fans out to matching subscriptions; a 2xx marks DELIVERED;
//! failures back off and flag FAILED after MAX_ATTEMPTS. Sender is mocked (no real HTTP).
//! Needs a running Docker daemon.

use fides_core::webhook::MAX_ATTEMPTS;
use fides_db::{
    connect_admin, connect_app, connect_worker, create_subscription, dispatch_pending, emit,
    set_tenant, SendReq,
};
use fides_shared::TenantId;
use serde_json::json;
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use uuid::Uuid;

async fn setup() -> (PgPool, PgPool, TenantId, impl Sized) {
    let pg = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .unwrap();
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let admin = connect_admin(&url).await.unwrap();
    fides_db::migrate(&admin).await.unwrap();
    admin.close().await;
    let app = connect_app(&url, 8).await.unwrap();
    let worker = connect_worker(&url, 4).await.unwrap();
    let tid: Uuid =
        sqlx::query_scalar("INSERT INTO tenants(name,currency) VALUES('acme','USD') RETURNING id")
            .fetch_one(&app)
            .await
            .unwrap();
    (app, worker, TenantId::new(tid), pg)
}

async fn count(pool: &PgPool, tenant: TenantId, sql: &str) -> i64 {
    let mut tx = pool.begin().await.unwrap();
    set_tenant(&mut tx, tenant).await.unwrap();
    let c: i64 = sqlx::query_scalar(sql).fetch_one(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    c
}

#[tokio::test]
async fn emit_fans_out_and_delivers_on_2xx() {
    let (app, worker, tenant, _pg) = setup().await;
    create_subscription(&app, tenant, "http://a", "customer.tier_upgraded", "s1")
        .await
        .unwrap();
    create_subscription(&app, tenant, "http://b", "*", "s2")
        .await
        .unwrap();

    // matching event → exact + wildcard = 2 outbox rows
    let n = emit(
        &app,
        tenant,
        "customer.tier_upgraded",
        &json!({"tier": "gold"}),
    )
    .await
    .unwrap();
    assert_eq!(n, 2);
    // unrelated event → only the wildcard
    let n2 = emit(&app, tenant, "order.created", &json!({}))
        .await
        .unwrap();
    assert_eq!(n2, 1);

    // deliver all with a 200-returning sender
    let dispatched = dispatch_pending(
        &worker,
        |_req: SendReq| async { Ok::<u16, String>(200) },
        50,
    )
    .await
    .unwrap();
    assert_eq!(dispatched, 3);
    assert_eq!(
        count(
            &app,
            tenant,
            "SELECT count(*) FROM webhook_logs WHERE status='DELIVERED'"
        )
        .await,
        3
    );
}

#[tokio::test]
async fn failures_back_off_then_flag_failed() {
    let (app, worker, tenant, _pg) = setup().await;
    create_subscription(&app, tenant, "http://dead", "ping", "s")
        .await
        .unwrap();
    emit(&app, tenant, "ping", &json!({})).await.unwrap();

    // Drive MAX_ATTEMPTS failing deliveries. Reset next_retry_at before each so we don't wait on
    // backoff between attempts.
    for _ in 0..MAX_ATTEMPTS {
        sqlx::query("UPDATE webhook_logs SET next_retry_at = now() WHERE status='PENDING'")
            .execute(&worker)
            .await
            .unwrap();
        dispatch_pending(&worker, |_r: SendReq| async { Ok::<u16, String>(500) }, 10)
            .await
            .unwrap();
    }

    assert_eq!(
        count(
            &app,
            tenant,
            "SELECT count(*) FROM webhook_logs WHERE status='FAILED'"
        )
        .await,
        1,
        "webhook must be FAILED after MAX_ATTEMPTS"
    );
}
