//! P2 exit criteria: an ingested event that matches a rule mints points end-to-end; a
//! non-matching event is PROCESSED with no mint; duplicate ingestion dedupes.
//! Needs a running Docker daemon.

use fides_db::{
    connect_admin, connect_app, connect_worker, create_rule, get_balance, ingest, migrate,
    process_pending, set_tenant,
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
        .expect("start postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let admin = connect_admin(&url).await.unwrap();
    migrate(&admin).await.unwrap();
    admin.close().await;

    let app = connect_app(&url, 8).await.unwrap();
    let worker = connect_worker(&url, 4).await.unwrap();
    let tenant_id: Uuid =
        sqlx::query_scalar("INSERT INTO tenants(name,currency) VALUES('acme','USD') RETURNING id")
            .fetch_one(&app)
            .await
            .unwrap();
    (app, worker, TenantId::new(tenant_id), pg)
}

#[tokio::test]
async fn matching_event_mints_and_nonmatching_is_noop() {
    let (app, worker, tenant, _pg) = setup().await;

    // rule: purchase >= 100 cents earns 50 points.
    create_rule(
        &app,
        tenant,
        "purchase",
        &json!({"field": "amount_cents", "op": ">=", "value": 100}),
        50,
        None,
    )
    .await
    .unwrap();

    // matching event
    let (_id, created) = ingest(
        &app,
        tenant,
        "cust-1",
        "purchase",
        &json!({"amount_cents": 250}),
        "evt-1",
    )
    .await
    .unwrap();
    assert!(created);

    // duplicate ingestion → same event, not created again
    let (_id2, created2) = ingest(
        &app,
        tenant,
        "cust-1",
        "purchase",
        &json!({"amount_cents": 250}),
        "evt-1",
    )
    .await
    .unwrap();
    assert!(!created2, "duplicate idempotency key must dedupe ingestion");

    // non-matching event (below threshold)
    ingest(
        &app,
        tenant,
        "cust-1",
        "purchase",
        &json!({"amount_cents": 50}),
        "evt-2",
    )
    .await
    .unwrap();

    // process the queue
    let handled = process_pending(&worker, 100).await.unwrap();
    assert_eq!(handled, 2, "two distinct events should be processed");

    // only the matching event minted 50 points
    let bal = get_balance(&app, tenant, "cust-1").await.unwrap().unwrap();
    assert_eq!(bal.spendable_balance, 50);
    assert_eq!(bal.lifetime_earned, 50);

    // statuses are terminal
    let mut tx = app.begin().await.unwrap();
    set_tenant(&mut tx, tenant).await.unwrap();
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM loyalty_events WHERE status = 'PENDING'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    let processed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM loyalty_events WHERE status = 'PROCESSED'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(pending, 0);
    assert_eq!(processed, 2);

    // re-processing is idempotent (no second mint even if an event were re-queued)
    let again = process_pending(&worker, 100).await.unwrap();
    assert_eq!(again, 0, "queue is drained");
}
