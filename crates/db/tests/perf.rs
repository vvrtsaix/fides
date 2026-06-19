//! P6: confirm the `<50ms` balance read path has an index to use (FR-1.2 / NFR perf). We force
//! `enable_seqscan = off` and assert the planner can satisfy the lookup via an index — a
//! regression guard against a dropped/renamed index. Needs a running Docker daemon.

use chrono::Utc;
use fides_core::TxnType;
use fides_db::{connect_admin, connect_app, migrate, post_ledger_txn, set_tenant, PostTxn};
use fides_shared::TenantId;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use uuid::Uuid;

#[tokio::test]
async fn balance_read_uses_an_index() {
    let pg = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .unwrap();
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let admin = connect_admin(&url).await.unwrap();
    migrate(&admin).await.unwrap();
    admin.close().await;
    let app = connect_app(&url, 4).await.unwrap();
    let tid: Uuid =
        sqlx::query_scalar("INSERT INTO tenants(name,currency) VALUES('acme','USD') RETURNING id")
            .fetch_one(&app)
            .await
            .unwrap();
    let tenant = TenantId::new(tid);

    post_ledger_txn(
        &app,
        tenant,
        PostTxn {
            external_id: "c1".into(),
            txn_type: TxnType::Earn,
            amount: 10,
            expires_at: None,
            idempotency_key: format!("seed-{}", Utc::now().timestamp_nanos_opt().unwrap()),
            request_fingerprint: "seed".into(),
            source_event_id: None,
        },
    )
    .await
    .unwrap();

    let mut tx = app.begin().await.unwrap();
    set_tenant(&mut tx, tenant).await.unwrap();
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .unwrap();
    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT b.spendable_balance FROM customer_balances b
         JOIN customers c ON c.id = b.customer_id WHERE c.external_id = 'c1'",
    )
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();

    let plan = plan.join("\n");
    assert!(
        plan.contains("Index"),
        "balance lookup should use an index, got plan:\n{plan}"
    );
}
