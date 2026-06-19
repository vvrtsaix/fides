//! P5 audit/anonymize exit criteria: anonymize zeroes identity but keeps the ledger, writes an
//! audit row, and the audit trail is immutable (no UPDATE privilege). Needs a running Docker daemon.

use chrono::Utc;
use fides_core::TxnType;
use fides_db::{
    anonymize, connect_admin, connect_app, get_balance, migrate, post_ledger_txn, set_tenant,
    PostTxn,
};
use fides_shared::TenantId;
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use uuid::Uuid;

async fn setup() -> (PgPool, TenantId, impl Sized) {
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
    let app = connect_app(&url, 6).await.unwrap();
    let tid: Uuid =
        sqlx::query_scalar("INSERT INTO tenants(name,currency) VALUES('acme','USD') RETURNING id")
            .fetch_one(&app)
            .await
            .unwrap();
    (app, TenantId::new(tid), pg)
}

#[tokio::test]
async fn anonymize_keeps_ledger_audits_change_and_trail_is_immutable() {
    let (app, tenant, _pg) = setup().await;

    // create a customer with identity + some ledger history
    post_ledger_txn(
        &app,
        tenant,
        PostTxn {
            external_id: "c1".into(),
            txn_type: TxnType::Earn,
            amount: 100,
            expires_at: None,
            idempotency_key: format!("seed-{}", Utc::now().timestamp_nanos_opt().unwrap()),
            request_fingerprint: "seed".into(),
            source_event_id: None,
        },
    )
    .await
    .unwrap();
    {
        let mut tx = app.begin().await.unwrap();
        set_tenant(&mut tx, tenant).await.unwrap();
        sqlx::query("UPDATE customers SET email='a@b.com', phone='123' WHERE external_id='c1'")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let ok = anonymize(&app, tenant, "c1", "system").await.unwrap();
    assert!(ok);

    let mut tx = app.begin().await.unwrap();
    set_tenant(&mut tx, tenant).await.unwrap();
    let (email, phone, status): (Option<String>, Option<String>, String) =
        sqlx::query_as("SELECT email, phone, status FROM customers WHERE external_id='c1'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    let audit_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_logs WHERE entity='customer' AND action='anonymize'",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(
        (email, phone, status),
        (None, None, "ANONYMIZED".to_string())
    );
    assert_eq!(audit_rows, 1, "anonymize must be audited");

    // ledger preserved (financial integrity)
    let bal = get_balance(&app, tenant, "c1").await.unwrap().unwrap();
    assert_eq!(
        bal.spendable_balance, 100,
        "ledger/balance must survive anonymization"
    );

    // audit trail is read-only: UPDATE must be denied to the app role
    let upd = sqlx::query("UPDATE audit_logs SET actor='tamper'")
        .execute(&app)
        .await;
    assert!(
        upd.is_err(),
        "audit_logs must reject UPDATE (immutable trail)"
    );
}
