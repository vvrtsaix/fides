//! P3 exit criteria: hold/fulfill/release move the cache correctly and keep the invariant
//! `spendable + locked == SUM(ledger)`; FEFO drains soonest-expiring first; the sweeper expires
//! due points out of spendable. Needs a running Docker daemon.

use chrono::{Duration, Utc};
use fides_core::TxnType;
use fides_db::{
    connect_admin, connect_app, connect_worker, create_lock, expire_due, fulfill_lock, get_balance,
    migrate, post_ledger_txn, release_lock, set_tenant, PostTxn,
};
use fides_shared::TenantId;
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
    migrate(&admin).await.unwrap();
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

fn earn(external_id: &str, amount: i64, key: &str, days: Option<i64>) -> PostTxn {
    PostTxn {
        external_id: external_id.into(),
        txn_type: TxnType::Earn,
        amount,
        expires_at: days.map(|d| Utc::now() + Duration::days(d)),
        idempotency_key: key.into(),
        request_fingerprint: key.into(),
        source_event_id: None,
    }
}

async fn ledger_sum(app: &PgPool, tenant: TenantId, ext: &str) -> i64 {
    let mut tx = app.begin().await.unwrap();
    set_tenant(&mut tx, tenant).await.unwrap();
    let s: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(pl.amount),0)::bigint FROM points_ledger pl
         JOIN customers c ON c.id = pl.customer_id WHERE c.external_id = $1",
    )
    .bind(ext)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    s
}

#[tokio::test]
async fn lock_lifecycle_and_invariant() {
    let (app, _worker, tenant, _pg) = setup().await;
    post_ledger_txn(&app, tenant, earn("c1", 100, "e1", None))
        .await
        .unwrap();

    // hold 30
    let lock = create_lock(&app, tenant, "c1", 30, "h1").await.unwrap();
    let b = get_balance(&app, tenant, "c1").await.unwrap().unwrap();
    assert_eq!((b.spendable_balance, b.locked_balance), (70, 30));
    assert_eq!(
        b.spendable_balance + b.locked_balance,
        ledger_sum(&app, tenant, "c1").await
    );

    // fulfill → permanent redeem
    fulfill_lock(&app, tenant, lock).await.unwrap();
    let b = get_balance(&app, tenant, "c1").await.unwrap().unwrap();
    assert_eq!(
        (b.spendable_balance, b.locked_balance, b.lifetime_redeemed),
        (70, 0, 30)
    );
    assert_eq!(
        b.spendable_balance + b.locked_balance,
        ledger_sum(&app, tenant, "c1").await
    );

    // hold 20 then release → restored
    let lock2 = create_lock(&app, tenant, "c1", 20, "h2").await.unwrap();
    release_lock(&app, tenant, lock2).await.unwrap();
    let b = get_balance(&app, tenant, "c1").await.unwrap().unwrap();
    assert_eq!((b.spendable_balance, b.locked_balance), (70, 0));
    assert_eq!(
        b.spendable_balance + b.locked_balance,
        ledger_sum(&app, tenant, "c1").await
    );
}

#[tokio::test]
async fn create_lock_is_idempotent() {
    let (app, _worker, tenant, _pg) = setup().await;
    post_ledger_txn(&app, tenant, earn("c1", 100, "e1", None))
        .await
        .unwrap();

    let a = create_lock(&app, tenant, "c1", 30, "same-key")
        .await
        .unwrap();
    let b = create_lock(&app, tenant, "c1", 30, "same-key")
        .await
        .unwrap();
    assert_eq!(a, b, "same idempotency key returns the same lock");

    // only one hold actually happened (locked == 30, not 60)
    let bal = get_balance(&app, tenant, "c1").await.unwrap().unwrap();
    assert_eq!((bal.spendable_balance, bal.locked_balance), (70, 30));
}

#[tokio::test]
async fn fefo_consumes_soonest_expiring_first() {
    let (app, _worker, tenant, _pg) = setup().await;
    // batch A: 40 pts expiring in 10 days; batch B: 60 pts expiring in 1 day (sooner).
    post_ledger_txn(&app, tenant, earn("c2", 40, "a", Some(10)))
        .await
        .unwrap();
    post_ledger_txn(&app, tenant, earn("c2", 60, "b", Some(1)))
        .await
        .unwrap();

    // redeem 50 via hold+fulfill → FEFO takes from the 1-day batch first.
    let lock = create_lock(&app, tenant, "c2", 50, "h3").await.unwrap();
    fulfill_lock(&app, tenant, lock).await.unwrap();

    let mut tx = app.begin().await.unwrap();
    set_tenant(&mut tx, tenant).await.unwrap();
    // sooner batch (60) drained by 50 → 10 left; later batch (40) untouched.
    let soon: i64 = sqlx::query_scalar(
        "SELECT available_amount FROM points_ledger WHERE amount = 60 AND txn_type='EARN'",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let later: i64 = sqlx::query_scalar(
        "SELECT available_amount FROM points_ledger WHERE amount = 40 AND txn_type='EARN'",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(soon, 10, "soonest-expiring batch consumed first");
    assert_eq!(later, 40, "later batch untouched");
}

#[tokio::test]
async fn sweeper_expires_due_points() {
    let (app, worker, tenant, _pg) = setup().await;
    // points already expired (1 day ago)
    post_ledger_txn(&app, tenant, earn("c3", 25, "x", Some(-1)))
        .await
        .unwrap();
    let b = get_balance(&app, tenant, "c3").await.unwrap().unwrap();
    assert_eq!(b.spendable_balance, 25);

    let n = expire_due(&worker, 100).await.unwrap();
    assert_eq!(n, 1);

    let b = get_balance(&app, tenant, "c3").await.unwrap().unwrap();
    assert_eq!(b.spendable_balance, 0, "expired points leave spendable");
    assert_eq!(
        b.spendable_balance + b.locked_balance,
        ledger_sum(&app, tenant, "c3").await
    );
}
