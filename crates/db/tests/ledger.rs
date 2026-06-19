//! P1 exit criteria:
//!   * concurrent EARNs hold the invariant `cache == SUM(ledger)`,
//!   * a duplicate Idempotency-Key mints exactly once,
//!   * a reused key with a different payload is rejected,
//!   * crossing a tier threshold sets current_tier_id.
//!
//! Needs a running Docker daemon.

use fides_core::TxnType;
use fides_db::{
    connect_admin, connect_app, create_subscription, create_tier, get_balance, migrate,
    post_ledger_txn, set_tenant, PostTxn,
};
use fides_shared::{AppError, TenantId};
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
        .expect("start postgres");
    let port = pg.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let admin = connect_admin(&url).await.unwrap();
    migrate(&admin).await.unwrap();
    admin.close().await;

    let pool = connect_app(&url, 16).await.unwrap();
    let tenant_id: Uuid =
        sqlx::query_scalar("INSERT INTO tenants(name,currency) VALUES('acme','USD') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
    // keep the container alive for the test's lifetime
    (pool, TenantId::new(tenant_id), pg)
}

fn earn(external_id: &str, amount: i64, key: &str) -> PostTxn {
    PostTxn {
        external_id: external_id.into(),
        txn_type: TxnType::Earn,
        amount,
        expires_at: None,
        idempotency_key: key.into(),
        request_fingerprint: format!("{external_id}|EARN|{amount}|None"),
        source_event_id: None,
    }
}

#[tokio::test]
async fn concurrent_earns_keep_cache_consistent_and_set_tier() {
    let (pool, tenant, _pg) = setup().await;

    // tier unlocks at 50 lifetime points.
    create_tier(&pool, tenant, "silver", 50).await.unwrap();

    // 20 concurrent EARNs of +10 to the same customer, each with a distinct key.
    let mut handles = Vec::new();
    for i in 0..20 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            post_ledger_txn(&pool, tenant, earn("cust-1", 10, &format!("k-{i}")))
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let bal = get_balance(&pool, tenant, "cust-1").await.unwrap().unwrap();
    assert_eq!(bal.spendable_balance, 200, "20 x 10 must land exactly");
    assert_eq!(bal.lifetime_earned, 200);

    // cache == SUM(ledger)
    let mut tx = pool.begin().await.unwrap();
    set_tenant(&mut tx, tenant).await.unwrap();
    let ledger_sum: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(pl.amount),0)::bigint FROM points_ledger pl
         JOIN customers c ON c.id = pl.customer_id WHERE c.external_id = 'cust-1'",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let tier: Option<Uuid> =
        sqlx::query_scalar("SELECT current_tier_id FROM customers WHERE external_id = 'cust-1'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(
        ledger_sum, bal.spendable_balance,
        "invariant: cache == SUM(ledger)"
    );
    assert!(tier.is_some(), "tier breach must set current_tier_id");
}

#[tokio::test]
async fn tier_upgrade_enqueues_webhook_and_config_create_is_audited() {
    let (pool, tenant, _pg) = setup().await;
    create_subscription(&pool, tenant, "http://x", "customer.tier_upgraded", "s")
        .await
        .unwrap();
    create_tier(&pool, tenant, "silver", 50).await.unwrap();

    // EARN 60 crosses the 50 threshold → upgrade → outbox row enqueued in the same txn.
    post_ledger_txn(&pool, tenant, earn("c1", 60, "k1"))
        .await
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    set_tenant(&mut tx, tenant).await.unwrap();
    let hooks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM webhook_logs WHERE event_type = 'customer.tier_upgraded'",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let tier_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_logs WHERE entity='tier' AND action='create'",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(hooks, 1, "tier upgrade must enqueue a webhook");
    assert_eq!(tier_audits, 1, "tier creation must be audited");
}

#[tokio::test]
async fn idempotency_mints_once_and_rejects_payload_change() {
    let (pool, tenant, _pg) = setup().await;

    let first = post_ledger_txn(&pool, tenant, earn("cust-2", 30, "dup"))
        .await
        .unwrap();
    assert!(!first.replayed);
    assert_eq!(first.spendable_balance(), 30);

    // same key, same payload → replay, no second mint.
    let again = post_ledger_txn(&pool, tenant, earn("cust-2", 30, "dup"))
        .await
        .unwrap();
    assert!(again.replayed, "duplicate key must replay");
    assert_eq!(again.spendable_balance(), 30, "must not double-mint");

    // exactly one ledger row exists.
    let mut tx = pool.begin().await.unwrap();
    set_tenant(&mut tx, tenant).await.unwrap();
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM points_ledger pl JOIN customers c ON c.id = pl.customer_id
         WHERE c.external_id = 'cust-2'",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(rows, 1);

    // same key, different amount → conflict.
    let err = post_ledger_txn(&pool, tenant, earn("cust-2", 99, "dup"))
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::IdempotencyConflict));
}

// small ergonomic helper for the assertions above
trait BalanceExt {
    fn spendable_balance(&self) -> i64;
}
impl BalanceExt for fides_db::TxnOutcome {
    fn spendable_balance(&self) -> i64 {
        self.balance.spendable_balance
    }
}
