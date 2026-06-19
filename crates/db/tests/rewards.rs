//! P4 exit criteria: redemption burns points + mints a voucher; out-of-stock and over-budget are
//! rejected; two concurrent claims on one unit yield exactly one winner; idempotent replay does
//! not double-issue. Needs a running Docker daemon.

use chrono::Utc;
use fides_core::TxnType;
use fides_db::{
    connect_admin, connect_app, create_campaign, create_reward, get_balance, migrate,
    post_ledger_txn, redeem_reward, set_tenant, use_voucher, NewReward, PostTxn,
};
use fides_shared::{AppError, TenantId};
use rust_decimal::Decimal;
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
    let app = connect_app(&url, 12).await.unwrap();
    let tid: Uuid =
        sqlx::query_scalar("INSERT INTO tenants(name,currency) VALUES('acme','USD') RETURNING id")
            .fetch_one(&app)
            .await
            .unwrap();
    (app, TenantId::new(tid), pg)
}

async fn earn(app: &PgPool, tenant: TenantId, ext: &str, amount: i64) {
    post_ledger_txn(
        app,
        tenant,
        PostTxn {
            external_id: ext.into(),
            txn_type: TxnType::Earn,
            amount,
            expires_at: None,
            idempotency_key: format!("seed-{ext}-{}", Utc::now().timestamp_nanos_opt().unwrap()),
            request_fingerprint: "seed".into(),
            source_event_id: None,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn redeem_mints_voucher_and_enforces_stock_budget_idempotency() {
    let (app, tenant, _pg) = setup().await;
    earn(&app, tenant, "c1", 1000).await;

    let campaign = create_campaign(&app, tenant, "summer", Decimal::from(100))
        .await
        .unwrap();
    let reward = create_reward(
        &app,
        tenant,
        NewReward {
            campaign_id: campaign,
            name: "gift card",
            cost_points: 200,
            reward_value: Decimal::from(100),
            available_stock: 1,
            valid_days: 30,
        },
    )
    .await
    .unwrap();

    // successful redemption
    let out = redeem_reward(&app, tenant, "c1", reward, "k1")
        .await
        .unwrap();
    assert!(!out.voucher_code.is_empty());
    assert_eq!(out.spendable_balance, 800);

    let b = get_balance(&app, tenant, "c1").await.unwrap().unwrap();
    assert_eq!((b.spendable_balance, b.lifetime_redeemed), (800, 200));

    // budget now at cap (100/100), stock now 0
    let (stock, spend): (i32, Decimal) = {
        let mut tx = app.begin().await.unwrap();
        set_tenant(&mut tx, tenant).await.unwrap();
        let s: i32 = sqlx::query_scalar("SELECT available_stock FROM rewards WHERE id=$1")
            .bind(reward)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let sp: Decimal = sqlx::query_scalar("SELECT current_spend FROM campaigns WHERE id=$1")
            .bind(campaign)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        (s, sp)
    };
    assert_eq!(stock, 0);
    assert_eq!(spend, Decimal::from(100));

    // out of stock
    let err = redeem_reward(&app, tenant, "c1", reward, "k2")
        .await
        .unwrap_err();
    assert!(
        matches!(err, AppError::Validation(_)),
        "expected out-of-stock"
    );

    // idempotent replay of the original → same voucher, no second issue
    let replay = redeem_reward(&app, tenant, "c1", reward, "k1")
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.voucher_code, out.voucher_code);
    let voucher_count: i64 = {
        let mut tx = app.begin().await.unwrap();
        set_tenant(&mut tx, tenant).await.unwrap();
        let c = sqlx::query_scalar("SELECT count(*) FROM vouchers")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        c
    };
    assert_eq!(
        voucher_count, 1,
        "idempotent replay must not mint a second voucher"
    );

    // over budget: a reward whose value would exceed the cap is rejected
    let pricey = create_reward(
        &app,
        tenant,
        NewReward {
            campaign_id: campaign,
            name: "too pricey",
            cost_points: 100,
            reward_value: Decimal::from(50),
            available_stock: 5,
            valid_days: 30,
        },
    )
    .await
    .unwrap();
    let err = redeem_reward(&app, tenant, "c1", pricey, "k3")
        .await
        .unwrap_err();
    assert!(
        matches!(err, AppError::Validation(_)),
        "expected budget exceeded"
    );
}

#[tokio::test]
async fn voucher_can_be_used_once() {
    let (app, tenant, _pg) = setup().await;
    earn(&app, tenant, "c1", 1000).await;
    let campaign = create_campaign(&app, tenant, "c", Decimal::from(1000))
        .await
        .unwrap();
    let reward = create_reward(
        &app,
        tenant,
        NewReward {
            campaign_id: campaign,
            name: "v",
            cost_points: 100,
            reward_value: Decimal::from(0),
            available_stock: 1,
            valid_days: 30,
        },
    )
    .await
    .unwrap();
    let out = redeem_reward(&app, tenant, "c1", reward, "k1")
        .await
        .unwrap();

    use_voucher(&app, tenant, &out.voucher_code).await.unwrap();
    // second use is rejected
    let err = use_voucher(&app, tenant, &out.voucher_code)
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Validation(_)));
}

#[tokio::test]
async fn concurrent_claims_on_one_unit_have_exactly_one_winner() {
    let (app, tenant, _pg) = setup().await;
    earn(&app, tenant, "c1", 1000).await;
    let campaign = create_campaign(&app, tenant, "flash", Decimal::from(1000))
        .await
        .unwrap();
    let reward = create_reward(
        &app,
        tenant,
        NewReward {
            campaign_id: campaign,
            name: "single voucher",
            cost_points: 100,
            reward_value: Decimal::from(0),
            available_stock: 1,
            valid_days: 30,
        },
    )
    .await
    .unwrap();

    let a = {
        let app = app.clone();
        tokio::spawn(async move { redeem_reward(&app, tenant, "c1", reward, "ka").await })
    };
    let b = {
        let app = app.clone();
        tokio::spawn(async move { redeem_reward(&app, tenant, "c1", reward, "kb").await })
    };
    let (ra, rb) = (a.await.unwrap(), b.await.unwrap());
    let wins = [ra.is_ok(), rb.is_ok()].iter().filter(|x| **x).count();
    assert_eq!(wins, 1, "exactly one concurrent claim wins the single unit");
}
