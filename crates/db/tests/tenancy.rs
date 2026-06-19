//! P0 exit criterion: a cross-tenant read is DENIED by Row-Level Security.
//! Spins a real Postgres in Docker (testcontainers) — matches prod engine exactly.
//! Requires a running Docker daemon.

use fides_db::{connect_admin, connect_app, migrate, set_tenant};
use fides_shared::TenantId;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use uuid::Uuid;

#[tokio::test]
async fn rls_blocks_cross_tenant_reads() {
    // Pin to PG 18 — matches prod, and gives built-in gen_random_uuid().
    let pg = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .expect("start postgres");
    let port = pg.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    // migrate as admin, then run all traffic through the RLS-bound app role.
    let admin = connect_admin(&url).await.expect("admin pool");
    migrate(&admin).await.expect("migrate");
    admin.close().await;

    let pool = connect_app(&url, 5).await.expect("app pool");

    // tenants table is platform-level (no RLS) — create two.
    let a: Uuid =
        sqlx::query_scalar("INSERT INTO tenants(name,currency) VALUES('A','USD') RETURNING id")
            .fetch_one(&pool)
            .await
            .expect("tenant a");
    let b: Uuid =
        sqlx::query_scalar("INSERT INTO tenants(name,currency) VALUES('B','EUR') RETURNING id")
            .fetch_one(&pool)
            .await
            .expect("tenant b");

    // insert a customer for tenant A inside a tenant-scoped txn.
    let mut tx = pool.begin().await.unwrap();
    set_tenant(&mut tx, TenantId::new(a)).await.unwrap();
    sqlx::query("INSERT INTO customers(tenant_id, external_id) VALUES($1, 'ext-1')")
        .bind(a)
        .execute(&mut *tx)
        .await
        .expect("insert customer");
    tx.commit().await.unwrap();

    // tenant B must NOT see tenant A's row.
    let mut tx = pool.begin().await.unwrap();
    set_tenant(&mut tx, TenantId::new(b)).await.unwrap();
    let seen_by_b: i64 = sqlx::query_scalar("SELECT count(*) FROM customers")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(seen_by_b, 0, "RLS leaked: tenant B saw tenant A rows");

    // tenant A sees exactly its own row.
    let mut tx = pool.begin().await.unwrap();
    set_tenant(&mut tx, TenantId::new(a)).await.unwrap();
    let seen_by_a: i64 = sqlx::query_scalar("SELECT count(*) FROM customers")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(seen_by_a, 1, "tenant A should see its own row");
}
