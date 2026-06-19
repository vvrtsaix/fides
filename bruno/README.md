# fides — Bruno collection

Open this folder in [Bruno](https://www.usebruno.com/), pick the **Local** environment.

## Layout

- `Runtime/` — the `:8080` customer-facing surface (balance, transactions, locks, redeem, events…).
- `Admin/` — the `:8081` management surface, one folder per entity, each with full CRUD:
  `Create · List · Get · Update · Delete`.
  - `Tenants/` is platform-level (`/platform/tenants`, no `X-Tenant-Id`).
  - `Customers/`, `Tiers/`, `Earning Rules/`, `Campaigns/`, `Rewards/`,
    `Webhook Subscriptions/`, `Segments/` are tenant-scoped (`/admin/*`, require `X-Tenant-Id`).
  - `Segments/` also has membership requests: `List/Add/Remove Member`.
    `Customers/Segments` is the reverse lookup (which segments a customer is in).

## Env vars (Local)

`runtime`, `admin`, `tenant_id`, `external_id`, and the per-entity ids
(`tier_id`, `rule_id`, `campaign_id`, `reward_id`, `subscription_id`, `lock_id`, `voucher_code`).
Each **Create** captures its new id into the matching var via a post-response script, so
Get/Update/Delete in the same folder just work after you run Create.

## Suggested first run

1. **Admin → Tenants → Create** — saves `tenant_id`.
2. **Admin → Campaigns → Create** → **Rewards → Create** — saves `campaign_id`, `reward_id`.
3. **Runtime → Post Transaction** (`EARN`) → **Get Balance**.
4. **Runtime → Redeem Reward** → **Use Voucher**.

## Conventions

- `/v1/*` and `/admin/*` need `X-Tenant-Id`; `/platform/*` and `/healthz` don't.
- Write endpoints (transactions, locks, redeem, events) need an `Idempotency-Key` header.
- **Update is partial** (PATCH): omit a field to leave it unchanged.
- **Delete is soft**: entities flip `active=false`; a tenant flips `status=SUSPENDED`.
- Money fields (`budget_cap`, `reward_value`) are decimal **strings**.
