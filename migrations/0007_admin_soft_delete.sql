-- Admin CRUD: give tiers and rewards a soft-delete flag so DELETE is uniform
-- across all admin entities (campaigns, earning_rules, webhook_subscriptions
-- already have `active`; tenants uses `status`).
--
-- ponytail: column + list-filter only. Redemption (rewards) and tier assignment
-- do NOT yet check `active` — a soft-deleted reward can still be redeemed until
-- that guard is added. Enforce in the redeem/assign paths when it matters.
ALTER TABLE tiers   ADD COLUMN active boolean NOT NULL DEFAULT true;
ALTER TABLE rewards ADD COLUMN active boolean NOT NULL DEFAULT true;
