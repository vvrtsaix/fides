-- Dynamic (rule-based) segments. `definition` holds a boolean-combinator DSL;
-- NULL = static segment (manual membership). When set, the worker owns the
-- segment's membership and reconciles it on a schedule.
--
-- DSL shape (validated in fides_db::segments before write):
--   leaf:        { "field": <whitelisted>, "op": <whitelisted>, "value": <typed> }
--   combinators: { "all": [node...] } | { "any": [node...] } | { "not": node }
ALTER TABLE segments ADD COLUMN definition jsonb;
