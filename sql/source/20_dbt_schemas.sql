-- ===========================================================================
-- commerce_db : schemas dbt materialises into
-- ===========================================================================
-- Unlike `raw`, these layers are where quality is ENFORCED. dbt's staging
-- models reject or quarantine bad rows and the analytics models aggregate only
-- what survived, so the dbt tests (generic + singular) can legitimately expect
-- not_null / unique / accepted_values to hold here.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

COMMENT ON SCHEMA staging IS
    'dbt staging layer: typed, cleaned, deduplicated. Quality rules apply.';
COMMENT ON SCHEMA analytics IS
    'dbt analytics layer: business aggregates built only from accepted rows.';

-- dbt also needs a place for its own test failures to be stored so a failing
-- test can be inspected rather than only counted.
CREATE SCHEMA IF NOT EXISTS dbt_test_failures;

COMMENT ON SCHEMA dbt_test_failures IS
    'Rows that failed a dbt test, written by `dbt test --store-failures`.';
