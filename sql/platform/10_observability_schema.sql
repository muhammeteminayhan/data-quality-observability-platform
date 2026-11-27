-- ===========================================================================
-- observability_db : the platform's own metadata store
-- ===========================================================================
-- Deliberately a SEPARATE database from commerce_db. The data being monitored
-- and the record of monitoring it have different owners, different retention
-- and a different blast radius; mixing them would mean a bad deploy of the
-- platform could damage the warehouse it is supposed to observe.
--
-- Unlike the raw layer, constraints here are wanted: this is platform metadata
-- written only by the platform, so an impossible row is a bug rather than a
-- data-quality signal to capture.
--
-- Correlation model
-- -----------------
-- `pipeline_runs` is the parent of everything: one row per execution of the
-- platform (an Airflow DAG run, or a manual CLI run). Every check, score, gate
-- decision and incident carries that run_id, so any number on a dashboard can
-- be traced back to the exact run that measured it.
-- ===========================================================================

SET client_min_messages = warning;

-- ===========================================================================
-- 1. datasets - the registry of what is monitored
-- ===========================================================================
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_name      text        NOT NULL UNIQUE,
    source_type       text        NOT NULL
        CHECK (source_type IN ('postgres', 'parquet')),
    layer             text        NOT NULL
        CHECK (layer IN ('raw', 'staging', 'analytics', 'lake')),
    physical_location text        NOT NULL,
    owner             text,
    criticality       text        NOT NULL DEFAULT 'medium'
        CHECK (criticality IN ('low', 'medium', 'high', 'critical')),
    description       text,
    is_active         boolean     NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE datasets IS
    'One row per monitored dataset. Populated from the contract files rather '
    'than hardcoded: adding contracts/<name>.yml registers a dataset.';
COMMENT ON COLUMN datasets.physical_location IS
    'schema.table for postgres datasets, the object-store prefix for parquet.';

-- ===========================================================================
-- 2. contract_versions - every version of every data contract
-- ===========================================================================
CREATE TABLE IF NOT EXISTS contract_versions (
    contract_version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id          bigint      NOT NULL REFERENCES datasets (dataset_id),
    version             text        NOT NULL,
    content_sha256      text        NOT NULL,
    content             jsonb       NOT NULL,
    is_active           boolean     NOT NULL DEFAULT true,
    first_seen_at       timestamptz NOT NULL DEFAULT now(),
    last_seen_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, version, content_sha256)
);

COMMENT ON TABLE contract_versions IS
    'Contract history. The checksum is taken over the normalised contract, so '
    'editing a contract without bumping its version still lands as a new row '
    'and cannot pass unnoticed.';

CREATE INDEX IF NOT EXISTS ix_contract_versions_dataset
    ON contract_versions (dataset_id, is_active);

-- ===========================================================================
-- 3. pipeline_runs - the correlation parent
-- ===========================================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           uuid        PRIMARY KEY,
    trigger          text        NOT NULL
        CHECK (trigger IN ('airflow', 'cli', 'test')),
    dag_id           text,
    dag_run_id       text,
    task_id          text,
    logical_date     date,
    fault_profile    text        NOT NULL DEFAULT 'clean',
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    status           text        NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'failed', 'blocked')),
    platform_version text,
    notes            text
);

COMMENT ON COLUMN pipeline_runs.fault_profile IS
    'Which fault profile the run executed under. "clean" means no faults were '
    'injected; anything else names the scenario or profile.';

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_started ON pipeline_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_logical ON pipeline_runs (logical_date DESC);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_dag_run ON pipeline_runs (dag_run_id);

-- ===========================================================================
-- 4. validation_runs - one Great Expectations checkpoint execution
-- ===========================================================================
CREATE TABLE IF NOT EXISTS validation_runs (
    validation_run_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id          bigint      NOT NULL REFERENCES datasets (dataset_id),
    contract_version_id bigint      REFERENCES contract_versions (contract_version_id),
    engine              text        NOT NULL DEFAULT 'great_expectations'
        CHECK (engine IN ('great_expectations', 'contract_engine', 'dbt')),
    engine_version      text,
    suite_name          text        NOT NULL,
    checkpoint_name     text,
    batch_identifier    text,
    started_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    duration_ms         integer,
    success             boolean,
    expectations_total  integer     NOT NULL DEFAULT 0,
    expectations_passed integer     NOT NULL DEFAULT 0,
    expectations_failed integer     NOT NULL DEFAULT 0,
    rows_evaluated      bigint,
    max_severity        text
        CHECK (max_severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    error_message       text
);

CREATE INDEX IF NOT EXISTS ix_validation_runs_run     ON validation_runs (run_id);
CREATE INDEX IF NOT EXISTS ix_validation_runs_dataset ON validation_runs (dataset_id, started_at DESC);

-- ===========================================================================
-- 5. validation_results - one row per expectation evaluated
-- ===========================================================================
CREATE TABLE IF NOT EXISTS validation_results (
    validation_result_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    validation_run_id    bigint      NOT NULL REFERENCES validation_runs (validation_run_id) ON DELETE CASCADE,
    run_id               uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id           bigint      NOT NULL REFERENCES datasets (dataset_id),
    rule_name            text        NOT NULL,
    expectation_type     text        NOT NULL,
    column_name          text,
    dimension            text        NOT NULL
        CHECK (dimension IN ('completeness', 'uniqueness', 'validity',
                             'consistency', 'freshness', 'volume')),
    severity             text        NOT NULL
        CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    success              boolean     NOT NULL,
    element_count        bigint,
    unexpected_count     bigint,
    unexpected_percent   numeric(9, 5),
    observed_value       jsonb,
    expected_value       jsonb,
    partial_unexpected   jsonb,
    kwargs               jsonb,
    evaluated_at         timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN validation_results.partial_unexpected IS
    'A bounded sample of offending values, never the full set: a failure has '
    'to be diagnosable without copying source data into the platform.';

CREATE INDEX IF NOT EXISTS ix_validation_results_run     ON validation_results (validation_run_id);
CREATE INDEX IF NOT EXISTS ix_validation_results_dataset ON validation_results (dataset_id, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS ix_validation_results_failed  ON validation_results (dataset_id, success)
    WHERE success = false;

-- ===========================================================================
-- 6. dbt_runs - one dbt invocation
-- ===========================================================================
CREATE TABLE IF NOT EXISTS dbt_runs (
    dbt_run_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id             uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    invocation_id      text        NOT NULL UNIQUE,
    command            text        NOT NULL,
    dbt_version        text,
    dbt_schema_version text,
    started_at         timestamptz,
    finished_at        timestamptz,
    elapsed_seconds    numeric(10, 3),
    success            boolean,
    models_total       integer     NOT NULL DEFAULT 0,
    models_success     integer     NOT NULL DEFAULT 0,
    models_error       integer     NOT NULL DEFAULT 0,
    models_skipped     integer     NOT NULL DEFAULT 0,
    tests_total        integer     NOT NULL DEFAULT 0,
    tests_pass         integer     NOT NULL DEFAULT 0,
    tests_fail         integer     NOT NULL DEFAULT 0,
    tests_warn         integer     NOT NULL DEFAULT 0,
    tests_skipped      integer     NOT NULL DEFAULT 0
);

COMMENT ON TABLE dbt_runs IS
    'Parsed from target/run_results.json. A successful model run means the SQL '
    'executed, NOT that its output is correct - correctness is what the dbt '
    'tests and the GX suites assert, which is why test counts are stored '
    'separately from model counts.';

CREATE INDEX IF NOT EXISTS ix_dbt_runs_run ON dbt_runs (run_id);

-- ===========================================================================
-- 7. dbt_test_results - one row per dbt node result
-- ===========================================================================
CREATE TABLE IF NOT EXISTS dbt_test_results (
    dbt_test_result_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dbt_run_id         bigint      NOT NULL REFERENCES dbt_runs (dbt_run_id) ON DELETE CASCADE,
    run_id             uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id         bigint      REFERENCES datasets (dataset_id),
    unique_id          text        NOT NULL,
    node_name          text        NOT NULL,
    resource_type      text        NOT NULL,
    test_type          text
        CHECK (test_type IS NULL OR test_type IN ('generic', 'singular')),
    status             text        NOT NULL,
    failures           bigint,
    severity           text
        CHECK (severity IS NULL OR severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    execution_time     numeric(10, 3),
    message            text,
    compiled_code_sha  text,
    evaluated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dbt_test_results_run  ON dbt_test_results (dbt_run_id);
CREATE INDEX IF NOT EXISTS ix_dbt_test_results_fail ON dbt_test_results (status)
    WHERE status <> 'pass';

-- ===========================================================================
-- 8. freshness_checks
-- ===========================================================================
CREATE TABLE IF NOT EXISTS freshness_checks (
    freshness_check_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                 uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id             bigint      NOT NULL REFERENCES datasets (dataset_id),
    timestamp_column       text,
    timestamp_source       text        NOT NULL
        CHECK (timestamp_source IN ('event_time', 'ingestion_time',
                                    'object_mtime', 'unavailable')),
    max_timestamp          timestamptz,
    reference_time         timestamptz NOT NULL,
    lag_seconds            bigint,
    warning_after_seconds  bigint,
    critical_after_seconds bigint,
    status                 text        NOT NULL
        CHECK (status IN ('ok', 'warning', 'critical', 'unknown')),
    severity               text        NOT NULL
        CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    detail                 text,
    checked_at             timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN freshness_checks.timestamp_source IS
    'Which clock the lag was measured against. event_time is preferred; '
    'unavailable means the dataset exposes no usable timestamp, which is '
    'recorded as status=unknown rather than silently falling back to now().';

CREATE INDEX IF NOT EXISTS ix_freshness_dataset ON freshness_checks (dataset_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS ix_freshness_run     ON freshness_checks (run_id);

-- ===========================================================================
-- 9. volume_checks
-- ===========================================================================
CREATE TABLE IF NOT EXISTS volume_checks (
    volume_check_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id               uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id           bigint      NOT NULL REFERENCES datasets (dataset_id),
    partition_key        text,
    row_count            bigint      NOT NULL,
    baseline_method      text        NOT NULL
        CHECK (baseline_method IN ('rolling_median_mad', 'absolute_bounds',
                                   'insufficient_history')),
    history_points       integer     NOT NULL DEFAULT 0,
    min_history_required integer     NOT NULL DEFAULT 0,
    baseline_median      numeric(18, 4),
    baseline_mad         numeric(18, 4),
    robust_z_score       numeric(12, 4),
    deviation_percent    numeric(12, 4),
    min_expected_rows    bigint,
    max_expected_rows    bigint,
    weekday              smallint,
    is_weekend           boolean,
    status               text        NOT NULL
        CHECK (status IN ('ok', 'warning', 'critical', 'unknown')),
    severity             text        NOT NULL
        CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    detail               text,
    checked_at           timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN volume_checks.baseline_method IS
    'insufficient_history means the baseline could not be computed from enough '
    'comparable points; the check then reports unknown instead of guessing.';
COMMENT ON COLUMN volume_checks.is_weekend IS
    'Baselines are built from same-day-type history, so a normal weekend dip '
    'is compared against other weekends rather than against weekdays.';

CREATE INDEX IF NOT EXISTS ix_volume_dataset ON volume_checks (dataset_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS ix_volume_run     ON volume_checks (run_id);

-- ===========================================================================
-- 10. schema_snapshots - a fingerprint of the observed schema, every run
-- ===========================================================================
CREATE TABLE IF NOT EXISTS schema_snapshots (
    schema_snapshot_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id             uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id         bigint      NOT NULL REFERENCES datasets (dataset_id),
    fingerprint        text        NOT NULL,
    column_count       integer     NOT NULL,
    columns            jsonb       NOT NULL,
    captured_at        timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN schema_snapshots.fingerprint IS
    'sha256 over the ordered (name, type, nullable) triples. Captured every '
    'run so a change can be located in time, not merely noticed.';

CREATE INDEX IF NOT EXISTS ix_schema_snapshots_dataset
    ON schema_snapshots (dataset_id, captured_at DESC);

-- ===========================================================================
-- 11. schema_changes - classified differences between two snapshots
-- ===========================================================================
CREATE TABLE IF NOT EXISTS schema_changes (
    schema_change_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id           uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id       bigint      NOT NULL REFERENCES datasets (dataset_id),
    from_snapshot_id bigint      REFERENCES schema_snapshots (schema_snapshot_id),
    to_snapshot_id   bigint      NOT NULL REFERENCES schema_snapshots (schema_snapshot_id),
    change_type      text        NOT NULL
        CHECK (change_type IN ('ADDITIVE', 'BREAKING', 'TYPE_CHANGE',
                               'COLUMN_REMOVAL', 'COLUMN_RENAME_CANDIDATE')),
    column_name      text,
    previous_column  text,
    previous_type    text,
    new_type         text,
    confidence       numeric(4, 3),
    is_confirmed     boolean     NOT NULL DEFAULT false,
    severity         text        NOT NULL
        CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    detail           text,
    detected_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN schema_changes.change_type IS
    'COLUMN_RENAME_CANDIDATE is a hypothesis, never a fact: a drop plus an add '
    'in one run is indistinguishable from a rename without outside knowledge.';
COMMENT ON COLUMN schema_changes.is_confirmed IS
    'Only a human, or an explicit contract change, can confirm a rename. The '
    'detector never sets this to true.';

CREATE INDEX IF NOT EXISTS ix_schema_changes_dataset ON schema_changes (dataset_id, detected_at DESC);

-- ===========================================================================
-- 12. distribution_profiles - per-column statistics captured each run
-- ===========================================================================
CREATE TABLE IF NOT EXISTS distribution_profiles (
    distribution_profile_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id      bigint      NOT NULL REFERENCES datasets (dataset_id),
    column_name     text        NOT NULL,
    column_kind     text        NOT NULL
        CHECK (column_kind IN ('numeric', 'categorical')),
    sample_size     bigint      NOT NULL,
    null_count      bigint      NOT NULL DEFAULT 0,
    distinct_count  bigint,
    mean_value      numeric(20, 6),
    stddev_value    numeric(20, 6),
    min_value       numeric(20, 6),
    p25_value       numeric(20, 6),
    median_value    numeric(20, 6),
    p75_value       numeric(20, 6),
    max_value       numeric(20, 6),
    bin_edges       jsonb,
    bin_counts      jsonb,
    category_counts jsonb,
    captured_at     timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN distribution_profiles.bin_edges IS
    'Bin edges are stored with the profile so a later comparison reuses the '
    'reference run''s binning instead of re-binning each side independently.';

CREATE INDEX IF NOT EXISTS ix_dist_profiles_dataset
    ON distribution_profiles (dataset_id, column_name, captured_at DESC);

-- ===========================================================================
-- 13. distribution_drift - PSI / chi-square comparisons
-- ===========================================================================
CREATE TABLE IF NOT EXISTS distribution_drift (
    distribution_drift_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id            bigint      NOT NULL REFERENCES datasets (dataset_id),
    column_name           text        NOT NULL,
    column_kind           text        NOT NULL
        CHECK (column_kind IN ('numeric', 'categorical')),
    method                text        NOT NULL
        CHECK (method IN ('psi', 'chi_square')),
    reference_profile_id  bigint      REFERENCES distribution_profiles (distribution_profile_id),
    current_profile_id    bigint      NOT NULL REFERENCES distribution_profiles (distribution_profile_id),
    reference_sample_size bigint,
    current_sample_size   bigint,
    min_sample_size       bigint      NOT NULL,
    statistic             numeric(18, 8),
    p_value               numeric(18, 12),
    degrees_of_freedom    integer,
    threshold_warning     numeric(18, 8),
    threshold_critical    numeric(18, 8),
    status                text        NOT NULL
        CHECK (status IN ('ok', 'warning', 'critical', 'inconclusive')),
    severity              text        NOT NULL
        CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    is_significant        boolean     NOT NULL DEFAULT false,
    detail                text,
    evaluated_at          timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN distribution_drift.status IS
    'inconclusive is a first-class outcome: when either side is below '
    'min_sample_size the test is not run and no drift is claimed.';
COMMENT ON COLUMN distribution_drift.is_significant IS
    'True only when the sample sizes met the minimum AND the statistic crossed '
    'a threshold. A large PSI on a tiny sample is never reported significant.';

CREATE INDEX IF NOT EXISTS ix_dist_drift_dataset
    ON distribution_drift (dataset_id, column_name, evaluated_at DESC);

-- ===========================================================================
-- 14. quality_scores - the weighted 0-100 score per dataset per run
-- ===========================================================================
CREATE TABLE IF NOT EXISTS quality_scores (
    quality_score_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id            bigint      NOT NULL REFERENCES datasets (dataset_id),
    overall_score         numeric(6, 2) NOT NULL
        CHECK (overall_score >= 0 AND overall_score <= 100),
    completeness_score    numeric(6, 2),
    uniqueness_score      numeric(6, 2),
    validity_score        numeric(6, 2),
    consistency_score     numeric(6, 2),
    freshness_score       numeric(6, 2),
    volume_score          numeric(6, 2),
    dimensions_measured   integer     NOT NULL,
    dimensions_total      integer     NOT NULL,
    weights_used          jsonb       NOT NULL,
    unmeasured_dimensions jsonb,
    has_critical_failure  boolean     NOT NULL DEFAULT false,
    health_status         text        NOT NULL
        CHECK (health_status IN ('healthy', 'degraded', 'unhealthy', 'unknown')),
    computed_at           timestamptz NOT NULL DEFAULT now(),
    -- A dataset with a critical contract failure can never be reported
    -- healthy, however high its weighted score happens to be. Enforced here
    -- as well as in code, so no future caller can write a contradictory row.
    CONSTRAINT ck_critical_is_not_healthy
        CHECK (NOT (has_critical_failure AND health_status = 'healthy'))
);

COMMENT ON COLUMN quality_scores.has_critical_failure IS
    'Set whenever any contract rule of CRITICAL severity failed.';
COMMENT ON COLUMN quality_scores.unmeasured_dimensions IS
    'Dimensions that produced no measurement this run. They are excluded from '
    'the weighted mean (weights renormalised) rather than counted as 100.';

CREATE INDEX IF NOT EXISTS ix_quality_scores_dataset ON quality_scores (dataset_id, computed_at DESC);
CREATE INDEX IF NOT EXISTS ix_quality_scores_run     ON quality_scores (run_id);

-- ===========================================================================
-- 15. incidents
-- ===========================================================================
CREATE TABLE IF NOT EXISTS incidents (
    incident_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fingerprint      text        NOT NULL,
    dataset_id       bigint      NOT NULL REFERENCES datasets (dataset_id),
    detector         text        NOT NULL
        CHECK (detector IN ('freshness', 'volume', 'schema', 'distribution',
                            'validation', 'dbt_test', 'quality_score')),
    rule_name        text,
    title            text        NOT NULL,
    description      text,
    severity         text        NOT NULL
        CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    status           text        NOT NULL DEFAULT 'OPEN'
        CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED', 'SUPPRESSED')),
    first_seen_at    timestamptz NOT NULL DEFAULT now(),
    last_seen_at     timestamptz NOT NULL DEFAULT now(),
    occurrence_count integer     NOT NULL DEFAULT 1,
    first_run_id     uuid        REFERENCES pipeline_runs (run_id),
    last_run_id      uuid        REFERENCES pipeline_runs (run_id),
    suppressed_until timestamptz,
    acknowledged_at  timestamptz,
    acknowledged_by  text,
    resolved_at      timestamptz,
    resolved_run_id  uuid        REFERENCES pipeline_runs (run_id),
    resolution_note  text,
    observed_value   jsonb,
    expected_value   jsonb,
    -- An incident may only carry a resolution timestamp while RESOLVED, and a
    -- RESOLVED incident must carry one. This is the schema-level half of the
    -- "never auto-resolve on silence" rule; the verified healthy re-check that
    -- justifies it is recorded in incident_events.
    CONSTRAINT ck_resolved_consistency
        CHECK ((status = 'RESOLVED' AND resolved_at IS NOT NULL)
               OR (status <> 'RESOLVED' AND resolved_at IS NULL))
);

COMMENT ON COLUMN incidents.fingerprint IS
    'Stable identity of a problem: dataset + detector + rule + the dimension '
    'that failed. Deduplication keys on this, so one recurring fault updates '
    'one incident instead of creating hundreds.';
COMMENT ON COLUMN incidents.resolved_at IS
    'Only ever set after a run has positively re-measured the dataset as '
    'healthy. An alert going quiet does NOT resolve an incident: a missing '
    'measurement is not a passing measurement.';
COMMENT ON COLUMN incidents.suppressed_until IS
    'Cooldown window. While set and in the future, recurrences update '
    'occurrence_count and last_seen_at but emit no new alert.';

CREATE INDEX IF NOT EXISTS ix_incidents_open    ON incidents (status, severity, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS ix_incidents_dataset ON incidents (dataset_id, status);

-- At most one live incident per fingerprint. Resolved ones stay as history, so
-- a recurrence after a genuine fix opens a new incident rather than reviving
-- the closed one.
CREATE UNIQUE INDEX IF NOT EXISTS ux_incidents_active_fingerprint
    ON incidents (fingerprint)
    WHERE status IN ('OPEN', 'ACKNOWLEDGED', 'SUPPRESSED');

-- ===========================================================================
-- 16. incident_events - the audit trail of every state transition
-- ===========================================================================
CREATE TABLE IF NOT EXISTS incident_events (
    incident_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_id       bigint      NOT NULL REFERENCES incidents (incident_id) ON DELETE CASCADE,
    run_id            uuid        REFERENCES pipeline_runs (run_id),
    event_type        text        NOT NULL
        CHECK (event_type IN ('OPENED', 'RECURRED', 'SUPPRESSED', 'ACKNOWLEDGED',
                              'RESOLVED', 'REOPENED', 'ESCALATED', 'HEALTHY_CHECK')),
    from_status       text,
    to_status         text,
    severity          text,
    actor             text        NOT NULL DEFAULT 'platform',
    detail            text,
    payload           jsonb,
    occurred_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN incident_events.event_type IS
    'HEALTHY_CHECK records a verified healthy re-measurement. A RESOLVED event '
    'is only ever written after at least one HEALTHY_CHECK for that incident.';

CREATE INDEX IF NOT EXISTS ix_incident_events_incident ON incident_events (incident_id, occurred_at DESC);

-- ===========================================================================
-- 17. gate_decisions - what the quality gate decided, and why
-- ===========================================================================
CREATE TABLE IF NOT EXISTS gate_decisions (
    gate_decision_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id           uuid        NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    gate_name        text        NOT NULL,
    dataset_id       bigint      REFERENCES datasets (dataset_id),
    decision         text        NOT NULL
        CHECK (decision IN ('PASS', 'PASS_WITH_WARNINGS', 'FAIL')),
    max_severity     text        NOT NULL
        CHECK (max_severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    info_count       integer     NOT NULL DEFAULT 0,
    warning_count    integer     NOT NULL DEFAULT 0,
    error_count      integer     NOT NULL DEFAULT 0,
    critical_count   integer     NOT NULL DEFAULT 0,
    blocked_tasks    jsonb,
    reason           text        NOT NULL,
    decided_at       timestamptz NOT NULL DEFAULT now(),
    -- A CRITICAL finding can never be recorded as a clean pass.
    CONSTRAINT ck_critical_fails_gate
        CHECK (NOT (critical_count > 0 AND decision = 'PASS'))
);

COMMENT ON TABLE gate_decisions IS
    'The record of why a pipeline continued or stopped. A FAIL decision must '
    'correspond to a failed Airflow task: a failed quality check is never '
    'reported as a successful run.';

CREATE INDEX IF NOT EXISTS ix_gate_decisions_run ON gate_decisions (run_id);

-- ===========================================================================
-- 18. alert_deliveries - outbound alert attempts and their outcome
-- ===========================================================================
CREATE TABLE IF NOT EXISTS alert_deliveries (
    alert_delivery_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_id         bigint      REFERENCES incidents (incident_id) ON DELETE SET NULL,
    run_id              uuid        REFERENCES pipeline_runs (run_id),
    channel             text        NOT NULL
        CHECK (channel IN ('webhook', 'grafana', 'log')),
    external_id         text,
    request_fingerprint text        NOT NULL,
    payload             jsonb       NOT NULL,
    status              text        NOT NULL
        CHECK (status IN ('received', 'duplicate', 'rejected', 'delivered', 'failed')),
    http_status         integer,
    error_message       text,
    received_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (channel, request_fingerprint)
);

COMMENT ON COLUMN alert_deliveries.request_fingerprint IS
    'Idempotency key. A redelivered alert is recorded as status=duplicate '
    'instead of being handled twice.';

CREATE INDEX IF NOT EXISTS ix_alert_deliveries_incident ON alert_deliveries (incident_id, received_at DESC);

-- ===========================================================================
-- 19. metric_history - business-level metric history, kept in Postgres
-- ===========================================================================
-- Prometheus is a short-retention sampling store with per-series cardinality
-- limits. Anything that must stay queryable months later, or be joined to a
-- dataset or an incident, is kept here instead.
CREATE TABLE IF NOT EXISTS metric_history (
    metric_history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id            uuid        REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    dataset_id        bigint      REFERENCES datasets (dataset_id),
    metric_name       text        NOT NULL,
    metric_value      numeric(20, 6) NOT NULL,
    unit              text,
    labels            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_metric_history_name
    ON metric_history (metric_name, recorded_at DESC);
CREATE INDEX IF NOT EXISTS ix_metric_history_dataset
    ON metric_history (dataset_id, metric_name, recorded_at DESC);
