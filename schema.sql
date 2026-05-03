-- ============================================================
--  Primis Reports — BigQuery Schema
--  Run once to create dataset + all 3 tables
--  Project: YOUR_PROJECT_ID  (replace before running)
-- ============================================================

-- 1. Create dataset
CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT_ID.primis_reports`
OPTIONS (
  location = 'US',
  description = 'Primis report data ingested from Gmail twice daily'
);

-- ============================================================
-- 2. Shared table DDL macro (used for all 3 report types)
--    Partitioned by report date, clustered by debug for fast
--    A/B test queries.
-- ============================================================

-- MEDIA REPORT
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.primis_reports.media_report`
(
  ingest_timestamp  TIMESTAMP   NOT NULL,
  report_type       STRING      NOT NULL,   -- always 'Media'
  date              DATE,
  debug             STRING,                 -- col B: e.g. "ABT / prebid_Version_10_23 / 10 / default"
  site              STRING,
  size              STRING,
  format            STRING,
  imps              INT64,                  -- col F: impressions for this row (hourly)
  clicks            INT64,
  ctr               FLOAT64,
  rpm               FLOAT64,
  cpm               FLOAT64,
  revenue           FLOAT64
)
PARTITION BY date
CLUSTER BY debug, site
OPTIONS (
  description = 'Primis Media report — one row per hourly site/debug/size combination',
  partition_expiration_days = 365
);

-- VIDEO REPORT
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.primis_reports.video_report`
(
  ingest_timestamp  TIMESTAMP   NOT NULL,
  report_type       STRING      NOT NULL,
  date              DATE,
  debug             STRING,
  site              STRING,
  size              STRING,
  format            STRING,
  imps              INT64,
  clicks            INT64,
  ctr               FLOAT64,
  rpm               FLOAT64,
  cpm               FLOAT64,
  revenue           FLOAT64
)
PARTITION BY date
CLUSTER BY debug, site
OPTIONS (
  description = 'Primis Video report — one row per hourly site/debug/size combination',
  partition_expiration_days = 365
);

-- CONTENT REPORT
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.primis_reports.content_report`
(
  ingest_timestamp  TIMESTAMP   NOT NULL,
  report_type       STRING      NOT NULL,
  date              DATE,
  debug             STRING,
  site              STRING,
  size              STRING,
  format            STRING,
  imps              INT64,
  clicks            INT64,
  ctr               FLOAT64,
  rpm               FLOAT64,
  cpm               FLOAT64,
  revenue           FLOAT64
)
PARTITION BY date
CLUSTER BY debug, site
OPTIONS (
  description = 'Primis Content report — one row per hourly site/debug/size combination',
  partition_expiration_days = 365
);


-- ============================================================
-- 3. Active A/B test detection view
--    Mirrors the analyzer logic:
--    - Variants qualify if ANY single row has imps > 10,000
--    - Groups by test key (all debug segments except last)
--    - Computes per-metric deltas between default and active
-- ============================================================

CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.primis_reports.active_ab_tests` AS

WITH

-- union all 3 report types
all_reports AS (
  SELECT *, 'Media'   AS report_type FROM `YOUR_PROJECT_ID.primis_reports.media_report`
  UNION ALL
  SELECT *, 'Video'   AS report_type FROM `YOUR_PROJECT_ID.primis_reports.video_report`
  UNION ALL
  SELECT *, 'Content' AS report_type FROM `YOUR_PROJECT_ID.primis_reports.content_report`
),

-- extract test key and variant label from debug string
-- debug format: "ABT / testName / weight / variantLabel"
parsed AS (
  SELECT
    *,
    TRIM(REGEXP_EXTRACT(debug, r'^(.*)/[^/]+$'))  AS test_key,
    TRIM(REGEXP_EXTRACT(debug, r'[^/]+$'))         AS variant_label
  FROM all_reports
  WHERE debug IS NOT NULL
),

-- qualify variants: must have at least ONE row with imps > 10,000
qualified_variants AS (
  SELECT DISTINCT
    report_type,
    test_key,
    variant_label,
    debug
  FROM parsed
  WHERE imps > 10000
),

-- aggregate metrics for qualified variants only
variant_metrics AS (
  SELECT
    p.report_type,
    p.test_key,
    p.variant_label,
    p.debug,
    COUNT(*)              AS row_count,
    MAX(p.imps)           AS peak_imps,
    SUM(p.imps)           AS total_imps,
    SUM(p.clicks)         AS total_clicks,
    AVG(p.ctr)            AS avg_ctr,
    AVG(p.rpm)            AS avg_rpm,
    AVG(p.cpm)            AS avg_cpm,
    SUM(p.revenue)        AS total_revenue,
    MIN(p.date)           AS first_seen,
    MAX(p.date)           AS last_seen
  FROM parsed p
  INNER JOIN qualified_variants q
    ON  p.report_type   = q.report_type
    AND p.test_key      = q.test_key
    AND p.variant_label = q.variant_label
  GROUP BY 1,2,3,4
),

-- pivot to get default vs active side by side
pivoted AS (
  SELECT
    report_type,
    test_key,
    MAX(IF(variant_label = 'default', total_imps,    NULL)) AS default_imps,
    MAX(IF(variant_label = 'active',  total_imps,    NULL)) AS active_imps,
    MAX(IF(variant_label = 'default', avg_ctr,       NULL)) AS default_ctr,
    MAX(IF(variant_label = 'active',  avg_ctr,       NULL)) AS active_ctr,
    MAX(IF(variant_label = 'default', avg_rpm,       NULL)) AS default_rpm,
    MAX(IF(variant_label = 'active',  avg_rpm,       NULL)) AS active_rpm,
    MAX(IF(variant_label = 'default', avg_cpm,       NULL)) AS default_cpm,
    MAX(IF(variant_label = 'active',  avg_cpm,       NULL)) AS active_cpm,
    MAX(IF(variant_label = 'default', total_revenue, NULL)) AS default_revenue,
    MAX(IF(variant_label = 'active',  total_revenue, NULL)) AS active_revenue,
    MAX(IF(variant_label = 'default', peak_imps,     NULL)) AS default_peak_imps,
    MAX(IF(variant_label = 'active',  peak_imps,     NULL)) AS active_peak_imps,
    COUNT(DISTINCT variant_label)                           AS variant_count,
    MIN(first_seen)                                         AS first_seen,
    MAX(last_seen)                                          AS last_seen
  FROM variant_metrics
  GROUP BY 1,2
)

-- final output with computed deltas
SELECT
  report_type,
  test_key,
  first_seen,
  last_seen,
  variant_count,

  -- impressions
  default_imps,
  active_imps,
  default_peak_imps,
  active_peak_imps,

  -- CTR delta
  default_ctr,
  active_ctr,
  SAFE_DIVIDE(active_ctr - default_ctr, default_ctr) * 100   AS ctr_delta_pct,

  -- RPM delta
  default_rpm,
  active_rpm,
  SAFE_DIVIDE(active_rpm - default_rpm, default_rpm) * 100   AS rpm_delta_pct,

  -- CPM delta
  default_cpm,
  active_cpm,
  SAFE_DIVIDE(active_cpm - default_cpm, default_cpm) * 100   AS cpm_delta_pct,

  -- Revenue delta
  default_revenue,
  active_revenue,
  SAFE_DIVIDE(active_revenue - default_revenue, default_revenue) * 100 AS revenue_delta_pct,

  -- Significance flag: any metric delta > 10%
  (
    ABS(SAFE_DIVIDE(active_ctr - default_ctr, default_ctr)) > 0.10
    OR ABS(SAFE_DIVIDE(active_rpm - default_rpm, default_rpm)) > 0.10
    OR ABS(SAFE_DIVIDE(active_cpm - default_cpm, default_cpm)) > 0.10
  ) AS has_significant_delta

FROM pivoted
WHERE variant_count >= 2   -- only show tests with both variants present
ORDER BY has_significant_delta DESC, ABS(COALESCE(cpm_delta_pct, 0)) DESC;
