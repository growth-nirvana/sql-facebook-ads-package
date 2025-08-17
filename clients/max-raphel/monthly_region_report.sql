-- monthly_region_report
-- Batch-based monthly snapshot table for Facebook Ads Region Insights (Ad level)
{% assign source_dataset_id = vars.source_dataset_id %}
{% assign target_dataset_id = vars.target_dataset_id %}
{% assign source_table_name = 'adsinsights_monthly_region_report_ad_level' %}
{% assign target_table_name = 'monthly_region_report' %}

DECLARE table_exists BOOL DEFAULT FALSE;
DECLARE min_date DATE;
DECLARE max_date DATE;

SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset_id}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_name}}'
);

IF table_exists THEN

ALTER TABLE `{{source_dataset_id}}.{{source_table_name}}`
ADD COLUMN IF NOT EXISTS actions STRING,
ADD COLUMN IF NOT EXISTS action_values STRING,
ADD COLUMN IF NOT EXISTS conversions STRING,
ADD COLUMN IF NOT EXISTS conversion_values STRING;

CREATE TABLE IF NOT EXISTS `{{target_dataset_id}}.{{target_table_name}}` (
  ad_id STRING,
  date DATE,
  _gn_id STRING,
  _gn_synced TIMESTAMP,
  account_id INT64,
  clicks INT64,
  impressions INT64,
  spend FLOAT64,
  region STRING,
  tenant STRING
);

CREATE TEMP TABLE latest_batch AS
WITH base AS (SELECT * FROM `{{source_dataset_id}}.{{source_table_name}}`),
ordered AS (
  SELECT *, TIMESTAMP_DIFF(_time_extracted, LAG(_time_extracted) OVER (ORDER BY _time_extracted), SECOND) AS diff_seconds
  FROM base
),
batches AS (
  SELECT *, SUM(CASE WHEN diff_seconds IS NULL OR diff_seconds > 120 THEN 1 ELSE 0 END)
    OVER (ORDER BY _time_extracted) AS batch_id
  FROM ordered
),
ranked AS (SELECT *, RANK() OVER (ORDER BY batch_id DESC) AS batch_rank FROM batches)
SELECT * FROM ranked WHERE batch_rank = 1;

SET min_date = (SELECT MIN(PARSE_DATE('%Y-%m-%d', date_start)) FROM latest_batch);
SET max_date = (SELECT MAX(PARSE_DATE('%Y-%m-%d', date_start)) FROM latest_batch);

BEGIN TRANSACTION;
  IF EXISTS (
    SELECT 1 FROM `{{target_dataset_id}}.{{target_table_name}}`
    WHERE date BETWEEN min_date AND max_date
      AND account_id IN (SELECT DISTINCT SAFE_CAST(account_id AS INT64) FROM latest_batch)
    LIMIT 1
  ) THEN
    DELETE FROM `{{target_dataset_id}}.{{target_table_name}}`
    WHERE date BETWEEN min_date AND max_date
      AND account_id IN (SELECT DISTINCT SAFE_CAST(account_id AS INT64) FROM latest_batch);
  END IF;

  INSERT INTO `{{target_dataset_id}}.{{target_table_name}}` (
    ad_id, date, _gn_id, _gn_synced, account_id, clicks, impressions, spend, region, tenant
  )
  SELECT
    SAFE_CAST(ad_id AS STRING),
    PARSE_DATE('%Y-%m-%d', date_start),
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(ad_id AS STRING),
      CAST(PARSE_DATE('%Y-%m-%d', date_start) AS STRING),
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(region AS STRING)
    ]))),
    _time_extracted,
    SAFE_CAST(account_id AS INT64),
    SAFE_CAST(clicks AS INT64),
    SAFE_CAST(impressions AS INT64),
    SAFE_CAST(spend AS FLOAT64),
    SAFE_CAST(region AS STRING),
    SAFE_CAST(tenant AS STRING)
  FROM latest_batch;
COMMIT TRANSACTION;

END IF;
