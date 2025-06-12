-- device_report_conversion_values
-- Batch-based daily snapshot table for Facebook Ads Device Insights Conversion Values
{% assign source_dataset_id = vars.source_dataset_id %}
{% assign target_dataset_id = vars.target_dataset_id %}
{% assign source_table_name = 'adsinsights_device_report' %}
{% assign target_table_name = 'device_report_conversion_values' %}

-- Declare all variables at the top
DECLARE table_exists BOOL DEFAULT FALSE;
DECLARE min_date DATE;
DECLARE max_date DATE;

-- Check if the source table exists
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset_id}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_name}}'
);

-- Only run the ETL logic if the source table exists
IF table_exists THEN

-- Step 0: Ensure required columns exist in the source table
ALTER TABLE `{{source_dataset_id}}.{{source_table_name}}`
ADD COLUMN IF NOT EXISTS actions STRING,
ADD COLUMN IF NOT EXISTS action_values STRING,
ADD COLUMN IF NOT EXISTS conversions STRING,
ADD COLUMN IF NOT EXISTS conversion_values STRING;

-- Step 0.5: Create target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset_id}}.{{target_table_name}}` (
  ad_id STRING,
  date DATE,
  _gn_id STRING,
  _gn_synced TIMESTAMP,
  account_id INT64,
  action_type STRING,
  value FLOAT64,
  inline FLOAT64,
  _7_d_click FLOAT64,
  _1_d_view FLOAT64,
  device_platform STRING,
  tenant STRING
);

-- Step 1: Create temp table for latest batch
CREATE TEMP TABLE latest_batch AS
WITH base AS (
  SELECT * FROM `{{source_dataset_id}}.{{source_table_name}}`
),
ordered AS (
  SELECT *,
    TIMESTAMP_DIFF(
      _time_extracted,
      LAG(_time_extracted) OVER (ORDER BY _time_extracted),
      SECOND
    ) AS diff_seconds
  FROM base
),
batches AS (
  SELECT *,
    SUM(CASE WHEN diff_seconds IS NULL OR diff_seconds > 120 THEN 1 ELSE 0 END)
      OVER (ORDER BY _time_extracted) AS batch_id
  FROM ordered
),
ranked_batches AS (
  SELECT *,
    RANK() OVER (ORDER BY batch_id DESC) AS batch_rank
  FROM batches
)
SELECT *
FROM ranked_batches
WHERE batch_rank = 1;

-- Step 2: Assign min/max dates using SET + scalar subqueries
SET min_date = (
  SELECT MIN(PARSE_DATE('%Y-%m-%d', date_start)) FROM latest_batch
);

SET max_date = (
  SELECT MAX(PARSE_DATE('%Y-%m-%d', date_start)) FROM latest_batch
);

-- Step 3: Conditional delete and insert
BEGIN TRANSACTION;

  IF EXISTS (
    SELECT 1
    FROM `{{target_dataset_id}}.{{target_table_name}}`
    WHERE date BETWEEN min_date AND max_date
      AND account_id IN (
        SELECT DISTINCT SAFE_CAST(account_id AS INT64)
        FROM latest_batch
      )
    LIMIT 1
  ) THEN
    DELETE FROM `{{target_dataset_id}}.{{target_table_name}}`
    WHERE date BETWEEN min_date AND max_date
      AND account_id IN (
        SELECT DISTINCT SAFE_CAST(account_id AS INT64)
        FROM latest_batch
      );
  END IF;

  INSERT INTO `{{target_dataset_id}}.{{target_table_name}}` (
    ad_id,
    date,
    _gn_id,
    _gn_synced,
    account_id,
    action_type,
    value,
    inline,
    _7_d_click,
    _1_d_view,
    device_platform,
    tenant
  )
  SELECT
    SAFE_CAST(ad_id AS STRING) AS ad_id,
    PARSE_DATE('%Y-%m-%d', date_start) AS date,
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(ad_id AS STRING),
      CAST(PARSE_DATE('%Y-%m-%d', date_start) AS STRING),
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(JSON_VALUE(conversion_value, '$.action_type') AS STRING),
      SAFE_CAST(device_platform AS STRING)
    ]))) AS _gn_id,
    _time_extracted AS _gn_synced,
    SAFE_CAST(account_id AS INT64) AS account_id,
    JSON_VALUE(conversion_value, '$.action_type') AS action_type,
    SAFE_CAST(JSON_VALUE(conversion_value, '$.value') AS FLOAT64) AS value,
    SAFE_CAST(JSON_VALUE(conversion_value, '$.inline') AS FLOAT64) AS inline,
    SAFE_CAST(JSON_VALUE(conversion_value, '$."7d_click"') AS FLOAT64) AS _7_d_click,
    SAFE_CAST(JSON_VALUE(conversion_value, '$."1d_view"') AS FLOAT64) AS _1_d_view,
    SAFE_CAST(device_platform AS STRING) AS device_platform,
    SAFE_CAST(tenant AS STRING) AS tenant
  FROM latest_batch
  , UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS conversion_value
  WHERE JSON_VALUE(conversion_value, '$.action_type') IS NOT NULL;

COMMIT TRANSACTION;
END IF; 