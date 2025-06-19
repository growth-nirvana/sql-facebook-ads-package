-- region_report_conversions
-- Batch-based daily snapshot table for Facebook Ads Region Conversions
{% assign source_dataset_id = vars.source_dataset_id %}
{% assign target_dataset_id = vars.target_dataset_id %}
{% assign source_table_name = 'adsinsights_region_report' %}
{% assign target_table_name = 'region_report_conversions' %}

DECLARE table_exists BOOL DEFAULT FALSE;
DECLARE min_date DATE;
DECLARE max_date DATE;

SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset_id}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_name}}'
);

IF table_exists THEN

CREATE TABLE IF NOT EXISTS `{{target_dataset_id}}.{{target_table_name}}` (
  ad_id STRING,
  date DATE,
  account_id STRING,
  conversion_type STRING,
  conversion_count INT64,
  region STRING,
  tenant STRING,
  _gn_id STRING,
  _gn_synced TIMESTAMP
);

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

SET min_date = (
  SELECT MIN(PARSE_DATE('%Y-%m-%d', date_start)) FROM latest_batch
);

SET max_date = (
  SELECT MAX(PARSE_DATE('%Y-%m-%d', date_start)) FROM latest_batch
);

BEGIN TRANSACTION;
  IF EXISTS (
    SELECT 1
    FROM `{{target_dataset_id}}.{{target_table_name}}`
    WHERE date BETWEEN min_date AND max_date
      AND account_id IN (
        SELECT DISTINCT account_id FROM latest_batch
      )
    LIMIT 1
  ) THEN
    DELETE FROM `{{target_dataset_id}}.{{target_table_name}}`
    WHERE date BETWEEN min_date AND max_date
      AND account_id IN (
        SELECT DISTINCT account_id FROM latest_batch
      );
  END IF;

  INSERT INTO `{{target_dataset_id}}.{{target_table_name}}` (
    ad_id,
    date,
    account_id,
    conversion_type,
    conversion_count,
    region,
    tenant,
    _gn_id,
    _gn_synced
  )
  SELECT
    SAFE_CAST(ad_id AS STRING) AS ad_id,
    PARSE_DATE('%Y-%m-%d', date_start) AS date,
    SAFE_CAST(account_id AS STRING) AS account_id,
    conversion.key AS conversion_type,
    SAFE_CAST(conversion.value AS INT64) AS conversion_count,
    SAFE_CAST(region AS STRING) AS region,
    SAFE_CAST(tenant AS STRING) AS tenant,
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(ad_id AS STRING),
      CAST(PARSE_DATE('%Y-%m-%d', date_start) AS STRING),
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(region AS STRING),
      conversion.key
    ]))) AS _gn_id,
    _time_extracted AS _gn_synced
  FROM latest_batch,
  UNNEST(SPLIT(conversions, ',')) AS conversion_pair,
  UNNEST([STRUCT(
    SPLIT(conversion_pair, ':')[OFFSET(0)] AS key,
    SPLIT(conversion_pair, ':')[OFFSET(1)] AS value
  )]) AS conversion;

COMMIT TRANSACTION;
END IF; 