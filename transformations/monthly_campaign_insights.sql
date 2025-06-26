-- monthly_campaign_insights_report
-- Incremental Facebook Ads Monthly Campaign Insights Report with predefined metrics and dimensions
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'reports__monthly_campaign_insights' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'adsinsights_monthly_campaign_insights' %}

-- Guard clause: check if source table exists
DECLARE table_exists BOOL DEFAULT FALSE;
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

IF table_exists THEN

-- Create the report table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  date DATE NOT NULL,
  campaign_id STRING NOT NULL,
  campaign_name STRING,
  account_id STRING,
  account_name STRING,
  cost FLOAT64,
  clicks INT64,
  impressions INT64,
  reach INT64,
  frequency FLOAT64,
  last_synced_at TIMESTAMP,
  last_data_date DATE,
  _gn_id STRING
);

-- Step 1: Create temp table for latest batch
CREATE TEMP TABLE latest_batch AS
WITH base AS (
  SELECT * FROM `{{source_dataset}}.{{source_table_id}}`
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

-- Step 2: Get min/max dates from latest batch
DECLARE min_date DATE;
DECLARE max_date DATE;

SET min_date = (
  SELECT MIN(PARSE_DATE('%Y-%m-%d', date_start)) FROM latest_batch
);

SET max_date = (
  SELECT MAX(PARSE_DATE('%Y-%m-%d', date_start)) FROM latest_batch
);

-- Extract latest data from source
WITH sync_info AS (
  SELECT
    MAX(_time_extracted) as max_synced_at,
    MAX(PARSE_DATE('%Y-%m-%d', date_start)) as max_data_date
  FROM latest_batch
),

campaigns AS (
  SELECT *
  FROM `{{source_dataset}}.campaign_history`
  WHERE is_current = TRUE
),

accounts AS (
  SELECT *
  FROM `{{source_dataset}}.account_history`
  WHERE is_current = TRUE
),

stats AS (
  SELECT
    PARSE_DATE('%Y-%m-%d', date_start) as date,
    campaign_id,
    SUM(SAFE_CAST(spend AS FLOAT64)) as cost,
    SUM(SAFE_CAST(clicks AS INT64)) as clicks,
    SUM(SAFE_CAST(impressions AS INT64)) as impressions,
    SUM(SAFE_CAST(reach AS INT64)) as reach,
    AVG(SAFE_CAST(frequency AS FLOAT64)) as frequency
  FROM latest_batch
  GROUP BY 1, 2
),

latest_data AS (
  SELECT
    stats.date,
    stats.campaign_id,
    campaigns.name as campaign_name,
    accounts.id as account_id,
    accounts.name as account_name,
    stats.cost,
    stats.clicks,
    stats.impressions,
    stats.reach,
    stats.frequency,
    sync_info.max_synced_at as last_synced_at,
    sync_info.max_data_date as last_data_date,
    TO_HEX(MD5(CONCAT(
      CAST(stats.date AS STRING),
      CAST(stats.campaign_id AS STRING)
    ))) as _gn_id
  FROM stats
  LEFT JOIN campaigns ON SAFE_CAST(stats.campaign_id AS STRING) = SAFE_CAST(campaigns.id AS STRING)
  LEFT JOIN accounts ON campaigns.account_id = accounts.id
  CROSS JOIN sync_info
)

-- Step 3: Delete existing data for the date range and insert new data
BEGIN TRANSACTION;

  -- Delete existing data for the date range
  DELETE FROM `{{target_dataset}}.{{target_table_id}}`
  WHERE date BETWEEN min_date AND max_date
    AND campaign_id IN (
      SELECT DISTINCT SAFE_CAST(campaign_id AS STRING) FROM latest_batch
    );

  -- Insert new data
  INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
    date, campaign_id, campaign_name, account_id, account_name,
    cost, clicks, impressions, reach, frequency,
    last_synced_at, last_data_date, _gn_id
  )
  SELECT
    date, campaign_id, campaign_name, account_id, account_name,
    cost, clicks, impressions, reach, frequency,
    last_synced_at, last_data_date, _gn_id
  FROM latest_data;

COMMIT TRANSACTION;

END IF; 