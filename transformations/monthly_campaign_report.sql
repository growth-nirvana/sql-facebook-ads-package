-- monthly_campaign_insights
-- Batch-based daily snapshot table for Facebook Ads Monthly Campaign Insights with source table existence check
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'monthly_campaign_report' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'adsinsights_monthly_campaign_insights' %}

-- Declare all variables at the top
DECLARE table_exists BOOL DEFAULT FALSE;
DECLARE min_date DATE;
DECLARE max_date DATE;

-- Check if the source table exists
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

-- Only run the ETL logic if the source table exists
IF table_exists THEN

  -- Create target table if it doesn't exist
  CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
    campaign_id STRING NOT NULL,
    date DATE NOT NULL,
    _gn_id STRING,
    account_id STRING,
    account_name STRING,
    campaign_name STRING,
    clicks INT64,
    impressions INT64,
    spend FLOAT64,
    reach INT64,
    frequency FLOAT64,
    run_id INT64,
    _fivetran_synced TIMESTAMP
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
      FROM `{{target_dataset}}.{{target_table_id}}`
      WHERE date BETWEEN min_date AND max_date
        AND account_id IN (
          SELECT DISTINCT SAFE_CAST(account_id AS STRING) FROM latest_batch
        )
      LIMIT 1
    ) THEN
      DELETE FROM `{{target_dataset}}.{{target_table_id}}`
      WHERE date BETWEEN min_date AND max_date
        AND account_id IN (
          SELECT DISTINCT SAFE_CAST(account_id AS STRING) FROM latest_batch
        );
    END IF;

    INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
      campaign_id,
      date,
      _gn_id,
      account_id,
      account_name,
      campaign_name,
      clicks,
      impressions,
      spend,
      reach,
      frequency,
      run_id,
      _fivetran_synced
    )
    SELECT
      SAFE_CAST(campaign_id AS STRING),
      PARSE_DATE('%Y-%m-%d', date_start) AS date,
      TO_HEX(MD5(TO_JSON_STRING([
        SAFE_CAST(campaign_id AS STRING),
        CAST(PARSE_DATE('%Y-%m-%d', date_start) AS STRING),
        SAFE_CAST(account_id AS STRING)
      ]))) AS _gn_id,
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(account_name AS STRING),
      SAFE_CAST(campaign_name AS STRING),
      SAFE_CAST(clicks AS INT64),
      SAFE_CAST(impressions AS INT64),
      SAFE_CAST(spend AS FLOAT64),
      SAFE_CAST(reach AS INT64),
      SAFE_CAST(frequency AS FLOAT64),
      batch_id as run_id,
      CURRENT_TIMESTAMP() AS _fivetran_synced
    FROM latest_batch;

  COMMIT TRANSACTION;

END IF; 