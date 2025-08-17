-- weekly_publisher_platform_account_report_actions
-- Batch-based pivoted actions table for Facebook Ads Weekly Publisher Platform Account Insights
{% assign source_dataset_id = vars.source_dataset_id %}
{% assign target_dataset_id = vars.target_dataset_id %}
{% assign source_table_name = 'adsinsights_weekly_publisher_platform_account_insights' %}
{% assign target_table_name = 'weekly_publisher_platform_account_report_actions' %}

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

  -- Create target table if it doesn't exist
  CREATE TABLE IF NOT EXISTS `{{target_dataset_id}}.{{target_table_name}}` (
    account_id INT64 NOT NULL,
    account_name STRING,
    date DATE NOT NULL,
    _gn_id STRING,
    _gn_synced TIMESTAMP,
    publisher_platform STRING NOT NULL,
    action_type STRING,
    value FLOAT64,
    inline FLOAT64,
    _7_d_click FLOAT64,
    _1_d_view FLOAT64
  );

  -- Step 1: Create temp table for latest batch using window function batch logic
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
          SELECT DISTINCT SAFE_CAST(account_id AS INT64) FROM latest_batch
        )
      LIMIT 1
    ) THEN
      DELETE FROM `{{target_dataset_id}}.{{target_table_name}}`
      WHERE date BETWEEN min_date AND max_date
        AND account_id IN (
          SELECT DISTINCT SAFE_CAST(account_id AS INT64) FROM latest_batch
        );
    END IF;

    INSERT INTO `{{target_dataset_id}}.{{target_table_name}}` (
      account_id,
      account_name,
      date,
      _gn_id,
      _gn_synced,
      publisher_platform,
      action_type,
      value,
      inline,
      _7_d_click,
      _1_d_view
    )
    SELECT
      SAFE_CAST(account_id AS INT64) AS account_id,
      SAFE_CAST(account_name AS STRING) AS account_name,
      PARSE_DATE('%Y-%m-%d', date_start) AS date,
      TO_HEX(MD5(TO_JSON_STRING([
        SAFE_CAST(account_id AS STRING),
        CAST(PARSE_DATE('%Y-%m-%d', date_start) AS STRING),
        SAFE_CAST(publisher_platform AS STRING),
        JSON_VALUE(action, '$.action_type')
      ]))) AS _gn_id,
      _time_extracted AS _gn_synced,
      SAFE_CAST(publisher_platform AS STRING) AS publisher_platform,
      JSON_VALUE(action, '$.action_type') AS action_type,
      SAFE_CAST(JSON_VALUE(action, '$.value') AS FLOAT64) AS value,
      SAFE_CAST(JSON_VALUE(action, '$.inline') AS FLOAT64) AS inline,
      SAFE_CAST(JSON_VALUE(action, '$."7d_click"') AS FLOAT64) AS _7_d_click,
      SAFE_CAST(JSON_VALUE(action, '$."1d_view"') AS FLOAT64) AS _1_d_view
    FROM latest_batch
    , UNNEST(JSON_EXTRACT_ARRAY(actions)) AS action
    WHERE JSON_VALUE(action, '$.action_type') IS NOT NULL;

  COMMIT TRANSACTION;

END IF;
