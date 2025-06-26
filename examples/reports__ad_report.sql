-- ad_report
-- Incremental Facebook Ads Report with predefined metrics and dimensions
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'reports__ad_report' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'ad_report' %}

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
  ad_id STRING NOT NULL,
  ad_name STRING,
  ad_set_id STRING,
  ad_set_name STRING,
  campaign_id STRING,
  campaign_name STRING,
  account_id STRING,
  account_name STRING,
  creative_id STRING,
  image_url STRING,
  campaign_start_date DATE,
  campaign_end_date DATE,
  cost FLOAT64,
  clicks INT64,
  impressions INT64,
  conversions FLOAT64,
  last_synced_at TIMESTAMP,
  last_data_date DATE,
  _gn_id STRING
);

-- Get the latest data date from the target table
DECLARE latest_data_date DATE;
SET latest_data_date = (
  SELECT COALESCE(MAX(last_data_date), DATE('1970-01-01'))
  FROM `{{target_dataset}}.{{target_table_id}}`
);

-- Extract latest data from source
WITH sync_info AS (
  SELECT
    MAX(datetime(_fivetran_synced, "{{ vars.timezone }}")) as max_synced_at,
    MAX(date) as max_data_date
  FROM `{{source_dataset}}.{{source_table_id}}`
  WHERE date > latest_data_date
),

campaigns AS (
  SELECT *
  FROM `{{source_dataset}}.campaign_history`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_time DESC) = 1
),

accounts AS (
  SELECT *
  FROM `{{source_dataset}}.account_history`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY _fivetran_synced DESC) = 1
),

ad_sets AS (
  SELECT *
  FROM `{{source_dataset}}.ad_set_history`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_time DESC) = 1
),

ads AS (
  SELECT *
  FROM `{{source_dataset}}.ad_history`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_time DESC) = 1
),

creatives AS (
  SELECT *
  FROM `{{source_dataset}}.creative_history`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY _fivetran_synced DESC) = 1
),

stats AS (
  SELECT
    date,
    ad_id,
    SUM(spend) as cost,
    SUM(clicks) as clicks,
    SUM(impressions) as impressions,
    SUM(conversions) as conversions
  FROM `{{source_dataset}}.{{source_table_id}}`
  WHERE date > latest_data_date
  GROUP BY 1, 2
),

latest_data AS (
  SELECT
    stats.date,
    stats.ad_id,
    ads.name as ad_name,
    ad_sets.id as ad_set_id,
    ad_sets.name as ad_set_name,
    campaigns.id as campaign_id,
    campaigns.name as campaign_name,
    accounts.id as account_id,
    accounts.name as account_name,
    ads.creative_id,
    creatives.image_url,
    DATE(campaigns.start_time) as campaign_start_date,
    DATE(campaigns.stop_time) as campaign_end_date,
    stats.cost,
    stats.clicks,
    stats.impressions,
    stats.conversions,
    sync_info.max_synced_at as last_synced_at,
    sync_info.max_data_date as last_data_date,
    TO_HEX(MD5(CONCAT(
      CAST(stats.date AS STRING),
      CAST(stats.ad_id AS STRING)
    ))) as _gn_id
  FROM stats
  LEFT JOIN ads ON SAFE_CAST(stats.ad_id AS STRING) = SAFE_CAST(ads.id AS STRING)
  LEFT JOIN ad_sets ON ads.ad_set_id = ad_sets.id
  LEFT JOIN campaigns ON ad_sets.campaign_id = campaigns.id
  LEFT JOIN accounts ON campaigns.account_id = accounts.id
  LEFT JOIN creatives ON ads.creative_id = creatives.id
  CROSS JOIN sync_info
)

-- Merge new data into the target table
MERGE `{{target_dataset}}.{{target_table_id}}` AS target
USING latest_data AS source
ON target.date = source.date AND target.ad_id = source.ad_id
WHEN MATCHED THEN
  UPDATE SET
    ad_name = source.ad_name,
    ad_set_id = source.ad_set_id,
    ad_set_name = source.ad_set_name,
    campaign_id = source.campaign_id,
    campaign_name = source.campaign_name,
    account_id = source.account_id,
    account_name = source.account_name,
    creative_id = source.creative_id,
    image_url = source.image_url,
    campaign_start_date = source.campaign_start_date,
    campaign_end_date = source.campaign_end_date,
    cost = source.cost,
    clicks = source.clicks,
    impressions = source.impressions,
    conversions = source.conversions,
    last_synced_at = source.last_synced_at,
    last_data_date = source.last_data_date,
    _gn_id = source._gn_id
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    date, ad_id, ad_name, ad_set_id, ad_set_name, campaign_id, campaign_name,
    account_id, account_name, creative_id, image_url, campaign_start_date,
    campaign_end_date, cost, clicks, impressions, conversions,
    last_synced_at, last_data_date, _gn_id
  )
  VALUES (
    source.date, source.ad_id, source.ad_name, source.ad_set_id, source.ad_set_name,
    source.campaign_id, source.campaign_name, source.account_id, source.account_name,
    source.creative_id, source.image_url, source.campaign_start_date,
    source.campaign_end_date, source.cost, source.clicks, source.impressions,
    source.conversions, source.last_synced_at, source.last_data_date, source._gn_id
  );

END IF; 