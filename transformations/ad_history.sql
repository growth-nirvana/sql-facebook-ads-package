-- ad_history
-- SCD Type 2 Table for Facebook Ads Ads
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'ads' %}

-- Guard clause: check if source table exists
DECLARE table_exists BOOL DEFAULT FALSE;
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

IF table_exists THEN

-- Create SCD table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  id INT64 NOT NULL,
  account_id INT64,
  campaign_id INT64,
  ad_set_id INT64,
  creative_id STRING,
  updated_time STRING,
  created_time STRING,
  name STRING,
  effective_status STRING,
  tenant STRING,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  is_current BOOLEAN,
  _gn_id STRING
);

ALTER TABLE `{{source_dataset}}.{{source_table_id}}`
  ADD COLUMN IF NOT EXISTS creative STRING;

ALTER TABLE `{{target_dataset}}.{{target_table_id}}`
  ADD COLUMN IF NOT EXISTS creative_id STRING;

-- Extract latest snapshot from source
CREATE TEMP TABLE latest_snapshot AS
SELECT
  *,
  JSON_EXTRACT_SCALAR(creative, '$.id') AS creative_id,
  ROW_NUMBER() OVER (PARTITION BY CAST(id AS INT64) ORDER BY updated_time DESC) AS rn
FROM `{{source_dataset}}.{{source_table_id}}`;

-- SCD Merge Logic
MERGE `{{target_dataset}}.{{target_table_id}}` AS target
USING (
  SELECT
    id,
    CAST(account_id AS INT64) AS account_id,
    CAST(campaign_id AS INT64) AS campaign_id,
    CAST(adset_id AS INT64) AS ad_set_id,
    creative_id,
    updated_time,
    created_time,
    name,
    effective_status,
    tenant,
    TIMESTAMP(updated_time) AS effective_from,
    CAST(NULL AS TIMESTAMP) AS effective_to,
    TRUE AS is_current,
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(id AS STRING),
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(campaign_id AS STRING),
      SAFE_CAST(adset_id AS STRING),
      SAFE_CAST(creative_id AS STRING),
      SAFE_CAST(updated_time AS STRING),
      SAFE_CAST(created_time AS STRING),
      SAFE_CAST(name AS STRING),
      SAFE_CAST(effective_status AS STRING),
      SAFE_CAST(tenant AS STRING)
    ]))) AS _gn_id
  FROM latest_snapshot
  WHERE rn = 1
) AS source
ON target.id = source.id AND target.is_current = TRUE
WHEN MATCHED THEN
  UPDATE SET
    effective_to = source.effective_from,
    is_current = FALSE
WHEN NOT MATCHED BY TARGET
  THEN INSERT (
    id, account_id, campaign_id, ad_set_id, creative_id, updated_time, created_time, name, effective_status, tenant, effective_from, effective_to, is_current, _gn_id
  )
  VALUES (
    source.id, source.account_id, source.campaign_id, source.ad_set_id, source.creative_id, source.updated_time, source.created_time, source.name, source.effective_status, source.tenant, source.effective_from, source.effective_to, source.is_current, source._gn_id
  );

-- Drop the source table after successful processing
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
END IF; 