-- ad_set_history
-- SCD Type 2 Table for Facebook Ads Ad Sets
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_set_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'adsets' %}

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
  name STRING,
  account_id INT64,
  updated_time STRING,
  campaign_id INT64,
  created_time STRING,
  tenant STRING,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  is_current BOOLEAN,
  _gn_id STRING
);

-- Extract latest snapshot from source
CREATE TEMP TABLE latest_snapshot AS
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY CAST(id AS INT64) ORDER BY updated_time DESC) AS rn
FROM `{{source_dataset}}.{{source_table_id}}`;

-- SCD Merge Logic
MERGE `{{target_dataset}}.{{target_table_id}}` AS target
USING (
  SELECT
    CAST(id AS INT64) AS id,
    name,
    CAST(account_id AS INT64) AS account_id,
    updated_time,
    CAST(campaign_id AS INT64) AS campaign_id,
    created_time,
    tenant,
    _time_extracted AS effective_from,
    CAST(NULL AS TIMESTAMP) AS effective_to,
    TRUE AS is_current,
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(id AS STRING),
      SAFE_CAST(name AS STRING),
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(updated_time AS STRING),
      SAFE_CAST(campaign_id AS STRING),
      SAFE_CAST(created_time AS STRING),
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
    id, name, account_id, updated_time, campaign_id, created_time, tenant, effective_from, effective_to, is_current, _gn_id
  )
  VALUES (
    source.id, source.name, source.account_id, source.updated_time, source.campaign_id, source.created_time, source.tenant, source.effective_from, source.effective_to, source.is_current, source._gn_id
  );

-- Drop the source table after successful processing
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
END IF; 