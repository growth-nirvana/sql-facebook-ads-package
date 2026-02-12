-- campaign_history
-- Table for Facebook Ads Campaigns
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'campaign_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'campaigns' %}

-- Guard clause: check if source table exists
DECLARE table_exists BOOL DEFAULT FALSE;
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

IF table_exists THEN
-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  id STRING NOT NULL,
  name STRING,
  account_id STRING,
  updated_time STRING,
  created_time STRING,
  tenant STRING,
  _gn_id STRING
);

-- Extract latest snapshot from source
CREATE TEMP TABLE latest_snapshot AS
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_time DESC) AS rn
FROM `{{source_dataset}}.{{source_table_id}}`;

-- Merge Logic
MERGE `{{target_dataset}}.{{target_table_id}}` AS target
USING (
  SELECT
    id,
    name,
    account_id,
    updated_time,
    created_time,
    tenant,
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(id AS STRING),
      SAFE_CAST(name AS STRING),
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(updated_time AS STRING),
      SAFE_CAST(created_time AS STRING),
      SAFE_CAST(tenant AS STRING)
    ]))) AS _gn_id
  FROM latest_snapshot
  WHERE rn = 1
) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET
    name = source.name,
    account_id = source.account_id,
    updated_time = source.updated_time,
    created_time = source.created_time,
    tenant = source.tenant,
    _gn_id = source._gn_id
WHEN NOT MATCHED BY TARGET
  THEN INSERT (
    id, name, account_id, updated_time, created_time, tenant, _gn_id
  )
  VALUES (
    source.id, source.name, source.account_id, source.updated_time, source.created_time, source.tenant, source._gn_id
  );

-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
END IF;