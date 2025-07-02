-- custom_conversions_history
-- SCD Type 2 Table for Facebook Ads Custom Conversions
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'custom_conversions_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'customconversions' %}

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
  account_id STRING,
  id STRING NOT NULL,
  name STRING,
  creation_time STRING,
  last_fired_time STRING,
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
  ROW_NUMBER() OVER (PARTITION BY id ORDER BY _time_extracted DESC) AS rn
FROM `{{source_dataset}}.{{source_table_id}}`;

-- SCD Merge Logic
MERGE `{{target_dataset}}.{{target_table_id}}` AS target
USING (
  SELECT
    account_id,
    id,
    name,
    creation_time,
    last_fired_time,
    tenant,
    _time_extracted AS effective_from,
    CAST(NULL AS TIMESTAMP) AS effective_to,
    TRUE AS is_current,
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(id AS STRING),
      SAFE_CAST(name AS STRING),
      SAFE_CAST(creation_time AS STRING),
      SAFE_CAST(last_fired_time AS STRING)
    ]))) AS _gn_id
  FROM latest_snapshot
  WHERE rn = 1
) AS source
ON target.id = source.id AND target.is_current = TRUE
WHEN MATCHED AND
  TO_HEX(MD5(TO_JSON_STRING([
    SAFE_CAST(target.account_id AS STRING),
    SAFE_CAST(target.id AS STRING),
    SAFE_CAST(target.name AS STRING),
    SAFE_CAST(target.creation_time AS STRING),
    SAFE_CAST(target.last_fired_time AS STRING)
  ]))) !=
  TO_HEX(MD5(TO_JSON_STRING([
    SAFE_CAST(source.account_id AS STRING),
    SAFE_CAST(source.id AS STRING),
    SAFE_CAST(source.name AS STRING),
    SAFE_CAST(source.creation_time AS STRING),
    SAFE_CAST(source.last_fired_time AS STRING)
  ])))
  THEN UPDATE SET
    effective_to = source.effective_from,
    is_current = FALSE
WHEN NOT MATCHED BY TARGET
  THEN INSERT (
    account_id, id, name, creation_time, last_fired_time, tenant, effective_from, effective_to, is_current, _gn_id
  )
  VALUES (
    source.account_id, source.id, source.name, source.creation_time, source.last_fired_time, source.tenant, source.effective_from, source.effective_to, source.is_current, source._gn_id
  );

END IF; 