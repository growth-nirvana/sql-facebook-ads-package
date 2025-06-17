-- adimages_history
-- SCD Type 2 Table for Facebook Ads Ad Images
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_image_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'adimages' %}

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
  id STRING NOT NULL,
  account_id STRING,
  created_time STRING,
  creatives STRING,
  `hash` STRING,
  height INT64,
  is_associated_creatives_in_adgroups BOOL,
  name STRING,
  original_height INT64,
  original_width INT64,
  permalink_url STRING,
  status STRING,
  updated_time STRING,
  url STRING,
  url_128 STRING,
  width INT64,
  run_id INT64,
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
    id,
    account_id,
    created_time,
    creatives,
    `hash`,
    height,
    is_associated_creatives_in_adgroups,
    name,
    original_height,
    original_width,
    permalink_url,
    status,
    updated_time,
    url,
    url_128,
    width,
    run_id,
    tenant,
    _time_extracted AS effective_from,
    CAST(NULL AS TIMESTAMP) AS effective_to,
    TRUE AS is_current,
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(id AS STRING),
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(created_time AS STRING),
      SAFE_CAST(creatives AS STRING),
      SAFE_CAST(`hash` AS STRING),
      SAFE_CAST(height AS STRING),
      SAFE_CAST(is_associated_creatives_in_adgroups AS STRING),
      SAFE_CAST(name AS STRING),
      SAFE_CAST(original_height AS STRING),
      SAFE_CAST(original_width AS STRING),
      SAFE_CAST(permalink_url AS STRING),
      SAFE_CAST(status AS STRING),
      SAFE_CAST(updated_time AS STRING),
      SAFE_CAST(url AS STRING),
      SAFE_CAST(url_128 AS STRING),
      SAFE_CAST(width AS STRING),
      SAFE_CAST(tenant AS STRING)
    ]))) AS _gn_id
  FROM latest_snapshot
  WHERE rn = 1
) AS source
ON target.id = source.id AND target.is_current = TRUE
WHEN MATCHED AND
  TO_HEX(MD5(TO_JSON_STRING([
    SAFE_CAST(target.id AS STRING),
    SAFE_CAST(target.account_id AS STRING),
    SAFE_CAST(target.created_time AS STRING),
    SAFE_CAST(target.creatives AS STRING),
    SAFE_CAST(target.`hash` AS STRING),
    SAFE_CAST(target.height AS STRING),
    SAFE_CAST(target.is_associated_creatives_in_adgroups AS STRING),
    SAFE_CAST(target.name AS STRING),
    SAFE_CAST(target.original_height AS STRING),
    SAFE_CAST(target.original_width AS STRING),
    SAFE_CAST(target.permalink_url AS STRING),
    SAFE_CAST(target.status AS STRING),
    SAFE_CAST(target.updated_time AS STRING),
    SAFE_CAST(target.url AS STRING),
    SAFE_CAST(target.url_128 AS STRING),
    SAFE_CAST(target.width AS STRING),
    SAFE_CAST(target.tenant AS STRING)
  ]))) !=
  TO_HEX(MD5(TO_JSON_STRING([
    SAFE_CAST(source.id AS STRING),
    SAFE_CAST(source.account_id AS STRING),
    SAFE_CAST(source.created_time AS STRING),
    SAFE_CAST(source.creatives AS STRING),
    SAFE_CAST(source.`hash` AS STRING),
    SAFE_CAST(source.height AS STRING),
    SAFE_CAST(source.is_associated_creatives_in_adgroups AS STRING),
    SAFE_CAST(source.name AS STRING),
    SAFE_CAST(source.original_height AS STRING),
    SAFE_CAST(source.original_width AS STRING),
    SAFE_CAST(source.permalink_url AS STRING),
    SAFE_CAST(source.status AS STRING),
    SAFE_CAST(source.updated_time AS STRING),
    SAFE_CAST(source.url AS STRING),
    SAFE_CAST(source.url_128 AS STRING),
    SAFE_CAST(source.width AS STRING),
    SAFE_CAST(source.tenant AS STRING)
  ])))
  THEN UPDATE SET
    effective_to = source.effective_from,
    is_current = FALSE
WHEN NOT MATCHED BY TARGET
  THEN INSERT (
    id, account_id, created_time, creatives, `hash`, height, is_associated_creatives_in_adgroups, name, original_height, original_width, permalink_url, status, updated_time, url, url_128, width, run_id, tenant, effective_from, effective_to, is_current, _gn_id
  )
  VALUES (
    source.id, source.account_id, source.created_time, source.creatives, source.`hash`, source.height, source.is_associated_creatives_in_adgroups, source.name, source.original_height, source.original_width, source.permalink_url, source.status, source.updated_time, source.url, source.url_128, source.width, source.run_id, source.tenant, source.effective_from, source.effective_to, source.is_current, source._gn_id
  );

-- Drop the source table after successful processing
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
END IF; 