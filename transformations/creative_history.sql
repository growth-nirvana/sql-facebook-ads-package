-- creative_history
-- SCD Type 2 Table for Facebook Ads Creatives
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'creative_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'creatives' %}

-- Guard clause: check if source table exists
DECLARE table_exists BOOL DEFAULT FALSE;
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

IF table_exists THEN

ALTER TABLE `{{source_dataset}}.{{source_table_id}}`
  ADD COLUMN IF NOT EXISTS account_id STRING,
  ADD COLUMN IF NOT EXISTS actor_id STRING,
  ADD COLUMN IF NOT EXISTS asset_feed_spec STRING,
  ADD COLUMN IF NOT EXISTS authorization_category STRING,
  ADD COLUMN IF NOT EXISTS body STRING,
  ADD COLUMN IF NOT EXISTS call_to_action_type STRING,
  ADD COLUMN IF NOT EXISTS degrees_of_freedom_spec STRING,
  ADD COLUMN IF NOT EXISTS effective_authorization_category STRING,
  ADD COLUMN IF NOT EXISTS effective_instagram_media_id STRING,
  ADD COLUMN IF NOT EXISTS effective_object_story_id STRING,
  ADD COLUMN IF NOT EXISTS enable_direct_install BOOL,
  ADD COLUMN IF NOT EXISTS image_hash STRING,
  ADD COLUMN IF NOT EXISTS image_url STRING,
  ADD COLUMN IF NOT EXISTS instagram_permalink_url STRING,
  ADD COLUMN IF NOT EXISTS link_og_id STRING,
  ADD COLUMN IF NOT EXISTS link_url STRING,
  ADD COLUMN IF NOT EXISTS name STRING,
  ADD COLUMN IF NOT EXISTS object_id STRING,
  ADD COLUMN IF NOT EXISTS object_story_id STRING,
  ADD COLUMN IF NOT EXISTS object_story_spec STRING,
  ADD COLUMN IF NOT EXISTS object_type STRING,
  ADD COLUMN IF NOT EXISTS status STRING,
  ADD COLUMN IF NOT EXISTS thumbnail_id STRING,
  ADD COLUMN IF NOT EXISTS thumbnail_url STRING,
  ADD COLUMN IF NOT EXISTS title STRING,
  ADD COLUMN IF NOT EXISTS use_page_actor_override BOOL,
  ADD COLUMN IF NOT EXISTS video_id STRING;

-- Create SCD table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  id STRING NOT NULL,
  account_id STRING,
  actor_id STRING,
  asset_feed_spec STRING,
  authorization_category STRING,
  body STRING,
  call_to_action_type STRING,
  degrees_of_freedom_spec STRING,
  effective_authorization_category STRING,
  effective_instagram_media_id STRING,
  effective_object_story_id STRING,
  enable_direct_install BOOL,
  image_hash STRING,
  image_url STRING,
  instagram_permalink_url STRING,
  link_og_id STRING,
  link_url STRING,
  name STRING,
  object_id STRING,
  object_story_id STRING,
  object_story_spec STRING,
  object_type STRING,
  status STRING,
  thumbnail_id STRING,
  thumbnail_url STRING,
  title STRING,
  use_page_actor_override BOOL,
  video_id STRING,
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
    image_hash,
    image_url,
    instagram_permalink_url,
    name,
    title,
    tenant,
    _time_extracted AS effective_from,
    CAST(NULL AS TIMESTAMP) AS effective_to,
    TRUE AS is_current,
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(id AS STRING),
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(image_hash AS STRING),
      SAFE_CAST(image_url AS STRING),
      SAFE_CAST(instagram_permalink_url AS STRING),
      SAFE_CAST(name AS STRING),
      SAFE_CAST(title AS STRING),
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
    SAFE_CAST(target.image_hash AS STRING),
    SAFE_CAST(target.image_url AS STRING),
    SAFE_CAST(target.instagram_permalink_url AS STRING),
    SAFE_CAST(target.name AS STRING),
    SAFE_CAST(target.title AS STRING),
    SAFE_CAST(target.tenant AS STRING)
  ]))) !=
  TO_HEX(MD5(TO_JSON_STRING([
    SAFE_CAST(source.id AS STRING),
    SAFE_CAST(source.account_id AS STRING),
    SAFE_CAST(source.image_hash AS STRING),
    SAFE_CAST(source.image_url AS STRING),
    SAFE_CAST(source.instagram_permalink_url AS STRING),
    SAFE_CAST(source.name AS STRING),
    SAFE_CAST(source.title AS STRING),
    SAFE_CAST(source.tenant AS STRING)
  ])))
  THEN UPDATE SET
    effective_to = source.effective_from,
    is_current = FALSE
WHEN NOT MATCHED BY TARGET
  THEN INSERT (
    id, account_id, image_hash, image_url, instagram_permalink_url, name, title, tenant, effective_from, effective_to, is_current, _gn_id
  )
  VALUES (
    source.id, source.account_id, source.image_hash, source.image_url, source.instagram_permalink_url, source.name, source.title, source.tenant, source.effective_from, source.effective_to, source.is_current, source._gn_id
  );

-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
END IF; 