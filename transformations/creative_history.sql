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
    actor_id,
    asset_feed_spec,
    authorization_category,
    body,
    call_to_action_type,
    degrees_of_freedom_spec,
    effective_authorization_category,
    effective_instagram_media_id,
    effective_object_story_id,
    enable_direct_install,
    image_hash,
    image_url,
    instagram_permalink_url,
    link_og_id,
    link_url,
    name,
    object_id,
    object_story_id,
    object_story_spec,
    object_type,
    status,
    thumbnail_id,
    thumbnail_url,
    title,
    use_page_actor_override,
    video_id,
    tenant,
    _time_extracted AS effective_from,
    CAST(NULL AS TIMESTAMP) AS effective_to,
    TRUE AS is_current,
    TO_HEX(MD5(TO_JSON_STRING([
      SAFE_CAST(id AS STRING),
      SAFE_CAST(account_id AS STRING),
      SAFE_CAST(actor_id AS STRING),
      SAFE_CAST(asset_feed_spec AS STRING),
      SAFE_CAST(authorization_category AS STRING),
      SAFE_CAST(body AS STRING),
      SAFE_CAST(call_to_action_type AS STRING),
      SAFE_CAST(degrees_of_freedom_spec AS STRING),
      SAFE_CAST(effective_authorization_category AS STRING),
      SAFE_CAST(effective_instagram_media_id AS STRING),
      SAFE_CAST(effective_object_story_id AS STRING),
      SAFE_CAST(enable_direct_install AS STRING),
      SAFE_CAST(image_hash AS STRING),
      SAFE_CAST(image_url AS STRING),
      SAFE_CAST(instagram_permalink_url AS STRING),
      SAFE_CAST(link_og_id AS STRING),
      SAFE_CAST(link_url AS STRING),
      SAFE_CAST(name AS STRING),
      SAFE_CAST(object_id AS STRING),
      SAFE_CAST(object_story_id AS STRING),
      SAFE_CAST(object_story_spec AS STRING),
      SAFE_CAST(object_type AS STRING),
      SAFE_CAST(status AS STRING),
      SAFE_CAST(thumbnail_id AS STRING),
      SAFE_CAST(thumbnail_url AS STRING),
      SAFE_CAST(title AS STRING),
      SAFE_CAST(use_page_actor_override AS STRING),
      SAFE_CAST(video_id AS STRING),
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
    id, account_id, actor_id, asset_feed_spec, authorization_category, body, call_to_action_type, degrees_of_freedom_spec, effective_authorization_category, effective_instagram_media_id, effective_object_story_id, enable_direct_install, image_hash, image_url, instagram_permalink_url, link_og_id, link_url, name, object_id, object_story_id, object_story_spec, object_type, status, thumbnail_id, thumbnail_url, title, use_page_actor_override, video_id, tenant, effective_from, effective_to, is_current, _gn_id
  )
  VALUES (
    source.id, source.account_id, source.actor_id, source.asset_feed_spec, source.authorization_category, source.body, source.call_to_action_type, source.degrees_of_freedom_spec, source.effective_authorization_category, source.effective_instagram_media_id, source.effective_object_story_id, source.enable_direct_install, source.image_hash, source.image_url, source.instagram_permalink_url, source.link_og_id, source.link_url, source.name, source.object_id, source.object_story_id, source.object_story_spec, source.object_type, source.status, source.thumbnail_id, source.thumbnail_url, source.title, source.use_page_actor_override, source.video_id, source.tenant, source.effective_from, source.effective_to, source.is_current, source._gn_id
  );

-- Drop the source table after successful insertion
-- DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;
END IF; 