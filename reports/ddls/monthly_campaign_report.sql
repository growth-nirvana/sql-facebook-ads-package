CREATE TABLE `monthly_campaign_report`
(
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