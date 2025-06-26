CREATE TABLE `adsinsights_monthly_campaign_insights`
(
  account_id STRING NOT NULL,
  account_name STRING,
  actions STRING,
  campaign_id STRING NOT NULL,
  campaign_name STRING,
  clicks STRING,
  date_start STRING NOT NULL,
  frequency STRING,
  impressions STRING,
  reach STRING,
  spend STRING,
  tenant STRING,
  _time_extracted TIMESTAMP,
  _time_loaded TIMESTAMP
);