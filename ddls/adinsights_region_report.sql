CREATE TABLE `adsinsights_region_report`
(
  account_id STRING NOT NULL,
  account_name STRING,
  actions STRING,
  ad_id STRING NOT NULL,
  ad_name STRING,
  adset_id STRING,
  adset_name STRING,
  campaign_id STRING,
  campaign_name STRING,
  clicks STRING,
  date_start STRING NOT NULL,
  date_stop STRING,
  impressions STRING,
  spend STRING,
  region STRING NOT NULL,
  tenant STRING,
  _time_extracted TIMESTAMP,
  _time_loaded TIMESTAMP
);