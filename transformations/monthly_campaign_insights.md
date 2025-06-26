# Monthly Campaign Insights Report Table

This table provides a comprehensive view of Facebook Ads campaign performance at a monthly level, combining raw insights data with campaign and account metadata for analysis and reporting.

## Table Structure

| Column              | Type      | Description                                                                                  |
|---------------------|-----------|----------------------------------------------------------------------------------------------|
| date                | DATE      | The reporting date (monthly granularity)                                                     |
| campaign_id         | STRING    | The unique identifier for the Facebook Campaign                                              |
| campaign_name       | STRING    | The display name of the campaign                                                             |
| account_id          | STRING    | The unique identifier for the Facebook Ads account                                          |
| account_name        | STRING    | The display name of the account                                                              |
| cost                | FLOAT64   | Total spend for the campaign on this date                                                   |
| clicks              | INT64     | Total clicks for the campaign on this date                                                  |
| impressions         | INT64     | Total impressions for the campaign on this date                                             |
| reach               | INT64     | Total reach for the campaign on this date                                                   |
| frequency           | FLOAT64   | Average frequency for the campaign on this date                                             |
| last_synced_at      | TIMESTAMP | Timestamp when the data was last synced from Facebook                                       |
| last_data_date      | DATE      | The most recent date for which data is available                                            |
| _gn_id              | STRING    | Unique identifier for deduplication and change tracking                                     |

## Key Features

- **Monthly Granularity**: Data is aggregated at the monthly level for trend analysis
- **Campaign Metadata**: Includes campaign names and account information for easy reporting
- **Performance Metrics**: Comprehensive set of key performance indicators (cost, clicks, impressions, reach, frequency)
- **Incremental Updates**: Only processes new or changed data for efficiency
- **Data Quality**: Uses SAFE_CAST for robust data type conversion
- **Batch Processing**: Handles multiple data batches with deduplication logic

## Usage Examples

### Get Monthly Campaign Performance
```sql
SELECT 
  date,
  campaign_name,
  cost,
  clicks,
  impressions,
  reach,
  frequency
FROM `your_dataset.reports__monthly_campaign_insights`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
ORDER BY date DESC, cost DESC;
```

### Campaign Performance by Account
```sql
SELECT 
  account_name,
  campaign_name,
  SUM(cost) as total_cost,
  SUM(clicks) as total_clicks,
  SUM(impressions) as total_impressions,
  AVG(frequency) as avg_frequency
FROM `your_dataset.reports__monthly_campaign_insights`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
GROUP BY account_name, campaign_name
ORDER BY total_cost DESC;
```

### Monthly Trends Analysis
```sql
SELECT 
  FORMAT_DATE('%Y-%m', date) as month,
  SUM(cost) as total_cost,
  SUM(clicks) as total_clicks,
  SUM(impressions) as total_impressions,
  SUM(reach) as total_reach,
  AVG(frequency) as avg_frequency
FROM `your_dataset.reports__monthly_campaign_insights`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
GROUP BY month
ORDER BY month;
```

### Campaign Efficiency Metrics
```sql
SELECT 
  campaign_name,
  SUM(cost) as total_cost,
  SUM(clicks) as total_clicks,
  SUM(impressions) as total_impressions,
  SAFE_DIVIDE(SUM(cost), SUM(clicks)) as cost_per_click,
  SAFE_DIVIDE(SUM(cost), SUM(impressions)) * 1000 as cpm,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 as ctr
FROM `your_dataset.reports__monthly_campaign_insights`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY campaign_name
HAVING total_cost > 0
ORDER BY total_cost DESC;
```

## Data Processing

The table is updated incrementally using the following process:

1. **Source Validation**: Checks if the source table exists before processing
2. **Batch Identification**: Identifies the latest batch of data using timestamp logic
3. **Data Aggregation**: Sums metrics by campaign and date
4. **Metadata Enrichment**: Joins with campaign and account history tables for names
5. **Incremental Update**: Deletes existing data for the date range and inserts new data
6. **Transaction Safety**: Uses transactions to ensure data consistency

## Notes

- The table processes data from `adsinsights_monthly_campaign_insights` source table
- Campaign and account names are pulled from the respective history tables
- Metrics are aggregated at the campaign level for each monthly period
- The table includes reach and frequency metrics which are unique to campaign-level insights
- Data is deduplicated using a hash-based approach with `_gn_id`
- The table supports efficient querying for both historical analysis and current performance monitoring 