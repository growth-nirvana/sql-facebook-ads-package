# Monthly Campaign Report Actions Table

This table provides a pivoted view of Facebook Ads campaign actions at a monthly level, unpacking the JSON `actions` array from the source insights data into individual rows for analysis and reporting.

## Table Structure

| Column              | Type      | Description                                                                                  |
|---------------------|-----------|----------------------------------------------------------------------------------------------|
| campaign_id         | STRING    | The unique identifier for the Facebook Campaign                                              |
| date                | DATE      | The reporting date (monthly granularity)                                                     |
| _gn_id              | STRING    | Unique identifier for deduplication and change tracking                                     |
| account_id          | STRING    | The unique identifier for the Facebook Ads account                                          |
| action_type         | STRING    | The type of action (e.g., 'link_click', 'post_engagement', 'page_engagement')               |
| value               | FLOAT64   | The total count/value for this action type                                                  |
| inline              | FLOAT64   | Inline action count (actions that happened immediately)                                     |
| _7_d_click          | FLOAT64   | 7-day click attribution window count                                                        |
| _1_d_view           | FLOAT64   | 1-day view attribution window count                                                         |

## Key Features

- **JSON Array Unpacking**: Transforms the nested `actions` JSON array into individual rows
- **Monthly Granularity**: Data is aggregated at the monthly level for trend analysis
- **Action Attribution**: Includes different attribution windows (inline, 7-day click, 1-day view)
- **Incremental Updates**: Only processes new or changed data for efficiency
- **Batch Processing**: Handles multiple data batches with deduplication logic
- **Data Quality**: Uses SAFE_CAST for robust data type conversion

## Data Processing

The table unpacks Facebook's `actions` JSON array which typically contains:
```json
[
  {
    "action_type": "link_click",
    "value": "150",
    "inline": "120",
    "7d_click": "25",
    "1d_view": "5"
  },
  {
    "action_type": "post_engagement",
    "value": "75",
    "inline": "60",
    "7d_click": "10",
    "1d_view": "5"
  }
]
```

## Usage Examples

### Get Campaign Actions Summary
```sql
SELECT 
  campaign_id,
  action_type,
  SUM(value) as total_actions,
  SUM(inline) as inline_actions,
  SUM(_7_d_click) as click_attributed,
  SUM(_1_d_view) as view_attributed
FROM `your_dataset.monthly_campaign_report_actions`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
GROUP BY campaign_id, action_type
ORDER BY total_actions DESC;
```

### Top Action Types by Campaign
```sql
SELECT 
  campaign_id,
  action_type,
  SUM(value) as total_actions
FROM `your_dataset.monthly_campaign_report_actions`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY campaign_id, action_type
QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY SUM(value) DESC) = 1
ORDER BY total_actions DESC;
```

### Action Attribution Analysis
```sql
SELECT 
  action_type,
  SUM(value) as total_actions,
  SUM(inline) as inline_actions,
  SUM(_7_d_click) as click_attributed,
  SUM(_1_d_view) as view_attributed,
  SAFE_DIVIDE(SUM(inline), SUM(value)) * 100 as inline_percentage,
  SAFE_DIVIDE(SUM(_7_d_click), SUM(value)) * 100 as click_attribution_percentage,
  SAFE_DIVIDE(SUM(_1_d_view), SUM(value)) * 100 as view_attribution_percentage
FROM `your_dataset.monthly_campaign_report_actions`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
GROUP BY action_type
HAVING total_actions > 0
ORDER BY total_actions DESC;
```

### Monthly Action Trends
```sql
SELECT 
  FORMAT_DATE('%Y-%m', date) as month,
  action_type,
  SUM(value) as total_actions
FROM `your_dataset.monthly_campaign_report_actions`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
GROUP BY month, action_type
ORDER BY month, total_actions DESC;
```

### Campaign Performance by Action Type
```sql
SELECT 
  campaign_id,
  SUM(CASE WHEN action_type = 'link_click' THEN value ELSE 0 END) as link_clicks,
  SUM(CASE WHEN action_type = 'post_engagement' THEN value ELSE 0 END) as post_engagements,
  SUM(CASE WHEN action_type = 'page_engagement' THEN value ELSE 0 END) as page_engagements,
  SUM(CASE WHEN action_type = 'video_view' THEN value ELSE 0 END) as video_views
FROM `your_dataset.monthly_campaign_report_actions`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY campaign_id
ORDER BY link_clicks DESC;
```

## Common Action Types

- **link_click**: Clicks on links in ads
- **post_engagement**: Engagements with posts (likes, comments, shares)
- **page_engagement**: Engagements with the page
- **video_view**: Video views
- **photo_view**: Photo views
- **onsite_conversion**: On-site conversions
- **offsite_conversion**: Off-site conversions

## Notes

- The table processes data from `adsinsights_monthly_campaign_insights` source table
- Only rows with valid `action_type` values are included (WHERE clause filters out NULLs)
- The `_gn_id` hash includes campaign_id, date, account_id, and action_type for unique identification
- Attribution windows help understand when actions occurred relative to ad exposure
- The table supports efficient querying for both historical analysis and current performance monitoring
- Data is deduplicated using batch processing logic to handle multiple data loads 