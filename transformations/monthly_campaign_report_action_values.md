# Monthly Campaign Report Action Values Table

This table provides a pivoted view of Facebook Ads campaign action values at a monthly level, unpacking the JSON `action_values` array from the source insights data into individual rows for analysis and reporting.

## Table Structure

| Column              | Type      | Description                                                                                  |
|---------------------|-----------|----------------------------------------------------------------------------------------------|
| campaign_id         | STRING    | The unique identifier for the Facebook Campaign                                              |
| date                | DATE      | The reporting date (monthly granularity)                                                     |
| _gn_id              | STRING    | Unique identifier for deduplication and change tracking                                     |
| account_id          | STRING    | The unique identifier for the Facebook Ads account                                          |
| action_type         | STRING    | The type of action (e.g., 'purchase', 'add_to_cart', 'lead')                                |
| value               | FLOAT64   | The total value for this action type (typically monetary)                                  |
| inline              | FLOAT64   | Inline action value (actions that happened immediately)                                     |
| _7_d_click          | FLOAT64   | 7-day click attribution window value                                                        |
| _1_d_view           | FLOAT64   | 1-day view attribution window value                                                         |

## Key Features

- **JSON Array Unpacking**: Transforms the nested `action_values` JSON array into individual rows
- **Monthly Granularity**: Data is aggregated at the monthly level for trend analysis
- **Value Attribution**: Includes different attribution windows (inline, 7-day click, 1-day view)
- **Incremental Updates**: Only processes new or changed data for efficiency
- **Batch Processing**: Handles multiple data batches with deduplication logic
- **Data Quality**: Uses SAFE_CAST for robust data type conversion

## Data Processing

The table unpacks Facebook's `action_values` JSON array which typically contains:
```json
[
  {
    "action_type": "purchase",
    "value": "1500.50",
    "inline": "1200.00",
    "7d_click": "250.00",
    "1d_view": "50.50"
  },
  {
    "action_type": "add_to_cart",
    "value": "750.25",
    "inline": "600.00",
    "7d_click": "100.00",
    "1d_view": "50.25"
  }
]
```

## Usage Examples

### Get Campaign Action Values Summary
```sql
SELECT 
  campaign_id,
  action_type,
  SUM(value) as total_value,
  SUM(inline) as inline_value,
  SUM(_7_d_click) as click_attributed_value,
  SUM(_1_d_view) as view_attributed_value
FROM `your_dataset.monthly_campaign_report_action_values`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
GROUP BY campaign_id, action_type
ORDER BY total_value DESC;
```

### Top Value-Generating Action Types
```sql
SELECT 
  action_type,
  SUM(value) as total_value,
  COUNT(DISTINCT campaign_id) as campaign_count
FROM `your_dataset.monthly_campaign_report_action_values`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY action_type
ORDER BY total_value DESC;
```

### Action Value Attribution Analysis
```sql
SELECT 
  action_type,
  SUM(value) as total_value,
  SUM(inline) as inline_value,
  SUM(_7_d_click) as click_attributed_value,
  SUM(_1_d_view) as view_attributed_value,
  SAFE_DIVIDE(SUM(inline), SUM(value)) * 100 as inline_percentage,
  SAFE_DIVIDE(SUM(_7_d_click), SUM(value)) * 100 as click_attribution_percentage,
  SAFE_DIVIDE(SUM(_1_d_view), SUM(value)) * 100 as view_attribution_percentage
FROM `your_dataset.monthly_campaign_report_action_values`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
GROUP BY action_type
HAVING total_value > 0
ORDER BY total_value DESC;
```

### Monthly Value Trends
```sql
SELECT 
  FORMAT_DATE('%Y-%m', date) as month,
  action_type,
  SUM(value) as total_value
FROM `your_dataset.monthly_campaign_report_action_values`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
GROUP BY month, action_type
ORDER BY month, total_value DESC;
```

### Campaign Revenue Performance
```sql
SELECT 
  campaign_id,
  SUM(CASE WHEN action_type = 'purchase' THEN value ELSE 0 END) as purchase_value,
  SUM(CASE WHEN action_type = 'add_to_cart' THEN value ELSE 0 END) as cart_value,
  SUM(CASE WHEN action_type = 'lead' THEN value ELSE 0 END) as lead_value,
  SUM(value) as total_value
FROM `your_dataset.monthly_campaign_report_action_values`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY campaign_id
ORDER BY total_value DESC;
```

### ROI Analysis by Attribution Window
```sql
WITH campaign_spend AS (
  SELECT 
    campaign_id,
    SUM(spend) as total_spend
  FROM `your_dataset.monthly_campaign_insights`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
  GROUP BY campaign_id
)
SELECT 
  av.campaign_id,
  cs.total_spend,
  SUM(av.value) as total_value,
  SUM(av.inline) as inline_value,
  SUM(av._7_d_click) as click_attributed_value,
  SUM(av._1_d_view) as view_attributed_value,
  SAFE_DIVIDE(SUM(av.value), cs.total_spend) as overall_roas,
  SAFE_DIVIDE(SUM(av.inline), cs.total_spend) as inline_roas,
  SAFE_DIVIDE(SUM(av._7_d_click), cs.total_spend) as click_roas,
  SAFE_DIVIDE(SUM(av._1_d_view), cs.total_spend) as view_roas
FROM `your_dataset.monthly_campaign_report_action_values` av
JOIN campaign_spend cs ON av.campaign_id = cs.campaign_id
WHERE av.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY av.campaign_id, cs.total_spend
HAVING total_spend > 0
ORDER BY overall_roas DESC;
```

## Common Action Value Types

- **purchase**: Purchase value/revenue
- **add_to_cart**: Add to cart value
- **lead**: Lead value/conversion value
- **signup**: Signup value
- **download**: Download value
- **video_view**: Video view value
- **page_engagement**: Page engagement value

## Notes

- The table processes data from `adsinsights_monthly_campaign_insights` source table
- Only rows with valid `action_type` values are included (WHERE clause filters out NULLs)
- The `_gn_id` hash includes campaign_id, date, account_id, and action_type for unique identification
- Attribution windows help understand when value-generating actions occurred relative to ad exposure
- Values are typically monetary but can represent other business metrics
- The table supports efficient querying for both historical analysis and current performance monitoring
- Data is deduplicated using batch processing logic to handle multiple data loads
- This table is particularly useful for ROI analysis and revenue attribution studies 