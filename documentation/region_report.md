# Region Report Table

This table contains Facebook Ads reporting data at the region level, with one row per ad, date, region, and account. It provides key performance metrics for each ad within a specific geographic region, enabling detailed regional analysis.

## Table Structure

| Column            | Type      | Description                                                                 |
|-------------------|-----------|-----------------------------------------------------------------------------|
| ad_id             | STRING    | The unique identifier for the Facebook Ad                                   |
| date              | DATE      | The reporting date for the record                                           |
| _gn_id            | STRING    | Hash of key dimensions for deduplication and uniqueness                     |
| _gn_synced        | TIMESTAMP | Timestamp of when the record was last synced                                |
| account_id        | STRING    | The ID of the account the ad belongs to                                     |
| clicks            | INT64     | Number of clicks the ad received                                            |
| impressions       | INT64     | Number of times the ad was shown                                            |
| spend             | FLOAT64   | Amount spent on the ad (in account currency)                                |
| region            | STRING    | The geographic region for the record                                        |
| tenant            | STRING    | Tenant identifier (for multi-tenant environments)                           |

## How to Use This Table

- **Regional Performance Analysis**: Aggregate or filter by `region`, `ad_id`, `account_id`, or `date` to analyze ad performance by region.
- **KPI Tracking**: Use `clicks`, `impressions`, and `spend` to calculate key metrics such as CTR (click-through rate), CPC (cost per click), and CPM (cost per mille) at the regional level.
- **Join with Dimensions**: Join with ad, campaign, or account dimension tables using `ad_id` and `account_id` for richer analysis.
- **Tenant Filtering**: Use the `tenant` column to filter results for specific clients or business units in multi-tenant environments.

## Notes

- The `_gn_id` column is a deterministic hash of key dimensions (`ad_id`, `date`, `account_id`, `region`) for uniqueness and deduplication.
- All fields used in the hash are cast to STRING for type safety and consistency.
- The table is designed for easy joins with other Facebook Ads reporting and dimension tables. 