# Age Report Table

This table contains Facebook Ads reporting data at the age demographic level, with one row per ad, date, age group, and account. It provides key performance metrics for each ad within a specific age demographic, enabling detailed demographic analysis.

## Table Structure

| Column      | Type      | Description                                                                 |
|-------------|-----------|-----------------------------------------------------------------------------|
| ad_id       | STRING    | The unique identifier for the Facebook Ad                                   |
| date        | DATE      | The reporting date for the record                                           |
| _gn_id      | STRING    | Hash of key dimensions for deduplication and uniqueness                     |
| _gn_synced  | TIMESTAMP | Timestamp of when the record was last synced                                |
| account_id  | INT64     | The numeric ID of the account the ad belongs to                             |
| clicks      | INT64     | Number of clicks the ad received                                            |
| impressions | INT64     | Number of times the ad was shown                                            |
| spend       | FLOAT64   | Amount spent on the ad (in account currency)                                |
| age         | STRING    | The age demographic group (e.g., "18-24", "25-34", etc.)                    |
| tenant      | STRING    | Tenant identifier (for multi-tenant environments)                           |

## How to Use This Table

- **Age Demographic Analysis**: Aggregate or filter by `age`, `ad_id`, `account_id`, or `date` to analyze ad performance by age group.
- **KPI Tracking**: Use `clicks`, `impressions`, and `spend` to calculate key metrics such as CTR (click-through rate), CPC (cost per click), and CPM (cost per mille) at the age demographic level.
- **Join with Dimensions**: Join with ad, campaign, or account dimension tables using `ad_id` and `account_id` for richer analysis.
- **Tenant Filtering**: Use the `tenant` column to filter results for specific clients or business units in multi-tenant environments.

## Notes

- The `_gn_id` column is a deterministic hash of key dimensions (`ad_id`, `date`, `account_id`, `age`) for uniqueness and deduplication.
- All fields used in the hash are cast to STRING for type safety and consistency.
- The table is designed for easy joins with other Facebook Ads reporting and dimension tables. 