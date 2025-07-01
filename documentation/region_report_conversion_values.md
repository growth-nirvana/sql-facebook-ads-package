# Region Report Conversion Values Table

This table contains Facebook Ads reporting data at the region level, with one row per ad, date, region, conversion type, and account. It provides conversion value metrics for each ad within a specific region, enabling detailed analysis of conversion values by geographic area.

## Table Structure

| Column           | Type      | Description                                                                 |
|------------------|-----------|-----------------------------------------------------------------------------|
| ad_id            | STRING    | The unique identifier for the Facebook Ad                                   |
| date             | DATE      | The reporting date for the record                                           |
| account_id       | STRING    | The ID of the account the ad belongs to                                     |
| conversion_type  | STRING    | The type of conversion (e.g., "purchase", "lead")                         |
| conversion_value | FLOAT64   | The value associated with the conversion type                               |
| region           | STRING    | The geographic region for the record                                        |
| tenant           | STRING    | Tenant identifier (for multi-tenant environments)                           |
| _gn_id           | STRING    | Hash of key dimensions for deduplication and uniqueness                     |
| _gn_synced       | TIMESTAMP | Timestamp of when the record was last synced                                |

## How to Use This Table

- **Regional Conversion Value Analysis**: Aggregate or filter by `region`, `ad_id`, `account_id`, `date`, or `conversion_type` to analyze conversion values by region.
- **KPI Tracking**: Use `conversion_value` to calculate total or average values for specific conversions in each region.
- **Join with Dimensions**: Join with ad, campaign, or account dimension tables using `ad_id` and `account_id` for richer analysis.
- **Tenant Filtering**: Use the `tenant` column to filter results for specific clients or business units in multi-tenant environments.

## Notes

- The `_gn_id` column is a deterministic hash of key dimensions (`ad_id`, `date`, `account_id`, `region`, `conversion_type`) for uniqueness and deduplication.
- All fields used in the hash are cast to STRING for type safety and consistency.
- The table is designed for easy joins with other Facebook Ads reporting and dimension tables. 