# Region Report Action Values Table

This table contains pivoted action value data from Facebook Ads region-level reporting, specifically unnesting the `action_values` JSON array from the region report source. Each row represents a single action type and its associated value for a given ad, date, account, and region.

## Table Structure

| Column      | Type      | Description                                                                 |
|-------------|-----------|-----------------------------------------------------------------------------|
| ad_id       | STRING    | The unique identifier for the Facebook Ad                                   |
| date        | DATE      | The reporting date for the record                                           |
| _gn_id      | STRING    | Hash of key dimensions for deduplication and uniqueness                     |
| _gn_synced  | TIMESTAMP | Timestamp of when the record was last synced                                |
| account_id  | STRING    | The ID of the account the ad belongs to                                     |
| action_type | STRING    | The type of action (e.g., `link_click`, `purchase`, etc.)                   |
| value       | FLOAT64   | The value associated with the action (e.g., revenue, conversions, etc.)     |
| inline      | FLOAT64   | Inline value for the action, if present                                     |
| _7_d_click  | FLOAT64   | 7-day click attribution value, if present                                   |
| _1_d_view   | FLOAT64   | 1-day view attribution value, if present                                    |
| region      | STRING    | The geographic region for the record                                        |
| tenant      | STRING    | Tenant identifier (for multi-tenant environments)                           |

## How to Use This Table

- **Regional Action Value Analysis**: Aggregate or filter by `region`, `ad_id`, `account_id`, `date`, or `action_type` to analyze action values by region.
- **KPI Tracking**: Use `value` to calculate total or average values for specific actions in each region.
- **Join with Dimensions**: Join with ad, campaign, or account dimension tables using `ad_id` and `account_id` for richer analysis.
- **Tenant Filtering**: Use the `tenant` column to filter results for specific clients or business units in multi-tenant environments.

## Notes

- The `_gn_id` column is a deterministic hash of key dimensions (`ad_id`, `date`, `account_id`, `action_type`, `region`) for uniqueness and deduplication.
- All fields used in the hash are cast to STRING for type safety and consistency.
- All JSON fields are safely extracted and cast to their appropriate types.
- The table is designed for easy joins with other Facebook Ads reporting and dimension tables. 