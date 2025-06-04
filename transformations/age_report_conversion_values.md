# Age Report Conversion Values Table

This table contains pivoted conversion value data from Facebook Ads age demographic reporting, specifically unnesting the `conversion_values` JSON array from the age report source. Each row represents a single conversion type and its associated value for a given ad, date, age group, and account.

## Table Structure

| Column      | Type      | Description                                                                 |
|-------------|-----------|-----------------------------------------------------------------------------|
| ad_id       | STRING    | The unique identifier for the Facebook Ad                                   |
| date        | DATE      | The reporting date for the record                                           |
| _gn_id      | STRING    | Hash of key dimensions for deduplication and uniqueness                     |
| _gn_synced  | TIMESTAMP | Timestamp of when the record was last synced                                |
| account_id  | INT64     | The numeric ID of the account the ad belongs to                             |
| action_type | STRING    | The type of conversion (e.g., `purchase`, `lead`, `add_to_cart`, etc.)      |
| value       | FLOAT64   | The value associated with the conversion (e.g., revenue, etc.)              |
| inline      | FLOAT64   | Inline value for the conversion, if present                                 |
| _7_d_click  | FLOAT64   | 7-day click attribution value, if present                                   |
| _1_d_view   | FLOAT64   | 1-day view attribution value, if present                                    |
| age         | STRING    | The age demographic group (e.g., "18-24", "25-34", etc.)                    |
| tenant      | STRING    | Tenant identifier (for multi-tenant environments)                           |

## What are Facebook Conversion Values?

In Facebook Ads reporting, `conversion_values` represent the numeric value associated with specific conversion actions, such as revenue from purchases or value assigned to leads, as tracked by the Facebook pixel or SDK.

**Common `action_type` values include:**
- `purchase`: Completed purchases attributed to the ad (typically revenue)
- `lead`: Lead form submissions (may have assigned value)
- `add_to_cart`: Add-to-cart events
- `initiate_checkout`: Checkout initiation events
- `complete_registration`: Completed registration events
- `view_content`: Views of a key page or content
- `search`: Searches performed on your site
- `add_payment_info`: Addition of payment information
- Custom conversion events as defined in your Facebook pixel setup

The `value` field typically represents the monetary value associated with the conversion (e.g., total revenue for purchases, assigned value for leads, etc.).

## How to Use This Table

- **Conversion Value Analysis**: Aggregate `value` by `action_type` to see total revenue or value driven by your ads in each age demographic.
- **Attribution Window Analysis**: Use `_7_d_click` and `_1_d_view` columns to break down conversion values by attribution window (7-day click, 1-day view).
- **Age/Ad/Account Performance**: Join with ad, campaign, or account dimension tables using `ad_id` and `account_id` to analyze performance by creative, campaign, or account within each age demographic.
- **Custom Conversion Value Tracking**: If you use custom conversion events, those will appear as additional `action_type` values.
- **Tenant Filtering**: Use the `tenant` column to filter results for specific clients or business units in multi-tenant environments.

## Notes

- The `_gn_id` column is a deterministic hash of key dimensions (`ad_id`, `date`, `account_id`, `action_type`, `age`) for uniqueness and deduplication.
- All fields used in the hash are cast to STRING for type safety and consistency.
- All JSON fields are safely extracted and cast to their appropriate types.
- The table is designed for easy joins with other Facebook Ads reporting and dimension tables. 