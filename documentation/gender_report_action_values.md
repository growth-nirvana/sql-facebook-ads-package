# Gender Report Action Values Table

This table contains pivoted action value data from Facebook Ads gender demographic reporting, specifically unnesting the `action_values` JSON array from the gender report source. Each row represents a single action type and its associated value for a given ad, date, gender, and account.

## Table Structure

| Column      | Type      | Description                                                                 |
|-------------|-----------|-----------------------------------------------------------------------------|
| ad_id       | STRING    | The unique identifier for the Facebook Ad                                   |
| date        | DATE      | The reporting date for the record                                           |
| _gn_id      | STRING    | Hash of key dimensions for deduplication and uniqueness                     |
| _gn_synced  | TIMESTAMP | Timestamp of when the record was last synced                                |
| account_id  | INT64     | The numeric ID of the account the ad belongs to                             |
| action_type | STRING    | The type of action (e.g., `link_click`, `purchase`, `add_to_cart`, etc.)    |
| value       | FLOAT64   | The value associated with the action (e.g., revenue, conversions, etc.)     |
| inline      | FLOAT64   | Inline value for the action, if present                                     |
| _7_d_click  | FLOAT64   | 7-day click attribution value, if present                                   |
| _1_d_view   | FLOAT64   | 1-day view attribution value, if present                                    |
| gender      | STRING    | The gender demographic group (e.g., "male", "female", "unknown")            |
| tenant      | STRING    | Tenant identifier (for multi-tenant environments)                           |

## What are Facebook Action Values?

In Facebook Ads reporting, `action_values` represent the numeric value associated with specific user actions taken after viewing or clicking an ad. These values are typically reported for conversion-related actions and can include metrics such as revenue, number of purchases, or other custom values set up in your Facebook pixel or events.

**Common `action_type` values include:**
- `link_click`: Clicks on links within the ad
- `purchase`: Completed purchases attributed to the ad
- `add_to_cart`: Add-to-cart events
- `initiate_checkout`: Checkout initiation events
- `lead`: Lead form submissions
- `complete_registration`: Completed registration events
- `view_content`: Views of a key page or content
- `search`: Searches performed on your site
- `add_payment_info`: Addition of payment information
- Custom events as defined in your Facebook pixel setup

The `value` field typically represents the monetary value or count associated with the action (e.g., total revenue for purchases, number of leads, etc.).

## How to Use This Table

- **Conversion Value Analysis**: Aggregate `value` by `action_type` to see total revenue, leads, or other conversion metrics driven by your ads in each gender demographic.
- **Attribution Window Analysis**: Use `_7_d_click` and `_1_d_view` columns to break down action values by attribution window (7-day click, 1-day view).
- **Gender/Ad/Account Performance**: Join with ad, campaign, or account dimension tables using `ad_id` and `account_id` to analyze performance by creative, campaign, or account within each gender demographic.
- **Custom Event Tracking**: If you use custom events in your Facebook pixel, those will appear as additional `action_type` values.
- **Tenant Filtering**: Use the `tenant` column to filter results for specific clients or business units in multi-tenant environments.

## Notes

- The `_gn_id` column is a deterministic hash of key dimensions (`ad_id`, `date`, `account_id`, `action_type`, `gender`) for uniqueness and deduplication.
- All fields used in the hash are cast to STRING for type safety and consistency.
- All JSON fields are safely extracted and cast to their appropriate types.
- The table is designed for easy joins with other Facebook Ads reporting and dimension tables. 