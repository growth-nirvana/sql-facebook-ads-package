# Platform Report Conversions Table

This table contains pivoted conversion data from Facebook Ads platform reporting, specifically unnesting the `conversions` JSON array from the platform report source. Each row represents a single conversion type and its associated metrics for a given ad, date, account, and publisher platform.

## Table Structure

| Column              | Type      | Description                                                      |
|---------------------|-----------|------------------------------------------------------------------|
| ad_id               | STRING    | The unique identifier for the Facebook Ad                        |
| date                | DATE      | The reporting date for the record                                |
| _gn_id              | STRING    | Hash of key dimensions for deduplication and uniqueness          |
| _gn_synced          | TIMESTAMP | Timestamp of when the record was last synced                     |
| account_id          | STRING    | The ID of the account the ad belongs to                          |
| action_type         | STRING    | The type of conversion (e.g., `purchase`, `lead`, etc.)          |
| value               | FLOAT64   | The count or value associated with the conversion                |
| inline              | FLOAT64   | Inline value for the conversion, if present                      |
| _7_d_click          | FLOAT64   | 7-day click attribution value, if present                        |
| _1_d_view           | FLOAT64   | 1-day view attribution value, if present                         |
| publisher_platform  | STRING    | The platform where the ad was shown (e.g., Facebook, Instagram)  |
| tenant              | STRING    | Tenant identifier (for multi-tenant environments)                |

## What are Facebook Conversions?

In Facebook Ads reporting, `conversions` represent user actions that are considered valuable outcomes, such as purchases, leads, or registrations, typically tracked via the Facebook pixel or SDK.

**Common `action_type` values include:**
- `purchase`: Completed purchases attributed to the ad
- `lead`: Lead form submissions
- `add_to_cart`: Add-to-cart events
- `initiate_checkout`: Checkout initiation events
- `complete_registration`: Completed registration events
- `view_content`: Views of a key page or content
- `search`: Searches performed on your site
- `add_payment_info`: Addition of payment information
- Custom events as defined in your Facebook pixel setup

The `value` field typically represents the count of conversions (e.g., number of purchases, leads, etc.).

## How to Use This Table

- **Conversion Analysis:** Aggregate `value` by `action_type` to see total conversions driven by your ads on each platform.
- **Attribution Window Analysis:** Use `_7_d_click` and `_1_d_view` columns to break down conversions by attribution window (7-day click, 1-day view).
- **Platform/Ad/Account Performance:** Join with ad, campaign, or account dimension tables using `ad_id` and `account_id` to analyze performance by creative, campaign, or account within each platform.
- **Custom Event Tracking:** If you use custom events in your Facebook pixel, those will appear as additional `action_type` values.
- **Tenant Filtering:** Use the `tenant` column to filter results for specific clients or business units in multi-tenant environments.

## Notes

- The `_gn_id` column is a deterministic hash of key dimensions (`ad_id`, `date`, `account_id`, `action_type`, `publisher_platform`) for uniqueness and deduplication.
- All fields used in the hash are cast to STRING for type safety and consistency.
- All JSON fields are safely extracted and cast to their appropriate types.
- The table is designed for easy joins with other Facebook Ads reporting and dimension tables. 