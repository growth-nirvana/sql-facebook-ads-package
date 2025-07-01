# Device Report Action Values Table

This table unpacks and reports Facebook Ads device-level action value metrics, providing a daily snapshot of action values by device platform. It supports granular analysis of value-based actions and batch-based deduplication.

## Table Structure

| Column           | Type      | Description                                                      |
|------------------|-----------|------------------------------------------------------------------|
| ad_id            | STRING    | The unique identifier for the Facebook Ad                        |
| date             | DATE      | The reporting date                                               |
| _gn_id           | STRING    | Hash of key attributes for deduplication                         |
| _gn_synced       | TIMESTAMP | Timestamp when the record was loaded                             |
| account_id       | INT64     | The numeric ID of the account                                    |
| action_type      | STRING    | The type of action (e.g., purchase, add_to_cart)                 |
| value            | FLOAT64   | Value associated with the action                                 |
| inline           | FLOAT64   | Inline value (if present)                                        |
| _7_d_click       | FLOAT64   | 7-day click attribution value                                    |
| _1_d_view        | FLOAT64   | 1-day view attribution value                                     |
| device_platform  | STRING    | Device platform (e.g., mobile, desktop)                          |
| tenant           | STRING    | Tenant identifier (for multi-tenant environments)                |

## ETL & Deduplication Logic

- Loads the latest batch of data from the source table (`adsinsights_device_report`)
- Unnests the `action_values` JSON array to produce one row per action value type per ad per device per day
- Deletes any overlapping records in the target table for the batch's date range and account IDs
- Computes `_gn_id` as a hash of ad_id, date, account_id, action_type, and device_platform

## Usage

- **Value-based action reporting**: Analyze value-based actions (e.g., purchase value) by device platform
- **Attribution analysis**: Use `_7_d_click` and `_1_d_view` for attribution window breakdowns
- **Join with main device report**: Use ad_id, date, and device_platform to join with device_report

## Notes

- The table is updated incrementally, only processing new or changed records
- The `action_type` field is critical for filtering/reporting on specific value-based actions
- The `tenant` field supports multi-tenant use cases 