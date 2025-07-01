# Device Report Table

This table provides a batch-based, daily snapshot of Facebook Ads device-level performance metrics. It is designed for efficient reporting and analysis by device platform, supporting deduplication and incremental batch loads.

## Table Structure

| Column           | Type      | Description                                                      |
|------------------|-----------|------------------------------------------------------------------|
| ad_id            | STRING    | The unique identifier for the Facebook Ad                        |
| date             | DATE      | The reporting date                                               |
| _gn_id           | STRING    | Hash of key attributes for deduplication                         |
| _gn_synced       | TIMESTAMP | Timestamp when the record was loaded                             |
| account_id       | INT64     | The numeric ID of the account                                    |
| clicks           | INT64     | Number of clicks                                                 |
| impressions      | INT64     | Number of impressions                                            |
| spend            | FLOAT64   | Amount spent                                                     |
| device_platform  | STRING    | Device platform (e.g., mobile, desktop)                          |
| tenant           | STRING    | Tenant identifier (for multi-tenant environments)                |

## ETL & Deduplication Logic

- Loads the latest batch of data from the source table (`adsinsights_device_report`)
- Identifies the most recent batch using `_time_extracted` and batch windowing
- Deletes any overlapping records in the target table for the batch's date range and account IDs
- Inserts deduplicated records for the latest batch
- Computes `_gn_id` as a hash of ad_id, date, account_id, and device_platform

## Usage

- **Device-level reporting**: Analyze performance metrics by device platform
- **Incremental loads**: Table is safe for repeated batch loads; only the latest batch is kept for each date/account
- **Join with actions/conversions**: Use ad_id, date, and device_platform to join with related action/conversion tables

## Notes

- The table is updated incrementally, only processing new or changed records
- A guard clause checks for source table existence before running ETL
- The `_gn_id` field ensures deduplication and idempotency
- The `tenant` field supports multi-tenant use cases 