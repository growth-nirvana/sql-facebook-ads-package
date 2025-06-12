# Platform Report Table

This table contains core Facebook Ads reporting data at the ad level, broken down by publisher platform (e.g., Facebook, Instagram, Audience Network, Messenger). Each row represents a single ad, date, account, and publisher platform, providing key performance metrics for each ad.

## Table Structure

| Column              | Type      | Description                                                      |
|---------------------|-----------|------------------------------------------------------------------|
| ad_id               | STRING    | The unique identifier for the Facebook Ad                        |
| date                | DATE      | The reporting date for the record                                |
| _gn_id              | STRING    | Hash of key dimensions for deduplication and uniqueness          |
| _gn_synced          | TIMESTAMP | Timestamp of when the record was last synced                     |
| account_id          | STRING    | The ID of the account the ad belongs to                          |
| clicks              | INT64     | Number of clicks the ad received                                 |
| impressions         | INT64     | Number of times the ad was shown                                 |
| spend               | FLOAT64   | Amount spent on the ad (in account currency)                     |
| publisher_platform  | STRING    | The platform where the ad was shown (e.g., Facebook, Instagram)  |
| tenant              | STRING    | Tenant identifier (for multi-tenant environments)                |

## How to Use This Table

- **Performance Analysis:** Aggregate or filter by `ad_id`, `account_id`, `publisher_platform`, or `date` to analyze ad performance over time and by platform.
- **KPI Tracking:** Use `clicks`, `impressions`, and `spend` to calculate key metrics such as CTR (click-through rate), CPC (cost per click), and CPM (cost per mille).
- **Join with Dimensions:** Join with ad, campaign, or account dimension tables using `ad_id` and `account_id` for richer analysis.
- **Tenant Filtering:** Use the `tenant` column to filter results for specific clients or business units in multi-tenant environments.

## Notes

- The `_gn_id` column is a deterministic hash of key dimensions (`ad_id`, `date`, `account_id`, `publisher_platform`) for uniqueness and deduplication.
- All fields used in the hash are cast to STRING for type safety and consistency.
- All JSON fields are safely extracted and cast to their appropriate types.
- The table is designed for easy joins with other Facebook Ads reporting and dimension tables. 