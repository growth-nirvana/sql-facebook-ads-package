# Ad History Table

This table implements a Slowly Changing Dimension (SCD) Type 2 pattern for Facebook Ads, tracking historical changes to ad attributes over time. It maintains a complete history of ad changes while providing an easy way to access the current state of each ad.

## Table Structure

| Column          | Type      | Description                                                                                  |
|-----------------|-----------|----------------------------------------------------------------------------------------------|
| id              | STRING    | The unique identifier for the Facebook Ad                                                   |
| account_id      | INT64     | The numeric ID of the account the ad belongs to                                            |
| campaign_id     | STRING    | The ID of the campaign the ad belongs to                                                   |
| ad_set_id       | STRING    | The ID of the ad set the ad belongs to                                                     |
| updated_time    | STRING    | Timestamp when the ad was last updated                                                     |
| created_time    | STRING    | Timestamp when the ad was created                                                          |
| name            | STRING    | The display name of the ad                                                                 |
| effective_status| STRING    | The current status of the ad (e.g., ACTIVE, PAUSED, DELETED)                               |
| tenant          | STRING    | Tenant identifier (for multi-tenant environments)                                          |
| effective_from  | TIMESTAMP | Start time of when this version of the record was valid                                    |
| effective_to    | TIMESTAMP | End time of when this version of the record was valid (NULL for current records)           |
| is_current      | BOOLEAN   | Flag indicating whether this is the current version of the record                          |
| _gn_id          | STRING    | Hash of key attributes used for change detection                                           |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes:
- id
- account_id
- campaign_id
- ad_set_id
- updated_time
- created_time
- name
- effective_status
- tenant

When any of these attributes change, a new version of the record is created with:
- `effective_from` set to the current timestamp
- `effective_to` set to NULL
- `is_current` set to TRUE

The previous version is updated with:
- `effective_to` set to the new version's `effective_from`
- `is_current` set to FALSE

## Usage

- **Get current ad state**: Filter where `is_current = TRUE`
- **Track historical changes**: Query without the `is_current` filter to see all versions
- **Point-in-time analysis**: Use `effective_from` and `effective_to` to see ad state at any point in time
- **Change analysis**: Compare different versions of the same ad to see what changed and when
- **Campaign/Ad Set Analysis**: Join with campaign_history and ad_set_history tables for complete campaign hierarchy analysis

## Notes

- The table is updated incrementally, only processing new or changed records
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table maintains referential integrity with other Facebook Ads tables through the `id`, `campaign_id`, and `ad_set_id` fields
- The `effective_status` field is particularly useful for tracking ad lifecycle changes 