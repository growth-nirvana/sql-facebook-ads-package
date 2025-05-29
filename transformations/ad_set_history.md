# Ad Set History Table

This table implements a Slowly Changing Dimension (SCD) Type 2 pattern for Facebook Ads ad sets, tracking historical changes to ad set attributes over time. It maintains a complete history of ad set changes while providing an easy way to access the current state of each ad set.

## Table Structure

| Column          | Type      | Description                                                                                  |
|-----------------|-----------|----------------------------------------------------------------------------------------------|
| id              | STRING    | The unique identifier for the Facebook Ad Set                                               |
| name            | STRING    | The display name of the ad set                                                              |
| account_id      | INT64     | The numeric ID of the account the ad set belongs to                                         |
| updated_time    | STRING    | Timestamp when the ad set was last updated                                                  |
| campaign_id     | STRING    | The ID of the campaign the ad set belongs to                                                |
| created_time    | STRING    | Timestamp when the ad set was created                                                       |
| tenant          | STRING    | Tenant identifier (for multi-tenant environments)                                           |
| effective_from  | TIMESTAMP | Start time of when this version of the record was valid                                     |
| effective_to    | TIMESTAMP | End time of when this version of the record was valid (NULL for current records)            |
| is_current      | BOOLEAN   | Flag indicating whether this is the current version of the record                           |
| _gn_id          | STRING    | Hash of key attributes used for change detection                                            |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes:
- id
- name
- account_id
- updated_time
- campaign_id
- created_time
- tenant

When any of these attributes change, a new version of the record is created with:
- `effective_from` set to the current timestamp
- `effective_to` set to NULL
- `is_current` set to TRUE

The previous version is updated with:
- `effective_to` set to the new version's `effective_from`
- `is_current` set to FALSE

## Usage

- **Get current ad set state**: Filter where `is_current = TRUE`
- **Track historical changes**: Query without the `is_current` filter to see all versions
- **Point-in-time analysis**: Use `effective_from` and `effective_to` to see ad set state at any point in time
- **Change analysis**: Compare different versions of the same ad set to see what changed and when
- **Campaign Analysis**: Join with campaign_history to analyze ad sets within their campaign context

## Notes

- The table is updated incrementally, only processing new or changed records
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table maintains referential integrity with other Facebook Ads tables through the `id` and `campaign_id` fields 