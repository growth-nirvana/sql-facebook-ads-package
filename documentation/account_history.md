# Account History Table

This table implements a Slowly Changing Dimension (SCD) Type 2 pattern for Facebook Ads accounts, tracking historical changes to account attributes over time. It maintains a complete history of account changes while providing an easy way to access the current state of each account.

## Table Structure

| Column          | Type      | Description                                                                                  |
|-----------------|-----------|----------------------------------------------------------------------------------------------|
| id              | STRING    | The unique identifier for the Facebook Ads account                                          |
| account_id      | INT64     | The numeric ID of the account                                                               |
| created_time    | STRING    | Timestamp when the account was created                                                      |
| name            | STRING    | The display name of the account                                                             |
| tenant          | STRING    | Tenant identifier (for multi-tenant environments)                                           |
| effective_from  | TIMESTAMP | Start time of when this version of the record was valid                                     |
| effective_to    | TIMESTAMP | End time of when this version of the record was valid (NULL for current records)            |
| is_current      | BOOLEAN   | Flag indicating whether this is the current version of the record                           |
| _gn_id          | STRING    | Hash of key attributes used for change detection                                            |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes:
- account_id
- created_time
- name
- tenant

When any of these attributes change, a new version of the record is created with:
- `effective_from` set to the current timestamp
- `effective_to` set to NULL
- `is_current` set to TRUE

The previous version is updated with:
- `effective_to` set to the new version's `effective_from`
- `is_current` set to FALSE

## Usage

- **Get current account state**: Filter where `is_current = TRUE`
- **Track historical changes**: Query without the `is_current` filter to see all versions
- **Point-in-time analysis**: Use `effective_from` and `effective_to` to see account state at any point in time
- **Change analysis**: Compare different versions of the same account to see what changed and when

## Notes

- The table is updated incrementally, only processing new or changed records
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table maintains referential integrity with other Facebook Ads tables through the `id` field 