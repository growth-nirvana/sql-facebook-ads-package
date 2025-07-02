# custom_conversions_history

_Last Updated: {{DATE}}_

## Overview

`custom_conversions_history` is a Slowly Changing Dimension (SCD) Type 2 table that tracks the full change history of Facebook Ads Custom Conversions for each account. This table enables you to analyze how custom conversion definitions and metadata have changed over time, supporting robust historical reporting and auditing.

## Table Schema

| Column            | Type      | Description                                                      |
|-------------------|-----------|------------------------------------------------------------------|
| account_id        | STRING    | Facebook Ads account ID associated with the custom conversion.   |
| id                | STRING    | Unique identifier for the custom conversion (NOT NULL).          |
| name              | STRING    | Name of the custom conversion.                                   |
| creation_time     | STRING    | Timestamp when the custom conversion was created (ISO8601).      |
| last_fired_time   | STRING    | Timestamp when the custom conversion was last triggered.         |
| tenant            | STRING    | Internal tenant or workspace identifier.                         |
| effective_from    | TIMESTAMP | Start time for this version of the record.                       |
| effective_to      | TIMESTAMP | End time for this version of the record (NULL if current).       |
| is_current        | BOOLEAN   | TRUE if this is the current version of the record.               |
| _gn_id            | STRING    | Unique hash for the business key fields (excludes metadata).     |

## SCD Type 2 Logic

- Each time a change is detected in the business fields (`account_id`, `id`, `name`, `creation_time`, `last_fired_time`), a new row is inserted with updated values and `is_current = TRUE`.
- The previous version of the record is closed out by setting `effective_to` and `is_current = FALSE`.
- The `tenant` field is treated as metadata and is not included in the SCD hash or change detection logic.
- The `_gn_id` column is a hash of the business key fields and is used for efficient change tracking and deduplication.

## Use Cases

- **Audit Trail:** Track all changes to custom conversions, including name changes, creation times, and last fired times.
- **Historical Reporting:** Analyze how custom conversions have evolved over time for compliance or business analysis.
- **Point-in-Time Analysis:** Reconstruct the state of custom conversions as of any historical date.

## Example Query: Get Current Custom Conversions

```sql
SELECT *
FROM `{{target_dataset}}.custom_conversions_history`
WHERE is_current = TRUE;
```

## Example Query: Change History for a Specific Conversion

```sql
SELECT *
FROM `{{target_dataset}}.custom_conversions_history`
WHERE id = 'YOUR_CONVERSION_ID'
ORDER BY effective_from DESC;
```

## Notes
- The table is updated automatically as part of the ETL pipeline.
- Only business fields are tracked for SCD changes; metadata fields like `tenant` do not trigger new versions.
- Timestamps are stored as strings in ISO8601 format for compatibility with Facebook Ads API exports.

---

For further questions or support, please contact your data engineering team or refer to the main project documentation. 