# Ad Image History Table

This table implements a Slowly Changing Dimension (SCD) Type 2 pattern for Facebook Ads Ad Images, tracking historical changes to ad image attributes over time. It maintains a complete history of ad image changes while providing an easy way to access the current state of each ad image.

## Table Structure

| Column                              | Type      | Description                                                                                  |
|-------------------------------------|-----------|----------------------------------------------------------------------------------------------|
| id                                  | STRING    | The unique identifier for the Facebook Ad Image                                              |
| account_id                          | STRING    | The ID of the account the ad image belongs to                                                |
| created_time                        | STRING    | Timestamp when the ad image was created                                                      |
| creatives                           | STRING    | JSON string of associated creatives                                                          |
| hash                                | STRING    | Hash value from the source, if present                                                       |
| height                              | INT64     | Height of the ad image in pixels                                                             |
| is_associated_creatives_in_adgroups | BOOL      | Whether the image is associated with creatives in ad groups                                  |
| name                                | STRING    | The display name of the ad image                                                             |
| original_height                     | INT64     | Original height of the ad image in pixels                                                    |
| original_width                      | INT64     | Original width of the ad image in pixels                                                     |
| permalink_url                       | STRING    | Permanent URL to the ad image                                                                |
| status                              | STRING    | Status of the ad image (e.g., ACTIVE, DELETED)                                               |
| updated_time                        | STRING    | Timestamp when the ad image was last updated                                                 |
| url                                 | STRING    | URL to the ad image                                                                          |
| url_128                             | STRING    | URL to the 128px version of the ad image                                                     |
| width                               | INT64     | Width of the ad image in pixels                                                              |
| run_id                              | INT64     | Run identifier for batch processing                                                          |
| tenant                              | STRING    | Tenant identifier (for multi-tenant environments)                                            |
| effective_from                      | TIMESTAMP | Start time of when this version of the record was valid                                      |
| effective_to                        | TIMESTAMP | End time of when this version of the record was valid (NULL for current records)             |
| is_current                          | BOOLEAN   | Flag indicating whether this is the current version of the record                            |
| _gn_id                              | STRING    | Hash of key attributes used for change detection                                             |

## Change Detection

The table uses a hash-based change detection mechanism (`_gn_id`) that includes:
- id
- account_id
- created_time
- creatives
- hash
- height
- is_associated_creatives_in_adgroups
- name
- original_height
- original_width
- permalink_url
- status
- updated_time
- url
- url_128
- width
- tenant

When any of these attributes change, a new version of the record is created with:
- `effective_from` set to the current extraction timestamp
- `effective_to` set to NULL
- `is_current` set to TRUE

The previous version is updated with:
- `effective_to` set to the new version's `effective_from`
- `is_current` set to FALSE

## Usage

- **Get current ad image state**: Filter where `is_current = TRUE`
- **Track historical changes**: Query without the `is_current` filter to see all versions
- **Point-in-time analysis**: Use `effective_from` and `effective_to` to see ad image state at any point in time
- **Change analysis**: Compare different versions of the same ad image to see what changed and when
- **Creative association**: Use the `creatives` field to analyze which creatives are linked to each image over time

## Notes

- The table is updated incrementally, only processing new or changed records
- A guard clause checks for source table existence before running ETL
- All fields in the hash are cast to STRING to ensure consistent change detection
- The table maintains referential integrity with other Facebook Ads tables through the `id` and `account_id` fields
- The `is_associated_creatives_in_adgroups` field is useful for understanding image usage in ad groups
- The `status` field helps track the lifecycle of ad images 