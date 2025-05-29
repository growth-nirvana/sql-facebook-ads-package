# Facebook Ads Connector Documentation

_Last Updated: 4/10/25 at 7:24 am_

## Overview

Facebook Ads is an online advertising platform developed by Facebook. It allows businesses to create and display targeted ads on Facebook and its network, including Instagram, using a range of ad formats. With advanced targeting options and performance reporting, Facebook Ads is a popular platform for driving conversions and brand awareness among a large user base.

This connector enables automated extraction, transformation, and loading (ETL) of Facebook Ads data into your analytics environment. The connector runs every **6 hours by default**, ensuring your reporting tables are kept up to date with the latest available data.

## Included Tables

| Table Name                        | Description                                                                                       |
|-----------------------------------|---------------------------------------------------------------------------------------------------|
| `account_history`                 | SCD Type 2 table tracking historical changes to Facebook Ads accounts.                            |
| `ad_history`                      | SCD Type 2 table tracking historical changes to Facebook Ads.                                     |
| `ad_set_history`                  | SCD Type 2 table tracking historical changes to Facebook Ads ad sets.                             |
| `campaign_history`                | SCD Type 2 table tracking historical changes to Facebook Ads campaigns.                           |
| `creative_history`                | SCD Type 2 table tracking historical changes to Facebook Ads creatives.                           |
| `ad_report`                       | Core ad-level performance metrics (clicks, impressions, spend, etc.) by ad, date, and account.    |
| `ad_report_actions`               | Pivoted table of action counts (e.g., clicks, purchases) by ad, date, account, and action type.   |
| `ad_report_action_values`         | Pivoted table of action values (e.g., revenue, value per action) by ad, date, account, and type.  |
| `ad_report_conversions`           | Pivoted table of conversion counts by ad, date, account, and conversion type.                     |
| `ad_report_conversion_values`     | Pivoted table of conversion values (e.g., revenue) by ad, date, account, and conversion type.     |
| `dma_report`                      | Ad-level performance metrics by Designated Market Area (DMA), ad, date, and account.              |
| `dma_report_actions`              | Pivoted table of action counts by DMA, ad, date, account, and action type.                        |
| `dma_report_action_values`        | Pivoted table of action values by DMA, ad, date, account, and action type.                        |
| `dma_report_conversions`          | Pivoted table of conversion counts by DMA, ad, date, account, and conversion type.                |
| `dma_report_conversion_values`    | Pivoted table of conversion values by DMA, ad, date, account, and conversion type.                |

Each table is designed for robust analytics, with type-safe fields, unique row hashes, and referential integrity for easy joining and historical analysis.

---

## Creating and Connecting the Connector

1. **Navigate to Connectors**
   - Click on **Connectors** in the left-hand navigation menu.
   - In the available sources search bar, enter **Facebook Ads**.

2. **Create the Connector**
   - Click on the Facebook Ads connector card. This will take you to the create page for the connector.
   - Add a display name and click on **Create Connector**. The display name will help you find everything as you add more connectors.

3. **Authorize the Connector**
   - After creating the connector, you will see the connector details page.
   - Click on the **Authorize** button. This will bring up a modal window.
   - Enter your account or account IDs that you would like to sync. These are located in your Facebook [Ad Manager](https://www.facebook.com/adsmanager/) account.

4. **Find Your Account IDs**
   - Navigate to the accounts you would like to sync in Facebook Ad Manager.
   - Grab the ID of the account from the URL in the events manager, or use the account picker on the right-hand side if you need to switch accounts.
   - Copy the value(s) and paste them into the **Account Ids** field, separated by commas if you have multiple.

5. **Connect and Authorize**
   - Click on **Connect your data**. This will bring up a modal window where you can authorize your Facebook Ads account.
   - Go through the authorization process as prompted.

6. **Confirmation**
   - Once you have finished, you will see that the modal has linked and you will be redirected to the connections index page.
   - Your data is now syncing automatically every 6 hours.

---

If you have any questions or need further assistance, please refer to the individual table documentation files or contact support. 