-- models/intermediate/int_campaigns.sql

    SELECT
        date_date,
        paid_source,
        campaign_key,
        campaign_name,
        ads_cost,
        impression,
        click
    FROM
        {{ ref("stg_raw_data__adwords") }}
    UNION ALL
    SELECT
        date_date,
        paid_source,
        campaign_key,
        campaign_name,
        ads_cost,
        impression,
        click
    FROM
        {{ ref("stg_raw_data__bing") }}
    UNION ALL
    SELECT
        date_date,
        paid_source,
        campaign_key,
        campaign_name,
        ads_cost,
        impression,
        click
    FROM
        {{ ref("stg_raw_data__criteo") }}
    UNION ALL
    SELECT
        date_date,
        paid_source,
        campaign_key,
        campaign_name,
        ads_cost,
        impression,
        click
    FROM
        {{ ref("stg_raw_data__facebook") }}
