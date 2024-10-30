-- models/intermediate/int_campaigns.sql

    
    WITH campaigns AS (
    
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
        {{ ref("stg_raw_data__facebook") }} )
SELECT
    date_date,
    SUM(ads_cost) AS ads_cost,
    SUM(impression) AS ads_impression,
    SUM(click) AS ads_clicks
FROM
    campaigns
GROUP BY
    date_date
ORDER BY
    date_date DESC