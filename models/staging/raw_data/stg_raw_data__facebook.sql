with 

source as (

    select * from {{ source('raw_data', 'facebook') }}

),

renamed as (

    SELECT
        date_date,
        paid_source,
        campaign_key,
        camPGN_name AS campaign_name,
        CAST(ads_cost AS FLOAT64) AS ads_cost,
        impression,
        click

    from source

)

select * from renamed
