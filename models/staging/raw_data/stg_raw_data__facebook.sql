with 

source as (

    select * from {{ source('raw_data', 'facebook') }}

),

renamed as (

    select

    from source

)

select * from renamed
