{{ dbt_utils.union_relations(
    relations=[
        ref('stg_raw_data__adwords'),
        ref('stg_raw_data__bing'),
        ref('stg_raw_data__criteo'),
        ref('stg_raw_data__facebook')
    ],
    
) }}