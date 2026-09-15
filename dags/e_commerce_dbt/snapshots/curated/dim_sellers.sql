{% snapshot dim_sellers %}

{{
    config(
        target_schema = 'raw_curated',
        unique_key = 'seller_id',
        strategy = 'check',
        check_cols = ['SELLER_ZIP_CODE_PREFIX' , 'SELLER_CITY' , 'SELLER_STATE']

    )

}}

SELECT * FROM {{ref('processed_sellers')}}

{% endsnapshot %}