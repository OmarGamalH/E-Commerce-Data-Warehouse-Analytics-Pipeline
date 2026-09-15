{% snapshot dim_customers %}

{{
    config(
        target_schema = 'raw_curated',
        unique_key = 'customer_id',
        strategy = 'check',
        check_cols = ['CUSTOMER_ZIP_CODE_PREFIX' , 'CUSTOMER_CITY' , 'CUSTOMER_STATE']

    )

}}

SELECT * FROM {{ref('processed_customers')}}

{% endsnapshot %}