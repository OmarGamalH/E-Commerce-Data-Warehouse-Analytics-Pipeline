{% snapshot dim_payments %}

{{
    config(
        target_schema = 'raw_curated',
        unique_key = dbt_utils.generate_surrogate_key(['order_id' , 'payment_sequential']),
        strategy = 'check',
        check_cols = ['PAYMENT_TYPE' , 'PAYMENT_INSTALLMENTS']

    )

}}

SELECT 
    ROW_NUMBER() OVER(ORDER BY ORDER_ID) AS PAYMENT_KEY,
    ORDER_ID,
    PAYMENT_SEQUENTIAL,
    PAYMENT_TYPE,
    PAYMENT_INSTALLMENTS
FROM {{ref('processed_order_payments')}} 


{% endsnapshot %}