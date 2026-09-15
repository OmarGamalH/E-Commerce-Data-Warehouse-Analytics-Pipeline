{% snapshot dim_products %}

{{
    config(
        target_schema = 'raw_curated',
        unique_key = 'product_id',
        strategy = 'check',
        check_cols = ['PRODUCT_CATEGORY_NAME']

    )

}}


SELECT 
    PR.PRODUCT_ID,
    PR.PRODUCT_CATEGORY_NAME,
    CN.PRODUCT_CATEGORY_NAME_ENGLISH,
    PR.PRODUCT_NAME_LENGHT,
    PR.PRODUCT_DESCRIPTION_LENGHT,
    PR.PRODUCT_PHOTOS_QTY,
    PR.PRODUCT_WEIGHT_G,
    PR.PRODUCT_LENGTH_CM,
    PR.PRODUCT_HEIGHT_CM,
    PR.PRODUCT_WIDTH_CM
FROM {{ref('processed_products')}} PR
LEFT JOIN  {{ref('processed_category_name')}} CN
ON PR.PRODUCT_CATEGORY_NAME = CN.PRODUCT_CATEGORY_NAME

{% endsnapshot %}