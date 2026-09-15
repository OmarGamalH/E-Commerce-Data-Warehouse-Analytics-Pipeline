SELECT 
    ORDER_ID , 
    PRODUCT_ID,
    SELLER_ID,
    ORDER_ITEM_ID,
    PRICE,
    FREIGHT_VALUE,
    SHIPPING_LIMIT_DATE,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM {{source('e_commerece' , 'raw_order_items')}}