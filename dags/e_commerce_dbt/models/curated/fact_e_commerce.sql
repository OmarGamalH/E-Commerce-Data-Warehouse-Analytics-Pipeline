SELECT 
    POI.ORDER_ID,
    POI.PRODUCT_ID,
    POI.SELLER_ID,
    PO.CUSTOMER_ID,
    POR.REVIEW_ID,
    POI.PRICE,
    POI.FREIGHT_VALUE,
    POI.SHIPPING_LIMIT_DATE
FROM {{ref('processed_order_items')}} POI
LEFT JOIN {{ref('processed_orders')}} PO 
ON POI.ORDER_ID = PO.ORDER_ID
LEFT JOIN {{ref('processed_order_payments')}} POP
ON POI.ORDER_ID = POP.ORDER_ID
LEFT JOIN {{ref('processed_order_reviews')}} POR
ON POI.ORDER_ID = POR.ORDER_ID
