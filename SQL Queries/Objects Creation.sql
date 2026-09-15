-- Creating the database and schema
CREATE DATABASE E_COMMERCE_DATABASE

USE DATABASE E_COMMERCE_DATABASE

CREATE SCHEMA RAW

USE SCHEMA RAW

-- Creating the tables 

CREATE OR REPLACE TABLE E_COMMERCE_DATABASE.RAW.CUSTOMERS(
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(255),
    customer_state VARCHAR(255),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
CREATE OR REPLACE TABLE E_COMMERCE_DATABASE.RAW.CATEGORY_NAME(
    product_category_name VARCHAR(255) PRIMARY KEY,
    product_category_name_english VARCHAR(255),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() 
)

CREATE OR REPLACE TABLE E_COMMERCE_DATABASE.RAW.PRODUCTS(
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR(255) FOREIGN KEY REFERENCES E_COMMERCE_DATABASE.RAW.CATEGORY_NAME (product_category_name),
    product_name_lenght FLOAT,
    product_description_lenght FLOAT,
    product_photos_qty FLOAT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)

CREATE OR REPLACE TABLE E_COMMERCE_DATABASE.RAW.SELLERS(
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(255),
    seller_state VARCHAR(255),
    updated_at TIMESTAMP_LTZ 
)

CREATE OR REPLACE TABLE E_COMMERCE_DATABASE.RAW.GEOLOCATION(
    geolocation_zip_code_prefix INT PRIMARY KEY,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR(255),
    geolocation_state VARCHAR(255),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)

CREATE OR REPLACE TABLE E_COMMERCE_DATABASE.RAW.ORDERS(
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR FOREIGN KEY REFERENCES E_COMMERCE_DATABASE.RAW.CUSTOMERS(customer_id),
    order_status VARCHAR(255),
    order_purchase_timestamp VARCHAR(255),
    order_approved_at VARCHAR(255),
    order_delivered_carrier_date VARCHAR(255),
    order_delivered_customer_date VARCHAR(255),
    order_estimated_delivery_date VARCHAR(255),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)

CREATE OR REPLACE TABLE E_COMMERCE_DATABASE.RAW.ORDER_REVIEWS(
    review_id VARCHAR PRIMARY KEY,
    order_id VARCHAR FOREIGN KEY REFERENCES E_COMMERCE_DATABASE.RAW.ORDERS(order_id),
    review_score INT,
    review_comment_title VARCHAR(255),
    review_comment_message VARCHAR(255),
    review_creation_date VARCHAR(255),
    review_answer_timestamp VARCHAR(255),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)

CREATE OR REPLACE TABLE E_COMMERCE_DATABASE.RAW.ORDER_PAYMENTS(
    order_id VARCHAR FOREIGN KEY REFERENCES E_COMMERCE_DATABASE.RAW.ORDERS (order_id),
    payment_sequential INT,
    payment_type VARCHAR(255),
    payment_installments INT,
    payment_value FLOAT,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)

CREATE OR REPLACE TABLE E_COMMERCE_DATABASE.RAW.ORDER_ITEMS
(

order_id VARCHAR FOREIGN KEY REFERENCES E_COMMERCE_DATABASE.RAW.ORDERS (order_id),

order_item_id VARCHAR,

product_id VARCHAR FOREIGN KEY REFERENCES E_COMMERCE_DATABASE.RAW.PRODUCTS(product_id),

seller_id VARCHAR FOREIGN KEY REFERENCES E_COMMERCE_DATABASE.RAW.SELLERS(seller_id),

shipping_limit_date VARCHAR ,
price FLOAT,
freight_value FLOAT,
 
updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
PRIMARY KEY (order_id , order_item_id)
)




