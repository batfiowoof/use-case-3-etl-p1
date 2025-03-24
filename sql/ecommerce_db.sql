CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;

USE DATABASE ECOMMERCE_DB;

CREATE SCHEMA IF NOT EXISTS STAGE_EXTERNAL;
CREATE SCHEMA IF NOT EXISTS TABLE_S3;

USE SCHEMA STAGE_EXTERNAL;

CREATE STAGE IF NOT EXISTS STAGE_EXTERNAL.STAGE_ECOMMERCE -- https://docs.snowflake.com/en/sql-reference/sql/create-stage
URL='s3://fakecompanydata/'
FILE_FORMAT = (TYPE = 'CSV');

USE SCHEMA TABLE_S3;

CREATE OR REPLACE TABLE TABLE_S3.TD_S3(
    order_id int,
    customer_id string,
    customer_name string,
    order_date date,
    product string,
    quantity int,
    price float,
    discount float,
    total_amount float,
    payment_method string, -- maybe i should do this an enum
    shipping_address string,
    status string -- maybe i should do this an enum
);

COPY INTO TABLE_S3.TD_S3
FROM @STAGE_EXTERNAL.STAGE_ECOMMERCE
FILE_FORMAT = (TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' -- защото snowflake гърми и мисли че има 13 колони :^)
    )
ON_ERROR = 'CONTINUE';