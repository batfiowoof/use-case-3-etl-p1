CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;

USE DATABASE ECOMMERCE_DB;

CREATE SCHEMA IF NOT EXISTS STAGE_EXTERNAL;
CREATE SCHEMA IF NOT EXISTS TABLE_S3;

CREATE SCHEMA IF NOT EXISTS CORRECTIONS;

USE SCHEMA STAGE_EXTERNAL;

CREATE STAGE IF NOT EXISTS STAGE_EXTERNAL.STAGE_ECOMMERCE -- https://docs.snowflake.com/en/sql-reference/sql/create-stage
URL='s3://fakecompanydata/'
FILE_FORMAT = (TYPE = 'CSV');

USE SCHEMA TABLE_S3;

CREATE OR REPLACE TABLE TABLE_S3.TD_S3(
    order_id int,
    customer_id string,
    customer_name string,
    order_date string,
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
ON_ERROR = 'CONTINUE'
;


USE SCHEMA CORRECTIONS;

-- Ако Адреса за доставка липсва, но статуса е Delivered - прехвърлете записа към отделна таблица, която да съдържа само и единствено такива доставки, гонови за ревю, td_for_review
CREATE OR REPLACE TABLE CORRECTIONS.TD_FOR_REVIEW LIKE TABLE_S3.TD_S3;

INSERT INTO CORRECTIONS.TD_FOR_REVIEW
SELECT * FROM TABLE_S3.TD_S3
WHERE SHIPPING_ADDRESS IS NULL AND STATUS = 'Delivered';

-- Премахване от таблицата
DELETE FROM TABLE_S3.TD_S3
WHERE SHIPPING_ADDRESS IS NULL AND STATUS = 'Delivered';

-- Ако в записа липсва данни за клиента Customer_id , то тогава този запис трябва да бъде прехвърлен към таблица td_suspisios_records
CREATE OR REPLACE TABLE CORRECTIONS.TD_SUSPICIOUS_RECORDS LIKE TABLE_S3.TD_S3;

INSERT INTO CORRECTIONS.TD_SUSPICIOUS_RECORDS
SELECT * FROM TABLE_S3.TD_S3
WHERE CUSTOMER_NAME IS NULL;

-- Премахване от таблицата
DELETE FROM TABLE_S3.TD_S3
WHERE CUSTOMER_NAME IS NULL;

-- Ако липсва информация за платежния метод, коригирайте със стойност по подразбиране Unknown
USE SCHEMA TABLE_S3;
UPDATE TABLE_S3.TD_S3
SET PAYMENT_METHOD = 'UNKNOWN'
WHERE PAYMENT_METHOD IS NULL;

USE SCHEMA CORRECTIONS;

-- Невалиден формат на дата
CREATE OR REPLACE TABLE CORRECTIONS.TD_INVALID_DATE_FORMAT LIKE TABLE_S3.TD_S3;

INSERT INTO CORRECTIONS.TD_INVALID_DATE_FORMAT
SELECT * FROM TABLE_S3.TD_S3
WHERE TRY_TO_DATE(ORDER_DATE) is  null; -- https://docs.snowflake.com/en/sql-reference/functions/try_to_date

UPDATE TABLE_S3.TD_S3
SET ORDER_DATE = '2024-03-01'
WHERE TRY_TO_DATE(ORDER_DATE) is  null;

-- Отрицателни или нулеви стойности за количество и цена
CREATE OR REPLACE TABLE CORRECTIONS.TD_INVALID_COUNT_AND_PRICE LIKE TABLE_S3.TD_S3;

INSERT INTO CORRECTIONS.TD_INVALID_COUNT_AND_PRICE
SELECT * FROM TABLE_S3.TD_S3
WHERE QUANTITY < 0 OR PRICE < 0;

-- Изтриване на тези записи

DELETE FROM TABLE_S3.TD_S3
WHERE QUANTITY < 0 OR PRICE < 0;

-- Невалидна отстъпка
UPDATE TABLE_S3.TD_S3
SET DISCOUNT = 0
WHERE DISCOUNT < 0;

UPDATE TABLE_S3.TD_S3
SET DISCOUNT = 0.50
WHERE DISCOUNT > 0.50;

-- Неправилно калкулирана крайна цена (където се налага)
UPDATE TABLE_S3.TD_S3
SET TOTAL_AMOUNT = QUANTITY * PRICE * (1 - DISCOUNT)
WHERE TOTAL_AMOUNT != QUANTITY * PRICE * (1 - DISCOUNT);

-- Неконсистентни данни за статуса на поръчката
UPDATE TABLE_S3.TD_S3
SET STATUS = 'Pending'
WHERE SHIPPING_ADDRESS IS NULL;

CREATE OR REPLACE TABLE CORRECTIONS.TD_DUPLICATES LIKE TABLE_S3.TD_S3; -- Извежда всички поръчки, които са дублирани в таблицата, от 5135 оригинални записа, 135 са дублирани, което свежда до 5000 уникални поръчки.

-- Вмъква всички редове от TD_S3, които са дублирани по ORDER_ID
INSERT INTO CORRECTIONS.TD_DUPLICATES
SELECT *
FROM TABLE_S3.TD_S3
WHERE ORDER_ID IN (
  SELECT ORDER_ID
  FROM TABLE_S3.TD_S3
  GROUP BY ORDER_ID
  HAVING COUNT(*) > 1
);

-- Създава нова таблица без дублирани редове, като оставя само по един запис от всяка група с еднакви стойности.
-- Дубликатите се определят по всички ключови колони, а редовете се номерират чрез ROW_NUMBER().
-- Остава редът с ROW_NUMBER = 1 от всяка група, останалите се премахват.
-- Полезно за почистване на данни преди анализ или замяна на оригиналната таблица.

CREATE OR REPLACE TABLE TABLE_S3.TD_CLEAN_RECORDS AS
SELECT *
FROM TABLE_S3.TD_S3
QUALIFY ROW_NUMBER() OVER ( -- https://docs.snowflake.com/en/sql-reference/functions/row_number.html
  PARTITION BY ORDER_ID, CUSTOMER_ID, ORDER_DATE, PRODUCT, QUANTITY, PRICE, DISCOUNT, TOTAL_AMOUNT, PAYMENT_METHOD, SHIPPING_ADDRESS, STATUS
  ORDER BY ORDER_ID
) = 1;
