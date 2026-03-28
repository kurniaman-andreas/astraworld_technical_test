
/*
 	TASK 2 
 */


-- clean sales_raw
CREATE TABLE sales_clean AS
SELECT
    vin,
    customer_id,
    model,
    invoice_date,
    CAST(REPLACE(REPLACE(price, '.', ''), ',', '') AS SIGNED) AS price,
    created_at
FROM warehouse.sales_raw;



-- Clean customers_raw
CREATE TABLE customers_clean AS
SELECT
    id AS customer_id,
    name,
    CASE
        WHEN dob LIKE '____-__-__' THEN STR_TO_DATE(dob, '%Y-%m-%d')
        WHEN dob LIKE '____/__/__' THEN STR_TO_DATE(dob, '%Y/%m/%d')
        WHEN dob LIKE '__/__/____' THEN STR_TO_DATE(dob, '%d/%m/%Y')
        ELSE NULL
    END AS dob,
    created_at
FROM warehouse.customers_raw
WHERE LOWER(name) NOT LIKE 'pt%';


-- Cleaning after_sales_raw
CREATE TABLE warehouse.after_sales_clean AS
SELECT
    UPPER(service_ticket) AS service_ticket,
    vin,
    customer_id,
    model,
    service_date,
    service_type,
    created_at
FROM warehouse.after_sales_raw;


-- Cleaning customer_addresses_raw
CREATE TABLE warehouse.customer_addresses_clean (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    address VARCHAR(255),
    city VARCHAR(100),
    province VARCHAR(100),
    created_at DATETIME
);


-- Insert statement for cleaning  
INSERT INTO warehouse.customer_addresses_clean (
    customer_id,
    address,
    city,
    province,
    created_at
)
SELECT
    customer_id,
    CONCAT('Jl. ', UPPER(LEFT(clean_address, 1)), SUBSTRING(clean_address, 2)) AS address,
    UPPER(TRIM(city))     AS city,
    UPPER(TRIM(province)) AS province,
    created_at
FROM (
    WITH ranked AS (
        SELECT
            customer_id,
            address,
            city,
            province,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY created_at DESC
            ) AS rn
        FROM warehouse.customer_addresses_raw
    ),
    latest AS (
        SELECT * FROM ranked WHERE rn = 1
    ),
    cleaned AS (
        SELECT
            customer_id,
            TRIM(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                    LOWER(address),
                'astra tower ', ''),
                'jalan ', ''),
                'jl.', ''),
                'jl ', '')
            ) AS clean_address,
            city,
            province,
            created_at
        FROM latest
    )
    SELECT * FROM cleaned
) t;


