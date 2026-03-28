

-- report 1
SET @run_date = '2025-03-02';

SELECT
    DATE_FORMAT(invoice_date, '%Y-%m') AS periode,
    CASE
        WHEN price BETWEEN 100000000 AND 250000000 THEN 'LOW'
        WHEN price BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
        WHEN price > 400000000 THEN 'HIGH'
    END AS class,
    model,
    SUM(price) AS total
FROM warehouse.sales_clean
WHERE invoice_date = CAST(@run_date AS DATE) - INTERVAL 1 DAY
GROUP BY
    DATE_FORMAT(invoice_date, '%Y-%m'),
    class,
    model
ORDER BY
    periode,
    class,
    model;
    

   
-- report 2

SET @run_date = '2025-07-12';

SELECT
    DATE_FORMAT(a.service_date, '%Y')   AS periode,
    a.vin,
    c.name                               AS customer_name,
    ca.address,
    COUNT(a.service_ticket)              AS count_service,
    CASE
        WHEN COUNT(a.service_ticket) > 10   THEN 'HIGH'
        WHEN COUNT(a.service_ticket) >= 5   THEN 'MED'
        ELSE 'LOW'
    END AS priority
FROM warehouse.after_sales_clean a
LEFT JOIN warehouse.customers_clean c
    ON a.customer_id = c.customer_id
LEFT JOIN warehouse.customer_addresses_clean ca
    ON a.customer_id = ca.customer_id
WHERE a.service_date = CAST(@run_date AS DATE) - INTERVAL 1 DAY
GROUP BY
    DATE_FORMAT(a.service_date, '%Y'),
    a.vin,
    c.name,
    ca.address
ORDER BY
    periode,
    count_service DESC;