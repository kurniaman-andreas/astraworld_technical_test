USE warehouse;

CREATE TABLE IF NOT EXISTS customers_raw (
    id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    dob VARCHAR(20) NULL,
    created_at DATETIME(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales_raw (
    vin VARCHAR(20) NOT NULL,
    customer_id INT NOT NULL,
    model VARCHAR(50) NOT NULL,
    invoice_date DATE NOT NULL,
    price VARCHAR(20) NOT NULL,
    created_at DATETIME(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS after_sales_raw (
    service_ticket VARCHAR(20) NOT NULL,
    vin VARCHAR(20) NOT NULL,
    customer_id INT NOT NULL,
    model VARCHAR(50) NOT NULL,
    service_date DATE NOT NULL,
    service_type VARCHAR(10) NOT NULL,
    created_at DATETIME(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS customer_addresses_raw (
    id INT NOT NULL,
    customer_id INT NOT NULL,
    address VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    province VARCHAR(100) NOT NULL,
    created_at DATETIME(3) NOT NULL
);
