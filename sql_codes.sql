--1 Creating schemas
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

--Creating tables
--raw_superstore
CREATE TABLE stage.raw_superstore (
    row_id VARCHAR(50),
    order_id VARCHAR(50),
    order_date VARCHAR(50),
    ship_date VARCHAR(50),
    ship_mode VARCHAR(50),
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(50),
    region VARCHAR(50),
    product_id VARCHAR(50),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name TEXT,
    sales VARCHAR(50),
    quantity VARCHAR(50),
    discount VARCHAR(50),
    profit VARCHAR(50)
);

--core tables
-- 1. Customer table
CREATE TABLE core.customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    segment VARCHAR(50)
);

-- 2. Product table
CREATE TABLE core.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name TEXT,
    category VARCHAR(50),
    sub_category VARCHAR(50)
);

-- 3. orders table
CREATE TABLE core.orders (
    order_id VARCHAR(50) PRIMARY KEY,
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(50) REFERENCES core.customers(customer_id),
    country VARCHAR(50),
    region VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(50),
    postal_code VARCHAR(50)
);

-- 4. Order Details table
CREATE TABLE core.order_details (
    row_id INTEGER PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES core.orders(order_id),
    product_id VARCHAR(50) REFERENCES core.products(product_id),
    sales NUMERIC,
    quantity INTEGER,
    discount NUMERIC,
    profit NUMERIC
);

--Inserting data into core tables
-- 1. Loading unique customers
INSERT INTO core.customers (customer_id, customer_name, segment)
SELECT DISTINCT customer_id, customer_name, segment
FROM stage.raw_superstore
WHERE customer_id IS NOT NULL;

-- Loading cleaned data 
WITH cte_products AS (
    SELECT 
        product_id, 
        product_name, 
        category, 
        sub_category,
        -- Har bir ID bo'yicha guruhlab, ularga 1, 2, 3 deb tartib raqami beramiz
        ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY product_name) as rn
    FROM stage.raw_superstore
    WHERE product_id IS NOT NULL
)
INSERT INTO core.products (product_id, product_name, category, sub_category)
SELECT product_id, product_name, category, sub_category
FROM cte_products
WHERE rn = 1; -- Faqat 1-tartib raqamdagisini (yagonasini) olamiz

-- 3. loading orders
INSERT INTO core.orders (order_id, order_date, ship_date, ship_mode, customer_id, country, region, state, city, postal_code)
SELECT DISTINCT 
    order_id, 
    TO_DATE(order_date, 'MM/DD/YYYY'), 
    TO_DATE(ship_date, 'MM/DD/YYYY'), 
    ship_mode, 
    customer_id, 
    country, 
    region, 
    state, 
    city, 
    postal_code
FROM stage.raw_superstore
WHERE order_id IS NOT NULL;

-- 4. Loading order details
INSERT INTO core.order_details (row_id, order_id, product_id, sales, quantity, discount, profit)
SELECT 
    CAST(row_id AS INTEGER),
    order_id,
    product_id,
    CAST(sales AS NUMERIC),
    CAST(quantity AS INTEGER),
    CAST(discount AS NUMERIC),
    CAST(profit AS NUMERIC)
FROM stage.raw_superstore
WHERE row_id IS NOT NULL;

--Mart tables
-- 1. Customer table (with additional columns for SCD1 and SCD2)
CREATE TABLE mart.dim_customer (
    customer_sk SERIAL PRIMARY KEY, -- Surrogate Key for DataWerhouse
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    valid_from DATE DEFAULT CURRENT_DATE,
    valid_to DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- 2. Location table
CREATE TABLE mart.dim_location (
    location_sk SERIAL PRIMARY KEY,
    country VARCHAR(50),
    region VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(50),
    postal_code VARCHAR(50)
);

-- 3. Product table
CREATE TABLE mart.dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    product_name TEXT,
    category VARCHAR(50),
    sub_category VARCHAR(50)
);

-- 4. Fact table
CREATE TABLE mart.fact_sales (
    sales_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    order_date DATE,
    ship_date DATE,
    customer_sk INTEGER REFERENCES mart.dim_customer(customer_sk),
    location_sk INTEGER REFERENCES mart.dim_location(location_sk),
    product_sk INTEGER REFERENCES mart.dim_product(product_sk),
    sales NUMERIC,
    quantity INTEGER,
    discount NUMERIC,
    profit NUMERIC
);

--insert data into mart tables
-- 1. loading customers
INSERT INTO mart.dim_customer (customer_id, customer_name, segment)
SELECT customer_id, customer_name, segment FROM core.customers;

-- 2. Loading unique locations
INSERT INTO mart.dim_location (country, region, state, city, postal_code)
SELECT DISTINCT country, region, state, city, postal_code FROM core.orders;

-- 3. loading products
INSERT INTO mart.dim_product (product_id, product_name, category, sub_category)
SELECT product_id, product_name, category, sub_category FROM core.products;

-- 4. Loading data into fact table
INSERT INTO mart.fact_sales (order_id, order_date, ship_date, customer_sk, location_sk, product_sk, sales, quantity, discount, profit)
SELECT 
    o.order_id, 
    o.order_date, 
    o.ship_date,
    c.customer_sk,
    l.location_sk,
    p.product_sk,
    od.sales,
    od.quantity,
    od.discount,
    od.profit
FROM core.order_details od
JOIN core.orders o ON od.order_id = o.order_id
JOIN mart.dim_customer c ON o.customer_id = c.customer_id AND c.is_current = TRUE
JOIN mart.dim_location l ON 
    COALESCE(o.country, '') = COALESCE(l.country, '') AND 
    COALESCE(o.region, '') = COALESCE(l.region, '') AND 
    COALESCE(o.state, '') = COALESCE(l.state, '') AND 
    COALESCE(o.city, '') = COALESCE(l.city, '') AND 
    COALESCE(o.postal_code, '') = COALESCE(l.postal_code, '')
JOIN mart.dim_product p ON od.product_id = p.product_id;


-- incremental load (SCD1 and SCD2) 
-- A) adding new customers who do not exist in the customers table 
INSERT INTO mart.dim_customer (customer_id, customer_name, segment)
SELECT DISTINCT s.customer_id, s.customer_name, s.segment
FROM stage.raw_superstore s
LEFT JOIN mart.dim_customer c ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- B) SCD TYPE 1 (name changing)
UPDATE mart.dim_customer c
SET customer_name = s.customer_name
FROM stage.raw_superstore s
WHERE c.customer_id = s.customer_id
  AND c.is_current = TRUE
  AND c.customer_name <> s.customer_name;

-- C) SCD TYPE 2 (Segment changed)
UPDATE mart.dim_customer c
SET valid_to = CURRENT_DATE, is_current = FALSE
FROM stage.raw_superstore s
WHERE c.customer_id = s.customer_id
  AND c.is_current = TRUE
  AND c.segment <> s.segment;

-- a new record will be inserted with a revised status
INSERT INTO mart.dim_customer (customer_id, customer_name, segment)
SELECT DISTINCT s.customer_id, s.customer_name, s.segment
FROM stage.raw_superstore s
JOIN mart.dim_customer c ON s.customer_id = c.customer_id
WHERE c.is_current = FALSE AND c.valid_to = CURRENT_DATE;

-- skipping duplicates and adding new sales 
INSERT INTO mart.fact_sales (order_id, order_date, ship_date, customer_sk, location_sk, product_sk, sales, quantity, discount, profit)
SELECT 
    s.order_id, 
    TO_DATE(s.order_date, 'MM/DD/YYYY'), 
    TO_DATE(s.ship_date, 'MM/DD/YYYY'),
    c.customer_sk,
    l.location_sk,
    p.product_sk,
    CAST(s.sales AS NUMERIC),
    CAST(s.quantity AS INTEGER),
    CAST(s.discount AS NUMERIC),
    CAST(s.profit AS NUMERIC)
FROM stage.raw_superstore 
JOIN mart.dim_customer c ON s.customer_id = c.customer_id AND c.is_current = TRUE
JOIN mart.dim_location l ON 
    COALESCE(s.country, '') = COALESCE(l.country, '') AND 
    COALESCE(s.region, '') = COALESCE(l.region, '') AND 
    COALESCE(s.state, '') = COALESCE(l.state, '') AND 
    COALESCE(s.city, '') = COALESCE(l.city, '') AND 
    COALESCE(s.postal_code, '') = COALESCE(l.postal_code, '')
JOIN mart.dim_product p ON s.product_id = p.product_id
LEFT JOIN mart.fact_sales f ON s.order_id = f.order_id AND p.product_sk = f.product_sk
WHERE f.sales_id IS NULL;