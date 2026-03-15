-- ============================================================================
-- Declarative Data Pipeline - SQL Transformations
-- ============================================================================

-- SILVER LAYER - Cleaned & Validated Data

-- Clean Customer Data
CREATE OR REPLACE TABLE silver_ecommerce.silver_customers AS
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    country,
    TO_DATE(registration_date, 'yyyy-MM-dd') AS registration_date,
    CURRENT_TIMESTAMP() AS processed_at
FROM bronze_ecommerce.bronze_customers
WHERE 
    customer_id IS NOT NULL
    AND email LIKE '%@%.%'
    AND phone IS NOT NULL;

-- Clean Product Data
CREATE OR REPLACE TABLE silver_ecommerce.silver_products AS
SELECT 
    product_id,
    product_name,
    category,
    price,
    stock_quantity,
    supplier_id,
    TO_DATE(created_date, 'yyyy-MM-dd') AS created_date,
    CURRENT_TIMESTAMP() AS processed_at
FROM bronze_ecommerce.bronze_products
WHERE 
    product_id IS NOT NULL
    AND price > 0
    AND stock_quantity >= 0;

-- Clean Order Data
CREATE OR REPLACE TABLE silver_ecommerce.silver_orders AS
SELECT 
    order_id,
    customer_id,
    product_id,
    quantity,
    TO_DATE(order_date, 'yyyy-MM-dd') AS order_date,
    TO_DATE(delivery_date, 'yyyy-MM-dd') AS delivery_date,
    order_status,
    total_amount,
    CURRENT_TIMESTAMP() AS processed_at
FROM bronze_ecommerce.bronze_orders
WHERE 
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND quantity > 0;

-- ============================================================================
-- GOLD LAYER - Business Analytics

-- Customer Order Summary
CREATE OR REPLACE TABLE gold_ecommerce.gold_customer_orders AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.country,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_spent,
    SUM(o.quantity) AS total_items_purchased,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    DATEDIFF(MAX(o.order_date), MIN(o.order_date)) AS days_as_customer
FROM silver_ecommerce.silver_customers c
LEFT JOIN silver_ecommerce.silver_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.country;

-- Product Sales Performance
CREATE OR REPLACE TABLE gold_ecommerce.gold_product_sales AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.stock_quantity,
    COUNT(o.order_id) AS total_orders,
    SUM(o.quantity) AS units_sold,
    SUM(o.total_amount) AS total_revenue,
    ROUND(SUM(o.total_amount) / NULLIF(COUNT(o.order_id), 0), 2) AS avg_order_value,
    ROUND(100.0 * SUM(o.quantity) / (SELECT SUM(quantity) FROM silver_ecommerce.silver_orders), 2) AS percentage_of_total_sales
FROM silver_ecommerce.silver_products p
LEFT JOIN silver_ecommerce.silver_orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.product_name, p.category, p.price, p.stock_quantity;

-- Order Metrics by Status
CREATE OR REPLACE TABLE gold_ecommerce.gold_order_metrics AS
SELECT 
    order_status,
    COUNT(order_id) AS order_count,
    SUM(total_amount) AS total_revenue,
    SUM(quantity) AS total_quantity,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM silver_ecommerce.silver_orders), 2) AS percentage
FROM silver_ecommerce.silver_orders
GROUP BY order_status;

-- Daily Sales Dashboard
CREATE OR REPLACE TABLE gold_ecommerce.gold_daily_sales AS
SELECT 
    order_date,
    COUNT(order_id) AS orders,
    SUM(total_amount) AS revenue,
    SUM(quantity) AS units,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM silver_ecommerce.silver_orders
GROUP BY order_date
ORDER BY order_date DESC;

-- Category Performance
CREATE OR REPLACE TABLE gold_ecommerce.gold_category_performance AS
SELECT 
    p.category,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    SUM(o.quantity) AS total_quantity,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value
FROM silver_ecommerce.silver_products p
LEFT JOIN silver_ecommerce.silver_orders o ON p.product_id = o.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;
