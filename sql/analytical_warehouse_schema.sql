-- ==============================================================================
-- End-to-End Transactional ETL Data Pipeline Warehouse Schema
-- Target Database: MySQL 8.0+
-- Database Name: transactional_data_warehouse
-- Schema Architecture: Star Schema (Dimensions + Facts + Audit Logging)
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS transactional_data_warehouse
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE transactional_data_warehouse;

-- ------------------------------------------------------------------------------
-- 1. Dimension Tables
-- ------------------------------------------------------------------------------

-- Customer Dimension
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    customer_name VARCHAR(128) NOT NULL,
    email VARCHAR(128) NOT NULL,
    phone VARCHAR(32),
    city VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL,
    signup_date DATE NOT NULL,
    customer_segment VARCHAR(32) DEFAULT 'Regular',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_cust_city (city),
    INDEX idx_cust_signup (signup_date)
) ENGINE=InnoDB;

-- Product Dimension
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(32) PRIMARY KEY,
    product_name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    cost_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_prod_cat (category)
) ENGINE=InnoDB;

-- Payment Method Dimension
CREATE TABLE IF NOT EXISTS dim_payment_methods (
    payment_method VARCHAR(64) PRIMARY KEY,
    method_category VARCHAR(64) NOT NULL,
    supported_gateways VARCHAR(128),
    is_active TINYINT(1) DEFAULT 1
) ENGINE=InnoDB;

-- ------------------------------------------------------------------------------
-- 2. Fact Tables
-- ------------------------------------------------------------------------------

-- Orders Fact Table
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    order_timestamp DATETIME NOT NULL,
    customer_id VARCHAR(32) NOT NULL,
    payment_id VARCHAR(64) NOT NULL,
    payment_method VARCHAR(64) NOT NULL,
    payment_gateway VARCHAR(64) NOT NULL,
    payment_status VARCHAR(32) NOT NULL,
    shipping_carrier VARCHAR(64) NOT NULL,
    shipping_fee DECIMAL(8, 2) NOT NULL DEFAULT 0.00,
    delivery_status VARCHAR(32) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    order_status VARCHAR(32) NOT NULL,
    INDEX idx_orders_cust (customer_id),
    INDEX idx_orders_dt (order_timestamp),
    INDEX idx_orders_status (order_status),
    INDEX idx_orders_gw (payment_gateway),
    INDEX idx_orders_pm (payment_method),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) 
        REFERENCES dim_customers(customer_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_orders_payment_method FOREIGN KEY (payment_method) 
        REFERENCES dim_payment_methods(payment_method) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;

-- Order Items Fact Table
CREATE TABLE IF NOT EXISTS fact_order_items (
    item_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(32) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    total_item_price DECIMAL(10, 2) NOT NULL,
    INDEX idx_items_order (order_id),
    INDEX idx_items_prod (product_id),
    CONSTRAINT fk_items_order FOREIGN KEY (order_id) 
        REFERENCES fact_orders(order_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_items_product FOREIGN KEY (product_id) 
        REFERENCES dim_products(product_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;

-- ------------------------------------------------------------------------------
-- 3. Operational Audit & Pipeline Logging Table
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl_batch_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    source_system VARCHAR(64) NOT NULL,
    records_extracted INT NOT NULL,
    records_loaded INT NOT NULL,
    records_rejected INT NOT NULL,
    execution_status VARCHAR(32) NOT NULL,
    execution_start_time DATETIME NOT NULL,
    execution_end_time DATETIME NOT NULL,
    runtime_seconds DECIMAL(8, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_batch_status (execution_status)
) ENGINE=InnoDB;

-- ------------------------------------------------------------------------------
-- 4. Analytical Views for Reporting & Business Intelligence
-- ------------------------------------------------------------------------------

-- Daily Executive Performance View
CREATE OR REPLACE VIEW vw_daily_executive_kpis AS
SELECT 
    DATE(order_timestamp) AS order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS active_customers,
    ROUND(SUM(total_amount), 2) AS gross_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
    SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders
FROM fact_orders
GROUP BY DATE(order_timestamp);

-- Customer Lifetime Metrics View
CREATE OR REPLACE VIEW vw_customer_order_metrics AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    COUNT(o.order_id) AS lifetime_orders,
    ROUND(COALESCE(SUM(o.total_amount), 0), 2) AS lifetime_spend,
    ROUND(COALESCE(AVG(o.total_amount), 0), 2) AS avg_order_value,
    MIN(o.order_timestamp) AS first_order_date,
    MAX(o.order_timestamp) AS most_recent_order_date,
    CASE 
        WHEN COUNT(o.order_id) = 1 THEN 'One-Time Customer'
        WHEN COUNT(o.order_id) BETWEEN 2 AND 4 THEN 'Repeat Customer'
        ELSE 'Loyal / High-Frequency'
    END AS customer_retention_tier
FROM dim_customers c
LEFT JOIN fact_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.city, c.state;

-- Product Category Revenue Performance View
CREATE OR REPLACE VIEW vw_product_category_performance AS
SELECT 
    p.category,
    COUNT(DISTINCT p.product_id) AS catalog_skus,
    SUM(i.quantity) AS total_units_sold,
    ROUND(SUM(i.total_item_price), 2) AS gross_category_revenue,
    ROUND(AVG(i.unit_price), 2) AS avg_unit_price
FROM dim_products p
JOIN fact_order_items i ON p.product_id = i.product_id
JOIN fact_orders o ON i.order_id = o.order_id
GROUP BY p.category;

-- Payment Gateway Performance View
CREATE OR REPLACE VIEW vw_payment_gateway_reliability AS
SELECT 
    payment_gateway,
    COUNT(order_id) AS total_transactions,
    SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
    SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_transactions,
    ROUND(SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2) AS success_rate_pct,
    ROUND(SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2) AS failure_rate_pct,
    ROUND(SUM(total_amount), 2) AS total_volume_inr
FROM fact_orders
GROUP BY payment_gateway;

-- ------------------------------------------------------------------------------
-- 5. Stored Procedures
-- ------------------------------------------------------------------------------

DELIMITER //

CREATE PROCEDURE sp_etl_record_batch_audit(
    IN p_batch_id VARCHAR(64),
    IN p_source_system VARCHAR(64),
    IN p_extracted INT,
    IN p_loaded INT,
    IN p_rejected INT,
    IN p_status VARCHAR(32),
    IN p_start DATETIME,
    IN p_end DATETIME,
    IN p_runtime DECIMAL(8, 2)
)
BEGIN
    INSERT INTO etl_batch_log (
        batch_id,
        source_system,
        records_extracted,
        records_loaded,
        records_rejected,
        execution_status,
        execution_start_time,
        execution_end_time,
        runtime_seconds
    ) VALUES (
        p_batch_id,
        p_source_system,
        p_extracted,
        p_loaded,
        p_rejected,
        p_status,
        p_start,
        p_end,
        p_runtime
    );
END //

CREATE PROCEDURE sp_get_monthly_revenue_summary(
    IN p_year_month VARCHAR(7)
)
BEGIN
    SELECT 
        DATE_FORMAT(order_timestamp, '%Y-%m') AS monthly_period,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_purchasers,
        ROUND(SUM(total_amount), 2) AS gross_revenue,
        ROUND(AVG(total_amount), 2) AS avg_ticket_size
    FROM fact_orders
    WHERE DATE_FORMAT(order_timestamp, '%Y-%m') = p_year_month
    GROUP BY DATE_FORMAT(order_timestamp, '%Y-%m');
END //

DELIMITER ;
