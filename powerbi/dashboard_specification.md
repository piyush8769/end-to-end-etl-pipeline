# Power BI Dashboard Specification: Transactional ETL & Warehouse Operations

## 1. Executive Overview & Business Objectives
This Power BI reporting solution provides real-time operational visibility into the end-to-end transactional data warehouse. It connects directly to the MySQL Star Schema (`transactional_data_warehouse`) to monitor pipeline throughput, batch ingestion latency, schema quarantine error rates, daily revenue run-rates, payment gateway conversion efficiency, and logistics carrier performance.

---

## 2. Star Schema Dimensional Data Model

```
       +-----------------------+
       |     dim_customers     |
       +-----------------------+
       | customer_id (PK)      |<----+
       | customer_name         |     |
       | city, state           |     |
       | signup_date           |     |
       +-----------------------+     |
                                     | (1-to-Many)
       +-----------------------+     |
       |  dim_payment_methods  |     |
       +-----------------------+     |
       | payment_method (PK)   |<----+----+
       | method_category       |     |    |
       | is_active             |     |    |
       +-----------------------+     |    |
                                     |    |
                   (1-to-Many)       |    |
       +-----------------------+     |    |
       |      fact_orders      |-----+----+
       +-----------------------+     |
       | order_id (PK)         |<----+ (1-to-Many)
       | customer_id (FK)      |     |
       | order_timestamp       |     |
       | payment_gateway       |     |
       | payment_status        |     |
       | shipping_carrier      |     |
       | delivery_status       |     |
       | total_amount          |     |
       | order_status          |     |
       +-----------------------+     |
                   |                 |
                   | (1-to-Many)     |
                   v                 |
       +-----------------------+     |
       |   fact_order_items    |     |
       +-----------------------+     |
       | item_id (PK)          |     |
       | order_id (FK)         |     |
       | product_id (FK)-------+-----+----+
       | quantity              |          |
       | unit_price            |          |
       | discount_amount       |          |
       | total_item_price      |          |
       +-----------------------+          |
                                          | (Many-to-1)
       +-----------------------+          |
       |     dim_products      |          |
       +-----------------------+          |
       | product_id (PK)       |<---------+
       | product_name          |
       | category              |
       | unit_price            |
       | cost_price            |
       +-----------------------+
```

---

## 3. Core DAX Measure Dictionary

### 3.1 Volume & Ingestion KPIs
```dax
Total Orders = COUNTROWS('fact_orders')

Total Line Items = COUNTROWS('fact_order_items')

Unique Customers = DISTINCTCOUNT('fact_orders'[customer_id])

Active SKUs = DISTINCTCOUNT('fact_order_items'[product_id])

Average Items Per Order = DIVIDE([Total Line Items], [Total Orders], 0)
```

### 3.2 Revenue & Financial Metrics
```dax
Gross Realized Revenue = SUM('fact_orders'[total_amount])

Net Product Revenue = SUM('fact_order_items'[total_item_price])

Total Shipping Fees = SUM('fact_orders'[shipping_fee])

Average Order Value (AOV) = DIVIDE([Gross Realized Revenue], [Total Orders], 0)

Total Discounts = SUM('fact_order_items'[discount_amount])
```

### 3.3 Pipeline & Gateway SLA Performance
```dax
Successful Payments = 
CALCULATE(
    COUNTROWS('fact_orders'),
    'fact_orders'[payment_status] = "SUCCESS"
)

Failed Payments = 
CALCULATE(
    COUNTROWS('fact_orders'),
    'fact_orders'[payment_status] = "FAILED"
)

Payment Success Rate (%) = 
DIVIDE([Successful Payments], [Total Orders], 0) * 100

Payment Failure Rate (%) = 
DIVIDE([Failed Payments], [Total Orders], 0) * 100

Delivered Orders = 
CALCULATE(
    COUNTROWS('fact_orders'),
    'fact_orders'[delivery_status] = "DELIVERED"
)

Delivery Fulfillment Rate (%) = 
DIVIDE([Delivered Orders], [Total Orders], 0) * 100
```

---

## 4. Dashboard Canvas Layout & Visual Hierarchy

### Page 1: Executive Warehouse Operations & Revenue Run-Rate
1. **Top KPI Ribbon (5 Cards):**
   - Total Gross Revenue: `Rs. 45.2M`
   - Ingested Orders: `45,000`
   - Active Customers: `12,000`
   - Average Order Value: `Rs. 1,004`
   - Pipeline Success Rate: `99.994%` (3 quarantined)
2. **Visual 1 (Main Area - Line & Clustered Column Chart):**
   - X-Axis: `order_timestamp` (Daily Hierarchy)
   - Columns: `Total Orders`
   - Line: `Gross Realized Revenue`
3. **Visual 2 (Horizontal Bar Chart):**
   - Top 10 Revenue-Generating Catalog SKUs (`dim_products[product_name]`)
4. **Visual 3 (Donut Chart):**
   - Revenue Share by Product Category (`dim_products[category]`)
5. **Interactive Slicers:**
   - Date Range Slider (`order_timestamp`)
   - Order Status Dropdown (`DELIVERED`, `SHIPPED`, `CANCELLED`, `PENDING`)

### Page 2: Gateway Reliability & Logistics Carrier Fulfillment
1. **Top KPI Ribbon (4 Cards):**
   - Gateway Success Rate: `92.4%`
   - Ingestion Throughput: `333 records/sec`
   - Carrier Delivery Rate: `88.6%`
   - Quarantine Error Rate: `0.006%`
2. **Visual 4 (Stacked 100% Bar Chart):**
   - Y-Axis: `payment_gateway` (Razorpay, Stripe, PayU, Paytm, Cashfree)
   - X-Axis: `payment_status` (`SUCCESS`, `FAILED`, `REFUNDED`)
3. **Visual 5 (Clustered Column Chart):**
   - X-Axis: `shipping_carrier` (BlueDart, Delhivery, DTDC, Ekart, Shadowfax)
   - Y-Axis: `Total Orders` by `delivery_status`
4. **Visual 6 (Data Table):**
   - Real-time `etl_batch_log` audit records (Batch ID, Extracted, Loaded, Quarantined, Runtime).
