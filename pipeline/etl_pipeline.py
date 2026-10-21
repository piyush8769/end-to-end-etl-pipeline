#!/usr/bin/env python3
"""
Production Transactional ETL Data Pipeline CLI
Author: Piyush Kumar Singh
Target: REST API / JSON Streams -> Normalization & Validation -> MySQL Data Warehouse

Usage:
    python pipeline/etl_pipeline.py --input-dir data/raw_json --db-host localhost --db-user root --db-password Qwerty#@123 --db-name transactional_data_warehouse
"""

import os
import sys
import json
import glob
import time
import argparse
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text, URL

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ETL-Pipeline")

REQUIRED_ORDER_FIELDS = [
    "order_id", "order_timestamp", "customer", 
    "payment", "shipping", "order_items", 
    "order_status", "total_amount"
]

def extract_json_batches(input_dir: str):
    """
    Extract raw JSON transaction payloads from source files.
    """
    raw_files = sorted(glob.glob(os.path.join(input_dir, "batch_*.json")))
    logger.info("Found %d batch file(s) in %s", len(raw_files), input_dir)
    
    records = []
    for fp in raw_files:
        logger.info("Extracting batch payload from %s...", os.path.basename(fp))
        with open(fp, "r", encoding="utf-8") as f:
            batch_data = json.load(f)
            records.extend(batch_data)
            logger.info("  Loaded %d records from %s", len(batch_data), os.path.basename(fp))
            
    logger.info("Total raw records extracted: %d", len(records))
    return records

def validate_and_quarantine(records: list):
    """
    Validate record payloads against schema requirements.
    Route valid payloads to transformation, invalid to quarantine.
    """
    valid = []
    quarantined = []
    
    for rec in records:
        is_valid = True
        for field in REQUIRED_ORDER_FIELDS:
            if field not in rec:
                is_valid = False
                break
        
        if is_valid:
            if rec.get("customer") is None or rec.get("total_amount", 0) <= 0:
                is_valid = False
        
        if is_valid:
            valid.append(rec)
        else:
            quarantined.append(rec)
            
    logger.info("Schema Validation Complete: %d Valid, %d Quarantined", len(valid), len(quarantined))
    return valid, quarantined

def transform_and_normalize(records: list):
    """
    Flatten nested JSON hierarchy into Relational Star Schema:
    - dim_customers
    - dim_products
    - dim_payment_methods
    - fact_orders
    - fact_order_items
    """
    logger.info("Transforming and flattening payload hierarchy...")
    
    customers_dict = {}
    products_dict = {}
    orders_list = []
    items_list = []
    
    for r in records:
        cust = r["customer"]
        cid = cust["customer_id"]
        if cid not in customers_dict:
            customers_dict[cid] = {
                "customer_id": cid,
                "customer_name": cust.get("name"),
                "email": cust.get("email"),
                "phone": cust.get("phone"),
                "city": cust.get("city"),
                "state": cust.get("state"),
                "signup_date": cust.get("signup_date")
            }
            
        pay = r["payment"]
        ship = r["shipping"]
        
        orders_list.append({
            "order_id": r["order_id"],
            "order_timestamp": r["order_timestamp"],
            "customer_id": cid,
            "payment_id": pay.get("payment_id"),
            "payment_method": pay.get("payment_method"),
            "payment_gateway": pay.get("payment_gateway"),
            "payment_status": pay.get("payment_status"),
            "shipping_carrier": ship.get("shipping_carrier"),
            "shipping_fee": float(ship.get("shipping_fee", 0.0)),
            "delivery_status": ship.get("delivery_status"),
            "total_amount": round(float(r["total_amount"]), 2),
            "order_status": r["order_status"]
        })
        
        for item in r.get("order_items", []):
            pid = item.get("product_id")
            if pid not in products_dict:
                products_dict[pid] = {
                    "product_id": pid,
                    "product_name": item.get("product_name"),
                    "category": item.get("category"),
                    "unit_price": round(float(item.get("unit_price", 0.0)), 2),
                    "cost_price": round(float(item.get("unit_price", 0.0)) * 0.65, 2)
                }
            items_list.append({
                "item_id": item.get("item_id"),
                "order_id": r["order_id"],
                "product_id": pid,
                "quantity": int(item.get("quantity", 1)),
                "unit_price": round(float(item.get("unit_price", 0.0)), 2),
                "discount_amount": round(float(item.get("discount_amount", 0.0)), 2),
                "total_item_price": round(float(item.get("total_item_price", 0.0)), 2)
            })
            
    df_customers = pd.DataFrame(list(customers_dict.values()))
    df_products = pd.DataFrame(list(products_dict.values()))
    df_orders = pd.DataFrame(orders_list)
    df_items = pd.DataFrame(items_list)
    
    logger.info("Transformation finished: %d customers, %d products, %d orders, %d items",
                len(df_customers), len(df_products), len(df_orders), len(df_items))
    
    return df_customers, df_products, df_orders, df_items

def load_to_mysql(engine, df_customers, df_products, df_orders, df_items, batch_id, start_time, quarantined_count):
    """
    Bulk load normalized data into MySQL transactional warehouse.
    """
    logger.info("Beginning database streaming into MySQL...")
    
    logger.info("Loading dim_customers (%d rows)...", len(df_customers))
    df_customers.to_sql("dim_customers", con=engine, if_exists="replace", index=False)
    
    logger.info("Loading dim_products (%d rows)...", len(df_products))
    df_products.to_sql("dim_products", con=engine, if_exists="replace", index=False)
    
    logger.info("Loading fact_orders (%d rows) in 2,500-row chunks...", len(df_orders))
    df_orders.to_sql("fact_orders", con=engine, if_exists="replace", index=False, chunksize=2500)
    
    logger.info("Loading fact_order_items (%d rows) in 3,500-row chunks...", len(df_items))
    df_items.to_sql("fact_order_items", con=engine, if_exists="replace", index=False, chunksize=3500)
    
    runtime = round(time.time() - start_time, 2)
    end_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time_str = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
    
    batch_audit = pd.DataFrame([{
        "batch_id": batch_id,
        "source_system": "REST_API_JSON_FEED",
        "records_extracted": len(df_orders) + quarantined_count,
        "records_loaded": len(df_orders),
        "records_rejected": quarantined_count,
        "execution_status": "SUCCESS",
        "execution_start_time": start_time_str,
        "execution_end_time": end_time_str,
        "runtime_seconds": runtime
    }])
    
    batch_audit.to_sql("etl_batch_log", con=engine, if_exists="append", index=False)
    logger.info("Logged audit record to etl_batch_log: Runtime = %.2f seconds", runtime)

def main():
    parser = argparse.ArgumentParser(description="Transactional ETL Pipeline")
    parser.add_argument("--input-dir", default="data/raw_json", help="Path to raw JSON feeds")
    parser.add_argument("--db-host", default="localhost", help="MySQL Server Host")
    parser.add_argument("--db-port", type=int, default=3306, help="MySQL Server Port")
    parser.add_argument("--db-user", default="root", help="MySQL Username")
    parser.add_argument("--db-password", default="Qwerty#@123", help="MySQL Password")
    parser.add_argument("--db-name", default="transactional_data_warehouse", help="MySQL Database Name")
    args = parser.parse_args()
    
    start_time = time.time()
    batch_id = f"BATCH-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    logger.info("=== Starting Pipeline Execution: %s ===", batch_id)
    
    # 1. Extraction
    raw_records = extract_json_batches(args.input_dir)
    if not raw_records:
        logger.warning("No records found to process. Exiting.")
        return
        
    # 2. Validation & Quarantine
    valid_records, quarantined = validate_and_quarantine(raw_records)
    
    # 3. Transformation & Normalization
    df_customers, df_products, df_orders, df_items = transform_and_normalize(valid_records)
    
    # 4. Warehouse Loading
    db_url = URL.create(
        "mysql+mysqlconnector",
        username=args.db_user,
        password=args.db_password,
        host=args.db_host,
        port=args.db_port,
        database=args.db_name
    )
    engine = create_engine(db_url)
    load_to_mysql(
        engine, df_customers, df_products, df_orders, df_items, 
        batch_id, start_time, len(quarantined)
    )
    
    logger.info("=== Pipeline Execution Successfully Completed in %.2f seconds ===", time.time() - start_time)

if __name__ == "__main__":
    main()
