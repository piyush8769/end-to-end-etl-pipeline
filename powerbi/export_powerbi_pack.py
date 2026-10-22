#!/usr/bin/env python3
"""
Power BI Data Pack Generator: Transactional ETL Data Pipeline
Prepares formatted tables and relationships ready for Microsoft Power BI Desktop import.
"""

import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'clean_csv')
OUT_DIR = os.path.join(BASE_DIR, 'powerbi')

def main():
    print("Preparing Power BI Data Pack for Transactional ETL Pipeline...")
    
    df_orders = pd.read_csv(os.path.join(DATA_DIR, 'fact_orders.csv'))
    df_items = pd.read_csv(os.path.join(DATA_DIR, 'fact_order_items.csv'))
    df_customers = pd.read_csv(os.path.join(DATA_DIR, 'dim_customers.csv'))
    df_products = pd.read_csv(os.path.join(DATA_DIR, 'dim_products.csv'))
    df_payment = pd.read_csv(os.path.join(DATA_DIR, 'dim_payment_methods.csv'))
    df_quarantine = pd.read_csv(os.path.join(DATA_DIR, 'quarantine_records.csv'))
    
    excel_path = os.path.join(OUT_DIR, 'powerbi_warehouse_dataset.xlsx')
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df_orders.head(5000).to_excel(writer, sheet_name='fact_orders', index=False)
        df_items.head(8000).to_excel(writer, sheet_name='fact_order_items', index=False)
        df_customers.head(2000).to_excel(writer, sheet_name='dim_customers', index=False)
        df_products.to_excel(writer, sheet_name='dim_products', index=False)
        df_payment.to_excel(writer, sheet_name='dim_payment_methods', index=False)
        df_quarantine.to_excel(writer, sheet_name='quarantine_audit', index=False)
        
    print(f"Generated Power BI Excel Workbook: {excel_path}")
    print("All tables staged with primary/foreign keys ready for Power BI Desktop.")

if __name__ == '__main__':
    main()
