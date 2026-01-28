#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import Forecast Excel into PSI_System/database/forecast.db
Excel Path: D:\\WEB\\BI\\PSI_System\\各區需求.xlsx

Logic Update:
- Uses Product Code (Column A) to map to Product Name from Inventory DB.
- This ensures that even if Excel names differ slightly, we link to the correct system Product Name.
- Inserts into forecast_data using the canonical Product Name.

Structure:
- Row 0 (header): ["產品代碼", "產品名稱", "業務本部", ...]
- Row 1 (sub-header): ["nan", "nan", "1月", "2月", ...]
"""

import os
import sys
import argparse
import pandas as pd
import sqlite3
from datetime import datetime

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # PSI_System root
PROJECT_ROOT = os.path.dirname(BASE_DIR) # D:\WEB\BI

DB_PATH = os.path.join(BASE_DIR, "database", "forecast.db")
EXCEL_PATH = os.path.join(BASE_DIR, "各區需求.xlsx")
# Inventory DB path for Code->Name mapping
INV_DB_PATH = os.path.join(PROJECT_ROOT, "Inventory_inquiry_system", "database", "inventory_data.db")

def get_db_connection(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)

def ensure_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS forecast_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            region TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_name, region, year, month)
        )
    ''')
    conn.commit()

def load_product_map():
    """Load Code -> Name mapping from Inventory DB"""
    if not os.path.exists(INV_DB_PATH):
        print(f"Warning: Inventory DB not found at {INV_DB_PATH}. Cannot map codes.")
        return {}
    
    try:
        conn = sqlite3.connect(INV_DB_PATH)
        # Assuming product_code matches Excel Column A
        # Get distinct mappings. If duplicates, we take one (first).
        df = pd.read_sql_query("SELECT product_code, product_name FROM inventory_data", conn)
        conn.close()
        
        # Clean
        df = df.dropna(subset=['product_code', 'product_name'])
        df['product_code'] = df['product_code'].astype(str).str.strip()
        df['product_name'] = df['product_name'].astype(str).str.strip()
        
        # Create map
        # If one code maps to multiple names, this simple dict takes the last one. 
        # Ideally we want a stable canonical name.
        product_map = dict(zip(df['product_code'], df['product_name']))
        return product_map
    except Exception as e:
        print(f"Error loading product map: {e}")
        return {}

def import_data(year):
    print(f"Importing for year: {year}")
    print(f"Reading Excel: {EXCEL_PATH}")
    
    if not os.path.exists(EXCEL_PATH):
        print("Error: Excel file not found.")
        sys.exit(1)

    # 1. Load Mapping
    code_to_name = load_product_map()
    print(f"Loaded {len(code_to_name)} product codes from Inventory DB.")

    # 2. Read Excel
    try:
        df = pd.read_excel(EXCEL_PATH, header=None)
    except Exception as e:
        print(f"Error reading Excel: {e}")
        sys.exit(1)

    row0 = df.iloc[0].tolist()
    
    # Identify Region columns
    regions_map = {
        "業務本部": None,
        "桃竹苗事業部": None,
        "中區事業部": None,
        "南區事業部": None
    }
    
    for i, val in enumerate(row0):
        val_str = str(val).strip()
        if val_str in regions_map:
            regions_map[val_str] = i
            
    missing = [k for k, v in regions_map.items() if v is None]
    if missing:
        print(f"Warning: Could not find columns for regions: {missing}")
    
    conn = get_db_connection(DB_PATH)
    ensure_table(conn)
    cursor = conn.cursor()
    
    data_start_row = 2
    count = 0
    mapped_count = 0
    skipped_count = 0
    
    for idx in range(data_start_row, len(df)):
        row = df.iloc[idx].tolist()
        
        # Column A is Product Code, Column B is Name (Reference)
        raw_code = str(row[0]).strip()
        raw_name = str(row[1]).strip()
        
        if raw_code.lower() == 'nan':
            continue
            
        # Determine Canonical Name
        # Prefer mapping from DB, fallback to Excel name if missing (or skip?)
        # User asked to use Code. If Code not in DB, we can't link to Inventory anyway.
        # But maybe we still import it?
        # Let's try Map -> Fallback to Raw Name
        
        canonical_name = code_to_name.get(raw_code)
        
        if not canonical_name:
            # print(f"Notice: Code '{raw_code}' not found in Inventory DB. Using Excel name '{raw_name}'.")
            canonical_name = raw_name
            if canonical_name.lower() == 'nan':
                continue
        else:
            mapped_count += 1
            
        # Iterate regions
        for region, start_col in regions_map.items():
            if start_col is None: continue
            
            # Read 12 months
            for m_offset in range(12):
                col_idx = start_col + m_offset
                if col_idx >= len(row): break
                
                qty_val = row[col_idx]
                try:
                    qty = int(float(qty_val))
                except (ValueError, TypeError):
                    qty = 0
                
                # We upsert even if 0 to clear old data if Excel changed
                month = m_offset + 1
                
                cursor.execute("""
                    INSERT INTO forecast_data (product_name, region, year, month, quantity)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(product_name, region, year, month) 
                    DO UPDATE SET quantity=excluded.quantity, updated_at=CURRENT_TIMESTAMP
                """, (canonical_name, region, year, month, qty))
                count += 1
                
    conn.commit()
    conn.close()
    print(f"Import completed.")
    print(f"Total Records Processed: {count}")
    print(f"Products Mapped via Code: {mapped_count} (Rows)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Forecast Year")
    args = parser.parse_args()
    
    import_data(args.year)
