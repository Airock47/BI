#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
庫存查詢系統
支援產品名稱、倉庫往來對象名稱關鍵字查詢
"""

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
import sqlite3
import os
import time
from functools import wraps

# 創建Blueprint
inventory_inquiry_bp = Blueprint(
    'inventory_inquiry',
    __name__,
    url_prefix='/inventory_inquiry',
    template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
    static_folder=os.path.join(os.path.dirname(__file__), 'static')
)

# 資料庫路徑
DATABASE_PATH = os.path.join(os.path.dirname(__file__), 'database', 'inventory_data.db')
SQLITE_QUERY_TIMEOUT_SECONDS = float(os.getenv("SQLITE_QUERY_TIMEOUT_SECONDS", "8"))

def _apply_query_timeout(conn, timeout=SQLITE_QUERY_TIMEOUT_SECONDS):
    start = [time.perf_counter()]

    def _trace(_sql):
        start[0] = time.perf_counter()

    def _handler():
        if time.perf_counter() - start[0] > timeout:
            raise sqlite3.OperationalError("query exceeded time limit")
        return 0

    conn.set_trace_callback(_trace)
    conn.set_progress_handler(_handler, 1000)
    return conn


def login_required(f):
    """登入驗證裝飾器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@inventory_inquiry_bp.route('/')
@login_required
def index():
    """庫存查詢主頁面"""
    return render_template('inventory_inquiry.html')

@inventory_inquiry_bp.route('/search', methods=['POST'])
@login_required
def search_inventory():
    """庫存查詢API"""
    try:
        # 獲取查詢參數
        product_name = request.form.get('product_name', '').strip()
        warehouse_partner = request.form.get('warehouse_partner', '').strip()
        inventory_type = request.form.get('inventory_type', '').strip()
        
        # 建立資料庫連接
        conn = _apply_query_timeout(sqlite3.connect(DATABASE_PATH))
        cursor = conn.cursor()
        
        # 建立查詢條件
        conditions = []
        params = []
        
        # 產品名稱關鍵字查詢
        if product_name:
            conditions.append("product_name LIKE ?")
            params.append(f"%{product_name}%")
        
        # 倉庫往來對象名稱關鍵字查詢
        if warehouse_partner:
            conditions.append("warehouse_partner_name LIKE ?")
            params.append(f"%{warehouse_partner}%")
        
        # 存貨屬性查詢（直接依照 Excel 匯入之欄位，不做額外計算）
        if inventory_type:
            conditions.append("inventory_type = ?")
            params.append(inventory_type)
        else:
            # 如果沒有選擇存貨屬性，則過濾掉數量為0的記錄
            conditions.append("quantity != 0")
        
        # 建立SQL查詢
        base_query = """
            SELECT 
                product_name,
                warehouse_name,
                inventory_type,
                warehouse_partner_name,
                quantity
            FROM inventory_data
        """
        
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            query = base_query
        
        query += " ORDER BY product_name, warehouse_name"
        
        # 執行查詢
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # 轉換為字典格式
        inventory_data = []
        for row in results:
            inventory_data.append({
                'product_name': row[0] or '',
                'warehouse_name': row[1] or '',
                'inventory_type': row[2] or '',
                'warehouse_partner_name': row[3] or '',
                'quantity': row[4] or 0
            })
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': inventory_data,
            'total': len(inventory_data)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })


