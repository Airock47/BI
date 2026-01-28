import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, jsonify
from functools import wraps

psi_bp = Blueprint('psi', __name__, template_folder='templates')

# Database Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # D:\WEB\BI\PSI_System
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR)) # D:\WEB\BI (assuming PSI_System is in D:\WEB\BI)
# Actually, BASE_DIR is D:\WEB\BI\PSI_System. parent is D:\WEB\BI.
PROJECT_ROOT = os.path.dirname(BASE_DIR)

PSI_SALES_DB = os.path.join(PROJECT_ROOT, "ai_assistant", "database", "sales_ai.db")
PSI_INVENTORY_DB = os.path.join(PROJECT_ROOT, "Inventory_inquiry_system", "database", "inventory_data.db")
PSI_PROCURE_DB = os.path.join(PROJECT_ROOT, "ProcureTrack", "database", "procure.db")
PSI_FORECAST_DB = os.path.join(BASE_DIR, "database", "forecast.db")

def get_db_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def init_forecast_db():
    """Ensure forecast database and table exist."""
    db_dir = os.path.dirname(PSI_FORECAST_DB)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    conn = sqlite3.connect(PSI_FORECAST_DB)
    cursor = conn.cursor()
    cursor.execute('''
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
    
    # New Table for Supplementary Notes
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS product_notes (
            product_name TEXT PRIMARY KEY,
            note TEXT,
            updated_by TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

# Initialize DB on module import
init_forecast_db()

# Login required decorator stub if not imported from main app. 
# Ideally we import from app.py but circular import risk.
# Or we just assume @login_required is handled by flask-login globally?
# The code in app.py uses `from functools import wraps` and custom decorator or flask_login?
# app.py: line 475 @login_required.
# Let's inspect app.py lines 1-50 to see where login_required comes from.
# It seems it was defined in app.py as seen in previous turn (line 40 in procure.py, likely similar in app.py).
# app.py snippet shows `from functools import wraps`.
# I should re-define a dummy login_required or import the session logic.
# For now, let's implement the decorator locally to check session 'logged_in'.
from flask import session, redirect, url_for

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or 'application/json' in request.accept_mimetypes:
                 return jsonify({'error': 'Unauthorized', 'redirect': url_for('login')}), 401
            return redirect(url_for('login')) # Assuming 'login' route exists in main app
        return f(*args, **kwargs)
    return decorated_function

def check_permission(region):
    """
    Check if current user can edit the given region.
    Rules:
    1. Admins (C4D002, B8A002) can edit ALL.
    2. Supervisors (is_supervisor='V' or 1) can edit their OWN department.
    3. Others cannot edit.
    """
    username = session.get('username', '').upper()
    if username in ['C4D002', 'B8A002']:
        return True
    
    user_dept = session.get('department', '')
    is_supervisor = str(session.get('is_supervisor', '')).strip().upper()
    
    # Check Supervisor Status (Assuming 'V' or 'Y' based on DB)
    if is_supervisor in ['V', 'Y']:
        if user_dept == region:
            return True
            
    return False


@psi_bp.route("/", methods=["GET"])
@login_required
def index():
    data, month_labels, next_12_months, procure_details, is_supervisor = get_psi_data()
    return render_template(
        "psi_annual.html",
        data=data,
        month_labels=month_labels,
        next_12_months_tuples=next_12_months,
        is_supervisor=is_supervisor
    )

@psi_bp.route("/get_inventory_detail")
@login_required
def get_inventory_detail():
    product_name = request.args.get('product_name')
    if not product_name:
        return jsonify([])
    try:
        conn = get_db_connection(PSI_INVENTORY_DB)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT warehouse_name, quantity 
            FROM inventory_data 
            WHERE product_name = ? 
              AND warehouse_name IN ('中壢', '台中', '高雄', '泰山')
              AND inventory_type = '世磊'
              AND quantity > 0
        """, (product_name,))
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify(rows)
    except Exception as e:
        print(f"Error fetching inventory detail: {e}")
        return jsonify({'error': str(e)}), 500

@psi_bp.route("/get_forecast_by_region")
@login_required
def get_forecast_by_region():
    region = request.args.get('region')
    if not region: return jsonify({})
    
    try:
        conn = get_db_connection(PSI_FORECAST_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT product_name, year, month, quantity FROM forecast_data WHERE region = ?", (region,))
        rows = cursor.fetchall()
        conn.close()
        
        result = {}
        for row in rows:
            p = row['product_name']
            k = f"{row['year']}-{row['month']}"
            if p not in result: result[p] = {}
            result[p][k] = row['quantity']
            
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@psi_bp.route("/get_forecast_detail_by_month")
@login_required
def get_forecast_detail_by_month():
    product_name = request.args.get('product_name')
    year = request.args.get('year')
    month = request.args.get('month')
    if not all([product_name, year, month]): return jsonify({})
    
    try:
        conn = get_db_connection(PSI_FORECAST_DB)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT region, quantity 
            FROM forecast_data 
            WHERE product_name = ? AND year = ? AND month = ?
        """, (product_name, year, month))
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        result = {row['region']: row['quantity'] for row in rows}
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@psi_bp.route("/get_forecast_detail_total")
@login_required
def get_forecast_detail_total():
    product_name = request.args.get('product_name')
    if not product_name: return jsonify({})
    
    try:
        # Calculate Next 12 Months (Same logic as in main route)
        today = datetime.now()
        current_year = today.year
        current_month = today.month
        
        next_12_months = []
        for i in range(12):
            m = current_month + i
            y = current_year + (m - 1) // 12
            m = (m - 1) % 12 + 1
            next_12_months.append((y, m))
            
        # Build query for these (year, month) pairs
        # We can use "WHERE product_name = ? AND ( (year=? AND month=?) OR ... )"
        # Or just fetch all for product and filter in python (easier query, maybe slightly more data but fine for this scale)
        
        conn = get_db_connection(PSI_FORECAST_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT region, year, month, quantity FROM forecast_data WHERE product_name = ?", (product_name,))
        rows = cursor.fetchall()
        conn.close()
        
        # Filter and Sum
        result = {}
        target_months = set(next_12_months)
        
        for row in rows:
            if (row['year'], row['month']) in target_months:
                reg = row['region']
                qty = row['quantity']
                result[reg] = result.get(reg, 0) + qty
                
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@psi_bp.route("/get_region_permission")
@login_required
def get_region_permission():
    region = request.args.get('region', '')
    allowed = check_permission(region)
    return jsonify({'allowed': allowed})

@psi_bp.route("/update_forecast", methods=["POST"])
@login_required
def update_forecast():
    try:
        data = request.json
        region = data.get('region')
        updates = data.get('updates', [])
        
        if not region:
            return jsonify({'error': 'Missing region'}), 400
            
        # Permission Check
        if not check_permission(region):
            return jsonify({'error': f'無權限修改【{region}】的預估需求'}), 403

        conn = get_db_connection(PSI_FORECAST_DB)
        cursor = conn.cursor()
        
        for item in updates:
            product_name = item.get('product_name')
            year = item.get('year')
            month = item.get('month')
            quantity = item.get('quantity')
            
            if not all([product_name, year, month, quantity is not None]):
                continue
                
            cursor.execute('''
                INSERT INTO forecast_data (product_name, region, year, month, quantity)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(product_name, region, year, month) 
                DO UPDATE SET quantity=excluded.quantity, updated_at=CURRENT_TIMESTAMP
            ''', (product_name, region, year, month, quantity))
            
        conn.commit()
        conn.close()
        
        return jsonify({'message': 'Saved successfully'})
        
    except Exception as e:
        print(f"Error updating forecast: {e}")
        return jsonify({'error': str(e)}), 500

@psi_bp.route("/get_procure_detail")
@login_required
def get_procure_detail():
    product_name = request.args.get('product_name')
    if not product_name: return jsonify([])
    
    try:
        conn = get_db_connection(PSI_PROCURE_DB)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT product_name, 
                   goods_status, 
                   (quantity - warehouse_qty) as pending_qty,
                   dispatch_date,
                   remarks,
                   warehouse
            FROM procure_items
            WHERE product_name = ?
              AND (quantity - warehouse_qty) > 0
              AND status = '生效'
            ORDER BY dispatch_date
        """, (product_name,))
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@psi_bp.route("/update_note", methods=["POST"])
@login_required
def update_note():
    try:
        data = request.json
        product_name = data.get('product_name')
        note = data.get('note')
        
        if not product_name:
            return jsonify({'error': 'Missing product name'}), 400

        # Check Supervisor Permission
        is_supervisor = str(session.get('is_supervisor', '')).strip().upper()
        if is_supervisor not in ['Y', 'V']:
             return jsonify({'error': 'Permission denied: Only supervisors can edit notes'}), 403
             
        user_name = session.get('name', 'Unknown')
        
        conn = get_db_connection(PSI_FORECAST_DB)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO product_notes (product_name, note, updated_by, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(product_name) 
            DO UPDATE SET note=excluded.note, updated_by=excluded.updated_by, updated_at=CURRENT_TIMESTAMP
        ''', (product_name, note, user_name))
        
        conn.commit()
        conn.close()
        
        now_str = datetime.now().strftime('%Y-%m-%d')
        return jsonify({'message': 'Note saved', 'updated_by': user_name, 'updated_at': now_str})
        
    except Exception as e:
        print(f"Error updating note: {e}")
        return jsonify({'error': str(e)}), 500

def get_psi_data():
    # 1. Inventory Data
    try:
        conn_inv = get_db_connection(PSI_INVENTORY_DB)
        df_inv = pd.read_sql_query("""
            SELECT product_name, SUM(quantity) as today_inventory
            FROM inventory_data
            WHERE warehouse_name IN ('中壢', '台中', '高雄', '泰山') 
              AND inventory_type = '世磊'
            GROUP BY product_name
        """, conn_inv)
        conn_inv.close()
    except Exception as e:
        print(f"Error loading inventory: {e}")
        df_inv = pd.DataFrame(columns=['product_name', 'today_inventory'])

    # 2. Sales Data (Last 12 Months)
    try:
        conn_sales = get_db_connection(PSI_SALES_DB)
        one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        query_sales = f"""
            SELECT 
                `產品名稱` as product_name,
                SUM(CASE WHEN `項目名稱` IS NOT NULL AND TRIM(`項目名稱`) != '' THEN `倉庫確認數量` ELSE 0 END) as project_sales_12m,
                SUM(CASE WHEN `項目名稱` IS NULL OR TRIM(`項目名稱`) = '' THEN `倉庫確認數量` ELSE 0 END) as retail_sales_12m
            FROM sales_data
            WHERE `發貨日期` >= '{one_year_ago}'
            GROUP BY `產品名稱`
        """
        df_sales_agg = pd.read_sql_query(query_sales, conn_sales)
        conn_sales.close()
        
        df_sales_agg['retail_monthly_avg'] = (df_sales_agg['retail_sales_12m'] / 12).round(1)
    except Exception as e:
        print(f"Error loading sales: {e}")
        df_sales_agg = pd.DataFrame(columns=['product_name', 'project_sales_12m', 'retail_sales_12m', 'retail_monthly_avg'])

    # 3. Procure Data
    procure_details = {}
    try:
        conn_proc = get_db_connection(PSI_PROCURE_DB)
        df_proc = pd.read_sql_query("""
            SELECT product_name, 
                   goods_status, 
                   (quantity - warehouse_qty) as pending_qty,
                   dispatch_date,
                   remarks
            FROM procure_items
            WHERE (quantity - warehouse_qty) > 0
              AND status = '生效'
        """, conn_proc)
        conn_proc.close()
        
        def format_remarks(x):
            valid_remarks = [str(r).strip() for r in x if r and str(r).strip() != '']
            if not valid_remarks: return ''
            if len(valid_remarks) == 1: return valid_remarks[0]
            return '\n'.join([f"{i+1}. {r}" for i, r in enumerate(valid_remarks)])

        df_proc_agg = df_proc.groupby('product_name').agg({
            'goods_status': lambda x: ', '.join(sorted({s for s in x if s and str(s).strip() != ''})),
            'pending_qty': 'sum',
            'dispatch_date': lambda x: min([d for d in x if d and str(d).strip() != ''] or ['']),
            'remarks': format_remarks
        }).reset_index()
        
        for _, row in df_proc.iterrows():
            p_name = row['product_name']
            if p_name not in procure_details:
                procure_details[p_name] = []
            procure_details[p_name].append({
                'pending_qty': row['pending_qty'],
                'dispatch_date': row['dispatch_date'],
                'dispatch_date': row['dispatch_date'],
                'goods_status': row['goods_status'],
                'remarks': row['remarks']
            })

    except Exception as e:
        print(f"Error loading procure: {e}")
        print(f"Error loading procure: {e}")
        df_proc_agg = pd.DataFrame(columns=['product_name', 'goods_status', 'pending_qty', 'dispatch_date', 'remarks'])

    # 4. Forecast Data
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    next_12_months = []
    y, m = current_year, current_month
    for _ in range(12):
        next_12_months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
            
    conn_fc = get_db_connection(PSI_FORECAST_DB)
    df_fc = pd.read_sql_query("SELECT * FROM forecast_data", conn_fc)
    conn_fc.close()
    
    if not df_fc.empty:
        df_fc['year_month_tuple'] = list(zip(df_fc['year'], df_fc['month']))
        df_fc_relevant = df_fc[df_fc['year_month_tuple'].isin(next_12_months)].copy()
        df_fc_agg = df_fc_relevant.groupby('product_name')['quantity'].sum().reset_index().rename(columns={'quantity': 'forecast_total'})
        
        pivot_data = []
        for p in df_fc_relevant['product_name'].unique():
            row = {'product_name': p}
            p_data = df_fc_relevant[df_fc_relevant['product_name'] == p]
            for i, (y, m) in enumerate(next_12_months):
                qty = p_data[(p_data['year'] == y) & (p_data['month'] == m)]['quantity'].sum()
                row[f'month_{i+1}'] = qty
            pivot_data.append(row)
        
        df_fc_pivot = pd.DataFrame(pivot_data)
        if df_fc_pivot.empty:
             df_fc_pivot = pd.DataFrame(columns=['product_name'] + [f'month_{i+1}' for i in range(12)])
    else:
        df_fc_agg = pd.DataFrame(columns=['product_name', 'forecast_total'])
        df_fc_pivot = pd.DataFrame(columns=['product_name'] + [f'month_{i+1}' for i in range(12)])

    # 4.5 Product Notes
    try:
        conn_notes = get_db_connection(PSI_FORECAST_DB)
        df_notes = pd.read_sql_query("SELECT * FROM product_notes", conn_notes)
        conn_notes.close()
    except Exception as e:
        print(f"Error loading notes: {e}")
        df_notes = pd.DataFrame(columns=['product_name', 'note', 'updated_by'])

    # 5. Merging All Data
    all_products = set(df_inv['product_name']).union(set(df_sales_agg['product_name'])).union(set(df_fc_agg['product_name']))
    df_master = pd.DataFrame({'product_name': list(all_products)})
    
    df_master = df_master.merge(df_inv, on='product_name', how='left').fillna({'today_inventory': 0})
    df_master = df_master.merge(df_sales_agg, on='product_name', how='left').fillna(0)
    df_master = df_master.merge(df_proc_agg, on='product_name', how='left')
    df_master = df_master.merge(df_fc_agg, on='product_name', how='left').fillna({'forecast_total': 0})
    df_master = df_master.merge(df_fc_pivot, on='product_name', how='left').fillna(0)
    df_master = df_master.merge(df_notes, on='product_name', how='left').fillna({'note': '', 'updated_by': ''})
    
    # --- Product Category Logic ---
    try:
        conn_inv_code = get_db_connection(PSI_INVENTORY_DB)
        # Optimized: Get 1 code per product directly via SQL
        df_codes = pd.read_sql_query("""
            SELECT product_name, MAX(product_code) as product_code
            FROM inventory_data
            WHERE product_code IS NOT NULL AND product_code != ''
            GROUP BY product_name
        """, conn_inv_code)
        conn_inv_code.close()
        
        if 'product_code' in df_master.columns:
            df_master = df_master.drop(columns=['product_code'])
            
        df_master = df_master.merge(df_codes, on='product_name', how='left')
    except Exception as e:
        print(f"Error loading product codes from Inventory: {e}")
        df_master['product_code'] = ''

    def get_category(code):
        if not code: return '其他'
        s = str(code).strip()[:2]
        if s == '10': return '廚電'
        if s == '20': return '小水'
        if s == '21': return '大水'
        if s == '30': return '氣泡水'
        return '其他'

    df_master['category'] = df_master['product_code'].apply(get_category)
    
    valid_cats = ['廚電', '小水', '大水', '氣泡水']
    df_master = df_master[df_master['category'].isin(valid_cats)]

    # Filter out inactive
    df_master['pending_qty'] = df_master['pending_qty'].fillna(0)
    condition_active = (
        (df_master['forecast_total'] > 0) | 
        ((df_master['project_sales_12m'] + df_master['retail_sales_12m']) > 0) | 
        (df_master['pending_qty'] > 0)
    )
    df_master = df_master[condition_active]

    df_master['remaining_stock'] = df_master['today_inventory'] - df_master['forecast_total']
    
    # Sort by Product Code (Ascending)
    df_master['product_code'] = df_master['product_code'].fillna('')
    df_master = df_master.sort_values(by='product_code', ascending=True)

    month_labels = [f"{y}/{m}" for y, m in next_12_months]
    
    month_labels = [f"{y}/{m}" for y, m in next_12_months]
    
    # Pass permission flag to frontend
    is_supervisor = str(session.get('is_supervisor', '')).strip().upper() == 'Y'
    
    return df_master.to_dict('records'), month_labels, next_12_months, procure_details, is_supervisor


