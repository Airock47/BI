from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, send_file
import sqlite3
import os
import pandas as pd
from datetime import datetime, date
import io

# 建立 Blueprint
inventory_bp = Blueprint(
    'inventory',
    __name__,
    url_prefix='/inventory',
    template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
    static_folder=os.path.join(os.path.dirname(__file__), 'static')
)

# 為盤點系統添加登入驗證
@inventory_bp.before_request
def require_login_for_inventory():
    """在處理盤點系統的任何請求前，先檢查使用者是否登入"""
    if 'logged_in' not in session:
        return redirect(url_for('login'))

# 資料庫連線函式
def get_inventory_db_connection(db_name):
    db_path = os.path.join(os.path.dirname(__file__), 'database', db_name)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# 盤點系統首頁
@inventory_bp.route('/')
def index():
    return render_template('inventory_index.html')

# 管理後台（僅限C4D002帳號）
@inventory_bp.route('/admin')
def admin():
    """管理後台，僅限C4D002帳號使用"""
    if session.get('username') != 'C4D002':
        return redirect(url_for('inventory.index'))
    return render_template('inventory_admin.html')

# 匯出盤點結果（僅限C4D002帳號）
@inventory_bp.route('/export_inventory', methods=['POST'])
def export_inventory():
    """匯出盤點結果"""
    if session.get('username') != 'C4D002':
        return jsonify({'success': False, 'error': '無權限執行此操作'})
    
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        conn = get_inventory_db_connection('inventory_records.db')
        cursor = conn.cursor()
        
        # 查詢指定日期範圍的盤點記錄
        if start_date and end_date:
            cursor.execute('''
                SELECT user_id, qr_code, product_name, warehouse_code, warehouse_name, 
                       quantity, inventory_date, inventory_time, updated_at
                FROM inventory_records 
                WHERE inventory_date BETWEEN ? AND ?
                ORDER BY inventory_date DESC, inventory_time DESC
            ''', (start_date, end_date))
        else:
            # 如果沒有指定日期，匯出所有記錄
            cursor.execute('''
                SELECT user_id, qr_code, product_name, warehouse_code, warehouse_name, 
                       quantity, inventory_date, inventory_time, updated_at
                FROM inventory_records 
                ORDER BY inventory_date DESC, inventory_time DESC
            ''')
        
        records = cursor.fetchall()
        conn.close()
        
        # 轉換為DataFrame並匯出Excel
        if records:
            df = pd.DataFrame([dict(record) for record in records])

            # 重新命名欄位
            df.columns = ['使用者帳號', 'QR Code', '產品名稱', '倉別代碼', '倉別名稱',
                         '盤點數量', '盤點日期', '盤點時間', '最後更新時間']

            # 產生檔案名稱
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'盤點結果_{timestamp}.xlsx'

            # 使用記憶體緩衝區來建立Excel檔案
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='盤點結果')

            output.seek(0)

            # 同時儲存到伺服器（備份）
            exports_dir = os.path.join(os.path.dirname(__file__), 'exports')
            os.makedirs(exports_dir, exist_ok=True)
            filepath = os.path.join(exports_dir, filename)
            df.to_excel(filepath, index=False, engine='openpyxl')

            # 直接返回檔案供下載
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
        else:
            return jsonify({'success': False, 'error': '查無資料'})
            
    except Exception as e:
        print(f"匯出錯誤：{str(e)}")
        return jsonify({'success': False, 'error': f'匯出失敗：{str(e)}'})

# 獲取盤點統計資料（僅限C4D002帳號）
@inventory_bp.route('/get_inventory_stats')
def get_inventory_stats():
    """獲取盤點統計資料"""
    if session.get('username') != 'C4D002':
        return jsonify({'error': '無權限執行此操作'})
    
    try:
        conn = get_inventory_db_connection('inventory_records.db')
        cursor = conn.cursor()
        
        # 總記錄數
        cursor.execute('SELECT COUNT(*) as total FROM inventory_records')
        total_records = cursor.fetchone()['total']
        
        # 今日記錄數
        today = date.today()
        cursor.execute('SELECT COUNT(*) as today_total FROM inventory_records WHERE inventory_date = ?', (today,))
        today_records = cursor.fetchone()['today_total']
        
        # 使用者統計
        cursor.execute('''
            SELECT user_id, COUNT(*) as count 
            FROM inventory_records 
            GROUP BY user_id 
            ORDER BY count DESC
        ''')
        user_stats = cursor.fetchall()
        
        # 倉別統計
        cursor.execute('''
            SELECT warehouse_code, warehouse_name, COUNT(*) as count 
            FROM inventory_records 
            GROUP BY warehouse_code, warehouse_name 
            ORDER BY count DESC
        ''')
        warehouse_stats = cursor.fetchall()
        
        # 最近7天的記錄統計
        cursor.execute('''
            SELECT inventory_date, COUNT(*) as count 
            FROM inventory_records 
            WHERE inventory_date >= date('now', '-7 days')
            GROUP BY inventory_date 
            ORDER BY inventory_date DESC
        ''')
        daily_stats = cursor.fetchall()
        
        conn.close()
        
        return jsonify({
            'total_records': total_records,
            'today_records': today_records,
            'user_stats': [dict(stat) for stat in user_stats],
            'warehouse_stats': [dict(stat) for stat in warehouse_stats],
            'daily_stats': [dict(stat) for stat in daily_stats]
        })
        
    except Exception as e:
        print(f"獲取統計資料錯誤：{str(e)}")
        return jsonify({'error': '獲取統計資料失敗'})

# 初始化資料庫
@inventory_bp.route('/init_database', methods=['POST'])
def init_database():
    """將Excel檔案轉換為SQLite資料庫"""
    try:
        # 確保database目錄存在
        db_dir = os.path.join(os.path.dirname(__file__), 'database')
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        
        # 轉換產品對照資料
        product_file = os.path.join('Inventory system', '產品對照資料.xlsx')
        if os.path.exists(product_file):
            df_products = pd.read_excel(product_file)
            
            # 建立產品資料庫
            conn = get_inventory_db_connection('products.db')
            cursor = conn.cursor()
            
            # 建立產品表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    qr_code TEXT UNIQUE,
                    product_name TEXT,
                    product_code TEXT,
                    specification TEXT,
                    unit TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 插入產品資料
            df_products.to_sql('products', conn, if_exists='replace', index=False, method='multi')
            
            # 建立索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_qr_code ON products(qr_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_product_code ON products(product_code)')
            
            conn.commit()
            conn.close()
        
        # 轉換倉別資料
        warehouse_file = os.path.join('Inventory system', '倉別.xlsx')
        if os.path.exists(warehouse_file):
            df_warehouses = pd.read_excel(warehouse_file)
            
            # 建立倉別資料庫
            conn = get_inventory_db_connection('warehouses.db')
            cursor = conn.cursor()
            
            # 建立倉別表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS warehouses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    warehouse_code TEXT UNIQUE,
                    warehouse_name TEXT,
                    description TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 插入倉別資料
            df_warehouses.to_sql('warehouses', conn, if_exists='replace', index=False, method='multi')
            
            conn.commit()
            conn.close()
        
        # 建立盤點記錄資料庫
        conn = get_inventory_db_connection('inventory_records.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                qr_code TEXT,
                product_name TEXT,
                warehouse_code TEXT,
                warehouse_name TEXT,
                quantity INTEGER,
                inventory_date DATE,
                inventory_time DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 建立索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_date ON inventory_records(user_id, inventory_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_qr_code_records ON inventory_records(qr_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_inventory_date ON inventory_records(inventory_date)')
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': '資料庫初始化成功'})
        
    except Exception as e:
        print(f"資料庫初始化錯誤：{str(e)}")
        return jsonify({'success': False, 'error': f'資料庫初始化失敗：{str(e)}'})

# 獲取倉別清單
@inventory_bp.route('/get_warehouses')
def get_warehouses():
    """獲取所有倉別資料"""
    try:
        conn = get_inventory_db_connection('warehouses.db')
        cursor = conn.cursor()
        
        cursor.execute('SELECT warehouse_code, warehouse_name FROM warehouses ORDER BY warehouse_code')
        warehouses = cursor.fetchall()
        conn.close()
        
        return jsonify([dict(warehouse) for warehouse in warehouses])
        
    except Exception as e:
        print(f"獲取倉別資料錯誤：{str(e)}")
        return jsonify({'error': '獲取倉別資料失敗'})

# 根據QR Code查詢產品
@inventory_bp.route('/get_product/<qr_code>')
def get_product(qr_code):
    """根據QR Code查詢產品資訊"""
    try:
        conn = get_inventory_db_connection('products.db')
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM products WHERE qr_code = ?', (qr_code,))
        product = cursor.fetchone()
        conn.close()

        if product:
            return jsonify(dict(product))
        else:
            return jsonify({'error': '找不到對應的產品'})

    except Exception as e:
        print(f"查詢產品錯誤：{str(e)}")
        return jsonify({'error': '查詢產品失敗'})

# 根據產品名稱搜尋產品
@inventory_bp.route('/search_products')
def search_products():
    """根據產品名稱搜尋產品"""
    try:
        keyword = request.args.get('keyword', '').strip()

        if not keyword:
            return jsonify({'error': '請輸入搜尋關鍵字'})

        if len(keyword) < 2:
            return jsonify({'error': '搜尋關鍵字至少需要2個字元'})

        conn = get_inventory_db_connection('products.db')
        cursor = conn.cursor()

        # 使用LIKE進行模糊搜尋，搜尋產品名稱和產品代碼
        search_pattern = f'%{keyword}%'
        cursor.execute('''
            SELECT * FROM products
            WHERE product_name LIKE ? OR product_code LIKE ?
            ORDER BY product_name
            LIMIT 20
        ''', (search_pattern, search_pattern))

        products = cursor.fetchall()
        conn.close()

        if products:
            return jsonify([dict(product) for product in products])
        else:
            return jsonify({'error': f'找不到包含「{keyword}」的產品'})

    except Exception as e:
        print(f"搜尋產品錯誤：{str(e)}")
        return jsonify({'error': '搜尋產品失敗'})

# 儲存盤點記錄
@inventory_bp.route('/save_inventory', methods=['POST'])
def save_inventory():
    """儲存盤點記錄"""
    try:
        data = request.get_json()
        qr_code = data.get('qr_code')
        product_name = data.get('product_name')
        warehouse_code = data.get('warehouse_code')
        warehouse_name = data.get('warehouse_name')
        quantity = data.get('quantity')
        user_id = session.get('username')
        
        if not all([qr_code, warehouse_code, quantity is not None]):
            return jsonify({'success': False, 'error': '缺少必要資料'})
        
        conn = get_inventory_db_connection('inventory_records.db')
        cursor = conn.cursor()
        
        # 檢查今日是否已有相同產品和倉別的記錄
        today = date.today()
        cursor.execute('''
            SELECT id FROM inventory_records 
            WHERE user_id = ? AND qr_code = ? AND warehouse_code = ? AND inventory_date = ?
        ''', (user_id, qr_code, warehouse_code, today))
        
        existing_record = cursor.fetchone()
        
        if existing_record:
            # 更新現有記錄
            cursor.execute('''
                UPDATE inventory_records 
                SET quantity = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (quantity, existing_record['id']))
        else:
            # 新增記錄
            cursor.execute('''
                INSERT INTO inventory_records 
                (user_id, qr_code, product_name, warehouse_code, warehouse_name, quantity, inventory_date, inventory_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, qr_code, product_name, warehouse_code, warehouse_name, quantity, today))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': '盤點記錄儲存成功'})
        
    except Exception as e:
        print(f"儲存盤點記錄錯誤：{str(e)}")
        return jsonify({'success': False, 'error': f'儲存失敗：{str(e)}'})

# 獲取當日盤點記錄
@inventory_bp.route('/get_today_records')
def get_today_records():
    """獲取當前使用者今日的盤點記錄"""
    try:
        user_id = session.get('username')
        today = date.today()
        
        conn = get_inventory_db_connection('inventory_records.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM inventory_records 
            WHERE user_id = ? AND inventory_date = ?
            ORDER BY inventory_time DESC
        ''', (user_id, today))
        
        records = cursor.fetchall()
        conn.close()
        
        return jsonify([dict(record) for record in records])
        
    except Exception as e:
        print(f"獲取今日記錄錯誤：{str(e)}")
        return jsonify({'error': '獲取今日記錄失敗'})

# 修改盤點記錄
@inventory_bp.route('/update_inventory/<int:record_id>', methods=['POST'])
def update_inventory(record_id):
    """修改盤點記錄"""
    try:
        data = request.get_json()
        quantity = data.get('quantity')
        user_id = session.get('username')

        if quantity is None:
            return jsonify({'success': False, 'error': '缺少數量資料'})

        conn = get_inventory_db_connection('inventory_records.db')
        cursor = conn.cursor()

        # 確認記錄屬於當前使用者
        cursor.execute('''
            UPDATE inventory_records
            SET quantity = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND user_id = ?
        ''', (quantity, record_id, user_id))

        if cursor.rowcount > 0:
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'message': '記錄更新成功'})
        else:
            conn.close()
            return jsonify({'success': False, 'error': '找不到記錄或無權限修改'})

    except Exception as e:
        print(f"更新記錄錯誤：{str(e)}")
        return jsonify({'success': False, 'error': f'更新失敗：{str(e)}'})

# 刪除盤點記錄
@inventory_bp.route('/delete_inventory/<int:record_id>', methods=['DELETE'])
def delete_inventory(record_id):
    """刪除盤點記錄"""
    try:
        user_id = session.get('username')

        conn = get_inventory_db_connection('inventory_records.db')
        cursor = conn.cursor()

        # 確認記錄屬於當前使用者且是今日記錄
        today = date.today()
        cursor.execute('''
            DELETE FROM inventory_records
            WHERE id = ? AND user_id = ? AND inventory_date = ?
        ''', (record_id, user_id, today))

        if cursor.rowcount > 0:
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'message': '記錄刪除成功'})
        else:
            conn.close()
            return jsonify({'success': False, 'error': '找不到記錄或無權限刪除'})

    except Exception as e:
        print(f"刪除記錄錯誤：{str(e)}")
        return jsonify({'success': False, 'error': f'刪除失敗：{str(e)}'})

# 獲取使用者今日選擇的倉別
@inventory_bp.route('/get_user_warehouse')
def get_user_warehouse():
    """獲取使用者今日選擇的倉別"""
    try:
        user_id = session.get('username')
        today = date.today()

        conn = get_inventory_db_connection('inventory_records.db')
        cursor = conn.cursor()

        # 查詢今日第一筆記錄的倉別
        cursor.execute('''
            SELECT warehouse_code, warehouse_name
            FROM inventory_records
            WHERE user_id = ? AND inventory_date = ?
            ORDER BY inventory_time ASC
            LIMIT 1
        ''', (user_id, today))

        result = cursor.fetchone()
        conn.close()

        if result:
            return jsonify({
                'warehouse_code': result['warehouse_code'],
                'warehouse_name': result['warehouse_name']
            })
        else:
            return jsonify({'warehouse_code': None, 'warehouse_name': None})

    except Exception as e:
        print(f"獲取使用者倉別錯誤：{str(e)}")
        return jsonify({'error': '獲取倉別資料失敗'})
