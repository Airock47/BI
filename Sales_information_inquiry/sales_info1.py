from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
import sqlite3
import os


CUSTOMER_CODE_COL = '客戶代碼'
CUSTOMER_NAME_COL = '客戶名稱'
CUSTOMER_PHONE_COL = '聯絡電話'
NOTE_COL = '備註'
DOC_NO_COL = '單據編號'
SERVICE_CARD_NO_COL = '服務登記號'
SERVICE_CARD_CUSTOMER_COL = '客戶'

# 建立 Blueprint
sales_bp = Blueprint(
    'sales_info',
    __name__,
    url_prefix='/sales_info',
    template_folder=os.path.join(os.path.dirname(__file__), 'templates')
)

# 為「銷售資訊查詢」藍圖添加登入驗證
@sales_bp.before_request
def require_login_for_sales_info():
    """在處理「銷售資訊查詢」的任何請求前，先檢查使用者是否登入"""
    if 'logged_in' not in session:
        # 若未登入，則重定向到登入頁面
        return redirect(url_for('login'))

# 資料庫連線共用函式
def get_db_connection(db_name):
    db_path = os.path.join(os.path.dirname(__file__), 'database', db_name)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# 首頁畫面
@sales_bp.route('/')
def index():
    return render_template('index.html')

# 關鍵字搜尋（銷售＋維修＋客戶資料）
@sales_bp.route('/search')
def search():
    keyword = request.args.get('keyword', '')
    if not keyword:
        return jsonify({'error': '請輸入搜尋關鍵字'})

    results = []
    search_pattern = f'%{keyword}%'
    normalized_keyword = keyword.strip()
    try:
        # 🔹 搜尋銷售資料
        sales_conn = get_db_connection('sales.db')
        sales_cursor = sales_conn.cursor()

        sales_query = """
        SELECT 'sales' as type, m.*
        FROM sales_main m
        WHERE m.客戶名稱 LIKE ?
        OR m.送貨地址 LIKE ?
        OR m.聯絡電話 LIKE ?
        OR m.備註 LIKE ?
        OR m.客戶代碼 LIKE ?
        """
        sales_cursor.execute(sales_query, (search_pattern,) * 5)
        sales_results = sales_cursor.fetchall()

        for row in sales_results:
            results.append({
                'type': 'sales',
                'data': dict(row)
            })
        sales_conn.close()

        # 🔹 搜尋維修資料
        repair_conn = get_db_connection('repair.db')
        repair_cursor = repair_conn.cursor()

        repair_query = """
        SELECT 'repair' as type, *
        FROM repair_data
        WHERE 客戶名稱 LIKE ?
        OR 服務地址 LIKE ?
        OR 備註 LIKE ?
        OR 客戶代碼 LIKE ?
        """
        repair_cursor.execute(repair_query, (search_pattern,) * 4)
        repair_results = repair_cursor.fetchall()

        for row in repair_results:
            results.append({
                'type': 'repair',
                'data': dict(row)
            })
        repair_conn.close()

        # 🔹 搜尋寄倉出貨資料
        custody_conn = get_db_connection('custody.db')
        custody_cursor = custody_conn.cursor()
        custody_query = """
        SELECT * FROM custody_main
        WHERE 客戶名稱 LIKE ?
        OR 送貨地址 LIKE ?
        OR 聯絡電話 LIKE ?
        OR 備註 LIKE ?
        OR 客戶代碼 LIKE ?
        """
        custody_cursor.execute(custody_query, (search_pattern,) * 5)
        custody_results = custody_cursor.fetchall()
        for row in custody_results:
            results.append({
                'type': 'custody',
                'data': dict(row)
            })
        custody_conn.close()

        # 🔹 搜尋客戶基本資料
        customer_conn = get_db_connection('customer_new.db')
        customer_cursor = customer_conn.cursor()
        customer_query = """
        SELECT 'customer' as type, *
        FROM customer_basic
        WHERE 客戶代碼 LIKE ?
        OR 客戶名稱 LIKE ?
        OR 聯絡地址 LIKE ?
        OR 聯絡電話 LIKE ?
        OR 聯絡人 LIKE ?
        OR 業務人員名稱 LIKE ?
        """
        customer_cursor.execute(customer_query, (search_pattern,) * 6)
        customer_results = customer_cursor.fetchall()

        for row in customer_results:
            results.append({
                'type': 'customer',
                'data': dict(row)
            })

        customer_conn.close()

        # 去重處理 - 根據類型和關鍵欄位去重
        unique_results = []
        seen_keys = set()

        for result in results:
            data = result['data']
            result_type = result['type']

            # 建立唯一鍵
            if result_type == 'customer':
                unique_key = f"customer_{data.get(CUSTOMER_CODE_COL, '')}"
            elif result_type in ['sales', 'repair', 'custody']:
                unique_key = f"{result_type}_{data.get(DOC_NO_COL, '')}"
            elif result_type == 'service_card':
                unique_key = f"service_card_{data.get(SERVICE_CARD_NO_COL, '')}"
            else:
                unique_key = f"{result_type}_{data.get(CUSTOMER_CODE_COL, '')}_{data.get(CUSTOMER_NAME_COL, '')}"

            if unique_key not in seen_keys:
                seen_keys.add(unique_key)
                unique_results.append(result)

        # 按日期排序 (由近到遠)
        def get_sort_date(item):
            from datetime import datetime
            import re

            data = item['data']
            date_str = data.get('發貨日期') or data.get('單據日期') or data.get('出勤開始時間') or ''

            if not date_str:
                return datetime(1900, 1, 1)  # 沒有日期的放最後

            # 統一日期格式處理
            date_str = str(date_str).strip()
            date_str = re.sub(r'\([^)]*\)', '', date_str)  # 移除括號內容

            try:
                # 民國年格式轉換 (例如: 113/12/25)
                if re.match(r'^\d{2,3}/\d{1,2}/\d{1,2}', date_str):
                    parts = date_str.split('/')
                    year = int(parts[0]) + 1911
                    month = int(parts[1])
                    day = int(parts[2].split(' ')[0])  # 處理可能包含時間的情況
                    return datetime(year, month, day)

                # 中文年月日格式 (例如: 2024年12月25日)
                year_match = re.search(r'(\d{4})年', date_str)
                month_match = re.search(r'(\d{1,2})月', date_str)
                day_match = re.search(r'(\d{1,2})日', date_str)

                if year_match and month_match and day_match:
                    year = int(year_match.group(1))
                    month = int(month_match.group(1))
                    day = int(day_match.group(1))
                    return datetime(year, month, day)

                # ISO格式或其他標準格式 (例如: 2024-12-25)
                date_only = date_str.split(' ')[0]  # 只取日期部分
                if '-' in date_only:
                    return datetime.strptime(date_only, '%Y-%m-%d')

                # 嘗試其他格式
                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

            except:
                # 如果所有解析都失敗，返回很早的日期
                return datetime(1900, 1, 1)

        # 排序：日期由近到遠 (新日期在前)
        unique_results.sort(key=get_sort_date, reverse=True)

        return jsonify(unique_results)

    except sqlite3.Error as e:
        print(f"資料庫錯誤：{str(e)}")
        return jsonify({'error': '資料庫查詢錯誤'})
    except Exception as e:
        print(f"搜尋過程發生錯誤：{str(e)}")
        return jsonify({'error': '系統發生錯誤'})

# 查詢銷售明細
@sales_bp.route('/sales_details/<doc_no>')
def sales_details(doc_no):
    if not doc_no:
        return jsonify({'error': '無效的單據編號'})

    try:
        conn = get_db_connection('sales.db')
        cursor = conn.cursor()

        # 主檔
        cursor.execute('SELECT * FROM sales_main WHERE 單據編號 = ?', (doc_no,))
        main_data = cursor.fetchone()
        if not main_data:
            conn.close()
            return jsonify({'error': '找不到指定的單據'})

        # 明細
        cursor.execute('SELECT * FROM sales_detail WHERE 單據編號 = ?', (doc_no,))
        details = cursor.fetchall()

        conn.close()

        return jsonify({
            'success': True,
            'main': dict(main_data),
            'details': [dict(detail) for detail in details]
        })

    except sqlite3.Error as e:
        print(f"資料庫錯誤：{str(e)}")
        return jsonify({'error': '資料庫查詢錯誤'})
    except Exception as e:
        print(f"查詢詳細資料時發生錯誤：{str(e)}")
        return jsonify({'error': '系統發生錯誤'})

# 查詢維修詳細資料（主檔中的說明欄位）
@sales_bp.route('/repair_details/<doc_no>')
def repair_details(doc_no):
    if not doc_no:
        return jsonify({'error': '無效的單據編號'})

    try:
        conn = get_db_connection('repair.db')
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM repair_data WHERE 單據編號 = ?', (doc_no,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return jsonify({'error': '找不到指定的維修單據'})

        return jsonify(dict(row))

    except sqlite3.Error as e:
        print(f"資料庫錯誤：{str(e)}")
        return jsonify({'error': '資料庫查詢錯誤'})
    except Exception as e:
        print(f"查詢維修詳細資料時發生錯誤：{str(e)}")
        return jsonify({'error': '系統發生錯誤'})

# 查詢寄倉出貨明細
@sales_bp.route('/custody_details/<doc_no>')
def custody_details(doc_no):
    if not doc_no:
        return jsonify({'error': '無效的單據編號'})
    try:
        conn = get_db_connection('custody.db')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM custody_main WHERE 單據編號 = ?', (doc_no,))
        main_data = cursor.fetchone()
        if not main_data:
            conn.close()
            return jsonify({'error': '找不到指定的單據'})
        cursor.execute('SELECT * FROM custody_detail WHERE 單據編號 = ?', (doc_no,))
        details = cursor.fetchall()
        conn.close()
        return jsonify({
            'main': dict(main_data),
            'details': [dict(detail) for detail in details]
        })
    except sqlite3.Error as e:
        print(f"custody.db 錯誤：{str(e)}")
        return jsonify({'error': 'custody.db 查詢錯誤'})
    except Exception as e:
        print(f"custody_details 查詢錯誤：{str(e)}")
        return jsonify({'error': '系統發生錯誤'})

# 查詢客戶詳細資料
@sales_bp.route('/customer_details/<customer_code>')
def customer_details(customer_code):
    if not customer_code:
        return jsonify({'error': '無效的客戶代碼'})

    try:
        conn = get_db_connection('customer_new.db')
        cursor = conn.cursor()

        # 查詢基本資訊
        cursor.execute('''
            SELECT * FROM customer_basic
            WHERE 客戶代碼 = ?
        ''', (customer_code,))
        basic_info = cursor.fetchone()

        if not basic_info:
            conn.close()
            return jsonify({'error': '找不到指定的客戶'})

        # 查詢聯絡人資料
        cursor.execute('''
            SELECT * FROM customer_contacts
            WHERE 客戶代碼 = ?
        ''', (customer_code,))
        contacts = cursor.fetchall()

        # 查詢送貨地址
        cursor.execute('''
            SELECT * FROM customer_addresses
            WHERE 客戶代碼 = ?
        ''', (customer_code,))
        addresses = cursor.fetchall()

        conn.close()

        return jsonify({
            'basic_info': dict(basic_info),
            'contacts': [dict(contact) for contact in contacts],
            'addresses': [dict(address) for address in addresses]
        })

    except sqlite3.Error as e:
        print(f"客戶資料庫錯誤：{str(e)}")
        return jsonify({'error': '客戶資料庫查詢錯誤'})
    except Exception as e:
        print(f"查詢客戶詳細資料時發生錯誤：{str(e)}")
        return jsonify({'error': '系統發生錯誤'})

# 查詢服務登記卡資料
@sales_bp.route('/service_card/<customer_code>')
def service_card(customer_code):
    if not customer_code:
        return jsonify({'error': '無效的客戶編號'})

    try:
        conn = get_db_connection('service_card.db')
        cursor = conn.cursor()

        # 查詢該客戶的所有服務登記卡主檔
        cursor.execute('''
            SELECT * FROM service_card_main
            WHERE 客戶 = ?
            ORDER BY 服務登記號
        ''', (customer_code,))
        main_records = cursor.fetchall()

        if not main_records:
            conn.close()
            return jsonify({'error': f'找不到客戶編號 {customer_code} 的服務登記卡資料'})

        # 為每個主檔記錄查詢對應的明細資料
        result = []
        for main_record in main_records:
            service_no = main_record['服務登記號']

            # 查詢明細資料
            cursor.execute('''
                SELECT * FROM service_card_detail
                WHERE 服務登記號 = ?
                ORDER BY id
            ''', (service_no,))
            detail_records = cursor.fetchall()

            result.append({
                'main': dict(main_record),
                'details': [dict(detail) for detail in detail_records]
            })

        conn.close()

        return jsonify(result)

    except sqlite3.Error as e:
        print(f"資料庫錯誤：{str(e)}")
        return jsonify({'error': '資料庫查詢錯誤'})
    except Exception as e:
        print(f"查詢服務登記卡時發生錯誤：{str(e)}")
        return jsonify({'error': '系統發生錯誤'})
