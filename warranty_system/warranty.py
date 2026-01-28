import os
import sqlite3
import threading
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, send_from_directory
from werkzeug.utils import secure_filename

# 建立 Blueprint
warranty_bp = Blueprint(
    'warranty',
    __name__,
    url_prefix='/warranty'
)

# --- 設定 ---
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
DATABASE_PATH = os.path.join(os.path.dirname(__file__), 'database', 'warranty.db')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# 用於確保資料庫只初始化一次
db_initialized = False
db_lock = threading.Lock()

# --- 輔助函式 ---

def get_db_connection():
    """建立並返回資料庫連線"""
    db_dir = os.path.dirname(DATABASE_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """清除現有資料並建立新資料表"""
    db = get_db_connection()
    cursor = db.cursor()
    # 建立客戶資料表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            mobile_phone TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            birthday TEXT,
            address TEXT
        );
    ''')
    # 建立保固註冊資料表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS registrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_phone TEXT NOT NULL,
            card_number TEXT NOT NULL UNIQUE,
            purchase_store TEXT,
            product_name TEXT,
            product_model TEXT,
            ship_date TEXT,
            photo_filename TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_phone) REFERENCES customers (mobile_phone)
        );
    ''')
    db.commit()
    db.close()
    global db_initialized
    db_initialized = True

def allowed_file(filename):
    """檢查上傳的檔案副檔名是否合法"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# --- Hooks & 路由 ---

@warranty_bp.before_request
def ensure_db_initialized():
    """在每個請求之前，確保資料庫已初始化"""
    global db_initialized
    if not db_initialized:
        with db_lock:
            if not db_initialized:
                init_db()

@warranty_bp.route('/register', methods=['GET', 'POST'])
def register():
    """處理保固登錄頁面的 GET 和 POST 請求"""
    if request.method == 'POST':
        # 從表單中獲取資料
        name = request.form.get('name')
        mobile_phone = request.form.get('mobile_phone')
        email = request.form.get('email')
        birthday = request.form.get('birthday')
        address = request.form.get('address')
        card_number = request.form.get('card_number')
        purchase_store = request.form.get('purchase_store')
        product_name = request.form.get('product_name')
        product_model = request.form.get('product_model')
        ship_date = request.form.get('ship_date')
        file = request.files.get('warranty_photo')

        # --- 資料驗證 ---
        if not all([name, mobile_phone, address, card_number, product_name, product_model, ship_date]) or not file or not file.filename:
            flash('所有欄位及保固書照片皆為必填！', 'danger')
            return redirect(request.url)

        if not allowed_file(file.filename):
            flash('僅允許上傳圖片檔案 (png, jpg, jpeg, gif)！', 'danger')
            return redirect(request.url)

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # 檢查服務保證書卡號是否已存在
            cursor.execute("SELECT id FROM registrations WHERE card_number = ?", (card_number,))
            if cursor.fetchone():
                flash(f'服務保證書卡號 {card_number} 已經被註冊過了。', 'warning')
                conn.close()
                return redirect(request.url)

            # --- 處理檔案上傳 ---
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            unique_filename = f"{timestamp}_{filename}"
            
            if not os.path.exists(UPLOAD_FOLDER):
                os.makedirs(UPLOAD_FOLDER)
            
            file_path = os.path.join(UPLOAD_FOLDER, unique_filename)
            file.save(file_path)

            # --- 處理客戶資料 ---
            cursor.execute("SELECT * FROM customers WHERE mobile_phone = ?", (mobile_phone,))
            customer = cursor.fetchone()

            if customer:
                # 更新現有客戶資料 (如果表單中的資料和資料庫中的不同)
                updates = {}
                if name and customer['name'] != name: updates['name'] = name
                if email and customer['email'] != email: updates['email'] = email
                if birthday and customer['birthday'] != birthday: updates['birthday'] = birthday
                if address and customer['address'] != address: updates['address'] = address
                
                if updates:
                    set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
                    values = list(updates.values())
                    values.append(mobile_phone)
                    cursor.execute(f"UPDATE customers SET {set_clause} WHERE mobile_phone = ?", tuple(values))
            else:
                # 新增客戶
                cursor.execute(
                    "INSERT INTO customers (mobile_phone, name, email, birthday, address) VALUES (?, ?, ?, ?, ?)",
                    (mobile_phone, name, email, birthday, address)
                )

            # --- 插入保固註冊資料 ---
            cursor.execute(
                """
                INSERT INTO registrations 
                (customer_phone, card_number, purchase_store, product_name, product_model, ship_date, photo_filename)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (mobile_phone, card_number, purchase_store, product_name, product_model, ship_date, unique_filename)
            )

            conn.commit()
            flash('保固資料登錄成功！感謝您的填寫。', 'success')

        except sqlite3.IntegrityError:
            flash(f'資料儲存失敗，服務保證書卡號 {card_number} 可能已經被註冊過了。', 'danger')
        except Exception as e:
            flash(f'發生未知錯誤：{str(e)}', 'danger')
        finally:
            if conn:
                conn.close()

        return redirect(url_for('warranty.register'))

    return render_template('warranty_registration.html')

@warranty_bp.route('/view')
def view():
    if 'logged_in' not in session:
        return redirect(url_for('login'))
    
    # 僅允許 C4D002 存取
    if session.get('username') != 'C4D002':
        return "Forbidden", 403

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            c.name,
            c.mobile_phone,
            c.email,
            c.birthday,
            c.address,
            r.card_number,
            r.purchase_store,
            r.product_name,
            r.product_model,
            r.ship_date,
            r.photo_filename,
            r.created_at
        FROM
            registrations r
        JOIN
            customers c ON r.customer_phone = c.mobile_phone
        ORDER BY
            r.created_at DESC
    """)
    registrations = cursor.fetchall()
    conn.close()
    
    return render_template('warranty_view.html', registrations=registrations)

@warranty_bp.route('/uploads/<filename>')
def uploaded_file(filename):
    if 'logged_in' not in session:
        return "Forbidden", 403
    return send_from_directory(UPLOAD_FOLDER, filename)