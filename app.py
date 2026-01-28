import sys
import logging
import time
import os
from dotenv import load_dotenv

load_dotenv()

from datetime import datetime, timedelta, timezone
import sqlite3

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    send_from_directory,
    jsonify,
)
import pandas as pd  # noqa: F401  (kept for existing code that may use it)
pd.set_option('future.no_silent_downcasting', True)
from functools import wraps

from Sales_information_inquiry.sales_info import sales_bp
from Inventory_inquiry_system.inventory_inquiry import inventory_inquiry_bp
from file_share import share_bp
from cti_integration import cti_bp
from crm_notes import crm_bp
from bonus_system.bonus import bonus_bp
from ProcureTrack.procure import procure_bp
from warranty_system.warranty import warranty_bp
from warranty_system.warranty import warranty_bp
from ai_assistant.ai_analysis import ai_analysis_bp
from PSI_System.psi import psi_bp


logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


SQLITE_QUERY_TIMEOUT_SECONDS = float(os.getenv("SQLITE_QUERY_TIMEOUT_SECONDS", "8"))
ACCOUNT_LOCK_THRESHOLD = 5  # 同一帳號連續錯誤 5 次
ACCOUNT_LOCK_MINUTES = 60  # 鎖定 60 分鐘


ACCOUNT_LOCK_MINUTES = 60  # 鎖定 60 分鐘

# --- PSI System Configuration ---
# PSI System Constants (Moved to PSI_System/psi.py)
# PSI_SALES_DB = r"D:\WEB\BI\ai_assistant\database\sales_ai.db"
# PSI_INVENTORY_DB = r"D:\WEB\BI\Inventory_inquiry_system\database\inventory_data.db"
# PSI_PROCURE_DB = r"D:\WEB\BI\ProcureTrack\database\procure.db"
# PSI_FORECAST_DB = r"D:\WEB\BI\PSI_System\database\forecast.db"

# init_forecast_db() moved to psi.py
# Initialize on Import (Or call in main)


def _apply_sqlite_timeout(conn, timeout=SQLITE_QUERY_TIMEOUT_SECONDS):
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


app = Flask(__name__)
app.secret_key = "your-secret-key-here"  # 建議改成環境變數與隨機長字串
app.permanent_session_lifetime = timedelta(hours=2)


@app.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    return response


# Register Blueprints
app.register_blueprint(sales_bp)
app.register_blueprint(inventory_inquiry_bp)
app.register_blueprint(share_bp)
app.register_blueprint(cti_bp)
app.register_blueprint(crm_bp)
app.register_blueprint(bonus_bp)
app.register_blueprint(procure_bp)
app.register_blueprint(warranty_bp)
app.register_blueprint(ai_analysis_bp)
app.register_blueprint(psi_bp, url_prefix='/psi')


@app.before_request
def refresh_session_activity():
    if "logged_in" not in session:
        return None
    now = datetime.now(timezone.utc)
    session["last_activity"] = now.isoformat()
    session.permanent = True
    return None


def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), "database", "id_database.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return _apply_sqlite_timeout(conn)


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "logged_in" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated_function


@app.route("/")
def index():
    # Root 轉址到公司主網站
    return redirect("https://www.shih-lei.com.tw/", code=302)


@app.route("/Internal_Portal", methods=["GET", "POST"])
def login():
    # GET：顯示登入頁
    if request.method == "GET":
        if "logged_in" in session:
            return redirect(url_for("dashboard"))
        return render_template("login.html")

    # POST：處理登入
    username = request.form.get("username")
    password = request.form.get("password")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 先查出該帳號目前的狀態（含鎖定欄位）
        cursor.execute(
            """
            SELECT username, password, name, department, is_supervisor, is_resigned,
                   failed_attempts, lock_until
            FROM id_data
            WHERE username = ?
            """,
            (username,),
        )
        user = cursor.fetchone()

        # 帳號不存在
        if not user:
            conn.close()
            return render_template("login.html", error="帳號或密碼錯誤")

        # 檢查是否已離職
        is_resigned = user["is_resigned"] if "is_resigned" in user.keys() else ""
        if is_resigned == "Y":
            conn.close()
            return render_template("login.html", error="此帳號已停用(離職)，請聯絡管理員。")

        # 檢查是否已被鎖定
        failed_attempts = (
            user["failed_attempts"] if "failed_attempts" in user.keys() else 0
        )
        lock_until = user["lock_until"] if "lock_until" in user.keys() else None

        if lock_until:
            try:
                lock_until_dt = datetime.fromisoformat(lock_until)
            except Exception:
                lock_until_dt = None
            if lock_until_dt and datetime.now(timezone.utc) < lock_until_dt:
                conn.close()
                return render_template(
                    "login.html",
                    error="此帳號已因多次登入錯誤被鎖定，請稍後再試或聯絡管理員。",
                )

        # 檢查密碼是否正確（目前仍為明碼比對）
        if password == user["password"]:
            # 登入成功 → 歸零錯誤次數與鎖定
            cursor.execute(
                """
                UPDATE id_data
                SET failed_attempts = 0,
                    lock_until = NULL
                WHERE username = ?
                """,
                (username,),
            )

            session.permanent = True
            session["logged_in"] = True
            session["username"] = username
            session["name"] = user["name"] if "name" in user.keys() else ""
            try:
                session["department"] = (
                    user["department"] if "department" in user.keys() else ""
                )
            except Exception:
                session["department"] = ""
            try:
                session["is_supervisor"] = (
                    user["is_supervisor"] if "is_supervisor" in user.keys() else ""
                )
            except Exception:
                session["is_supervisor"] = ""
            session["last_activity"] = datetime.now(timezone.utc).isoformat()

            conn.commit()
            conn.close()
            return redirect(url_for("dashboard"))

        # 密碼錯誤 → 增加失敗次數
        failed_attempts = (failed_attempts or 0) + 1
        lock_until_value = None

        if failed_attempts >= ACCOUNT_LOCK_THRESHOLD:
            # 達到門檻 → 鎖定一段時間
            lock_until_dt = datetime.now(timezone.utc) + timedelta(
                minutes=ACCOUNT_LOCK_MINUTES
            )
            lock_until_value = lock_until_dt.isoformat()

        cursor.execute(
            """
            UPDATE id_data
            SET failed_attempts = ?,
                lock_until = ?
            WHERE username = ?
            """,
            (failed_attempts, lock_until_value, username),
        )

        conn.commit()
        conn.close()

        if lock_until_value:
            return render_template(
                "login.html",
                error=f"帳號已因連續錯誤 {ACCOUNT_LOCK_THRESHOLD} 次被鎖定，"
                f"請 {ACCOUNT_LOCK_MINUTES} 分鐘後再試或聯絡管理員。",
            )

        return render_template("login.html", error="帳號或密碼錯誤")

    except sqlite3.Error as e:
        print(f"登入時資料庫錯誤：{str(e)}")
        return render_template("login.html", error="系統設定錯誤，請聯絡管理員")
    except Exception as e:
        print(f"登入時發生例外：{str(e)}")
        return render_template("login.html", error="系統發生錯誤，請稍後再試")


# Legacy path should not be used
@app.route("/login", methods=["GET", "POST"])
def login_disabled():
    return "Not Found", 404


# Some older templates may still call url_for('login_post'); keep alias to avoid 500, but serve login.
@app.route("/login_post", methods=["GET", "POST"])
def login_post():
    return login()


@app.route("/dashboard")
@login_required
def dashboard():
    user_department = session.get("department")
    username = session.get("username")
    can_view_procure = True
    return render_template("dashboard.html", can_view_procure=can_view_procure)


@app.route("/sales")
@login_required
def sales():
    return render_template("sales.html")


@app.route("/BI_GM_Dashboard")
def gm():
    return render_template("GM.html")


@app.route("/executive-summary-bigbear")
def gm_legacy():
    return ("Not Found", 404)


@app.route("/assistant")
@login_required
def assistant():
    return render_template("assistant.html")


# Public, unlisted route to serve filter-guide site
@app.route("/filter-guide/")
@app.route("/FG/")
@app.route("/fg/")
def filter_guide_index_slash():
    base_dir = os.path.join(os.path.dirname(__file__), "filter-guide")
    return send_from_directory(base_dir, "index.html")


# Redirect no-trailing-slash to trailing slash to keep relative assets working
@app.route("/filter-guide")
@app.route("/FG")
@app.route("/fg")
def filter_guide_index_noslash():
    return redirect(request.path + "/")


@app.route("/filter-guide/<path:filename>")
@app.route("/FG/<path:filename>")
@app.route("/fg/<path:filename>")
def filter_guide_static(filename):
    base_dir = os.path.join(os.path.dirname(__file__), "filter-guide")
    return send_from_directory(base_dir, filename)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/change_password", methods=["GET"])
def change_password_page():
    return render_template("change_password.html")


@app.route("/change_password", methods=["POST"])
def change_password():
    username = request.form.get("username")
    current_password = request.form.get("current_password")
    new_password = request.form.get("new_password")
    confirm_password = request.form.get("confirm_password")

    if new_password != confirm_password:
        return render_template("change_password.html", error="新密碼與確認密碼不一致")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 檢查目前密碼
        cursor.execute(
            "SELECT * FROM id_data WHERE username = ? AND password = ?",
            (username, current_password),
        )
        user = cursor.fetchone()

        if not user:
            conn.close()
            return render_template("change_password.html", error="帳號或目前密碼錯誤")

        # 更新密碼（目前仍為明碼）
        cursor.execute(
            "UPDATE id_data SET password = ? WHERE username = ?",
            (new_password, username),
        )
        conn.commit()
        conn.close()

        return render_template("change_password.html", success="密碼已成功更新")

    except sqlite3.Error as e:
        print(f"密碼變更時資料庫錯誤：{str(e)}")
        return render_template(
            "change_password.html", error="系統設定錯誤，請聯絡管理員"
        )
    except Exception as e:
        print(f"密碼變更時發生例外：{str(e)}")
        return render_template("change_password.html", error="系統發生錯誤，請稍後再試")


@app.route("/admin/unlock", methods=["GET"])
@login_required
def admin_unlock():
    """
    只有帳號 C4D002 可以使用的解鎖功能。
    使用方式：
      /admin/unlock?username=要解鎖的帳號
    """
    if session.get("username") != "C4D002":
        return "Forbidden", 403

    target_username = request.args.get("username")
    if not target_username:
        return "請在網址後面帶上 ?username=要解鎖的帳號", 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE id_data
            SET failed_attempts = 0,
                lock_until = NULL
            WHERE username = ?
            """,
            (target_username,),
        )
        conn.commit()
        updated = cursor.rowcount
        conn.close()
    except sqlite3.Error as e:
        print(f"解鎖帳號時資料庫錯誤：{str(e)}")
        return "解鎖失敗（資料庫錯誤），請查看伺服器日誌。", 500

    if updated == 0:
        return f"找不到帳號 {target_username}，沒有解除任何鎖定。"

    return f"帳號 {target_username} 的登入鎖定已解除。"


@app.route("/admin/locked")
@login_required
def admin_locked():
    """C4D002 檢視目前被鎖定帳號的頁面。"""
    if (session.get("username") or "").upper() != "C4D002":
        return "Forbidden", 403

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT username, name, failed_attempts, lock_until
        FROM id_data
        WHERE lock_until IS NOT NULL
        ORDER BY lock_until DESC
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return render_template("locked_accounts.html", locked_accounts=rows)


# PSI System Logic has been moved to PSI_System/psi.py

@app.route("/test_search")
def test_search():
    """測試搜尋畫面"""
    return render_template("test_search.html")


@app.route("/input_test")
def input_test():
    """測試輸入畫面"""
    return render_template("input_test.html")


@app.route("/warranty_registration")
def warranty_registration_form():
    """提供保固登錄頁面"""
    return render_template("warranty_registration.html")


@app.route("/static/images/<path:filename>")
def custom_static_images(filename):
    # 將 /static/images/ 指向 d:/WEB/BI/資料來源/images/
    base_dir = os.path.join(os.path.dirname(__file__), "資料來源", "images")
    # 使用 absolute path 確保正確
    abs_path = os.path.abspath(base_dir)
    return send_from_directory(abs_path, filename)


from waitress import serve


if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=5167, threads=200)
