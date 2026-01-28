import os
import sqlite3
import io
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

from flask import Blueprint, jsonify, render_template, request, session, redirect, url_for, make_response
from functools import wraps


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database" / "procure.db"
INVENTORY_DB_PATH = BASE_DIR.parent / "Inventory_inquiry_system" / "database" / "inventory_data.db"
WAREHOUSE_LIST = ["中壢", "台中", "高雄", "淨水", "泰山"]

# 固定狀態選單
STATUS_OPTIONS: List[str] = [
    "生產中",
    "空運運送中",
    "海運運送中",
    "已抵港",
    "已報關",
    "貨運中",
    "延誤",
    "結案",
]

# 可編輯的部門
ALLOWED_DEPARTMENTS = {"國貿部暨商品部", "總經理室"}

procure_bp = Blueprint(
    "procure",
    __name__,
    url_prefix="/procure",
    template_folder="templates",
    static_folder="static",
)


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "logged_in" not in session:
            return redirect(url_for("login"))
        return fn(*args, **kwargs)

    return wrapper


def ensure_db() -> None:
    """確保資料庫與資料表存在。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        # 建立procure_items主表
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS procure_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number TEXT,
                item_serial_number TEXT,
                product_code TEXT,
                product_name TEXT,
                quantity INTEGER,
                warehouse_qty INTEGER,
                delivery_date TEXT,
                dispatch_date TEXT,
                warehouse TEXT,
                arrival_date TEXT,
                ship_info TEXT,
                status TEXT,
                goods_status TEXT,
                UNIQUE(po_number, item_serial_number)
            )
            """
        )
        # 檢查並新增欄位以相容舊資料庫
        try:
            cur = conn.execute("PRAGMA table_info(procure_items)")
            cols = {row[1] for row in cur.fetchall()}
            if "product_code" not in cols:
                conn.execute("ALTER TABLE procure_items ADD COLUMN product_code TEXT")
            if "warehouse_qty" not in cols:
                conn.execute("ALTER TABLE procure_items ADD COLUMN warehouse_qty INTEGER")
            if "warehouse" not in cols:
                conn.execute("ALTER TABLE procure_items ADD COLUMN warehouse TEXT")
            if "dispatch_date" not in cols:
                conn.execute("ALTER TABLE procure_items ADD COLUMN dispatch_date TEXT")
            if "item_serial_number" not in cols:
                conn.execute("ALTER TABLE procure_items ADD COLUMN item_serial_number TEXT")
            if "goods_status" not in cols:
                conn.execute("ALTER TABLE procure_items ADD COLUMN goods_status TEXT")
            if "remarks" not in cols:
                conn.execute("ALTER TABLE procure_items ADD COLUMN remarks TEXT")
        except Exception:
            pass
        conn.commit()


def get_conn() -> sqlite3.Connection:
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_inventory_stock() -> tuple[dict, dict]:
    """
    從 Inventory_inquiry_system/database/inventory_data.db 依產品代碼/品名加總良品庫存（不含門市）。
    條件：inventory_type = '世磊' 且 warehouse_name 在指定清單。
    回傳 (code_map, name_map)
    """
    inv_path = INVENTORY_DB_PATH
    if not inv_path.exists():
        return {}, {}

    wh_list = WAREHOUSE_LIST
    placeholders = ",".join(["?"] * len(wh_list))
    sql = f"""
        SELECT product_code, product_name, SUM(quantity) AS qty
        FROM inventory_data
        WHERE inventory_type = '世磊'
          AND warehouse_name IN ({placeholders})
        GROUP BY product_code, product_name
    """
    try:
        with sqlite3.connect(inv_path) as conn:
            cur = conn.cursor()
            cur.execute(sql, wh_list)
            rows = cur.fetchall()
            code_map = {}
            name_map = {}
            for code, name, qty in rows:
                qty_val = qty or 0
                if code:
                    code_map[str(code)] = qty_val
                if name:
                    name_map[str(name)] = qty_val
            return code_map, name_map
    except Exception:
        return {}, {}


@procure_bp.route("/")
@login_required
def index():
    # 使用專屬模板名稱，避免與其他子系統的 index.html 混淆
    dept = session.get("department") or ""
    can_edit = dept in ALLOWED_DEPARTMENTS
    return render_template(
        "procure_index.html",
        status_options=STATUS_OPTIONS,
        can_edit=can_edit,
        api_data_url=url_for("procure.api_data"),
        api_update_url=url_for("procure.api_update"),
        static_css_url=url_for("procure.static", filename="css/style.css"),
        static_js_url=url_for("procure.static", filename="js/app.js"),
    )


@procure_bp.route("/api/data")
@login_required
def api_data():
    """取得全部採購資料"""
    inv_code_map, inv_name_map = load_inventory_stock()
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            id,
            po_number,
            item_serial_number,
            product_code,
            product_name,
            quantity,
            warehouse_qty,
            delivery_date,
            dispatch_date,
            warehouse,
            arrival_date,
            ship_info,
            status,
            goods_status,
            remarks
        FROM procure_items
        ORDER BY COALESCE(delivery_date, '') DESC, po_number, item_serial_number
        """
    ).fetchall()
    conn.close()
    data: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        code = str(d.get("product_code") or "")
        name = str(d.get("product_name") or "")
        po = str(d.get("po_number") or "")
        warehouse_val = (d.get("warehouse") or "")
        warehouse_str = str(warehouse_val).strip() if warehouse_val is not None else ""
        if warehouse_str.lower() in {"nan", "none"}:
            warehouse_str = ""
        # OO 開頭預設倉庫為中壢（若未提供）
        if not warehouse_str and po.startswith("OO"):
            warehouse_str = "中壢"
        d["warehouse"] = warehouse_str

        if warehouse_str.lower() == "nan":
            d["warehouse"] = "中壢"
        good = inv_code_map.get(code)
        if good is None:
            good = inv_name_map.get(name, 0)
        d["good_stock"] = good
        data.append(d)
    return jsonify(data)


@procure_bp.route("/api/stock_detail")
@login_required
def stock_detail():
    """取得單一產品的分倉庫良品庫存"""
    code = request.args.get("code", "").strip()
    name = request.args.get("name", "").strip()

    if not INVENTORY_DB_PATH.exists():
        return jsonify({"warehouses": [], "total": 0})

    wh_list = WAREHOUSE_LIST
    placeholders = ",".join(["?"] * len(wh_list))
    params = list(wh_list)

    where_clause = []
    if code:
        where_clause.append("product_code = ?")
        params.append(code)
    if name:
        where_clause.append("product_name = ?")
        params.append(name)
    if not where_clause:
        return jsonify({"warehouses": [], "total": 0})

    sql = f"""
        SELECT warehouse_name, SUM(quantity) AS qty
        FROM inventory_data
        WHERE inventory_type = '世磊'
          AND warehouse_name IN ({placeholders})
          AND ({' OR '.join(where_clause)})
        GROUP BY warehouse_name
    """
    try:
        with sqlite3.connect(INVENTORY_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            wh_map = {r[0]: r[1] or 0 for r in rows}
            total = sum(wh_map.values())
            warehouses = [{"name": w, "qty": wh_map.get(w, 0)} for w in WAREHOUSE_LIST]
            return jsonify({"warehouses": warehouses, "total": total})
    except Exception:
        return jsonify({"warehouses": [], "total": 0})


@procure_bp.route("/api/update", methods=["POST"])
@login_required
def api_update():
    """更新單筆欄位"""
    dept = session.get("department") or ""
    if dept not in ALLOWED_DEPARTMENTS:
        return jsonify({"error": "無權限修改"}), 403

    payload = request.get_json(silent=True) or {}
    row_id = payload.get("id")
    field = payload.get("field")
    value = payload.get("value")

    if row_id is None or not field:
        return jsonify({"error": "缺少必要欄位"}), 400

    # 僅允許更新指定欄位
    allowed_fields = {"arrival_date", "dispatch_date", "ship_info", "goods_status", "remarks"}
    if field not in allowed_fields:
        return jsonify({"error": "不允許更新此欄位"}), 400

    if field == "goods_status" and value not in STATUS_OPTIONS:
        return jsonify({"error": "狀態值不合法"}), 400
        
    if field == "remarks" and value and len(str(value)) > 300:
        return jsonify({"error": "備註不能超過300個字"}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM procure_items WHERE id = ?", (row_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "找不到指定的採購項目"}), 404

    cur.execute(
        f"UPDATE procure_items SET {field} = ? WHERE id = ?",
        (value, row_id),
    )
    conn.commit()
    conn.close()

    return jsonify({"success": True})

# --- EXPORT FUNCTIONALITY ---

def get_category_from_code(code: str) -> str:
    """Helper to determine product category from product code."""
    prefix = (str(code) or "").strip()[:2]
    if prefix == "10":
        return "10"
    if prefix == "20":
        return "20"
    if prefix == "21":
        return "21"
    if prefix == "30":
        return "30"
    return "other"


@procure_bp.route("/export")
@login_required
def export_data():
    """Export filtered procurement data to an XLSX file."""
    try:
        # Step 1: Load all data (similar to api_data)
        inv_code_map, inv_name_map = load_inventory_stock()
        conn = get_conn()
        db_rows = conn.execute(
            """
            SELECT id, po_number, item_serial_number, product_code, product_name,
                   quantity, warehouse_qty, delivery_date, dispatch_date, warehouse,
                   arrival_date, ship_info, status, goods_status, remarks
            FROM procure_items
            ORDER BY COALESCE(delivery_date, '') DESC, po_number, item_serial_number
            """
        ).fetchall()
        conn.close()

        all_data = []
        for r in db_rows:
            d = dict(r)
            code = str(d.get("product_code") or "")
            name = str(d.get("product_name") or "")
            po = str(d.get("po_number") or "")
            warehouse_val = d.get("warehouse") or ""
            warehouse_str = str(warehouse_val).strip() if warehouse_val is not None else ""
            if warehouse_str.lower() in {"nan", "none"}:
                warehouse_str = ""
            if not warehouse_str and po.startswith("OO"):
                warehouse_str = "中壢"
            d["warehouse"] = warehouse_str
            if warehouse_str.lower() == "nan": # Legacy case
                d["warehouse"] = "中壢"
            
            good = inv_code_map.get(code)
            if good is None:
                good = inv_name_map.get(name, 0)
            d["good_stock"] = good
            all_data.append(d)

        # Step 2: Get filters from request arguments
        type_filter = request.args.get("type", "all")
        category_filter = request.args.get("category", "all")
        status_filter = request.args.get("status", "active")
        search_term = request.args.get("search", "").lower()

        # Step 3: Apply filters
        filtered_data = []
        for row in all_data:
            # Filter by undelivered quantity (must be > 0)
            try:
                quantity = int(row.get("quantity") or 0)
                warehouse_qty = int(row.get("warehouse_qty") or 0)
            except (ValueError, TypeError):
                quantity = 0
                warehouse_qty = 0
            
            if (quantity - warehouse_qty) <= 0:
                continue

            # Filter by type (OO/PO)
            po_number = (row.get("po_number") or "").upper()
            if type_filter == "OO" and not po_number.startswith("OO"):
                continue
            if type_filter == "PO" and not po_number.startswith("PO"):
                continue
            
            # Filter by category
            if category_filter != "all":
                row_category = get_category_from_code(row.get("product_code"))
                if category_filter == "other":
                    if row_category in ["10", "20", "21", "30"]:
                        continue
                elif row_category != category_filter:
                    continue

            # Filter by status (結案/生效)
            row_status = (row.get("status") or "").strip()
            if status_filter == "active" and row_status == "結案":
                continue
            if status_filter == "closed" and row_status != "結案":
                continue
            
            # Filter by global search term
            if search_term:
                searchable_fields = [
                    str(row.get(field, "")) for field in 
                    ["po_number", "product_code", "product_name", "warehouse"]
                ]
                if not any(search_term in field.lower() for field in searchable_fields):
                    continue

            filtered_data.append(row)

        if not filtered_data:
            return "沒有符合條件的資料可匯出。", 404

        # Step 4: Create DataFrame and format for export
        df = pd.DataFrame(filtered_data)
        df["undelivered_qty"] = pd.to_numeric(df["quantity"], errors='coerce').fillna(0) - pd.to_numeric(df["warehouse_qty"], errors='coerce').fillna(0)
        
        export_columns = {
            "po_number": "採購單號",
            "product_code": "產品代碼",
            "product_name": "商品",
            "goods_status": "貨物狀態",
            "quantity": "採購數量",
            "delivery_date": "交貨日期(系統預計交期)",
            "dispatch_date": "派送日期(下次交貨日期)",
            "arrival_date": "到港日",
            "warehouse": "倉庫",
            "good_stock": "良品庫存(不含門市)",
            "undelivered_qty": "未交貨數量",
            "remarks": "備註",
        }
        df_export = df[list(export_columns.keys())]
        df_export = df_export.rename(columns=export_columns)

        for col in ["交貨日期(系統預計交期)", "派送日期(下次交貨日期)", "到港日"]:
            df_export[col] = pd.to_datetime(df_export[col], errors="coerce").dt.strftime("%Y-%m-%d").replace("NaT", "")

        # Step 5: Generate Excel file in memory and return as response
        output = io.BytesIO()
        df_export.to_excel(output, index=False, sheet_name="採購進度追蹤")
        output.seek(0)

        response = make_response(output.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=procurement_export.xlsx"
        response.headers["Content-type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return response

    except Exception as e:
        # Log the error e
        return str(e), 500