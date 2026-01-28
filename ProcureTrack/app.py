import os
import sqlite3
from pathlib import Path
from typing import List, Dict, Any

from flask import Flask, jsonify, render_template, request, abort


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database" / "procure.db"

# 固定狀態選單
STATUS_OPTIONS: List[str] = [
    "生產中",
    "海/空運送中",
    "已抵港",
    "已報關",
    "貨運中",
    "延誤",
]


def ensure_db() -> None:
    """確保資料庫與資料表存在。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS procure_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number TEXT UNIQUE,
                product_name TEXT,
                quantity INTEGER,
                delivery_date TEXT,
                arrival_date TEXT,
                ship_info TEXT,
                status TEXT
            )
            """
        )
        conn.commit()


def get_conn() -> sqlite3.Connection:
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html", status_options=STATUS_OPTIONS)


@app.route("/api/data")
def api_data():
    """取得全部採購資料"""
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            po_number,
            product_name,
            quantity,
            delivery_date,
            arrival_date,
            ship_info,
            status
        FROM procure_items
        ORDER BY COALESCE(delivery_date, '') DESC, po_number
        """
    ).fetchall()
    conn.close()
    data: List[Dict[str, Any]] = [dict(r) for r in rows]
    return jsonify(data)


@app.route("/api/update", methods=["POST"])
def api_update():
    """更新單筆欄位"""
    payload = request.get_json(silent=True) or {}
    po_number = payload.get("po_number")
    field = payload.get("field")
    value = payload.get("value")

    if not po_number or not field:
        abort(400, "缺少必要欄位")

    # 僅允許更新指定欄位
    allowed_fields = {"arrival_date", "ship_info", "status"}
    if field not in allowed_fields:
        abort(400, "不允許更新此欄位")

    if field == "status" and value not in STATUS_OPTIONS:
        abort(400, "狀態值不合法")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM procure_items WHERE po_number = ?", (po_number,))
    row = cur.fetchone()
    if not row:
        conn.close()
        abort(404, "找不到指定的採購單號")

    cur.execute(
        f"UPDATE procure_items SET {field} = ? WHERE po_number = ?",
        (value, po_number),
    )
    conn.commit()
    conn.close()

    return jsonify({"success": True})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5170"))
    app.run(host="0.0.0.0", port=port, debug=True)
