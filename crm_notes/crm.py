import os
import sqlite3
import time
from typing import List
from flask import Blueprint, render_template, request, session, redirect, url_for

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

crm_bp = Blueprint(
    'crm',
    __name__,
    url_prefix='/crm',
    template_folder='templates'
)

TABLE_NAME = 'crm_notes'
SEARCH_COLUMNS = [
    '記事內容',
    '發生日期',
    '發生時間',
    '建立人員',
    '修改人員',
    '姓名',
    '公司名',
    '公司行動電話',
    '私人行動電話',
    '地址',
    '公司地址',
    '統一編號',
    '公司電話',
]
LIST_COLUMNS = [
    '發生時間',
    '記事內容',
    '建立日期',
    '姓名',
    '地址',
    '統一編號',
    '公司電話',
]


def get_crm_db_path() -> str:
    """Return absolute path to CRM記事資料庫."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(base_dir)
    return os.path.join(project_root, 'CRM', 'CRM記事.db')


def get_connection():
    db_path = get_crm_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return _apply_query_timeout(conn)


def fetch_available_columns(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.execute(f'PRAGMA table_info({TABLE_NAME})')
    return [row[1] for row in cursor.fetchall()]


@crm_bp.before_request
def ensure_logged_in():
    if 'logged_in' not in session:
        return redirect(url_for('login'))


@crm_bp.route('/', methods=['GET'])
def index():
    keyword = request.args.get('keyword', '').strip()
    results = []
    error_message = None
    display_columns: List[str] = []

    try:
        conn = get_connection()
    except sqlite3.Error as exc:
        error_message = f'無法開啟 CRM 資料庫：{exc}'
        return render_template(
            'crm/index.html',
            keyword=keyword,
            results=results,
            display_columns=display_columns,
            error=error_message,
        )

    try:
        available_columns = fetch_available_columns(conn)
        searchable_columns = [col for col in SEARCH_COLUMNS if col in available_columns]
        display_columns = [col for col in LIST_COLUMNS if col in available_columns]

        order_priority = ['發生時間', '發生日期', '建立日期']
        order_column = next((col for col in order_priority if col in available_columns), 'rowid')

        if order_column != 'rowid' and order_column not in display_columns:
            display_columns.insert(0, order_column)

        # 移除重複欄位保持顯示順序
        seen = set()
        display_columns = [col for col in display_columns if not (col in seen or seen.add(col))]

        if not display_columns:
            # 若指定欄位不存在，退回所有欄位 (排除 rowid)
            display_columns = [col for col in available_columns if col.lower() != 'rowid'][:6]

        order_clause = 'ORDER BY rowid DESC' if order_column == 'rowid' else f'ORDER BY "{order_column}" DESC'

        if keyword and searchable_columns:
            like_pattern = f'%{keyword}%'
            where_clause = ' OR '.join([f'"{col}" LIKE ?' for col in searchable_columns])
            sql = f'SELECT rowid, * FROM {TABLE_NAME} WHERE {where_clause} ' + order_clause
            cursor = conn.execute(sql, [like_pattern] * len(searchable_columns))
            rows = cursor.fetchall()

            for row in rows:
                values = {col: row[col] if col in row.keys() else None for col in display_columns}
                results.append({'rowid': row['rowid'], 'data': values})

        elif keyword and not searchable_columns:
            error_message = 'CRM 資料表中沒有可搜尋的欄位，請確認資料來源。'

    except sqlite3.Error as exc:
        print(f'CRM 查詢錯誤：{exc}')
        error_message = 'CRM 資料查詢發生錯誤，請稍後再試。'
    finally:
        conn.close()

    return render_template(
        'crm/index.html',
        keyword=keyword,
        results=results,
        display_columns=display_columns,
        error=error_message,
    )


@crm_bp.route('/detail/<int:rowid>')
def detail(rowid: int):
    try:
        conn = get_connection()
    except sqlite3.Error as exc:
        error_message = f'無法開啟 CRM 資料庫：{exc}'
        return render_template('crm/detail.html', error=error_message, rowid=rowid), 500

    try:
        cursor = conn.execute(f'SELECT rowid, * FROM {TABLE_NAME} WHERE rowid = ?', (rowid,))
        row = cursor.fetchone()
        if row is None:
            return render_template('crm/detail.html', error='找不到指定的資料。', rowid=rowid), 404

        columns = [key for key in row.keys()]
        record = {key: row[key] for key in columns}
    except sqlite3.Error as exc:
        print(f'CRM 詳細資料查詢錯誤：{exc}')
        return render_template('crm/detail.html', error='查詢詳情時發生錯誤。', rowid=rowid), 500
    finally:
        conn.close()

    return render_template('crm/detail.html', record=record, columns=columns, rowid=rowid)




