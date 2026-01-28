from flask import Blueprint, render_template, request, redirect, url_for, session, abort, flash, send_file
import os
import sqlite3
from datetime import datetime, timezone, timedelta
import json
import hmac
import hashlib
import base64
import requests
import io
import pandas as pd


bonus_bp = Blueprint('bonus', __name__)


def get_db_path():
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'database', 'id_database.db')


def get_bonus_db_dir():
    return os.path.join(os.path.dirname(__file__), 'database')


def get_bonus_db_path():
    return os.path.join(get_bonus_db_dir(), 'bonus.db')


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def get_conn_bonus():
    os.makedirs(get_bonus_db_dir(), exist_ok=True)
    conn = sqlite3.connect(get_bonus_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def ensure_bonus_table():
    with get_conn_bonus() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bonus_request (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                applicant_username TEXT,
                target_username TEXT,
                score INTEGER,
                reason TEXT,
                apply_date DATETIME,
                status TEXT,
                reviewer_username TEXT,
                review_date DATETIME,
                line_reply_token TEXT,
                review_reason TEXT
            );
            """
        )
        conn.commit()


def ensure_pending_table():
    """For capturing reject reasons via subsequent message within 3 minutes."""
    with get_conn_bonus() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS line_pending_reason (
                user_id TEXT PRIMARY KEY,
                request_id INTEGER,
                action TEXT,
                created_at DATETIME
            );
            """
        )
        conn.commit()


def ensure_column_exists(table: str, column: str, definition: str):
    # Route to the correct database depending on table
    if table in {"bonus_request", "line_pending_reason"}:
        conn = get_conn_bonus()
    else:
        conn = get_conn()
    with conn:
        cur = conn.execute(f"PRAGMA table_info({table});")
        cols = {row[1] for row in cur.fetchall()}
        if column not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition};")
            conn.commit()


def login_required():
    return 'logged_in' in session


def supervisor_required():
    return login_required() and str(session.get('is_supervisor', '')).upper() == 'Y'


def gm_username():
    # Default to B8A002 if not set via environment
    return os.getenv('GM_USERNAME', 'B8A002')


def gm_required():
    # Allow case-insensitive, trimmed username match
    if not login_required():
        return False
    su = (session.get('username') or '').strip().upper()
    gu = (gm_username() or '').strip().upper()
    return bool(gu) and su == gu


@bonus_bp.before_app_request
def _init():
    ensure_bonus_table()
    ensure_pending_table()
    # Ensure new columns for richer LINE review tracking
    try:
        ensure_column_exists('bonus_request', 'review_reason', 'TEXT')
    except Exception:
        pass
    try:
        ensure_column_exists('bonus_request', 'reviewer_line_user_id', 'TEXT')
    except Exception:
        pass
    # Ensure id_data has resignation flag for filtering
    try:
        ensure_column_exists('id_data', 'is_resigned', 'TEXT')
    except Exception:
        pass


@bonus_bp.route('/bonus')
def bonus_index():
    # 主畫面入口：顯示員工獎勵排行
    if not supervisor_required():
        return render_template('no_permission.html', message='僅主管可查看獎勵排行')
    # 聚合每位同仁的總分與次數
    with get_conn_bonus() as bconn:
        rows = [
            dict(r)
            for r in bconn.execute(
                """
                SELECT target_username,
                       SUM(CASE WHEN score > 0 THEN score ELSE 0 END) AS positive_score,
                       SUM(CASE WHEN score < 0 THEN score ELSE 0 END) AS negative_score,
                       SUM(score) AS total_score,
                       COUNT(*) AS times
                FROM bonus_request
                GROUP BY target_username
                ORDER BY total_score DESC, times DESC
                """
            ).fetchall()
        ]

    rankings = []
    if rows:
        usernames = [r['target_username'] for r in rows if r.get('target_username')]
        info_map = {}
        if usernames:
            placeholders = ','.join(['?'] * len(usernames))
            with get_conn() as conn:
                q = (
                    "SELECT username, name, department, is_resigned "
                    f"FROM id_data WHERE username IN ({placeholders})"
                )
                info_rows = conn.execute(q, usernames).fetchall()
                info_map = {
                    row['username']: {
                        'name': row['name'],
                        'department': row['department'],
                        'is_resigned': row['is_resigned'],
                    }
                    for row in info_rows
                }
        # 組合排行資料並排除已離職人員
        for r in rows:
            u = r.get('target_username')
            info = info_map.get(u, {}) if u else {}
            is_resigned = (info.get('is_resigned') or '').strip().upper()
            if is_resigned == 'Y':
                continue
            rankings.append(
                {
                    'target_username': u,
                    'positive_score': r.get('positive_score') or 0,
                    'negative_score': r.get('negative_score') or 0,
                    'total_score': r.get('total_score') or 0,
                    'times': r.get('times') or 0,
                    'name': info.get('name'),
                    'department': info.get('department'),
                }
            )

    return render_template('bonus_ranking.html', rankings=rankings)


@bonus_bp.route('/bonus/apply')
def bonus_apply():
    if not supervisor_required():
        return render_template('no_permission.html', message='僅限主管申請')
    me = session.get('username')
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT username, name, department FROM id_data "
            "WHERE COALESCE(UPPER(is_resigned), '') <> 'Y' "
            "ORDER BY name"
        )
        users = [dict(row) for row in cur.fetchall()]
    users = [u for u in users if u['username'] != me]

    # show my recent requests (with names)
    with get_conn_bonus() as bconn:
        cur = bconn.execute(
            "SELECT * FROM bonus_request WHERE applicant_username = ? ORDER BY apply_date DESC LIMIT 50",
            (me,),
        )
        my_requests = [dict(row) for row in cur.fetchall()]
    # enrich names from id db
    if my_requests:
        usernames = set()
        for r in my_requests:
            usernames.add(r.get('target_username'))
            usernames.add(r.get('applicant_username'))
        usernames = [u for u in usernames if u]
        name_map = {}
        if usernames:
            placeholders = ','.join(['?'] * len(usernames))
            with get_conn() as conn:
                q = f"SELECT username, name FROM id_data WHERE username IN ({placeholders})"
                rows = conn.execute(q, usernames).fetchall()
                name_map = {row['username']: row['name'] for row in rows}
        for r in my_requests:
            r['target_name'] = name_map.get(r.get('target_username'), r.get('target_username'))
            r['applicant_name'] = name_map.get(r.get('applicant_username'), r.get('applicant_username'))

    return render_template('bonus_form.html', users=users, my_requests=my_requests)


@bonus_bp.route('/bonus/export/my')
def bonus_export_my():
    # 僅登入者，且建議為主管使用本功能
    if not login_required():
        abort(403)
    me = session.get('username')
    with get_conn_bonus() as conn:
        cur = conn.execute(
            "SELECT applicant_username, target_username, score, apply_date, status, reason FROM bonus_request WHERE applicant_username = ? ORDER BY apply_date DESC",
            (me,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    # enrich names
    name_map = {}
    if rows:
        usernames = set()
        for r in rows:
            usernames.add(r.get('target_username'))
            usernames.add(r.get('applicant_username'))
        usernames = [u for u in usernames if u]
        if usernames:
            placeholders = ','.join(['?'] * len(usernames))
            with get_conn() as iconn:
                q = f"SELECT username, name FROM id_data WHERE username IN ({placeholders})"
                name_map = {row['username']: row['name'] for row in iconn.execute(q, usernames).fetchall()}
        # map to export rows
        for r in rows:
            r['target_name'] = name_map.get(r.get('target_username'), r.get('target_username'))
            r['applicant_name'] = name_map.get(r.get('applicant_username'), r.get('applicant_username'))

    # 轉成 DataFrame 並格式化日期（YYYY-MM-DD）
    def _fmt_date(v):
        if not v:
            return ""
        try:
            # 常見格式："YYYY-MM-DD HH:MM:SS"
            return str(v).split(" ")[0]
        except Exception:
            return str(v)

    df = pd.DataFrame(rows)
    if not df.empty:
        df['apply_date'] = df['apply_date'].map(_fmt_date)
        df = df.rename(columns={
            'target_name': '加分對象',
            'applicant_name': '申請主管',
            'score': '分數',
            'apply_date': '申請時間',
            'status': '狀態',
            'reason': '理由',
        })
        df = df[['加分對象','申請主管','分數','申請時間','狀態','理由']]
    else:
        df = pd.DataFrame(columns=['加分對象','申請主管','分數','申請時間','狀態','理由'])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='我的申請紀錄')
    output.seek(0)

    fname = datetime.now(timezone.utc).strftime('bonus_my_%Y%m%d.xlsx')
    return send_file(output, as_attachment=True, download_name=fname, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@bonus_bp.route('/bonus/submit/v1', methods=['POST'])
def bonus_submit_v1():
    if not supervisor_required():
        abort(403)
    applicant = session.get('username')
    target = request.form.get('target_username', '').strip()
    score = request.form.get('score', '').strip()
    reason = request.form.get('reason', '').strip()

    try:
        score_int = int(score)
    except Exception:
        score_int = 0
    if not target or score_int < 1 or score_int > 5:
        flash('請選擇被加分員工，且分數需介於 1–5')
        return redirect(url_for('bonus.bonus_index'))

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    with get_conn_bonus() as conn:
        cur = conn.execute(
            """
            INSERT INTO bonus_request (applicant_username, target_username, score, reason, apply_date, status)
            VALUES (?, ?, ?, ?, ?, '待審核')
            """,
            (applicant, target, score_int, reason, now),
        )
        request_id = cur.lastrowid
        conn.commit()

    # Notify GM via LINE if configured
    try:
        send_line_notification(request_id)
    except Exception:
        pass

    flash('已送出加分申請')
    return redirect(url_for('bonus.bonus_index'))


def send_line_notification(request_id: int):
    access_token = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
    # Normalize token
    access_token = access_token.strip() if access_token else None
    # Support multiple reviewers env, fallback to single GM id
    ids_raw = os.getenv('LINE_REVIEWER_USER_IDS', '')
    to_ids = [i.strip() for i in ids_raw.replace(';', ',').split(',') if i.strip()]
    if not to_ids:
        gm_id = os.getenv('LINE_GM_USER_ID')
        to_ids = [gm_id.strip()] if gm_id else []
    if not access_token or not to_ids:
        return

    with get_conn_bonus() as bconn:
        req_row = bconn.execute(
            "SELECT applicant_username, target_username, score, reason FROM bonus_request WHERE id = ?",
            (request_id,),
        ).fetchone()
    if not req_row:
        return
    # Lookup names
    app_u = req_row[0]
    tgt_u = req_row[1]
    with get_conn() as iconn:
        rows = iconn.execute(
            "SELECT username, name FROM id_data WHERE username IN (?,?)",
            (app_u, tgt_u),
        ).fetchall()
        name_map = {r[0]: r[1] for r in rows}
    req = {
        'score': req_row[2],
        'reason': req_row[3],
        'applicant_name': name_map.get(app_u, app_u),
        'target_name': name_map.get(tgt_u, tgt_u),
    }
    

    title = '📣 加分申請通知'
    text = f"主管：{req['applicant_name'] or ''}\n員工：{req['target_name'] or ''}\n分數：{req['score']}分\n理由：{req['reason'] or ''}"
    data_approve = f"action=approve&request_id={request_id}"
    data_reject = f"action=reject&request_id={request_id}"

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    for to_user in to_ids:
        payload = {
            "to": to_user,
            "messages": [
                {
                    "type": "template",
                    "altText": "員工加分申請通知",
                    "template": {
                        "type": "buttons",
                        "title": title,
                        "text": text,
                        "actions": [
                            {"type": "postback", "label": "✅ 核准", "data": data_approve},
                            {"type": "postback", "label": "❌ 駁回", "data": data_reject},
                        ],
                    },
                }
            ],
        }
        try:
            resp = requests.post('https://api.line.me/v2/bot/message/push', headers=headers, data=json.dumps(payload), timeout=10)
            if resp.status_code >= 300:
                print(f"[LINE PUSH] Failed to {to_user} status={resp.status_code} body={resp.text}")
        except Exception:
            pass


@bonus_bp.route('/line/test_push')
def line_test_push():
    # Restrict to GM user to avoid abuse
    if not gm_required():
        abort(403)
    access_token = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
    access_token = access_token.strip() if access_token else None
    to = request.args.get('to') or os.getenv('LINE_GM_USER_ID')
    if not access_token or not to:
        return {"ok": False, "error": "missing token or to userId"}, 400
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    payload = {
        'to': to.strip(),
        'messages': [{ 'type': 'text', 'text': f'測試推播 OK {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}' }]
    }
    try:
        resp = requests.post('https://api.line.me/v2/bot/message/push', headers=headers, data=json.dumps(payload), timeout=10)
        return {"ok": resp.status_code < 300, "status": resp.status_code, "body": resp.text}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


@bonus_bp.route('/line/webhook', methods=['POST'])
def line_webhook():
    channel_secret = os.getenv('LINE_CHANNEL_SECRET')
    body = request.get_data(as_text=True)
    sig = request.headers.get('X-Line-Signature', '')

    if channel_secret:
        mac = hmac.new(channel_secret.encode('utf-8'), body.encode('utf-8'), hashlib.sha256).digest()
        expected = base64.b64encode(mac).decode('utf-8')
        if expected != sig:
            return 'invalid signature', 400

    event = request.get_json(silent=True) or {}
    events = event.get('events', [])
    for e in events:
        etype = e.get('type')
        if etype == 'postback':
            data = e.get('postback', {}).get('data', '')
            reply_token = e.get('replyToken', '')
            src_user_id = e.get('source', {}).get('userId', '')
            params = dict([kv.split('=') for kv in data.split('&') if '=' in kv])
            action = params.get('action')
            rid = int(params.get('request_id', '0') or '0')
            if action == 'approve' and rid:
                with get_conn_bonus() as conn:
                    conn.execute(
                        "UPDATE bonus_request SET status = ?, reviewer_username = ?, review_date = ?, line_reply_token = ?, reviewer_line_user_id = ? WHERE id = ?",
                        (
                            '核准',
                            gm_username(),
                            datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                            reply_token,
                            src_user_id,
                            rid,
                        ),
                    )
                    conn.commit()
                line_reply(reply_token, '✅ 已完成審核')
            elif action == 'reject' and rid:
                # Create/replace pending record awaiting reason
                with get_conn_bonus() as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO line_pending_reason (user_id, request_id, action, created_at) VALUES (?, ?, 'reject', ?)",
                        (src_user_id, rid, datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')),
                    )
                    conn.commit()
                line_reply(reply_token, '請在 3 分鐘內輸入駁回原因（輸入「取消」可中止）')

        elif etype == 'message':
            # Capture reject reason if pending
            reply_token = e.get('replyToken', '')
            src_user_id = e.get('source', {}).get('userId', '')
            msg = (e.get('message', {}) or {}).get('text', '')
            if not src_user_id or not msg:
                continue
            # Check pending
            with get_conn_bonus() as conn:
                row = conn.execute(
                    "SELECT user_id, request_id, action, created_at FROM line_pending_reason WHERE user_id = ?",
                    (src_user_id,),
                ).fetchone()
            if not row:
                continue
            # Validate timeout (3 minutes)
            try:
                created = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            except Exception:
                created = datetime.now(timezone.utc) - timedelta(minutes=10)
            if datetime.now(timezone.utc) - created > timedelta(minutes=3):
                # Expired
                with get_conn_bonus() as conn:
                    conn.execute("DELETE FROM line_pending_reason WHERE user_id = ?", (src_user_id,))
                    conn.commit()
                line_reply(reply_token, '已逾時，請重新操作駁回')
                continue
            # Cancel flow
            if msg.strip() in {'取消', '取消作業', 'cancel', 'Cancel', 'CANCEL'}:
                with get_conn() as conn:
                    conn.execute("DELETE FROM line_pending_reason WHERE user_id = ?", (src_user_id,))
                    conn.commit()
                line_reply(reply_token, '已取消駁回')
                continue
            # Apply reject with reason
            rid = int(row[1] or 0)
            reason_text = msg.strip()
            if rid:
                with get_conn_bonus() as conn:
                    conn.execute(
                        "UPDATE bonus_request SET status = '駁回', reviewer_username = ?, review_date = ?, review_reason = ?, reviewer_line_user_id = ? WHERE id = ?",
                        (
                            gm_username(),
                            datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                            reason_text,
                            src_user_id,
                            rid,
                        ),
                    )
                    conn.execute("DELETE FROM line_pending_reason WHERE user_id = ?", (src_user_id,))
                    conn.commit()
                line_reply(reply_token, f'❌ 已駁回申請，原因：{reason_text}')
    return 'OK'


def line_reply(reply_token: str, text: str):
    access_token = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
    if not access_token or not reply_token:
        return
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    payload = {
        'replyToken': reply_token,
        'messages': [{
            'type': 'text',
            'text': text,
        }]
    }
    try:
        requests.post('https://api.line.me/v2/bot/message/reply', headers=headers, data=json.dumps(payload), timeout=10)
    except Exception:
        pass


@bonus_bp.route('/bonus/review')
def bonus_review():
    if not gm_required():
        return render_template('no_permission.html', message='僅限總經理使用（請設定 GM_USERNAME）')
    # fetch from bonus DB
    with get_conn_bonus() as bconn:
        pending_rows = [dict(r) for r in bconn.execute(
            "SELECT * FROM bonus_request WHERE status = '待審核' ORDER BY apply_date DESC LIMIT 500"
        ).fetchall()]
        reviewed_rows = [dict(r) for r in bconn.execute(
            "SELECT * FROM bonus_request WHERE COALESCE(status,'') <> '待審核' ORDER BY apply_date DESC LIMIT 500"
        ).fetchall()]
    # enrich names
    usernames = set()
    for r in pending_rows + reviewed_rows:
        usernames.add(r.get('applicant_username'))
        usernames.add(r.get('target_username'))
    usernames = [u for u in usernames if u]
    name_map = {}
    if usernames:
        placeholders = ','.join(['?'] * len(usernames))
        with get_conn() as iconn:
            q = f"SELECT username, name FROM id_data WHERE username IN ({placeholders})"
            name_map = {row['username']: row['name'] for row in iconn.execute(q, usernames).fetchall()}
    for r in pending_rows:
        r['applicant_name'] = name_map.get(r.get('applicant_username'), r.get('applicant_username'))
        r['target_name'] = name_map.get(r.get('target_username'), r.get('target_username'))
    for r in reviewed_rows:
        r['applicant_name'] = name_map.get(r.get('applicant_username'), r.get('applicant_username'))
        r['target_name'] = name_map.get(r.get('target_username'), r.get('target_username'))
    return render_template('bonus_review.html', pending_requests=pending_rows, reviewed_requests=reviewed_rows)


@bonus_bp.route('/bonus/review/<int:rid>/<action>', methods=['POST'])
def bonus_review_action(rid: int, action: str):
    if not gm_required():
        abort(403)
    status = '核准' if action == 'approve' else ('駁回' if action == 'reject' else None)
    if not status:
        abort(400)
    review_reason = (request.form.get('review_reason') or '').strip()
    with get_conn_bonus() as conn:
        conn.execute(
            "UPDATE bonus_request SET status = ?, reviewer_username = ?, review_date = ?, review_reason = ? WHERE id = ?",
            (
                status,
                session.get('username'),
                datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                review_reason,
                rid,
            ),
        )
        conn.commit()
    return redirect(url_for('bonus.bonus_review'))
@bonus_bp.route('/bonus/submit', methods=['POST'])
def bonus_submit():
    if not supervisor_required():
        abort(403)
    applicant = session.get('username')
    target = request.form.get('target_username', '').strip()
    score = request.form.get('score', '').strip()
    reason = request.form.get('reason', '').strip()

    try:
        score_int = int(score)
    except Exception:
        score_int = 0
    # 允許負分進行扣分，但不得為 0，範圍預設為 -5 至 5
    if not target or score_int == 0 or score_int < -5 or score_int > 5:
        flash('�п�ܳQ�[�����u�A�B���ƻݤ��� -5�V5�]0 �����^')
        return redirect(url_for('bonus.bonus_apply'))

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    with get_conn_bonus() as conn:
        cur = conn.execute(
            """
            INSERT INTO bonus_request (applicant_username, target_username, score, reason, apply_date, status)
            VALUES (?, ?, ?, ?, ?, '�ݼf��')
            """,
            (applicant, target, score_int, reason, now),
        )
        request_id = cur.lastrowid
        conn.commit()

    # Notify GM via LINE if configured
    try:
        send_line_notification(request_id)
    except Exception:
        pass

    flash('�w�e�X�[���ӽ�')
    return redirect(url_for('bonus.bonus_apply'))
