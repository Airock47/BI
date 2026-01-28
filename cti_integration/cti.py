import os
import logging
import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from queue import Queue, Empty
from typing import Dict, Any, Optional

import requests
from flask import Blueprint, request, jsonify, Response, session, redirect, url_for
from functools import wraps


cti_bp = Blueprint("cti", __name__)

# Bridge service endpoint for ERP forwarding (default to bridge host if env not set)
BRIDGE_SERVICE_URL = os.getenv("BRIDGE_SERVICE_URL") or "http://192.168.10.101:5001/send-to-erp"

SQLITE_QUERY_TIMEOUT_SECONDS = float(os.getenv("SQLITE_QUERY_TIMEOUT_SECONDS", "8"))
SSE_IDLE_TIMEOUT_SECONDS = int(os.getenv("CTI_SSE_IDLE_TIMEOUT_SEC", "1800"))

# ---- CTI file logger (keeps ~2 days: current + 1 backup) ----
CTI_LOG_BACKUPS = int(os.getenv("CTI_LOG_BACKUPS", "1"))
try:
    _base_dir = os.path.dirname(os.path.dirname(__file__))
    _logs_dir = os.path.join(_base_dir, "logs")
    os.makedirs(_logs_dir, exist_ok=True)
    _log_path = os.path.join(_logs_dir, "cti_events.log")
    from logging.handlers import TimedRotatingFileHandler

    cti_logger = logging.getLogger("cti")
    cti_logger.setLevel(logging.INFO)
    if not cti_logger.handlers:
        _fh = TimedRotatingFileHandler(
            _log_path, when="midnight", interval=1, backupCount=CTI_LOG_BACKUPS, encoding="utf-8"
        )
        _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        cti_logger.addHandler(_fh)
        # Avoid duplicating to root logger/console
        cti_logger.propagate = False
except Exception:
    # If logger init fails, continue without file logging
        cti_logger = logging.getLogger("cti")
        cti_logger.addHandler(logging.NullHandler())

cti_logger.info(f"Bridge service URL in use: {BRIDGE_SERVICE_URL}")


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

# ---- Auth helpers ----
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "logged_in" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated_function


# ---- Phone normalization ----
def normalize_phone_tw(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = "".join(ch for ch in str(raw) if ch.isdigit() or ch == "+")
    if not s:
        return None
    # Already E.164
    if s.startswith("+"):
        # normalize +8860X -> +886X (should not happen, just in case)
        if s.startswith("+8860"):
            return "+886" + s[5:]
        return s
    # Strip non-digits except leading + handled above
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return None
    # Handle Taiwan numbers
    if digits.startswith("886"):
        rest = digits[3:]
        if rest.startswith("0"):
            rest = rest[1:]
        return "+886" + rest
    # Domestic starting with 0 => replace leading 0 with +886
    if digits.startswith("0"):
        return "+886" + digits[1:]
    # Fallback: assume already international without '+'
    return "+" + digits


def _to_local_phone_from_e164(e164: Optional[str]) -> str:
    if not e164:
        return ""
    s = str(e164)
    if s.startswith("+886"):
        return "0" + s[4:]
    # If number starts with '+' but not Taiwan country code, drop '+' for local search/display
    if s.startswith("+"):
        return s[1:]
    return s


# ---- DB helpers ----
def _db_path() -> str:
    base = os.path.dirname(os.path.dirname(__file__))
    db_dir = os.path.join(base, "database")
    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, "cti.db")


def _get_db():
    # Improve concurrency for SQLite: set timeout and WAL mode
    conn = sqlite3.connect(_db_path(), timeout=5.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA busy_timeout=3000")
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return conn


def _init_db():
    conn = _get_db()
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS customer_phones (
            phone_e164 TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS customer_addresses (
            customer_id TEXT NOT NULL,
            postal_code TEXT,
            is_default INTEGER DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_addr_cust ON customer_addresses(customer_id);

        CREATE TABLE IF NOT EXISTS agent_mapping (
            agent_account TEXT PRIMARY KEY,
            user_id TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS call_sessions (
            conf_id TEXT PRIMARY KEY,
            key_fallback TEXT,
            phone_e164 TEXT,
            postal_code TEXT,
            customer_id TEXT,
            agent_account TEXT,
            pushed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_call_phone ON call_sessions(phone_e164);
        """
    )
    conn.commit()
    conn.close()


def _lookup_customer_by_phone_cti_db(phone_e164: str) -> tuple[Optional[str], Optional[str]]:
    """Return (customer_id, postal_code) from local cti.db mapping."""
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("SELECT customer_id FROM customer_phones WHERE phone_e164 = ?", (phone_e164,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None, None
    customer_id = row[0]
    cur.execute(
        "SELECT postal_code FROM customer_addresses WHERE customer_id = ? ORDER BY is_default DESC LIMIT 1",
        (customer_id,),
    )
    row2 = cur.fetchone()
    postal = row2[0] if row2 and row2[0] else None
    conn.close()
    return customer_id, postal


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(__file__))


def _digits_only(s: str) -> str:
    return "".join(ch for ch in s if ch.isdigit())


def _lookup_customer_by_phone_customer_new(phone_e164: str) -> tuple[Optional[str], Optional[str]]:
    """Lookup in Sales_information_inquiry/customer_new.db by scanning contact phones.

    Heuristic: compare digits-only suffix against normalized phone (both e164 and local 0-leading).
    Returns (customer_id, postal_code) if found.
    """
    try:
        base = _project_root()
        db_path = os.path.join(base, "Sales_information_inquiry", "database", "customer_new.db")
        if not os.path.exists(db_path):
            return None, None
        conn = _apply_query_timeout(sqlite3.connect(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # Try a direct LIKE match on common phone fields first (best effort)
        targets = [
            ("customer_contacts", [
                "聯絡電話", "聯絡?話", "?絡?話", "?絡電話", "行動電話", "手機", "?絡?", "?絡手機"
            ]),
            ("customer_basic", [
                "聯絡電話", "電話", "?絡?話", "?絡電話", "行動電話", "手機"
            ]),
            # Also try addresses' contact phone
            ("customer_addresses", [
                "聯絡電話"
            ]),
        ]
        # Prepare phone candidates
        d_e164 = _digits_only(phone_e164 or "")
        local = None
        if phone_e164 and phone_e164.startswith("+886"):
            local = "0" + phone_e164[4:]
        d_local = _digits_only(local or "")
        suffix = d_local[-8:] if d_local else d_e164[-8:]
        cust_code: Optional[str] = None

        for table, cols in targets:
            # Check if table exists
            try:
                cur.execute(f"PRAGMA table_info({table})")
                columns = [r[1] for r in cur.fetchall()]
            except Exception:
                continue
            if not columns:
                continue
            # Build WHERE with available columns
            like_cols = [c for c in cols if c in columns]
            if like_cols and suffix:
                wh = " OR ".join([f"{c} LIKE ?" for c in like_cols])
                args = [f"%{suffix}"] * len(like_cols)
                try:
                    # Assume customer code column name in Traditional Chinese
                    code_col = "客戶代碼" if "客戶代碼" in columns else ("客戶代號" if "客戶代號" in columns else None)
                    select_cols = f"{code_col}, *" if code_col else "*"
                    cur.execute(f"SELECT {select_cols} FROM {table} WHERE {wh} LIMIT 1", args)
                    row = cur.fetchone()
                    if row:
                        if code_col and code_col in row.keys():
                            cust_code = row[code_col]
                        else:
                            # Try common key names
                            for k in ("客戶代碼", "客戶代號", "customer_code", "customer_id"):
                                if k in row.keys():
                                    cust_code = row[k]
                                    break
                except Exception:
                    pass
            if cust_code:
                break

        # If still not found, fallback to scanning contacts and comparing digits in Python (heavier)
        if not cust_code:
            try:
                cur.execute("SELECT * FROM customer_contacts")
                for row in cur.fetchall():
                    rd = dict(row)
                    for k, v in rd.items():
                        if not isinstance(v, str):
                            continue
                        dv = _digits_only(v)
                        if dv.endswith(suffix) and suffix:
                            for ckey in ("客戶代碼", "客戶代號", "customer_code", "customer_id"):
                                if ckey in rd and rd[ckey]:
                                    cust_code = rd[ckey]
                                    break
                            if cust_code:
                                break
                    if cust_code:
                        break
            except Exception:
                pass

        if not cust_code:
            conn.close()
            return None, None

        # Find postal code from addresses — precise per provided schema
        postal: Optional[str] = None
        try:
            # Prefer explicit columns per schema
            cur.execute("PRAGMA table_info(customer_addresses)")
            infos = cur.fetchall()
            columns = [r[1] for r in infos] if infos else []
            if "客戶代碼" in columns and "郵遞區號" in columns and "是否預設" in columns:
                # Prefer non-empty postal code, prioritize default rows
                cur.execute(
                    "SELECT \"郵遞區號\" FROM customer_addresses WHERE \"客戶代碼\" = ? AND COALESCE(\"郵遞區號\", '') <> '' ORDER BY \"是否預設\" DESC, id DESC LIMIT 1",
                    (cust_code,),
                )
                rowp = cur.fetchone()
                if rowp and rowp[0]:
                    val = rowp[0]
                    postal = val.decode("utf-8", "ignore") if isinstance(val, bytes) else str(val)
                # If still empty, try scan a few latest rows for any non-empty value
                if not postal:
                    cur.execute(
                        "SELECT \"郵遞區號\" FROM customer_addresses WHERE \"客戶代碼\" = ? ORDER BY \"是否預設\" DESC, id DESC LIMIT 5",
                        (cust_code,),
                    )
                    for r in cur.fetchall() or []:
                        if r and r[0]:
                            val = r[0]
                            postal = val.decode("utf-8", "ignore") if isinstance(val, bytes) else str(val)
                            if postal:
                                break
            # Fallback: try with discovered 5th column
            if not postal and len(columns) >= 5:
                postal_col = columns[4]
                code_col = "客戶代碼" if "客戶代碼" in columns else None
                if postal_col and code_col:
                    cur.execute(f"SELECT \"{postal_col}\" FROM customer_addresses WHERE \"{code_col}\" = ? LIMIT 1", (cust_code,))
                    rowp = cur.fetchone()
                    if rowp and rowp[0]:
                        val = rowp[0]
                        postal = val.decode("utf-8", "ignore") if isinstance(val, bytes) else str(val)
            # Try customer_basic explicit column if present
            if not postal:
                try:
                    cur.execute("PRAGMA table_info(customer_basic)")
                    cb_cols = [r[1] for r in cur.fetchall()] or []
                    if "客戶代碼" in cb_cols and "郵遞區號" in cb_cols:
                        cur.execute("SELECT \"郵遞區號\" FROM customer_basic WHERE \"客戶代碼\" = ? LIMIT 1", (cust_code,))
                        rowb = cur.fetchone()
                        if rowb and rowb[0]:
                            val = rowb[0]
                            postal = val.decode("utf-8", "ignore") if isinstance(val, bytes) else str(val)
                except Exception:
                    pass
            # Last fallback: regex from any likely column
            if not postal:
                cur.execute("SELECT * FROM customer_addresses WHERE \"客戶代碼\" = ? LIMIT 1", (cust_code,))
                addr = cur.fetchone()
                if addr:
                    ad = dict(addr)
                    for k, v in ad.items():
                        if not isinstance(v, (str, bytes)):
                            continue
                        ks = str(k)
                        if ("郵" in ks) or ("postal" in ks.lower()):
                            s = v.decode("utf-8", "ignore") if isinstance(v, bytes) else v
                            import re
                            m = re.search(r"\b(\d{5}|\d{3})\b", s)
                            if m:
                                postal = m.group(1)
                                break
        except Exception:
            pass
        conn.close()
        return cust_code, postal
    except Exception:
        return None, None


def _upsert_call_session(conf_id: str, key_fb: Optional[str], phone_e164: Optional[str], postal_code: Optional[str], customer_id: Optional[str]):
    conn = _get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO call_sessions(conf_id, key_fallback, phone_e164, postal_code, customer_id)
        VALUES(?,?,?,?,?)
        ON CONFLICT(conf_id) DO UPDATE SET
            key_fallback=excluded.key_fallback,
            phone_e164=COALESCE(excluded.phone_e164, call_sessions.phone_e164),
            postal_code=COALESCE(excluded.postal_code, call_sessions.postal_code),
            customer_id=COALESCE(excluded.customer_id, call_sessions.customer_id),
            updated_at=CURRENT_TIMESTAMP
        """,
        (conf_id, key_fb, phone_e164, postal_code, customer_id),
    )
    conn.commit()
    conn.close()


def _get_call_session(conf_id: str) -> Optional[sqlite3.Row]:
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM call_sessions WHERE conf_id = ?", (conf_id,))
    row = cur.fetchone()
    conn.close()
    return row


def _update_call_session_agent_and_pushed(conf_id: str, agent_account: Optional[str], mark_pushed: bool):
    conn = _get_db()
    cur = conn.cursor()
    if mark_pushed:
        cur.execute(
            "UPDATE call_sessions SET agent_account = COALESCE(?, agent_account), pushed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE conf_id = ?",
            (agent_account, conf_id),
        )
    else:
        cur.execute(
            "UPDATE call_sessions SET agent_account = COALESCE(?, agent_account), updated_at = CURRENT_TIMESTAMP WHERE conf_id = ?",
            (agent_account, conf_id),
        )
    conn.commit()
    conn.close()


def _map_agent_to_user(agent_account: str) -> str:
    # Allow fallback: same as agent account
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM agent_mapping WHERE agent_account = ?", (agent_account,))
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        return row[0]
    return agent_account


# ---- SSE broker ----
class _SSEBroker:
    def __init__(self):
        self._lock = threading.Lock()
        self._subs: Dict[str, list[Queue]] = {}

    def subscribe(self, user_id: str) -> Queue:
        q: Queue = Queue(maxsize=100)
        with self._lock:
            self._subs.setdefault(user_id, []).append(q)
        return q

    def unsubscribe(self, user_id: str, q: Queue):
        with self._lock:
            if user_id in self._subs and q in self._subs[user_id]:
                self._subs[user_id].remove(q)
                if not self._subs[user_id]:
                    del self._subs[user_id]

    def publish(self, user_id: str, event: dict):
        payload = json.dumps(event, ensure_ascii=False)
        with self._lock:
            subs = list(self._subs.get(user_id, []))
        for q in subs:
            try:
                q.put_nowait(payload)
            except Exception:
                pass


_broker = _SSEBroker()


def _send_data_to_bridge_async(data: dict):
    """Fire-and-forget send of raw CTI payload to the bridge service."""
    def task():
        try:
            requests.post(BRIDGE_SERVICE_URL, json=data, timeout=5)
            cti_logger.info(f"Sent data to bridge service ({BRIDGE_SERVICE_URL}) for ERP processing.")
        except Exception as e:
            cti_logger.warning(f"Could not send data to bridge service {BRIDGE_SERVICE_URL}: {e}")

    thread = threading.Thread(target=task)
    thread.daemon = True
    thread.start()


# ---- Routes ----
@cti_bp.route("/cti/event", methods=["POST"], strict_slashes=False)
def cti_event():
    # DB schema is ensured on import; avoid re-initializing per request
    try:
        # Capture raw inbound for logging first
        in_meta = {
            "ts": datetime.now().isoformat(),
            "remote_addr": request.remote_addr,
            "path": request.path,
            "method": request.method,
            "headers": {k: v for k, v in request.headers.items()},
            "body": request.get_data(as_text=True),
        }

        data = request.get_json(force=True, silent=False)
        in_meta["json"] = data if isinstance(data, dict) else None
        if not isinstance(data, dict):
            resp = {"ok": False, "message": "invalid json"}
            try:
                cti_logger.info(json.dumps({"incoming": in_meta, "response": resp, "status": 400}, ensure_ascii=False))
            except Exception:
                pass
            return jsonify(resp), 400

        # Asynchronously forward the raw event to the bridge service for ERP processing.
        _send_data_to_bridge_async(data)

        remote_no = (data.get("remoteNo") or "").strip()
        conf_id_raw = (data.get("confID") or data.get("confId") or "").strip()
        key_fb = (data.get("key") or "").strip()
        if not conf_id_raw and not key_fb and not remote_no:
            resp = {"ok": False, "message": "missing confID/key"}
            try:
                cti_logger.info(json.dumps({"incoming": in_meta, "response": resp, "status": 400}, ensure_ascii=False))
            except Exception:
                pass
            return jsonify(resp), 400
        conf_id = conf_id_raw
        session_id = conf_id_raw or key_fb or ""
        event = (data.get("event") or "").strip().lower()
        state = data.get("state")
        io = data.get("io")
        user_info = data.get("userInfo")
        agent_account = ""
        if isinstance(user_info, dict):
            agent_account = (user_info.get("agentAccount") or "").strip()

        # Infer event if provider didn't send it
        if not event:
            if agent_account:
                event = "bind"
            elif (state == 1 and io == 0) or remote_no:
                event = "ring"
            else:
                event = "unknown"

        normalized = normalize_phone_tw(remote_no) if remote_no else None

        # First event: ring
        if event == "ring":
            cust_id, postal = (None, None)
            if normalized:
                # Priority: look up from customer_new.db
                cust_id, postal = _lookup_customer_by_phone_customer_new(normalized)
                if not cust_id:
                    # Fallback to local mapping DB if present
                    cust_id, postal = _lookup_customer_by_phone_cti_db(normalized)
            if session_id:
                _upsert_call_session(session_id, key_fb, normalized, postal, cust_id)
            resp = {
                "ok": True,
                "confID": conf_id or key_fb or "",
                "normalizedPhone": normalized,
                "postalCode": postal or "",
                "found": bool(cust_id),
                "customerId": cust_id or "",
            }
            try:
                cti_logger.info(json.dumps({"incoming": in_meta, "response": resp, "status": 200}, ensure_ascii=False))
            except Exception:
                pass
            return jsonify(resp), 200

        # Second event: agent bind / connect
        if event in ("bind", "connect", "answer"):
            sess = _get_call_session(session_id) if session_id else None
            phone = sess["phone_e164"] if sess else normalized
            postal = sess["postal_code"] if sess else None
            cust_id = sess["customer_id"] if sess else None
            if not sess and normalized:
                cust_id_lookup, postal_lookup = _lookup_customer_by_phone_customer_new(normalized)
                if not cust_id_lookup:
                    cust_id_lookup, postal_lookup = _lookup_customer_by_phone_cti_db(normalized)
                cust_id = cust_id or cust_id_lookup
                postal = postal or postal_lookup
                if session_id:
                    _upsert_call_session(session_id, key_fb, normalized, postal, cust_id)

            # Publish popup to mapped user
            target_user = _map_agent_to_user(agent_account) if agent_account else None
            phone_local = _to_local_phone_from_e164(phone)
            base_url = (request.host_url or "").rstrip("/")
            payload = {
                "type": "CTI_POP",
                "confID": conf_id or key_fb or "",
                "phone": phone or "",
                "postalCode": postal or "",
                "customerId": cust_id or "",
                "links": {
                    # Open local sales search page with phone prefilled to avoid 404s
                    "crm": f"{base_url}/sales_info/?phone={phone_local}" if (phone_local or phone) else f"{base_url}/sales_info/",
                },
            }
            if target_user:
                _broker.publish(target_user, payload)
            if session_id:
                _update_call_session_agent_and_pushed(session_id, agent_account, mark_pushed=True)
            resp = {
                "ok": True,
                "confID": conf_id or key_fb or "",
                "normalizedPhone": phone or "",
                "postalCode": postal or "",
                "found": bool(cust_id),
                "customerId": cust_id or "",
            }
            try:
                cti_logger.info(json.dumps({"incoming": in_meta, "response": resp, "status": 200}, ensure_ascii=False))
            except Exception:
                pass
            return jsonify(resp), 200

        # Default: upsert what we can
        if session_id:
            _upsert_call_session(session_id, key_fb, normalized, None, None)
        resp = {
            "ok": True,
            "confID": conf_id or key_fb or "",
            "normalizedPhone": normalized or "",
            "postalCode": "",
            "found": False,
            "customerId": "",
        }
        try:
            cti_logger.info(json.dumps({"incoming": in_meta, "response": resp, "status": 200}, ensure_ascii=False))
        except Exception:
            pass
        return jsonify(resp), 200

    except Exception as e:
        try:
            # Best-effort logging if in_meta not available
            _in_meta = locals().get("in_meta", {
                "ts": datetime.now().isoformat(),
                "remote_addr": getattr(request, "remote_addr", None),
                "path": getattr(request, "path", None),
                "method": getattr(request, "method", None),
            })
            resp = {"ok": False, "message": str(e)}
            cti_logger.info(json.dumps({"incoming": _in_meta, "response": resp, "status": 500}, ensure_ascii=False))
        except Exception:
            pass
        return jsonify({"ok": False, "message": str(e)}), 500


@cti_bp.route("/sse/notifications")
@login_required
def sse_notifications():
    user = session.get("username") or session.get("user_id")
    if not user:
        return redirect(url_for("login"))

    q = _broker.subscribe(user)

    def gen():
        idle_timeout = SSE_IDLE_TIMEOUT_SECONDS
        last_activity = time.time()
        try:
            # Send a hello event
            yield "event: ping\n" f"data: {json.dumps({'ok': True, 'user': user})}\n\n"
            ping_interval = int(os.getenv("CTI_SSE_PING_SEC", "25"))
            last_ping = time.time()
            while True:
                # Try to get message with timeout to allow periodic ping
                timeout = max(1, min(10, ping_interval))
                try:
                    msg = q.get(timeout=timeout)
                    last_activity = time.time()
                    yield f"data: {msg}\n\n"
                except Empty:
                    pass
                now = time.time()
                if idle_timeout > 0 and now - last_activity >= idle_timeout:
                    break
                if now - last_ping >= ping_interval:
                    yield "event: ping\n" f"data: {json.dumps({'ts': int(now)})}\n\n"
                    last_ping = now
        finally:
            _broker.unsubscribe(user, q)


    return Response(gen(), mimetype="text/event-stream")


# Initialize DB on import for safety
try:
    _init_db()
except Exception:
    pass
