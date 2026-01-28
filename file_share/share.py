import os
import io
import mimetypes
import shutil
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path
import logging

from flask import (
    Blueprint,
    request,
    jsonify,
    session,
    redirect,
    url_for,
    send_file,
    render_template,
    current_app,
)
from functools import wraps


share_bp = Blueprint(
    "share",
    __name__,
    url_prefix="",
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "logged_in" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated_function


def _tz_tw():
    return timezone(timedelta(hours=8))


def _get_storage_root() -> Path:
    root = os.getenv("STORAGE_ROOT")
    if not root:
        # default to project-local folder to avoid errors
        root = os.path.join(os.path.dirname(os.path.dirname(__file__)), "shared_storage")
    p = Path(root).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _get_preview_cache_root() -> Path:
    # cache folder for converted previews (PDF/images)
    root = _get_storage_root() / ".previews"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _safe_join(base: Path, *paths: str) -> Path:
    # Prevent path traversal and ensure final path under base
    target = base
    for part in paths:
        if part is None:
            continue
        # normalize separators and strip dangerous patterns
        part = part.replace("\\", "/")
        if part.startswith("/"):
            part = part[1:]
        if ".." in part:
            raise ValueError("Invalid path component")
        target = target / part
    resolved = target.resolve()
    base_resolved = base.resolve()
    if os.path.commonpath([str(resolved), str(base_resolved)]) != str(base_resolved):
        raise ValueError("Path escapes storage root")
    return resolved


def _allowed_ext_set() -> set:
    exts = os.getenv(
        "SHARE_ALLOWED_EXT",
        "pdf,docx,xlsx,jpg,png,mp4,mov,avi",
    )
    return set(e.strip().lower() for e in exts.split(",") if e.strip())


def _max_file_size_bytes() -> int:
    # Default 200MB
    mb = int(os.getenv("SHARE_MAX_FILE_MB", "200"))
    return mb * 1024 * 1024


def _is_admin() -> bool:
    # Admin users from env var (comma separated), default to C4D002 for parity with inventory admin
    admins = os.getenv("SHARE_ADMIN_USERS", "C4D002")
    admin_set = {u.strip() for u in admins.split(",") if u.strip()}
    return (session.get("username") or "") in admin_set


@share_bp.route("/share")
@login_required
def share_page():
    # Pass current user and admin flag to template for UI controls
    return render_template(
        "share.html",
        current_user=session.get("username"),
        is_admin=_is_admin(),
        allowed_ext=sorted(list(_allowed_ext_set())),
    )


def _ensure_libreoffice_path() -> str | None:
    # Try env path first, then common locations on Windows, else typical command name
    env_path = os.getenv("LIBREOFFICE_PATH")
    if env_path and Path(env_path).exists():
        return env_path
    # Windows common installs
    if os.name == "nt":
        candidates = [
            r"C:\\Program Files\\LibreOffice\\program\\soffice.exe",
            r"C:\\Program Files (x86)\\LibreOffice\\program\\soffice.exe",
        ]
        for c in candidates:
            if Path(c).exists():
                return c
        return "soffice.exe"
    # Posix
    return "soffice"


def _convert_office_to_pdf(src: Path) -> Path | None:
    """Convert Office file to PDF using LibreOffice headless. Returns PDF path or None.

    Detailed stdout/stderr are logged on failure for diagnostics.
    """
    try:
        out_dir = _get_preview_cache_root() / src.parent.relative_to(_get_storage_root())
        out_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        out_dir = _get_preview_cache_root()
        out_dir.mkdir(parents=True, exist_ok=True)

    pdf_name = src.with_suffix(".pdf").name
    dest_pdf = out_dir / pdf_name
    # If cached and newer than source, reuse
    if dest_pdf.exists() and dest_pdf.stat().st_mtime >= src.stat().st_mtime:
        return dest_pdf

    soffice = _ensure_libreoffice_path()
    if not soffice:
        return None

    try:
        # Run conversion; set cwd to src directory to avoid some LO path quirks
        timeout_s = int(os.getenv("SHARE_CONVERT_TIMEOUT", "180"))
        cmd = [
            soffice,
            "--headless",
            "--norestore",
            "--invisible",
            "--convert-to",
            "pdf",
            "--outdir",
            str(out_dir),
            str(src),
        ]
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_s,
            check=False,
            cwd=str(src.parent),
        )
        if result.returncode == 0 and dest_pdf.exists():
            return dest_pdf
        logging.error(
            "LibreOffice convert failed rc=%s stdout=%s stderr=%s cmd=%s",
            result.returncode,
            result.stdout.decode(errors="ignore"),
            result.stderr.decode(errors="ignore"),
            cmd,
        )
        return None
    except subprocess.TimeoutExpired as e:
        logging.error("LibreOffice convert timeout after %ss: %s", e.timeout, e)
        return None
    except Exception as e:
        logging.exception("LibreOffice convert exception: %s", e)
        return None


def _iso8601_tw(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=_tz_tw())
    return dt.isoformat()

def _display_time_zh(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=_tz_tw())
    return dt.strftime("%Y年%m月%d日 %H:%M:%S")


def _load_meta(dir_path: Path) -> dict:
    meta_file = dir_path / ".meta.json"
    if meta_file.exists():
        try:
            import json

            with meta_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data.setdefault("files", {})
                return data
        except Exception:
            pass
    return {"files": {}}


def _save_meta(dir_path: Path, data: dict):
    try:
        import json

        meta_file = dir_path / ".meta.json"
        tmp = meta_file.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        tmp.replace(meta_file)
    except Exception:
        pass


def _update_meta_on_upload(dir_path: Path, filename: str, uploader: str, uploader_name: str | None = None):
    data = _load_meta(dir_path)
    files = data.setdefault("files", {})
    files[filename] = {
        "uploader": uploader,
        "uploader_username": uploader,
        "uploader_name": uploader_name or "",
        "uploaded_at": datetime.now(tz=_tz_tw()).isoformat(),
    }
    _save_meta(dir_path, data)


def _update_meta_on_delete(dir_path: Path, name: str):
    data = _load_meta(dir_path)
    files = data.setdefault("files", {})
    if name in files:
        files.pop(name, None)
        _save_meta(dir_path, data)


def _update_meta_on_rename(dir_path: Path, old: str, new: str):
    data = _load_meta(dir_path)
    files = data.setdefault("files", {})
    if old in files:
        files[new] = files.pop(old)
        _save_meta(dir_path, data)


def _get_uploader_for(dir_path: Path, name: str) -> str | None:
    data = _load_meta(dir_path)
    meta = (data.get("files", {}).get(name, {}) or {})
    return meta.get("uploader_username") or meta.get("uploader")


def _list_dir(root: Path, rel_dir: str | None, keyword: str | None):
    base = _safe_join(root, rel_dir or "")
    if not base.exists() or not base.is_dir():
        return [], 0
    keyword_l = (keyword or "").lower().strip()

    items = []
    # If keyword provided, include matches in subdirectories as well
    if keyword_l:
        # Cache meta per directory to avoid repeated file I/O
        meta_cache: dict[str, dict] = {}
        for dirpath, dirnames, filenames in os.walk(base):
            # skip internal preview cache directories
            dirnames[:] = [d for d in dirnames if d.lower() != ".previews"]
            cur_dir = Path(dirpath)
            rel_path_from_base = (
                (Path(rel_dir or "") / cur_dir.relative_to(base)).as_posix()
                if cur_dir != base
                else Path(rel_dir or "").as_posix()
            )
            # Directories match
            for d in dirnames:
                if d.lower() == ".previews":
                    continue
                if keyword_l in d.lower():
                    p = cur_dir / d
                    try:
                        mtime = p.stat().st_mtime
                        items.append(
                            {
                                "name": d,
                                "type": "",
                                "size": 0,
                                "modified_at": _iso8601_tw(mtime),
                                "modified_at_display": _display_time_zh(mtime),
                                "is_dir": True,
                                "rel_path": (Path(rel_path_from_base) / d).as_posix(),
                                "uploader": "",
                                "uploader_name": "",
                            }
                        )
                    except Exception:
                        pass
            # Files match
            # Load meta once per directory
            meta = meta_cache.get(dirpath)
            if meta is None:
                meta = _load_meta(cur_dir)
                meta_cache[dirpath] = meta
            for f in filenames:
                if f.lower() == ".meta.json":
                    continue
                # Search only by base filename (exclude extension)
                base_name = Path(f).stem.lower()
                if keyword_l in base_name:
                    p = cur_dir / f
                    try:
                        stat = p.stat()
                        ext = p.suffix[1:].lower()
                        fmeta = (meta.get("files", {}).get(f, {}) or {})
                        uploader = fmeta.get("uploader_username") or fmeta.get("uploader", "")
                        uploader_name = fmeta.get("uploader_name") or ""
                        items.append(
                            {
                                "name": f,
                                "type": ext,
                                "size": stat.st_size,
                                "modified_at": _iso8601_tw(stat.st_mtime),
                                "modified_at_display": _display_time_zh(stat.st_mtime),
                                "is_dir": False,
                                "rel_path": (Path(rel_path_from_base) / f).as_posix(),
                                "uploader": uploader,
                                "uploader_name": uploader_name,
                            }
                        )
                    except Exception:
                        pass
    else:
        # Non-recursive listing in current directory
        meta = _load_meta(base)
        for entry in base.iterdir():
            try:
                name = entry.name
                if name.lower() == ".meta.json" or name.lower() == ".previews":
                    continue
                is_dir = entry.is_dir()
                size = entry.stat().st_size if entry.is_file() else 0
                mtime = entry.stat().st_mtime
                ext = (entry.suffix[1:].lower() if entry.suffix else "")
                uploader = ""
                uploader_name = ""
                if entry.is_file():
                    fmeta = (meta.get("files", {}).get(name, {}) or {})
                    uploader = fmeta.get("uploader_username") or fmeta.get("uploader", "")
                    uploader_name = fmeta.get("uploader_name") or ""
                items.append(
                    {
                        "name": name,
                        "type": ext,
                        "size": size,
                        "modified_at": _iso8601_tw(mtime),
                        "modified_at_display": _display_time_zh(mtime),
                        "is_dir": is_dir,
                        "rel_path": (Path(rel_dir or "") / name).as_posix(),
                        "uploader": uploader,
                        "uploader_name": uploader_name,
                    }
                )
            except Exception:
                continue
    total = len(items)
    return items, total


def _apply_sort(items, field: str | None, order: str | None):
    """Sort with folders first, then by the selected field.

    Default: modified_at desc (newest first).
    Always keep directories before files regardless of order.
    """
    from datetime import datetime

    def _ts(s: str) -> float:
        try:
            return datetime.fromisoformat(s).timestamp()
        except Exception:
            return 0.0

    field = (field or "modified_at").lower()
    reverse = (order or "desc").lower() == "desc"

    def val_key(x):
        if field == "name":
            return (x.get("name") or "").lower()
        if field == "type":
            return x.get("type") or ""
        if field == "size":
            return x.get("size") or 0
        # modified_at or unknown -> use timestamp
        return _ts(x.get("modified_at") or "")

    dirs = [i for i in items if i.get("is_dir")]
    files = [i for i in items if not i.get("is_dir")]

    dirs.sort(key=val_key, reverse=reverse)
    files.sort(key=val_key, reverse=reverse)

    items[:] = dirs + files


@share_bp.route("/api/share/files")
@login_required
def api_list_files():
    try:
        rel_dir = request.args.get("path", "")
        keyword = request.args.get("q")
        page = int(request.args.get("page", "1"))
        size = int(request.args.get("size", "20"))
        sort_field = request.args.get("sort")
        sort_order = request.args.get("order")

        if page < 1 or size < 1 or size > 200:
            return jsonify({"error": "invalid pagination"}), 400

        root = _get_storage_root()
        items, total = _list_dir(root, rel_dir, keyword)
        _apply_sort(items, sort_field, sort_order)

        start = (page - 1) * size
        end = start + size
        page_items = items[start:end]

        return jsonify(
            {
                "path": rel_dir,
                "page": page,
                "size": size,
                "total": total,
                "items": page_items,
            }
        )
    except ValueError:
        return jsonify({"error": "invalid path"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@share_bp.route("/api/share/upload", methods=["POST"])
@login_required
def api_upload():
    try:
        rel_dir = request.args.get("path", "")
        root = _get_storage_root()
        target_dir = _safe_join(root, rel_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        files = request.files.getlist("files[]") or request.files.getlist("files")
        if not files:
            return jsonify({"error": "no files"}), 400

        allowed = _allowed_ext_set()
        max_bytes = _max_file_size_bytes()

        uploaded = []
        skipped = []

        for f in files:
            filename = f.filename or ""
            ext = Path(filename).suffix[1:].lower()
            if not filename:
                skipped.append({"name": filename, "reason": "empty name"})
                continue
            if ext not in allowed:
                skipped.append({"name": filename, "reason": "extension not allowed"})
                continue
            # Enforce size by reading stream length if provided
            stream = f.stream
            pos = stream.tell()
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(pos)
            if size and size > max_bytes:
                skipped.append({"name": filename, "reason": "file too large"})
                continue

            safe_name = os.path.basename(filename)
            dest = target_dir / safe_name
            f.save(str(dest))
            _update_meta_on_upload(
                target_dir,
                safe_name,
                session.get("username") or "",
                session.get("name") or "",
            )
            logging.info("share.upload user=%s path=%s file=%s size=%s", session.get("username"), rel_dir, safe_name, os.path.getsize(dest))
            uploaded.append(safe_name)

        return jsonify({"ok": True, "uploaded": uploaded, "skipped": skipped})
    except ValueError:
        return jsonify({"error": "invalid path"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@share_bp.route("/api/share/download")
@login_required
def api_download():
    try:
        rel_dir = request.args.get("path", "")
        name = request.args.get("name", "")
        if not name:
            return jsonify({"error": "missing name"}), 400
        root = _get_storage_root()
        file_path = _safe_join(root, rel_dir, name)
        if not file_path.exists() or not file_path.is_file():
            return jsonify({"error": "not found"}), 404
        logging.info("share.download user=%s path=%s file=%s", session.get("username"), rel_dir, name)
        return send_file(
            str(file_path),
            as_attachment=True,
            download_name=file_path.name,
        )
    except ValueError:
        return jsonify({"error": "invalid path"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@share_bp.route("/api/share/preview")
@login_required
def api_preview():
    try:
        rel_dir = request.args.get("path", "")
        name = request.args.get("name", "")
        if not name:
            return jsonify({"error": "missing name"}), 400
        root = _get_storage_root()
        file_path = _safe_join(root, rel_dir, name)
        if not file_path.exists() or not file_path.is_file():
            return jsonify({"error": "not found"}), 404

        ext = file_path.suffix[1:].lower()
        # Strategy A: convert Office to PDF, images served directly, pdf direct
        if ext == "pdf":
            logging.info("share.preview pdf user=%s path=%s file=%s", session.get("username"), rel_dir, name)
            token = _make_signed_token(str(file_path))
            return jsonify({"ok": True, "viewer_url": url_for("share.view_pdf", token=token), "kind": "pdf"})
        elif ext in {"jpg", "jpeg", "png"}:
            logging.info("share.preview image user=%s path=%s file=%s", session.get("username"), rel_dir, name)
            token = _make_signed_token(str(file_path))
            return jsonify({"ok": True, "viewer_url": url_for("share.view_inline", token=token), "kind": "image"})
        elif ext in {"doc", "docx", "xls", "xlsx", "ppt", "pptx"}:
            pdf = _convert_office_to_pdf(file_path)
            if pdf and pdf.exists():
                logging.info("share.preview office->pdf user=%s path=%s file=%s", session.get("username"), rel_dir, name)
                token = _make_signed_token(str(pdf))
                return jsonify({"ok": True, "viewer_url": url_for("share.view_pdf", token=token), "kind": "pdf"})
            # More descriptive hint for admins/operators
            hint = "preview conversion failed (LibreOffice not found or conversion error). Check LIBREOFFICE_PATH/SHARE_CONVERT_TIMEOUT and server logs."
            return jsonify({"ok": False, "error": hint}), 501
        else:
            return jsonify({"ok": False, "error": "preview not supported"}), 415
    except ValueError:
        return jsonify({"error": "invalid path"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _make_signed_token(abs_path: str) -> str:
    # very simple HMAC-like token using secret_key; includes expiry
    import hmac
    import hashlib

    secret = (current_app.secret_key or "").encode("utf-8")
    expires = int(datetime.now(tz=_tz_tw()).timestamp()) + int(
        os.getenv("SHARE_PREVIEW_TTL", "600")
    )
    payload = f"{abs_path}|{expires}".encode("utf-8")
    sig = hmac.new(secret, payload, hashlib.sha256).hexdigest()
    return f"{abs_path}|{expires}|{sig}"


def _verify_signed_token(token: str) -> Path | None:
    import hmac
    import hashlib

    try:
        abs_path, expires_str, sig = token.rsplit("|", 2)
        expires = int(expires_str)
    except Exception:
        return None
    secret = (current_app.secret_key or "").encode("utf-8")
    payload = f"{abs_path}|{expires}".encode("utf-8")
    expected = hmac.new(secret, payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        return None
    now_ts = int(datetime.now(tz=_tz_tw()).timestamp())
    if now_ts > expires:
        return None

    root = _get_storage_root()
    p = Path(abs_path)
    try:
        # Ensure still under root
        _safe_join(root, str(p.relative_to(root)))
    except Exception:
        return None
    return p


@share_bp.route("/api/share/view_pdf")
@login_required
def view_pdf():
    token = request.args.get("token", "")
    p = _verify_signed_token(token)
    if not p or not p.exists() or not p.is_file():
        return jsonify({"error": "invalid token"}), 400
    return send_file(str(p), mimetype="application/pdf", as_attachment=False, download_name=p.name)


@share_bp.route("/api/share/view_inline")
@login_required
def view_inline():
    token = request.args.get("token", "")
    p = _verify_signed_token(token)
    if not p or not p.exists() or not p.is_file():
        return jsonify({"error": "invalid token"}), 400
    mime, _ = mimetypes.guess_type(p.name)
    return send_file(str(p), mimetype=mime or "application/octet-stream", as_attachment=False, download_name=p.name)


@share_bp.route("/api/share/mkdir", methods=["POST"])
@login_required
def api_mkdir():
    try:
        data = request.get_json(force=True)
        rel_path = data.get("path", "")
        name = data.get("name", "").strip()
        if not name:
            return jsonify({"error": "missing name"}), 400
        root = _get_storage_root()
        target = _safe_join(root, rel_path, name)
        target.mkdir(parents=True, exist_ok=True)
        return jsonify({"ok": True})
    except ValueError:
        return jsonify({"error": "invalid path"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@share_bp.route("/api/share/file", methods=["DELETE"])
@login_required
def api_delete_file():
    try:
        rel_dir = request.args.get("path", "")
        name = request.args.get("name", "")
        if not name:
            return jsonify({"error": "missing name"}), 400
        root = _get_storage_root()
        target = _safe_join(root, rel_dir, name)
        if not target.exists():
            return jsonify({"error": "not found"}), 404
        if target.is_file():
            # Permission: admin or uploader of the file
            uploader = _get_uploader_for(target.parent, target.name) or ""
            if not (_is_admin() or (uploader and uploader == (session.get("username") or ""))):
                return jsonify({"error": "forbidden"}), 403
            target.unlink()
            _update_meta_on_delete(target.parent, target.name)
            logging.info("share.delete file user=%s path=%s file=%s", session.get("username"), rel_dir, name)
            return jsonify({"ok": True, "deleted": name})
        if target.is_dir():
            # Only delete empty dir to be safe
            try:
                # Keep directory deletion conservative; admin only
                if not _is_admin():
                    return jsonify({"error": "forbidden"}), 403
                target.rmdir()
                logging.info("share.delete dir user=%s path=%s dir=%s", session.get("username"), rel_dir, name)
                return jsonify({"ok": True, "deleted": name})
            except OSError:
                return jsonify({"error": "directory not empty"}), 400
        return jsonify({"error": "unsupported type"}), 400
    except ValueError:
        return jsonify({"error": "invalid path"}), 400
    except Exception as e:
        logging.exception("share.delete error: %s", e)
        return jsonify({"error": str(e)}), 500


@share_bp.route("/api/share/rename", methods=["POST"])
@login_required
def api_rename():
    """Rename a file or directory within the same path.

    - Files: allowed for admin or original uploader
    - Directories: admin only
    """
    try:
        data = request.get_json(force=True)
        rel_path = (data.get("path") or "").strip()
        old_name = (data.get("old_name") or "").strip()
        new_name = (data.get("new_name") or "").strip()
        if not old_name or not new_name:
            return jsonify({"error": "missing name"}), 400
        # basic validation to avoid separators
        if any(x in old_name for x in ["/", "\\"]) or any(x in new_name for x in ["/", "\\"]):
            return jsonify({"error": "invalid name"}), 400

        root = _get_storage_root()
        src = _safe_join(root, rel_path, old_name)
        dst = _safe_join(root, rel_path, new_name)
        if not src.exists():
            return jsonify({"error": "not found"}), 404
        if dst.exists():
            return jsonify({"error": "target exists"}), 409

        # Permission + validation
        if src.is_dir():
            # Directories: admin only
            if not _is_admin():
                return jsonify({"error": "forbidden"}), 403
        else:
            # Files: admin or uploader of the file
            uploader = _get_uploader_for(src.parent, src.name) or ""
            if not (_is_admin() or (uploader and uploader == (session.get("username") or ""))):
                return jsonify({"error": "forbidden"}), 403
            # Enforce keeping the same extension to avoid format change
            old_ext = Path(old_name).suffix.lower()
            new_ext_full = Path(new_name).suffix.lower()
            if old_ext != new_ext_full:
                return jsonify({"error": "cannot change extension"}), 400
            # Ensure extension is still allowed (defensive)
            allowed = _allowed_ext_set()
            if old_ext and old_ext[1:] not in allowed:
                return jsonify({"error": "extension not allowed"}), 400

        src.rename(dst)
        _update_meta_on_rename(dst.parent, old_name, new_name)

        # Try to move cached preview if any (best-effort)
        try:
            prev_dir = _get_preview_cache_root() / Path(rel_path)
            if src.is_file():
                # move .pdf cache for office files or pdf itself
                src_pdf = prev_dir / (Path(old_name).with_suffix(".pdf").name)
                dst_pdf = prev_dir / (Path(new_name).with_suffix(".pdf").name)
                if src_pdf.exists() and not dst_pdf.exists():
                    src_pdf.rename(dst_pdf)
        except Exception:
            pass

        logging.info("share.rename user=%s path=%s old=%s new=%s", session.get("username"), rel_path, old_name, new_name)
        return jsonify({"ok": True, "old": old_name, "new": new_name})
    except ValueError:
        return jsonify({"error": "invalid path"}), 400
    except Exception as e:
        logging.exception("share.rename error: %s", e)
        return jsonify({"error": str(e)}), 500

