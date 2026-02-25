#!/usr/bin/env python3
"""
PhotoNest
- Incremental index scan for TB-scale photo libraries
- Publish + ACL access model (admin/user)
- Cache-first image serving (thumb/preview/cover)
"""

import json
import logging
import mimetypes
import os
import shlex
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from functools import wraps
from pathlib import Path

from flask import (
    Flask,
    Response,
    abort,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from PIL import Image, ImageOps
from werkzeug.security import check_password_hash, generate_password_hash

try:
    import pillow_heif
except Exception:
    pillow_heif = None

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PHOTO_ROOT_DEFAULT = os.environ.get("PHOTO_ROOT", "/mnt/nas/photos")
CACHE_ROOT_DEFAULT = os.environ.get("CACHE_ROOT", "/var/lib/photopanel/cache")
DATA_ROOT_DEFAULT = os.environ.get("DATA_ROOT", "/var/lib/photopanel/data")
PANEL_REPO_URL_DEFAULT = os.environ.get("PANEL_REPO_URL", "https://github.com/cyeinfpro/PhotoPanel")

DB_PATH = os.environ.get("PHOTO_DB", os.path.join(DATA_ROOT_DEFAULT, "photopanel.db"))
SECRET_KEY = os.environ.get("SECRET_KEY", "photopanel-change-me")

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))

CACHE_EXT = ".webp"
THUMB_MAX_EDGE_DEFAULT = int(os.environ.get("THUMB_MAX_EDGE", "480"))
PREVIEW_MAX_EDGE_DEFAULT = int(os.environ.get("PREVIEW_MAX_EDGE", "1920"))
THUMB_QUALITY_DEFAULT = int(os.environ.get("THUMB_QUALITY", "74"))
PREVIEW_QUALITY_DEFAULT = int(os.environ.get("PREVIEW_QUALITY", "80"))
THUMB_TARGET_KB_DEFAULT = int(os.environ.get("THUMB_TARGET_KB", "180"))
PREVIEW_TARGET_KB_DEFAULT = int(os.environ.get("PREVIEW_TARGET_KB", "950"))
WEBP_METHOD_DEFAULT = int(os.environ.get("WEBP_METHOD", "4"))

ENABLE_X_ACCEL = os.environ.get("ENABLE_X_ACCEL", "0") == "1"
ACCEL_CACHE_PREFIX = os.environ.get("ACCEL_CACHE_PREFIX", "/_cache")
ACCEL_ORIG_PREFIX = os.environ.get("ACCEL_ORIG_PREFIX", "/_orig")

DEFAULT_SETTINGS = {
    "photo_root": PHOTO_ROOT_DEFAULT,
    "cache_root": CACHE_ROOT_DEFAULT,
    "max_scan_albums_per_run": "80",
    "max_scan_files_per_album": "5000",
    "max_new_thumbs_per_run": "3000",
    "workers": "4",
    "time_budget_seconds": "900",
    "preheat_count": "200",
    "thumb_max_edge": str(THUMB_MAX_EDGE_DEFAULT),
    "preview_max_edge": str(PREVIEW_MAX_EDGE_DEFAULT),
    "thumb_quality": str(THUMB_QUALITY_DEFAULT),
    "preview_quality": str(PREVIEW_QUALITY_DEFAULT),
    "thumb_target_kb": str(THUMB_TARGET_KB_DEFAULT),
    "preview_target_kb": str(PREVIEW_TARGET_KB_DEFAULT),
    "webp_method": str(WEBP_METHOD_DEFAULT),
    "allowed_extensions": ".jpg,.jpeg,.png,.webp,.heic,.heif",
    "exclude_dirs": "@eaDir,.git,.cache,.thumbnails",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("photopanel")

if SECRET_KEY == "photopanel-change-me":
    logger.warning("SECRET_KEY is default. Set SECRET_KEY in production.")

if pillow_heif is not None:
    try:
        pillow_heif.register_heif_opener()
        logger.info("HEIF/HEIC support enabled via pillow-heif.")
    except Exception as exc:
        logger.warning("pillow-heif exists but failed to register: %s", exc)
else:
    logger.warning("pillow-heif not installed. HEIC/HEIF decode may be unavailable.")

# ---------------------------------------------------------------------------
# App / global state
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config["JSON_AS_ASCII"] = False

scan_status_lock = threading.Lock()
update_status_lock = threading.Lock()
runtime_settings_lock = threading.Lock()
runtime_settings_cache = None
runtime_settings_cache_expires_at = 0.0
RUNTIME_SETTINGS_CACHE_TTL = float(os.environ.get("SETTINGS_CACHE_TTL_SECONDS", "5"))
SCAN_LOCK_STALE_SECONDS = int(os.environ.get("SCAN_LOCK_STALE_SECONDS", "21600"))
SCAN_ABORT_FORCE_SECONDS = int(os.environ.get("SCAN_ABORT_FORCE_SECONDS", "8"))
PANEL_UPDATE_LOCK_STALE_SECONDS = int(os.environ.get("PANEL_UPDATE_LOCK_STALE_SECONDS", "7200"))
PANEL_UPDATE_LOG_LIMIT = int(os.environ.get("PANEL_UPDATE_LOG_LIMIT", "220"))
PANEL_UPDATE_TERMINAL_TTL_SECONDS = int(os.environ.get("PANEL_UPDATE_TERMINAL_TTL_SECONDS", "6"))
DB_CONNECT_TIMEOUT_SECONDS = float(os.environ.get("DB_CONNECT_TIMEOUT_SECONDS", "60"))
DB_BUSY_TIMEOUT_MS = int(os.environ.get("DB_BUSY_TIMEOUT_MS", "120000"))
DB_WRITE_RETRY_ATTEMPTS = int(os.environ.get("DB_WRITE_RETRY_ATTEMPTS", "8"))
DB_WRITE_RETRY_SLEEP_SECONDS = float(os.environ.get("DB_WRITE_RETRY_SLEEP_SECONDS", "0.12"))
PREHEAT_RETRY_ATTEMPTS = int(os.environ.get("PREHEAT_RETRY_ATTEMPTS", "2"))
PREHEAT_RETRY_DELAY_SECONDS = float(os.environ.get("PREHEAT_RETRY_DELAY_SECONDS", "0.2"))

RUNTIME_SCAN_STATUS_KEY = "scan_status_json"
RUNTIME_SCAN_LOCK_KEY = "scan_lock_json"
RUNTIME_SCAN_CONTROL_KEY = "scan_control_json"
RUNTIME_UPDATE_STATUS_KEY = "panel_update_status_json"
RUNTIME_UPDATE_LOCK_KEY = "panel_update_lock_json"

DEFAULT_SCAN_STATUS = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "trigger": None,
    "message": "idle",
    "processed_albums": 0,
    "skipped_albums": 0,
    "visited_albums": 0,
    "scanned_files": 0,
    "updated_files": 0,
    "deleted_files": 0,
    "generated_cache_pairs": 0,
    "found_albums": 0,
    "scan_target_albums": 0,
    "total_cache_tasks": 0,
    "completed_cache_tasks": 0,
    "current_cache_file": "",
    "recent_cache_items": [],
    "phase": "idle",
    "paused": False,
    "pause_requested": False,
    "abort_requested": False,
    "stop_reason": None,
    "summary": {},
}

DEFAULT_SCAN_CONTROL = {
    "pause_requested": False,
    "abort_requested": False,
    "updated_at": None,
}

DEFAULT_UPDATE_STATUS = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "message": "idle",
    "stage": "idle",
    "progress_pct": 0,
    "steps_total": 0,
    "steps_done": 0,
    "current_step": "",
    "logs": [],
    "error": "",
    "needs_reload": False,
    "updated_at": None,
}

DEFAULT_LOCK_STATE = {
    "running": False,
    "owner": "",
    "started_at": None,
    "heartbeat_at": None,
    "finished_at": None,
}

scan_status = dict(DEFAULT_SCAN_STATUS)
panel_update_status = dict(DEFAULT_UPDATE_STATUS)

# ---------------------------------------------------------------------------
# DB / settings helpers
# ---------------------------------------------------------------------------


def ensure_dir(path_str):
    os.makedirs(path_str, exist_ok=True)


def is_db_lock_error(exc):
    text = str(exc).lower()
    return "database is locked" in text or "database is busy" in text


def retry_sleep(attempt):
    base = max(0.02, DB_WRITE_RETRY_SLEEP_SECONDS)
    time.sleep(base * (attempt + 1))


def get_db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=max(1.0, DB_CONNECT_TIMEOUT_SECONDS))
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {max(1000, DB_BUSY_TIMEOUT_MS)}")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    ensure_dir(os.path.dirname(DB_PATH) or ".")
    ensure_dir(DATA_ROOT_DEFAULT)

    with get_db_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin', 'user')),
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS runtime_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS albums (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year TEXT NOT NULL,
                name TEXT NOT NULL,
                rel_path TEXT NOT NULL UNIQUE,
                dir_mtime REAL NOT NULL DEFAULT 0,
                photo_count INTEGER NOT NULL DEFAULT 0,
                published INTEGER NOT NULL DEFAULT 0,
                cover_photo_id INTEGER,
                last_indexed_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS photos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                album_id INTEGER NOT NULL,
                rel_path TEXT NOT NULL UNIQUE,
                filename TEXT NOT NULL,
                mtime REAL NOT NULL,
                size INTEGER NOT NULL,
                ext TEXT NOT NULL,
                cached_thumb_mtime REAL NOT NULL DEFAULT 0,
                cached_preview_mtime REAL NOT NULL DEFAULT 0,
                indexed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(album_id) REFERENCES albums(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS user_album_acl (
                user_id INTEGER NOT NULL,
                album_id INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(user_id, album_id),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(album_id) REFERENCES albums(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_albums_year ON albums(year);
            CREATE INDEX IF NOT EXISTS idx_photos_album_id ON photos(album_id);
            CREATE INDEX IF NOT EXISTS idx_acl_user ON user_album_acl(user_id);
            CREATE INDEX IF NOT EXISTS idx_acl_album ON user_album_acl(album_id);
            """
        )

    with get_db_conn() as conn:
        for key, value in DEFAULT_SETTINGS.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings(key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (key, value),
            )
        conn.execute(
            "INSERT OR IGNORE INTO runtime_state(key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (RUNTIME_SCAN_STATUS_KEY, json.dumps(DEFAULT_SCAN_STATUS, ensure_ascii=False)),
        )
        conn.execute(
            "INSERT OR IGNORE INTO runtime_state(key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (RUNTIME_SCAN_LOCK_KEY, json.dumps(DEFAULT_LOCK_STATE, ensure_ascii=False)),
        )
        conn.execute(
            "INSERT OR IGNORE INTO runtime_state(key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (RUNTIME_SCAN_CONTROL_KEY, json.dumps(DEFAULT_SCAN_CONTROL, ensure_ascii=False)),
        )
        conn.execute(
            "INSERT OR IGNORE INTO runtime_state(key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (RUNTIME_UPDATE_STATUS_KEY, json.dumps(DEFAULT_UPDATE_STATUS, ensure_ascii=False)),
        )
        conn.execute(
            "INSERT OR IGNORE INTO runtime_state(key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (RUNTIME_UPDATE_LOCK_KEY, json.dumps(DEFAULT_LOCK_STATE, ensure_ascii=False)),
        )

    ensure_default_admin()
    ensure_runtime_dirs()


def ensure_default_admin():
    with get_db_conn() as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        if count > 0:
            return

        admin_user = os.environ.get("ADMIN_USER", "admin").strip() or "admin"
        admin_pass = os.environ.get("ADMIN_PASSWORD", "admin123456")
        if admin_pass == "admin123456":
            logger.warning("Using default admin password. Please change it in admin panel.")

        conn.execute(
            """
            INSERT INTO users(username, password_hash, role, active, created_at, updated_at)
            VALUES (?, ?, 'admin', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (admin_user, generate_password_hash(admin_pass)),
        )
        logger.info("Created initial admin user: %s", admin_user)


def get_setting(key, default=None):
    for attempt in range(max(1, DB_WRITE_RETRY_ATTEMPTS)):
        try:
            with get_db_conn() as conn:
                row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default
        except sqlite3.OperationalError as exc:
            if is_db_lock_error(exc) and attempt + 1 < max(1, DB_WRITE_RETRY_ATTEMPTS):
                retry_sleep(attempt)
                continue
            raise


def upsert_setting_conn(conn, key, value):
    conn.execute(
        """
        INSERT INTO settings(key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = CURRENT_TIMESTAMP
        """,
        (key, str(value)),
    )


def set_setting(key, value):
    for attempt in range(max(1, DB_WRITE_RETRY_ATTEMPTS)):
        try:
            with get_db_conn() as conn:
                upsert_setting_conn(conn, key, value)
            break
        except sqlite3.OperationalError as exc:
            if is_db_lock_error(exc) and attempt + 1 < max(1, DB_WRITE_RETRY_ATTEMPTS):
                retry_sleep(attempt)
                continue
            raise
    invalidate_runtime_settings_cache()


def upsert_runtime_state_conn(conn, key, value):
    conn.execute(
        """
        INSERT INTO runtime_state(key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = CURRENT_TIMESTAMP
        """,
        (key, str(value)),
    )


def get_runtime_state(key, default=None):
    for attempt in range(max(1, DB_WRITE_RETRY_ATTEMPTS)):
        try:
            with get_db_conn() as conn:
                row = conn.execute("SELECT value FROM runtime_state WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default
        except sqlite3.OperationalError as exc:
            if is_db_lock_error(exc) and attempt + 1 < max(1, DB_WRITE_RETRY_ATTEMPTS):
                retry_sleep(attempt)
                continue
            raise


def set_runtime_state(key, value):
    for attempt in range(max(1, DB_WRITE_RETRY_ATTEMPTS)):
        try:
            with get_db_conn() as conn:
                upsert_runtime_state_conn(conn, key, value)
            return
        except sqlite3.OperationalError as exc:
            if is_db_lock_error(exc) and attempt + 1 < max(1, DB_WRITE_RETRY_ATTEMPTS):
                retry_sleep(attempt)
                continue
            raise


def parse_json_dict(raw, default):
    if raw is None:
        return dict(default)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return dict(default)
    if not isinstance(data, dict):
        return dict(default)

    merged = dict(default)
    merged.update(data)
    return merged


def get_json_runtime_state(key, default):
    raw = get_runtime_state(key)
    return parse_json_dict(raw, default)


def set_json_runtime_state(key, value):
    set_runtime_state(key, json.dumps(value, ensure_ascii=False))


def now_ts():
    return int(time.time())


def _acquire_runtime_lock(lock_key, stale_seconds):
    owner = f"{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex[:10]}"
    now = now_ts()
    with get_db_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT value FROM runtime_state WHERE key = ?", (lock_key,)).fetchone()
        lock_data = parse_json_dict(row["value"], DEFAULT_LOCK_STATE) if row else dict(DEFAULT_LOCK_STATE)
        running = bool(lock_data.get("running"))
        heartbeat_at = int(lock_data.get("heartbeat_at") or lock_data.get("started_at") or 0)
        stale = running and heartbeat_at > 0 and (now - heartbeat_at) > stale_seconds

        if running and not stale:
            return None

        next_lock = {
            "running": True,
            "owner": owner,
            "started_at": now,
            "heartbeat_at": now,
            "finished_at": None,
        }
        upsert_runtime_state_conn(conn, lock_key, json.dumps(next_lock, ensure_ascii=False))
        conn.commit()
    return owner


def _touch_runtime_lock(lock_key, owner):
    now = now_ts()
    with get_db_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT value FROM runtime_state WHERE key = ?", (lock_key,)).fetchone()
        lock_data = parse_json_dict(row["value"], DEFAULT_LOCK_STATE) if row else dict(DEFAULT_LOCK_STATE)
        current_owner = str(lock_data.get("owner") or "")
        if not lock_data.get("running") or (current_owner and owner and current_owner != owner):
            return False
        lock_data["heartbeat_at"] = now
        upsert_runtime_state_conn(conn, lock_key, json.dumps(lock_data, ensure_ascii=False))
        conn.commit()
    return True


def _release_runtime_lock(lock_key, owner):
    now = now_ts()
    with get_db_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT value FROM runtime_state WHERE key = ?", (lock_key,)).fetchone()
        lock_data = parse_json_dict(row["value"], DEFAULT_LOCK_STATE) if row else dict(DEFAULT_LOCK_STATE)
        current_owner = str(lock_data.get("owner") or "")
        if current_owner and owner and current_owner != owner:
            return False

        lock_data.update(
            {
                "running": False,
                "owner": "",
                "heartbeat_at": now,
                "finished_at": now,
            }
        )
        upsert_runtime_state_conn(conn, lock_key, json.dumps(lock_data, ensure_ascii=False))
        conn.commit()
    return True


def reconcile_runtime_states():
    now = now_ts()

    scan_lock = get_json_runtime_state(RUNTIME_SCAN_LOCK_KEY, DEFAULT_LOCK_STATE)
    scan_hb = int(scan_lock.get("heartbeat_at") or scan_lock.get("started_at") or 0)
    if scan_lock.get("running") and scan_hb > 0 and (now - scan_hb) > SCAN_LOCK_STALE_SECONDS:
        scan_lock.update(
            {
                "running": False,
                "owner": "",
                "heartbeat_at": now,
                "finished_at": now,
            }
        )
        set_json_runtime_state(RUNTIME_SCAN_LOCK_KEY, scan_lock)

        stale_scan = get_json_runtime_state(RUNTIME_SCAN_STATUS_KEY, DEFAULT_SCAN_STATUS)
        if stale_scan.get("running"):
            stale_scan.update(
                {
                    "running": False,
                    "finished_at": now,
                    "message": "failed",
                    "phase": "idle",
                    "paused": False,
                    "pause_requested": False,
                    "abort_requested": False,
                    "stop_reason": "stale_scan_lock_recovered",
                }
            )
            set_json_runtime_state(RUNTIME_SCAN_STATUS_KEY, stale_scan)
        set_json_runtime_state(RUNTIME_SCAN_CONTROL_KEY, dict(DEFAULT_SCAN_CONTROL))

    update_lock = get_json_runtime_state(RUNTIME_UPDATE_LOCK_KEY, DEFAULT_LOCK_STATE)
    update_hb = int(update_lock.get("heartbeat_at") or update_lock.get("started_at") or 0)
    if update_lock.get("running") and update_hb > 0 and (now - update_hb) > PANEL_UPDATE_LOCK_STALE_SECONDS:
        update_lock.update(
            {
                "running": False,
                "owner": "",
                "heartbeat_at": now,
                "finished_at": now,
            }
        )
        set_json_runtime_state(RUNTIME_UPDATE_LOCK_KEY, update_lock)

        stale_update = get_json_runtime_state(RUNTIME_UPDATE_STATUS_KEY, DEFAULT_UPDATE_STATUS)
        if stale_update.get("running"):
            logs = list(stale_update.get("logs") or [])
            logs.append("检测到旧更新任务锁超时，已自动恢复为失败状态")
            stale_update.update(
                {
                    "running": False,
                    "finished_at": now,
                    "message": "failed",
                    "stage": "failed",
                    "error": "stale_update_lock_recovered",
                    "logs": logs[-PANEL_UPDATE_LOG_LIMIT:],
                }
            )
            set_json_runtime_state(RUNTIME_UPDATE_STATUS_KEY, stale_update)


def invalidate_runtime_settings_cache():
    global runtime_settings_cache, runtime_settings_cache_expires_at
    with runtime_settings_lock:
        runtime_settings_cache = None
        runtime_settings_cache_expires_at = 0.0


def get_runtime_settings(force_refresh=False):
    global runtime_settings_cache, runtime_settings_cache_expires_at
    now = time.time()

    with runtime_settings_lock:
        if (
            not force_refresh
            and runtime_settings_cache is not None
            and now < runtime_settings_cache_expires_at
        ):
            return dict(runtime_settings_cache)

    with get_db_conn() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
    values = {r["key"]: r["value"] for r in rows}
    merged = dict(DEFAULT_SETTINGS)
    merged.update(values)

    with runtime_settings_lock:
        runtime_settings_cache = dict(merged)
        runtime_settings_cache_expires_at = time.time() + max(1.0, RUNTIME_SETTINGS_CACHE_TTL)

    return merged


def get_int_setting(settings, key, default, min_value, max_value):
    try:
        value = int(settings.get(key, default))
    except (TypeError, ValueError):
        value = default
    return max(min_value, min(max_value, value))


def parse_csv(value):
    return [p.strip() for p in str(value or "").split(",") if p.strip()]


def parse_exts(value):
    exts = set()
    for ext in parse_csv(value):
        normalized = ext.lower()
        if not normalized.startswith("."):
            normalized = f".{normalized}"
        exts.add(normalized)

    if pillow_heif is None:
        exts.discard(".heic")
        exts.discard(".heif")

    return exts


def get_cache_encode_settings(settings):
    return {
        "thumb_max_edge": get_int_setting(settings, "thumb_max_edge", THUMB_MAX_EDGE_DEFAULT, 240, 1024),
        "preview_max_edge": get_int_setting(settings, "preview_max_edge", PREVIEW_MAX_EDGE_DEFAULT, 960, 3840),
        "thumb_quality": get_int_setting(settings, "thumb_quality", THUMB_QUALITY_DEFAULT, 50, 92),
        "preview_quality": get_int_setting(settings, "preview_quality", PREVIEW_QUALITY_DEFAULT, 55, 95),
        "thumb_target_kb": get_int_setting(settings, "thumb_target_kb", THUMB_TARGET_KB_DEFAULT, 40, 600),
        "preview_target_kb": get_int_setting(
            settings, "preview_target_kb", PREVIEW_TARGET_KB_DEFAULT, 200, 6000
        ),
        "webp_method": get_int_setting(settings, "webp_method", WEBP_METHOD_DEFAULT, 0, 6),
    }


def _prepare_cache_root(cache_root):
    raw = str(cache_root or "").strip()
    if not raw:
        raise OSError("empty_cache_root")
    root = Path(raw).expanduser()

    ensure_dir(str(root))
    for sub in ("thumb", "preview", "cover"):
        ensure_dir(str(root / sub))

    probe = root / f".perm_probe_{os.getpid()}_{threading.get_ident()}_{uuid.uuid4().hex[:8]}"
    try:
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
    finally:
        try:
            if probe.exists():
                probe.unlink()
        except OSError:
            pass

    return str(root)


def _cache_root_candidates(requested_root):
    values = []
    requested = str(requested_root or "").strip()
    if requested:
        values.append(requested)

    env_fallback = os.environ.get("CACHE_ROOT_FALLBACK", "").strip()
    if env_fallback:
        values.append(env_fallback)

    values.append(os.path.join(DATA_ROOT_DEFAULT, "cache_fallback"))
    values.append(f"/tmp/photopanel-cache-{os.getuid()}")
    values.append("/tmp/photopanel-cache")

    uniq = []
    seen = set()
    for v in values:
        if not v:
            continue
        key = str(Path(v).expanduser())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(key)
    return uniq


def ensure_runtime_dirs(settings=None):
    cfg = settings or get_runtime_settings()
    requested = cfg.get("cache_root", "")
    errors = []

    for candidate in _cache_root_candidates(requested):
        try:
            prepared = _prepare_cache_root(candidate)
            cfg["cache_root"] = prepared
            if prepared != str(requested):
                logger.warning(
                    "Configured cache_root unavailable (%s). Auto fallback to %s",
                    requested,
                    prepared,
                )
                try:
                    set_setting("cache_root", prepared)
                except Exception as exc:
                    logger.warning("Persist fallback cache_root failed: %s", exc)
            return prepared
        except OSError as exc:
            errors.append(f"{candidate}: {exc}")

    detail = "; ".join(errors[-5:]) if errors else "unknown_error"
    raise PermissionError(f"no_writable_cache_root: {detail}")


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def is_api_request():
    return request.path.startswith("/api/")


def safe_next_url(url):
    if url and url.startswith("/") and not url.startswith("//"):
        return url
    return "/"


def parse_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def normalize_years_filter(raw_years):
    if isinstance(raw_years, str):
        items = raw_years.replace("，", ",").split(",")
    elif isinstance(raw_years, list):
        items = raw_years
    else:
        return None

    values = set()
    for item in items:
        year = str(item).strip()
        if year:
            values.add(year)
    return values or None


def normalize_album_rel_path(raw_path):
    rel = str(raw_path or "").strip().replace("\\", "/")
    if not rel:
        return ""

    parts = []
    for part in rel.split("/"):
        p = part.strip()
        if not p or p == ".":
            continue
        if p == "..":
            return ""
        parts.append(p)

    return "/".join(parts)


def normalize_album_filter(raw_album_paths):
    if isinstance(raw_album_paths, str):
        items = raw_album_paths.splitlines()
    elif isinstance(raw_album_paths, list):
        items = raw_album_paths
    else:
        return None

    values = set()
    for item in items:
        rel = normalize_album_rel_path(item)
        if rel:
            values.add(rel)
    return values or None


def normalize_album_folder_path(raw_folder):
    rel = str(raw_folder or "").strip().replace("\\", "/")
    if not rel:
        return ""

    parts = []
    for part in rel.split("/"):
        p = part.strip()
        if not p or p == ".":
            continue
        if p == "..":
            return None
        parts.append(p)
    return "/".join(parts)


def sql_like_prefix(value):
    # Escape SQL LIKE wildcards so folder names containing %/_ are treated literally.
    v = str(value or "")
    v = v.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"{v}%"


def _probe_dir_readable(path_obj):
    try:
        with os.scandir(path_obj) as it:
            next(it, None)
    except PermissionError as exc:
        return False, str(exc)
    except OSError as exc:
        return False, str(exc)
    return True, ""


def _probe_dir_writable(path_obj):
    probe_dir = path_obj / f".photopanel_probe_{uuid.uuid4().hex[:10]}"
    probe_file = probe_dir / "probe.txt"
    try:
        probe_dir.mkdir(parents=False, exist_ok=False)
        with open(probe_file, "w", encoding="utf-8") as f:
            f.write("ok")
        with open(probe_file, "r", encoding="utf-8") as f:
            _ = f.read(2)
    except OSError as exc:
        return False, str(exc)
    finally:
        try:
            if probe_file.exists():
                probe_file.unlink()
        except OSError:
            pass
        try:
            if probe_dir.exists():
                probe_dir.rmdir()
        except OSError:
            pass
    return True, ""


def check_photo_root_access(raw_path, must_be_absolute=True):
    raw = str(raw_path or "").strip()
    if not raw:
        return None, "photo_root_empty", ""

    path_obj = Path(raw).expanduser()
    if must_be_absolute and not path_obj.is_absolute():
        return None, "photo_root_must_be_absolute", ""
    if not path_obj.exists():
        return None, "photo_root_not_found", ""
    if not path_obj.is_dir():
        return None, "photo_root_not_directory", ""

    readable, detail = _probe_dir_readable(path_obj)
    if not readable:
        return None, "photo_root_not_readable", detail
    return path_obj, "", ""


def check_cache_root_access(raw_path, must_be_absolute=True):
    raw = str(raw_path or "").strip()
    if not raw:
        return None, "cache_root_empty", ""

    path_obj = Path(raw).expanduser()
    if must_be_absolute and not path_obj.is_absolute():
        return None, "cache_root_must_be_absolute", ""

    try:
        ensure_dir(str(path_obj))
    except OSError as exc:
        return None, "cache_root_create_failed", str(exc)

    if not path_obj.is_dir():
        return None, "cache_root_not_directory", ""

    writable, detail = _probe_dir_writable(path_obj)
    if not writable:
        return None, "cache_root_not_writable", detail
    return path_obj, "", ""


def resolve_cache_root_path(raw_path, must_be_absolute=True, allow_fallback=True):
    raw = str(raw_path or "").strip()
    if not raw:
        return None, "cache_root_empty", "", False

    requested = Path(raw).expanduser()
    if must_be_absolute and not requested.is_absolute():
        return None, "cache_root_must_be_absolute", "", False

    candidates = [str(requested)]
    if allow_fallback:
        candidates = _cache_root_candidates(str(requested))

    errors = []
    for c in candidates:
        candidate = Path(c).expanduser()
        if must_be_absolute and not candidate.is_absolute():
            continue
        try:
            prepared = _prepare_cache_root(str(candidate))
            used_fallback = str(Path(prepared)) != str(requested)
            return Path(prepared), "", "", used_fallback
        except OSError as exc:
            errors.append(f"{candidate}: {exc}")

    detail = "; ".join(errors[-5:]) if errors else "unknown_error"
    return None, "cache_root_prepare_failed", detail, False


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not g.user:
            if is_api_request():
                return jsonify({"error": "auth_required"}), 401
            return redirect(url_for("login_page", next=request.path))
        return fn(*args, **kwargs)

    return wrapper


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not g.user:
            if is_api_request():
                return jsonify({"error": "auth_required"}), 401
            return redirect(url_for("login_page", next=request.path))
        if g.user["role"] != "admin":
            if is_api_request():
                return jsonify({"error": "admin_required"}), 403
            abort(403)
        return fn(*args, **kwargs)

    return wrapper


@app.before_request
def load_user():
    g.user = None
    uid = session.get("uid")
    if not uid:
        return

    with get_db_conn() as conn:
        row = conn.execute(
            "SELECT id, username, role, active FROM users WHERE id = ?",
            (uid,),
        ).fetchone()

    if not row or not row["active"]:
        session.clear()
        return

    g.user = {
        "id": row["id"],
        "username": row["username"],
        "role": row["role"],
        "active": bool(row["active"]),
    }


# ---------------------------------------------------------------------------
# Path / cache helpers
# ---------------------------------------------------------------------------


def is_hidden_name(name):
    return name.startswith(".")


def safe_join(root, rel_path):
    root_path = Path(root).resolve()
    target = (root_path / rel_path).resolve()
    if target == root_path or root_path in target.parents:
        return target
    raise ValueError("path escapes root")


def cache_rel_without_ext(rel_path):
    return os.path.splitext(rel_path)[0] + CACHE_EXT


def cache_file_path(cache_root, variant, rel_path):
    return Path(cache_root) / variant / cache_rel_without_ext(rel_path)


def cover_file_path(cache_root, album_rel_path):
    return Path(cache_root) / "cover" / f"{album_rel_path}{CACHE_EXT}"


def is_valid_webp_cache_file(path_obj):
    try:
        st = path_obj.stat()
        if st.st_size < 96:
            return False
        with path_obj.open("rb") as f:
            header = f.read(12)
        return len(header) >= 12 and header[0:4] == b"RIFF" and header[8:12] == b"WEBP"
    except OSError:
        return False


def encode_webp_with_budget(image, target_path, quality, min_quality, target_kb, method):
    target_bytes = max(0, int(target_kb) * 1024)
    tmp_suffix = f".tmp.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}"
    tmp_path = Path(str(target_path) + tmp_suffix)
    current_quality = max(min_quality, quality)
    encoded_size = 0

    try:
        while True:
            image.save(tmp_path, "WEBP", quality=current_quality, method=method)
            encoded_size = tmp_path.stat().st_size

            if target_bytes <= 0 or encoded_size <= target_bytes or current_quality <= min_quality:
                break

            next_quality = max(min_quality, current_quality - 6)
            if next_quality == current_quality:
                break
            current_quality = next_quality

        os.replace(tmp_path, target_path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

    return encoded_size, current_quality


def convert_image_to_webp(source_path, target_path, max_edge, quality, min_quality, target_kb, method):
    target = Path(target_path)
    ensure_dir(str(target.parent))
    with Image.open(source_path) as img:
        img = ImageOps.exif_transpose(img)
        if img.mode in ("RGBA", "P", "LA"):
            img = img.convert("RGBA")
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[-1])
            img = bg
        elif img.mode != "RGB":
            img = img.convert("RGB")

        img.thumbnail((max_edge, max_edge), Image.LANCZOS)
        encode_webp_with_budget(img, target, quality, min_quality, target_kb, method)


def ensure_cached_variant(rel_path, src_mtime, variant):
    settings = get_runtime_settings()
    encode_cfg = get_cache_encode_settings(settings)
    src = safe_join(settings["photo_root"], rel_path)
    cache_path = cache_file_path(settings["cache_root"], variant, rel_path)

    if not src.exists():
        raise FileNotFoundError(str(src))

    cache_exists = cache_path.exists()
    if cache_exists:
        try:
            cache_stat = cache_path.stat()
            if cache_stat.st_mtime >= src_mtime:
                valid_cache = is_valid_webp_cache_file(cache_path)
                if valid_cache:
                    return str(cache_path), False
                try:
                    cache_path.unlink()
                except OSError:
                    pass
        except OSError:
            pass

    if variant == "thumb":
        max_edge = encode_cfg["thumb_max_edge"]
        quality = encode_cfg["thumb_quality"]
        min_quality = max(45, quality - 18)
        target_kb = encode_cfg["thumb_target_kb"]
    else:
        max_edge = encode_cfg["preview_max_edge"]
        quality = encode_cfg["preview_quality"]
        min_quality = max(50, quality - 16)
        target_kb = encode_cfg["preview_target_kb"]

    convert_image_to_webp(
        str(src),
        cache_path,
        max_edge=max_edge,
        quality=quality,
        min_quality=min_quality,
        target_kb=target_kb,
        method=encode_cfg["webp_method"],
    )
    try:
        os.utime(cache_path, (src_mtime, src_mtime))
    except OSError:
        pass

    return str(cache_path), True


def update_photo_cache_marker(photo_id, src_mtime, variant, conn=None):
    col = "cached_thumb_mtime" if variant == "thumb" else "cached_preview_mtime"
    query = f"UPDATE photos SET {col} = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?"

    if conn is None:
        for attempt in range(max(1, DB_WRITE_RETRY_ATTEMPTS)):
            try:
                with get_db_conn() as local_conn:
                    local_conn.execute(query, (src_mtime, photo_id))
                return
            except sqlite3.OperationalError as exc:
                if is_db_lock_error(exc) and attempt + 1 < max(1, DB_WRITE_RETRY_ATTEMPTS):
                    retry_sleep(attempt)
                    continue
                raise
    else:
        conn.execute(query, (src_mtime, photo_id))


def accel_or_send(local_path, internal_rel_path, mimetype, as_attachment=False, download_name=None):
    if ENABLE_X_ACCEL:
        resp = Response(status=200)
        resp.headers["Content-Type"] = mimetype
        if as_attachment and download_name:
            resp.headers["Content-Disposition"] = f'attachment; filename="{download_name}"'
        resp.headers["X-Accel-Redirect"] = internal_rel_path
        return resp

    return send_file(
        local_path,
        mimetype=mimetype,
        as_attachment=as_attachment,
        download_name=download_name,
        max_age=86400 * 7,
    )


# ---------------------------------------------------------------------------
# Access helpers
# ---------------------------------------------------------------------------


def get_album_for_user(album_id):
    with get_db_conn() as conn:
        if g.user["role"] == "admin":
            row = conn.execute("SELECT * FROM albums WHERE id = ?", (album_id,)).fetchone()
        else:
            row = conn.execute(
                """
                SELECT a.*
                FROM albums a
                JOIN user_album_acl acl ON acl.album_id = a.id
                WHERE a.id = ? AND a.published = 1 AND acl.user_id = ?
                """,
                (album_id, g.user["id"]),
            ).fetchone()
    return row


def get_photo_for_user(photo_id):
    with get_db_conn() as conn:
        if g.user["role"] == "admin":
            row = conn.execute(
                """
                SELECT p.*, a.rel_path AS album_rel_path, a.published
                FROM photos p
                JOIN albums a ON a.id = p.album_id
                WHERE p.id = ?
                """,
                (photo_id,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT p.*, a.rel_path AS album_rel_path, a.published
                FROM photos p
                JOIN albums a ON a.id = p.album_id
                JOIN user_album_acl acl ON acl.album_id = a.id
                WHERE p.id = ? AND a.published = 1 AND acl.user_id = ?
                """,
                (photo_id, g.user["id"]),
            ).fetchone()

    return row


# ---------------------------------------------------------------------------
# Scan engine (incremental + throttled)
# ---------------------------------------------------------------------------


def set_scan_status(**kwargs):
    with scan_status_lock:
        try:
            current = get_json_runtime_state(RUNTIME_SCAN_STATUS_KEY, DEFAULT_SCAN_STATUS)
        except sqlite3.OperationalError as exc:
            if is_db_lock_error(exc):
                logger.warning("set_scan_status read locked, fallback to in-memory status")
                current = dict(scan_status) if scan_status else dict(DEFAULT_SCAN_STATUS)
            else:
                raise
        current.update(kwargs)
        scan_status.clear()
        scan_status.update(current)
        try:
            set_json_runtime_state(RUNTIME_SCAN_STATUS_KEY, current)
        except sqlite3.OperationalError as exc:
            if is_db_lock_error(exc):
                logger.warning("set_scan_status persist locked, keep in-memory status")
            else:
                raise


def snapshot_scan_status():
    with scan_status_lock:
        try:
            current = get_json_runtime_state(RUNTIME_SCAN_STATUS_KEY, DEFAULT_SCAN_STATUS)
        except sqlite3.OperationalError as exc:
            if is_db_lock_error(exc):
                logger.warning("snapshot_scan_status read locked, fallback to in-memory status")
                return dict(scan_status) if scan_status else dict(DEFAULT_SCAN_STATUS)
            raise
        scan_status.clear()
        scan_status.update(current)
        return dict(scan_status)


def update_scan_control_state(**kwargs):
    with scan_status_lock:
        current = get_json_runtime_state(RUNTIME_SCAN_CONTROL_KEY, DEFAULT_SCAN_CONTROL)
        current.update(kwargs)
        current["updated_at"] = now_ts()
        set_json_runtime_state(RUNTIME_SCAN_CONTROL_KEY, current)
        return dict(current)


def snapshot_scan_control_state():
    with scan_status_lock:
        current = get_json_runtime_state(RUNTIME_SCAN_CONTROL_KEY, DEFAULT_SCAN_CONTROL)
        return dict(current)


def try_acquire_scan_lock():
    return _acquire_runtime_lock(RUNTIME_SCAN_LOCK_KEY, SCAN_LOCK_STALE_SECONDS)


def touch_scan_lock(owner):
    return _touch_runtime_lock(RUNTIME_SCAN_LOCK_KEY, owner)


def release_scan_lock(owner):
    return _release_runtime_lock(RUNTIME_SCAN_LOCK_KEY, owner)


def discover_albums(photo_root, years_filter=None, album_filter=None, exclude_dirs=None):
    root = Path(photo_root)
    if not root.is_dir():
        return []

    excludes = set(exclude_dirs or [])
    albums = []

    try:
        for year_name in sorted(os.listdir(root)):
            if is_hidden_name(year_name):
                continue
            if years_filter and year_name not in years_filter:
                continue

            year_path = root / year_name
            if not year_path.is_dir():
                continue

            for album_name in sorted(os.listdir(year_path)):
                if is_hidden_name(album_name) or album_name in excludes:
                    continue

                album_path = year_path / album_name
                if not album_path.is_dir():
                    continue

                rel = f"{year_name}/{album_name}"
                if album_filter and rel not in album_filter:
                    continue

                albums.append((year_name, album_name, rel, album_path))
    except FileNotFoundError:
        return []

    return albums


def upsert_album(conn, year, name, rel_path, dir_mtime):
    row = conn.execute("SELECT * FROM albums WHERE rel_path = ?", (rel_path,)).fetchone()
    if row:
        conn.execute(
            """
            UPDATE albums
            SET year = ?, name = ?, dir_mtime = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (year, name, dir_mtime, row["id"]),
        )
        return row["id"], bool(row["published"]), row["dir_mtime"]

    cur = conn.execute(
        """
        INSERT INTO albums(year, name, rel_path, dir_mtime, photo_count, published, created_at, updated_at)
        VALUES (?, ?, ?, ?, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (year, name, rel_path, dir_mtime),
    )
    return cur.lastrowid, False, 0


def index_single_album(
    conn,
    album_id,
    album_path,
    album_rel_path,
    allowed_exts,
    max_files_per_album,
    exclude_dirs=None,
    control_hook=None,
):
    existing_rows = conn.execute(
        "SELECT id, rel_path, mtime, size FROM photos WHERE album_id = ?",
        (album_id,),
    ).fetchall()
    existing = {r["rel_path"]: r for r in existing_rows}

    seen_paths = set()
    sorted_photo_items = []
    scanned_files = 0
    updated_files = 0

    excluded_names = {str(v).strip().lower() for v in (exclude_dirs or []) if str(v).strip()}

    for root, dirnames, filenames in os.walk(album_path, topdown=True):
        if control_hook:
            action = control_hook()
            if action == "abort":
                raise ScanAbortRequested()
        dirnames[:] = [
            d
            for d in sorted(dirnames)
            if not is_hidden_name(d) and d.strip().lower() not in excluded_names
        ]

        for name in sorted(filenames):
            if control_hook and scanned_files % 24 == 0:
                action = control_hook()
                if action == "abort":
                    raise ScanAbortRequested()
            if scanned_files >= max_files_per_album:
                break
            if is_hidden_name(name):
                continue

            ext = Path(name).suffix.lower()
            if ext not in allowed_exts:
                continue

            full = Path(root) / name
            if not full.is_file():
                continue

            try:
                rel_inside = full.relative_to(album_path).as_posix()
            except ValueError:
                continue
            rel = f"{album_rel_path}/{rel_inside}"

            try:
                stat = full.stat()
                src_mtime = stat.st_mtime
                src_size = stat.st_size
            except OSError:
                continue

            scanned_files += 1
            seen_paths.add(rel)

            old = existing.get(rel)
            photo_id = None
            changed = False

            if old:
                photo_id = old["id"]
                if old["mtime"] != src_mtime or old["size"] != src_size:
                    changed = True
                    conn.execute(
                        """
                        UPDATE photos
                        SET filename = ?, mtime = ?, size = ?, ext = ?, indexed_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (name, src_mtime, src_size, ext, photo_id),
                    )
            else:
                changed = True
                cur = conn.execute(
                    """
                    INSERT INTO photos(album_id, rel_path, filename, mtime, size, ext, indexed_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (album_id, rel, name, src_mtime, src_size, ext),
                )
                photo_id = cur.lastrowid

            if changed:
                updated_files += 1

            sorted_photo_items.append((rel_inside, photo_id, rel, src_mtime, changed))

        if scanned_files >= max_files_per_album:
            break

    deleted_files = 0
    for rel, row in existing.items():
        if rel not in seen_paths:
            conn.execute("DELETE FROM photos WHERE id = ?", (row["id"],))
            deleted_files += 1

    sorted_photo_items.sort(key=lambda x: x[0])
    cover_photo_id = sorted_photo_items[0][1] if sorted_photo_items else None
    photo_count = len(sorted_photo_items)

    conn.execute(
        """
        UPDATE albums
        SET photo_count = ?, cover_photo_id = ?, last_indexed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (photo_count, cover_photo_id, album_id),
    )

    return {
        "photo_count": photo_count,
        "cover_photo_id": cover_photo_id,
        "scanned_files": scanned_files,
        "updated_files": updated_files,
        "deleted_files": deleted_files,
        "photo_items": sorted_photo_items,
    }


def preheat_worker(task):
    photo_id, rel_path, src_mtime = task
    generated = False
    error = ""
    attempts = max(1, PREHEAT_RETRY_ATTEMPTS)
    for attempt in range(attempts):
        try:
            _, g1 = ensure_cached_variant(rel_path, src_mtime, "thumb")
            _, g2 = ensure_cached_variant(rel_path, src_mtime, "preview")
            generated = g1 or g2
            error = ""
            break
        except FileNotFoundError as exc:
            generated = False
            error = str(exc)
            break
        except Exception as exc:
            generated = False
            error = str(exc)
            if attempt + 1 >= attempts:
                logger.warning("Preheat failed for %s (attempt %s/%s): %s", rel_path, attempt + 1, attempts, exc)
                break
            delay = max(0.05, PREHEAT_RETRY_DELAY_SECONDS) * (attempt + 1)
            time.sleep(delay)
    return photo_id, rel_path, src_mtime, generated, error


class ScanAbortRequested(Exception):
    pass


def check_scan_runtime_control(lock_owner, phase):
    control = snapshot_scan_control_state()
    if bool(control.get("abort_requested")):
        set_scan_status(
            phase=phase,
            paused=False,
            pause_requested=bool(control.get("pause_requested")),
            abort_requested=True,
            message="aborting",
        )
        return "abort"

    if not bool(control.get("pause_requested")):
        return "continue"

    set_scan_status(
        phase=phase,
        paused=True,
        pause_requested=True,
        abort_requested=False,
        message="paused",
    )
    while True:
        if lock_owner:
            touch_scan_lock(lock_owner)
        time.sleep(0.5)
        control = snapshot_scan_control_state()
        if bool(control.get("abort_requested")):
            set_scan_status(
                phase=phase,
                paused=False,
                pause_requested=bool(control.get("pause_requested")),
                abort_requested=True,
                message="aborting",
            )
            return "abort"
        if not bool(control.get("pause_requested")):
            set_scan_status(
                phase=phase,
                paused=False,
                pause_requested=False,
                abort_requested=False,
                message="running",
            )
            return "continue"


def wait_scan_stopped(timeout_seconds=2.5, interval_seconds=0.15):
    deadline = time.time() + max(0.1, float(timeout_seconds))
    while time.time() < deadline:
        status = snapshot_scan_status()
        lock_state = get_json_runtime_state(RUNTIME_SCAN_LOCK_KEY, DEFAULT_LOCK_STATE)
        if not bool(status.get("running")) and not bool(lock_state.get("running")):
            return True
        time.sleep(max(0.05, float(interval_seconds)))
    return False


def force_finish_scan_runtime(stop_reason="aborted_by_user_forced"):
    now = now_ts()
    lock_state = get_json_runtime_state(RUNTIME_SCAN_LOCK_KEY, DEFAULT_LOCK_STATE)
    owner = str(lock_state.get("owner") or "")
    if owner:
        release_scan_lock(owner)

    lock_state = get_json_runtime_state(RUNTIME_SCAN_LOCK_KEY, DEFAULT_LOCK_STATE)
    if lock_state.get("running"):
        forced_lock = dict(lock_state)
        forced_lock.update(
            {
                "running": False,
                "owner": "",
                "heartbeat_at": now,
                "finished_at": now,
            }
        )
        set_json_runtime_state(RUNTIME_SCAN_LOCK_KEY, forced_lock)

    status = snapshot_scan_status()
    summary = dict(status.get("summary") or {})
    summary["stop_reason"] = stop_reason
    summary["forced_abort_at"] = now
    set_scan_status(
        running=False,
        finished_at=now,
        message="aborted",
        phase="idle",
        paused=False,
        pause_requested=False,
        abort_requested=False,
        current_cache_file="",
        stop_reason=stop_reason,
        summary=summary,
    )
    update_scan_control_state(pause_requested=False, abort_requested=False)


def run_scan_task(trigger, years_filter=None, album_filter=None, force=False, lock_owner=None):
    settings = get_runtime_settings()
    ensure_runtime_dirs(settings)

    workers = get_int_setting(settings, "workers", 4, 1, 16)
    max_scan_albums = get_int_setting(settings, "max_scan_albums_per_run", 80, 1, 50000)
    max_files_per_album = get_int_setting(settings, "max_scan_files_per_album", 5000, 10, 1000000)
    max_new_thumbs = get_int_setting(settings, "max_new_thumbs_per_run", 3000, 1, 1000000)
    time_budget = get_int_setting(settings, "time_budget_seconds", 900, 10, 86400)
    preheat_count = get_int_setting(settings, "preheat_count", 200, 0, 10000)
    if trigger == "manual" and preheat_count <= 0:
        preheat_count = int(SETTINGS_DEFAULTS.get("preheat_count", "200"))

    allowed_exts = parse_exts(settings.get("allowed_extensions"))
    exclude_dirs = parse_csv(settings.get("exclude_dirs"))

    start_ts = int(time.time())
    start = time.time()

    set_scan_status(
        running=True,
        started_at=start_ts,
        finished_at=None,
        trigger=trigger,
        message="running",
        phase="scanning",
        paused=False,
        pause_requested=False,
        abort_requested=False,
        processed_albums=0,
        skipped_albums=0,
        visited_albums=0,
        scanned_files=0,
        updated_files=0,
        deleted_files=0,
        generated_cache_pairs=0,
        found_albums=0,
        scan_target_albums=0,
        total_cache_tasks=0,
        completed_cache_tasks=0,
        current_cache_file="",
        recent_cache_items=[],
        stop_reason=None,
        summary={},
    )

    summary = {
        "trigger": trigger,
        "limits": {
            "max_scan_albums_per_run": max_scan_albums,
            "max_scan_files_per_album": max_files_per_album,
            "max_new_thumbs_per_run": max_new_thumbs,
            "workers": workers,
            "time_budget_seconds": time_budget,
            "preheat_count": preheat_count,
        },
        "years_filter": sorted(list(years_filter or [])),
        "album_filter": sorted(list(album_filter or [])),
    }

    try:
        albums = discover_albums(
            settings["photo_root"],
            years_filter=years_filter,
            album_filter=album_filter,
            exclude_dirs=exclude_dirs,
        )
        scan_target_albums = min(len(albums), max_scan_albums)
        set_scan_status(found_albums=len(albums), scan_target_albums=scan_target_albums)

        processed_albums = 0
        skipped_albums = 0
        visited_albums = 0
        scanned_files = 0
        updated_files = 0
        deleted_files = 0
        generated_cache_pairs = 0
        completed_cache_tasks = 0
        stop_reason = "no_album_matched" if not albums else None

        cache_tasks = []
        queued_photo_ids = set()
        cache_budget = max_new_thumbs
        recent_cache_items = []
        manual_scan_mode = trigger == "manual"

        with get_db_conn() as conn:
            for year, album_name, rel_path, album_path in albums:
                action = check_scan_runtime_control(lock_owner, "scanning")
                if action == "abort":
                    stop_reason = "aborted_by_user"
                    break
                if lock_owner:
                    touch_scan_lock(lock_owner)
                if visited_albums >= max_scan_albums:
                    stop_reason = "max_scan_albums_per_run"
                    break
                if time.time() - start >= time_budget:
                    stop_reason = "time_budget_seconds"
                    break

                visited_albums += 1

                try:
                    dir_mtime = album_path.stat().st_mtime
                except OSError:
                    skipped_albums += 1
                    set_scan_status(
                        processed_albums=processed_albums,
                        skipped_albums=skipped_albums,
                        visited_albums=visited_albums,
                    )
                    continue

                album_id, is_published, old_dir_mtime = upsert_album(
                    conn, year, album_name, rel_path, dir_mtime
                )

                # For manual scans, do not short-circuit by album dir mtime.
                # Admin explicitly triggered a scan and expects immediate processing.
                if not force and old_dir_mtime == dir_mtime and not manual_scan_mode:
                    skipped_albums += 1
                    set_scan_status(
                        processed_albums=processed_albums,
                        skipped_albums=skipped_albums,
                        visited_albums=visited_albums,
                    )
                    continue

                try:
                    res = index_single_album(
                        conn,
                        album_id,
                        album_path,
                        rel_path,
                        allowed_exts,
                        max_files_per_album,
                        exclude_dirs=exclude_dirs,
                        control_hook=lambda: check_scan_runtime_control(lock_owner, "scanning"),
                    )
                except ScanAbortRequested:
                    stop_reason = "aborted_by_user"
                    break

                processed_albums += 1
                scanned_files += res["scanned_files"]
                updated_files += res["updated_files"]
                deleted_files += res["deleted_files"]

                if preheat_count > 0 and cache_budget > 0:
                    preheat_added = 0
                    for _, photo_id, photo_rel, src_mtime, changed in res["photo_items"]:
                        if preheat_added >= preheat_count or cache_budget <= 0:
                            break
                        should_preheat = False
                        if is_published and changed:
                            should_preheat = True
                        elif manual_scan_mode:
                            # Manual scans allow cache warmup even when album is not
                            # published or files are unchanged.
                            should_preheat = True

                        if not should_preheat:
                            continue
                        if photo_id in queued_photo_ids:
                            continue

                        cache_tasks.append((photo_id, photo_rel, src_mtime))
                        queued_photo_ids.add(photo_id)
                        cache_budget -= 1
                        preheat_added += 1

                conn.commit()

                set_scan_status(
                    processed_albums=processed_albums,
                    skipped_albums=skipped_albums,
                    visited_albums=visited_albums,
                    scanned_files=scanned_files,
                    updated_files=updated_files,
                    deleted_files=deleted_files,
                )

            # Preheat queue:
            # 1) default: changed photos in published albums
            # 2) manual targeted scan: selected albums can warm cache regardless
            #    of publish state and change flag
            if stop_reason != "aborted_by_user":
                queue_preview = [t[1] for t in cache_tasks[:8]]
                recent_cache_items = [f"[排队] {p}" for p in queue_preview]
                current_cache_file = queue_preview[0] if queue_preview else ""
                set_scan_status(
                    phase="transcoding",
                    total_cache_tasks=len(cache_tasks),
                    completed_cache_tasks=0,
                    current_cache_file=current_cache_file,
                    recent_cache_items=recent_cache_items,
                )
                if cache_tasks and time.time() - start < time_budget:
                    pool = ThreadPoolExecutor(max_workers=workers)
                    future_to_task = {}
                    pending_index = 0
                    pending_total = len(cache_tasks)
                    try:
                        last_status_push = 0.0
                        last_done_ts = time.time()
                        stall_notice_added = False

                        while future_to_task or pending_index < pending_total:
                            action = check_scan_runtime_control(lock_owner, "transcoding")
                            if action == "abort":
                                stop_reason = "aborted_by_user"
                                break
                            if lock_owner:
                                touch_scan_lock(lock_owner)
                            if time.time() - start >= time_budget:
                                stop_reason = stop_reason or "time_budget_seconds"
                                break

                            while pending_index < pending_total and len(future_to_task) < workers:
                                task = cache_tasks[pending_index]
                                pending_index += 1
                                fut = pool.submit(preheat_worker, task)
                                future_to_task[fut] = task

                            if not future_to_task:
                                continue

                            done, _ = wait(
                                tuple(future_to_task.keys()),
                                timeout=1.0,
                                return_when=FIRST_COMPLETED,
                            )
                            if not done:
                                now = time.time()
                                pending_task = next(iter(future_to_task.values()), None)
                                if pending_task:
                                    current_cache_file = pending_task[1]
                                if now - last_done_ts >= 20 and not stall_notice_added:
                                    recent_cache_items.append("[等待] 转码较慢，可能受 SMB 读写速度影响")
                                    stall_notice_added = True
                                if now - last_status_push >= 2.0:
                                    if len(recent_cache_items) > 80:
                                        recent_cache_items = recent_cache_items[-80:]
                                    set_scan_status(
                                        generated_cache_pairs=generated_cache_pairs,
                                        completed_cache_tasks=completed_cache_tasks,
                                        current_cache_file=current_cache_file,
                                        recent_cache_items=recent_cache_items,
                                    )
                                    last_status_push = now
                                continue

                            for fut in done:
                                task = future_to_task.pop(fut, None)
                                if task is None:
                                    continue
                                try:
                                    photo_id, rel_path, src_mtime, generated, cache_err = fut.result()
                                except Exception as exc:
                                    photo_id, rel_path, src_mtime = task
                                    generated = False
                                    cache_err = str(exc)

                                completed_cache_tasks += 1
                                last_done_ts = time.time()
                                stall_notice_added = False
                                current_cache_file = rel_path
                                if generated:
                                    generated_cache_pairs += 1
                                # whether generated or not, cache is now expected to be valid
                                marker_err = ""
                                try:
                                    update_photo_cache_marker(photo_id, src_mtime, "thumb")
                                    update_photo_cache_marker(photo_id, src_mtime, "preview")
                                except Exception as marker_exc:
                                    marker_err = f"marker_update_failed: {marker_exc}"
                                    logger.warning("Cache marker update failed for %s: %s", rel_path, marker_exc)
                                if cache_err:
                                    recent_cache_items.append(f"[失败] {rel_path} ({cache_err})")
                                elif marker_err:
                                    recent_cache_items.append(f"[警告] {rel_path} ({marker_err})")
                                elif generated:
                                    recent_cache_items.append(f"[生成] {rel_path}")
                                else:
                                    recent_cache_items.append(f"[命中] {rel_path}")
                                if len(recent_cache_items) > 80:
                                    recent_cache_items = recent_cache_items[-80:]

                                set_scan_status(
                                    generated_cache_pairs=generated_cache_pairs,
                                    completed_cache_tasks=completed_cache_tasks,
                                    current_cache_file=current_cache_file,
                                    recent_cache_items=recent_cache_items,
                                )
                    finally:
                        if stop_reason in {"time_budget_seconds", "aborted_by_user"} and future_to_task:
                            for fut in future_to_task:
                                fut.cancel()
                            pool.shutdown(wait=False, cancel_futures=True)
                        else:
                            pool.shutdown(wait=True)

        duration = round(time.time() - start, 2)
        summary.update(
            {
                "duration_seconds": duration,
                "found_albums": len(albums),
                "processed_albums": processed_albums,
                "skipped_albums": skipped_albums,
                "visited_albums": visited_albums,
                "scanned_files": scanned_files,
                "updated_files": updated_files,
                "deleted_files": deleted_files,
                "generated_cache_pairs": generated_cache_pairs,
                "scan_target_albums": scan_target_albums,
                "total_cache_tasks": len(cache_tasks),
                "completed_cache_tasks": completed_cache_tasks,
                "stop_reason": stop_reason,
            }
        )

        set_setting("last_scan_summary", json.dumps(summary, ensure_ascii=False))

        final_message = "completed"
        if stop_reason in {"max_scan_albums_per_run", "time_budget_seconds"}:
            final_message = "completed_with_limit"
        elif stop_reason == "aborted_by_user":
            final_message = "aborted"
        elif stop_reason == "no_album_matched":
            final_message = "completed_no_match"
        elif stop_reason:
            final_message = "completed_with_notice"

        set_scan_status(
            running=False,
            finished_at=int(time.time()),
            message=final_message,
            phase="idle",
            paused=False,
            pause_requested=False,
            abort_requested=False,
            processed_albums=processed_albums,
            skipped_albums=skipped_albums,
            visited_albums=visited_albums,
            scanned_files=scanned_files,
            updated_files=updated_files,
            deleted_files=deleted_files,
            generated_cache_pairs=generated_cache_pairs,
            found_albums=len(albums),
            scan_target_albums=scan_target_albums,
            total_cache_tasks=len(cache_tasks),
            completed_cache_tasks=completed_cache_tasks,
            current_cache_file="",
            recent_cache_items=recent_cache_items,
            stop_reason=stop_reason,
            summary=summary,
        )

    except Exception as exc:
        logger.exception("scan task failed: %s", exc)
        summary.update({"stop_reason": "exception", "error": str(exc)})
        set_setting("last_scan_summary", json.dumps(summary, ensure_ascii=False))
        set_scan_status(
            running=False,
            finished_at=int(time.time()),
            message="failed",
            phase="idle",
            paused=False,
            pause_requested=False,
            abort_requested=False,
            current_cache_file="",
            stop_reason="exception",
            summary=summary,
        )
    finally:
        update_scan_control_state(pause_requested=False, abort_requested=False)
        if lock_owner:
            release_scan_lock(lock_owner)


def start_scan(trigger="manual", years_filter=None, album_filter=None, force=False):
    lock_owner = try_acquire_scan_lock()
    if not lock_owner:
        return False

    try:
        update_scan_control_state(pause_requested=False, abort_requested=False)
        th = threading.Thread(
            target=run_scan_task,
            args=(trigger, years_filter, album_filter, force, lock_owner),
            daemon=True,
        )
        th.start()
        return True
    except Exception:
        release_scan_lock(lock_owner)
        raise


# ---------------------------------------------------------------------------
# Panel update runtime
# ---------------------------------------------------------------------------


def snapshot_panel_update_status():
    with update_status_lock:
        current = get_json_runtime_state(RUNTIME_UPDATE_STATUS_KEY, DEFAULT_UPDATE_STATUS)
        panel_update_status.clear()
        panel_update_status.update(current)
        return dict(panel_update_status)


def set_panel_update_status(**kwargs):
    with update_status_lock:
        current = get_json_runtime_state(RUNTIME_UPDATE_STATUS_KEY, DEFAULT_UPDATE_STATUS)
        current.update(kwargs)
        current["updated_at"] = now_ts()
        panel_update_status.clear()
        panel_update_status.update(current)
        set_json_runtime_state(RUNTIME_UPDATE_STATUS_KEY, current)


def append_panel_update_log(line):
    if line is None:
        return
    text = str(line).rstrip()
    if not text:
        return
    with update_status_lock:
        current = get_json_runtime_state(RUNTIME_UPDATE_STATUS_KEY, DEFAULT_UPDATE_STATUS)
        logs = list(current.get("logs") or [])
        logs.append(text)
        if len(logs) > PANEL_UPDATE_LOG_LIMIT:
            logs = logs[-PANEL_UPDATE_LOG_LIMIT:]
        current["logs"] = logs
        current["updated_at"] = now_ts()
        panel_update_status.clear()
        panel_update_status.update(current)
        set_json_runtime_state(RUNTIME_UPDATE_STATUS_KEY, current)


def try_acquire_panel_update_lock():
    return _acquire_runtime_lock(RUNTIME_UPDATE_LOCK_KEY, PANEL_UPDATE_LOCK_STALE_SECONDS)


def touch_panel_update_lock(owner):
    return _touch_runtime_lock(RUNTIME_UPDATE_LOCK_KEY, owner)


def release_panel_update_lock(owner):
    return _release_runtime_lock(RUNTIME_UPDATE_LOCK_KEY, owner)


def run_bash_command(command, cwd):
    proc = subprocess.Popen(
        ["/bin/bash", "-lc", command],
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    try:
        if proc.stdout:
            for line in proc.stdout:
                append_panel_update_log(line.rstrip("\n"))
        proc.wait()
    finally:
        if proc.stdout:
            proc.stdout.close()
    return proc.returncode


def read_process_cmdline(pid):
    proc_file = Path(f"/proc/{pid}/cmdline")
    if proc_file.exists():
        try:
            raw = proc_file.read_bytes()
            return raw.replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()
        except OSError:
            pass
    try:
        out = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True)
        return out.strip()
    except Exception:
        return ""


def detect_app_revision(app_dir):
    root = Path(app_dir).resolve()
    git_dir = root / ".git"
    if git_dir.exists() and shutil.which("git"):
        try:
            rev = subprocess.check_output(
                ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
                text=True,
            ).strip()
            if rev:
                return f"git:{rev}"
        except Exception:
            pass

    app_file = root / "app.py"
    try:
        st = app_file.stat()
        return f"mtime:{int(st.st_mtime)} size:{st.st_size}"
    except OSError:
        return "unknown"


def build_repo_full_sync_command(app_dir, repo_url):
    app_q = shlex.quote(str(Path(app_dir).resolve()))
    repo_q = shlex.quote(str(repo_url))
    return (
        "set -e; "
        f"mkdir -p {app_q}; "
        "tmp_dir=/tmp/photopanel-sync.$(date +%s).$$; "
        "rm -rf \"$tmp_dir\"; "
        f"git clone {repo_q} \"$tmp_dir\"; "
        "if command -v rsync >/dev/null 2>&1; then "
        f"rsync -a --delete --exclude '.venv/' --exclude '__pycache__/' --exclude '*.pyc' \"$tmp_dir/\" {app_q}/; "
        "else "
        f"cp -a \"$tmp_dir/.\" {app_q}/; "
        "fi; "
        "rm -rf \"$tmp_dir\""
    )


def build_panel_update_steps():
    app_dir = Path(os.environ.get("PANEL_APP_DIR", str(Path(__file__).resolve().parent))).resolve()
    custom_update_cmd = os.environ.get("PANEL_UPDATE_CMD", "").strip()
    repo_url = os.environ.get("PANEL_REPO_URL", PANEL_REPO_URL_DEFAULT).strip() or PANEL_REPO_URL_DEFAULT
    steps = []

    if custom_update_cmd:
        steps.append(("执行自定义更新命令", custom_update_cmd))
    else:
        if not shutil.which("git"):
            raise RuntimeError("系统缺少 git，无法执行面板更新")
        steps.append(("同步最新代码", build_repo_full_sync_command(app_dir, repo_url)))

        requirements_file = app_dir / "requirements.txt"
        custom_venv_python = os.environ.get("PANEL_VENV_PYTHON", "").strip()
        if custom_venv_python:
            venv_python = Path(custom_venv_python).expanduser()
        else:
            venv_python = app_dir / ".venv" / "bin" / "python"

        if requirements_file.exists():
            if venv_python.exists():
                steps.append(
                    (
                        "安装/更新 Python 依赖",
                        f"{shlex.quote(str(venv_python))} -m pip install -r {shlex.quote(str(requirements_file))}",
                    )
                )
            else:
                steps.append(
                    (
                        "安装/更新 Python 依赖",
                        f"python3 -m pip install -r {shlex.quote(str(requirements_file))}",
                    )
                )

    return app_dir, steps


def try_reload_panel(app_dir):
    custom_restart_cmd = os.environ.get("PANEL_RESTART_CMD", "").strip()
    if custom_restart_cmd:
        append_panel_update_log(f"$ {custom_restart_cmd}")
        code = run_bash_command(custom_restart_cmd, app_dir)
        if code == 0:
            return True, "自定义重启命令执行成功"
        return False, f"自定义重启命令失败，退出码 {code}"

    service_name = os.environ.get("PANEL_SERVICE_NAME", "photopanel").strip() or "photopanel"
    if shutil.which("systemctl"):
        restart_cmd = f"systemctl restart {shlex.quote(service_name)}"
        append_panel_update_log(f"$ {restart_cmd}")
        code = run_bash_command(restart_cmd, app_dir)
        if code == 0:
            return True, f"systemctl 已重启服务: {service_name}"
        append_panel_update_log(f"systemctl 重启失败，退出码 {code}，尝试 gunicorn HUP")

    parent_pid = os.getppid()
    parent_cmd = read_process_cmdline(parent_pid).lower()
    if "gunicorn" in parent_cmd:
        try:
            os.kill(parent_pid, signal.SIGHUP)
            return True, f"已向 gunicorn master({parent_pid}) 发送 HUP"
        except OSError as exc:
            return False, f"向 gunicorn master 发送 HUP 失败: {exc}"

    return False, "无法自动重启面板进程，请配置 PANEL_RESTART_CMD"


def run_panel_update_task(trigger, lock_owner):
    started = now_ts()
    set_panel_update_status(
        running=True,
        started_at=started,
        finished_at=None,
        message="running",
        stage="prepare",
        progress_pct=2,
        steps_total=0,
        steps_done=0,
        current_step="准备更新任务",
        logs=[],
        error="",
        needs_reload=False,
    )
    append_panel_update_log(f"开始更新任务（trigger={trigger}）")

    try:
        app_dir, steps = build_panel_update_steps()
        before_rev = detect_app_revision(app_dir)
        append_panel_update_log(f"更新前版本: {before_rev}")
        if not steps:
            raise RuntimeError("没有可执行的更新步骤，请检查仓库和更新命令配置")

        total_steps = len(steps) + 1
        set_panel_update_status(steps_total=total_steps, steps_done=0, progress_pct=5)

        done = 0
        for name, cmd in steps:
            touch_panel_update_lock(lock_owner)
            done += 1
            set_panel_update_status(
                stage="running",
                current_step=name,
                steps_done=done - 1,
                progress_pct=min(90, int(((done - 1) / total_steps) * 100)),
            )
            append_panel_update_log(f"[{done}/{total_steps}] {name}")
            append_panel_update_log(f"$ {cmd}")
            rc = run_bash_command(cmd, app_dir)
            if rc != 0:
                raise RuntimeError(f"{name}失败，退出码 {rc}")

            touch_panel_update_lock(lock_owner)
            set_panel_update_status(
                steps_done=done,
                progress_pct=min(92, int((done / total_steps) * 100)),
            )

        after_rev = detect_app_revision(app_dir)
        append_panel_update_log(f"更新后版本: {after_rev}")
        if before_rev == after_rev:
            append_panel_update_log("提示: 代码版本未变化（可能已是最新版本）")

        done += 1
        touch_panel_update_lock(lock_owner)
        set_panel_update_status(
            stage="reloading",
            current_step="重载面板进程",
            steps_done=done - 1,
            progress_pct=95,
            needs_reload=True,
        )
        append_panel_update_log(f"[{done}/{total_steps}] 重载面板进程")
        ok, msg = try_reload_panel(app_dir)
        append_panel_update_log(msg)
        if not ok:
            raise RuntimeError(msg)

        set_panel_update_status(
            running=False,
            finished_at=now_ts(),
            message="completed",
            stage="completed",
            current_step="更新完成",
            steps_done=total_steps,
            progress_pct=100,
            needs_reload=True,
        )
    except Exception as exc:
        append_panel_update_log(f"更新失败: {exc}")
        set_panel_update_status(
            running=False,
            finished_at=now_ts(),
            message="failed",
            stage="failed",
            error=str(exc),
            current_step="更新失败",
        )
    finally:
        release_panel_update_lock(lock_owner)


def start_panel_update(trigger="manual"):
    lock_owner = try_acquire_panel_update_lock()
    if not lock_owner:
        return False

    try:
        th = threading.Thread(
            target=run_panel_update_task,
            args=(trigger, lock_owner),
            daemon=True,
        )
        th.start()
        return True
    except Exception:
        release_panel_update_lock(lock_owner)
        raise


# ---------------------------------------------------------------------------
# Cover generation
# ---------------------------------------------------------------------------


def ensure_album_cover(album_row):
    settings = get_runtime_settings()
    encode_cfg = get_cache_encode_settings(settings)
    cache_root = settings["cache_root"]
    cover_path = cover_file_path(cache_root, album_row["rel_path"])

    with get_db_conn() as conn:
        photos = conn.execute(
            """
            SELECT id, rel_path, mtime
            FROM photos
            WHERE album_id = ?
            ORDER BY filename ASC
            LIMIT 4
            """,
            (album_row["id"],),
        ).fetchall()

    if not photos:
        return None

    newest_mtime = max([p["mtime"] for p in photos])
    if cover_path.exists() and cover_path.stat().st_mtime >= newest_mtime:
        return str(cover_path)

    thumbs = []
    for p in photos:
        path, _ = ensure_cached_variant(p["rel_path"], p["mtime"], "thumb")
        thumbs.append(path)
        update_photo_cache_marker(p["id"], p["mtime"], "thumb")

    ensure_dir(str(cover_path.parent))
    canvas_size = 960
    cell = canvas_size // 2

    canvas = Image.new("RGB", (canvas_size, canvas_size), (242, 242, 247))
    coords = [(0, 0), (cell, 0), (0, cell), (cell, cell)]

    for idx, thumb_file in enumerate(thumbs[:4]):
        with Image.open(thumb_file) as img:
            tile = ImageOps.fit(img.convert("RGB"), (cell, cell), Image.LANCZOS)
            canvas.paste(tile, coords[idx])

    cover_quality = max(68, encode_cfg["thumb_quality"])
    canvas.save(cover_path, "WEBP", quality=cover_quality, method=encode_cfg["webp_method"])
    try:
        os.utime(cover_path, (newest_mtime, newest_mtime))
    except OSError:
        pass

    return str(cover_path)


# ---------------------------------------------------------------------------
# Web pages
# ---------------------------------------------------------------------------


@app.route("/login")
def login_page():
    if g.user:
        return redirect("/")
    next_url = safe_next_url(request.args.get("next"))
    return render_template("login.html", next_url=next_url)


@app.route("/")
@login_required
def gallery_page():
    return render_template("index.html", user={"username": g.user["username"], "role": g.user["role"]})


@app.route("/admin")
@admin_required
def admin_page():
    return render_template("admin.html", user={"username": g.user["username"], "role": g.user["role"]})


# ---------------------------------------------------------------------------
# Auth APIs
# ---------------------------------------------------------------------------


@app.route("/api/auth/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    next_url = safe_next_url(data.get("next") or "/")

    if not username or not password:
        return jsonify({"error": "missing_credentials"}), 400

    with get_db_conn() as conn:
        row = conn.execute(
            "SELECT id, username, password_hash, role, active FROM users WHERE username = ?",
            (username,),
        ).fetchone()

    if not row or not row["active"] or not check_password_hash(row["password_hash"], password):
        return jsonify({"error": "invalid_credentials"}), 401

    session.clear()
    session["uid"] = row["id"]

    return jsonify(
        {
            "ok": True,
            "next": next_url,
            "user": {"id": row["id"], "username": row["username"], "role": row["role"]},
        }
    )


@app.route("/api/auth/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/me")
@login_required
def api_me():
    return jsonify(g.user)


# ---------------------------------------------------------------------------
# Gallery APIs (index-based)
# ---------------------------------------------------------------------------


@app.route("/api/stats")
@login_required
def api_stats():
    with get_db_conn() as conn:
        if g.user["role"] == "admin":
            row = conn.execute(
                "SELECT COUNT(*) AS c, COALESCE(SUM(size), 0) AS s FROM photos"
            ).fetchone()
            photo_count = row["c"]
            total_size = row["s"]

            album_row = conn.execute("SELECT COUNT(*) AS c FROM albums").fetchone()
            album_count = album_row["c"]
        else:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c, COALESCE(SUM(p.size), 0) AS s
                FROM photos p
                JOIN albums a ON a.id = p.album_id
                JOIN user_album_acl acl ON acl.album_id = a.id
                WHERE a.published = 1 AND acl.user_id = ?
                """,
                (g.user["id"],),
            ).fetchone()
            photo_count = row["c"]
            total_size = row["s"]

            album_row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM albums a
                JOIN user_album_acl acl ON acl.album_id = a.id
                WHERE a.published = 1 AND acl.user_id = ?
                """,
                (g.user["id"],),
            ).fetchone()
            album_count = album_row["c"]

        cached_row = conn.execute(
            "SELECT COUNT(*) AS c FROM photos WHERE cached_thumb_mtime > 0"
        ).fetchone()

    return jsonify(
        {
            "albums": album_count,
            "photos": photo_count,
            "cached_thumbs": cached_row["c"],
            "total_size_gb": round(total_size / (1024**3), 2),
        }
    )


@app.route("/api/years")
@login_required
def api_years():
    with get_db_conn() as conn:
        if g.user["role"] == "admin":
            rows = conn.execute(
                """
                SELECT year, COUNT(*) AS album_count, COALESCE(SUM(photo_count), 0) AS photo_count
                FROM albums
                GROUP BY year
                ORDER BY year DESC
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT a.year, COUNT(*) AS album_count, COALESCE(SUM(a.photo_count), 0) AS photo_count
                FROM albums a
                JOIN user_album_acl acl ON acl.album_id = a.id
                WHERE a.published = 1 AND acl.user_id = ?
                GROUP BY a.year
                ORDER BY a.year DESC
                """,
                (g.user["id"],),
            ).fetchall()

        result = []
        for r in rows:
            if g.user["role"] == "admin":
                cover = conn.execute(
                    """
                    SELECT cover_photo_id FROM albums
                    WHERE year = ? AND cover_photo_id IS NOT NULL
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (r["year"],),
                ).fetchone()
            else:
                cover = conn.execute(
                    """
                    SELECT a.cover_photo_id
                    FROM albums a
                    JOIN user_album_acl acl ON acl.album_id = a.id
                    WHERE a.year = ? AND a.published = 1 AND acl.user_id = ? AND a.cover_photo_id IS NOT NULL
                    ORDER BY a.updated_at DESC
                    LIMIT 1
                    """,
                    (r["year"], g.user["id"]),
                ).fetchone()

            result.append(
                {
                    "year": r["year"],
                    "albums": r["album_count"],
                    "photos": r["photo_count"],
                    "cover_photo_id": cover["cover_photo_id"] if cover else None,
                }
            )

    return jsonify({"years": result})


@app.route("/api/albums")
@login_required
def api_albums():
    year = (request.args.get("year") or "").strip()
    keyword = (request.args.get("q") or "").strip()

    where = []
    params = []

    if year:
        where.append("a.year = ?")
        params.append(year)

    if keyword:
        where.append("(a.name LIKE ? OR a.rel_path LIKE ?)")
        like = f"%{keyword}%"
        params.extend([like, like])

    if g.user["role"] == "admin":
        sql = """
            SELECT a.id, a.year, a.name, a.rel_path, a.photo_count, a.published,
                   a.cover_photo_id, a.last_indexed_at, a.updated_at
            FROM albums a
        """
    else:
        sql = """
            SELECT a.id, a.year, a.name, a.rel_path, a.photo_count, a.published,
                   a.cover_photo_id, a.last_indexed_at, a.updated_at
            FROM albums a
            JOIN user_album_acl acl ON acl.album_id = a.id
        """
        where.append("a.published = 1")
        where.append("acl.user_id = ?")
        params.append(g.user["id"])

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY a.year DESC, a.name ASC"

    with get_db_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    albums = []
    for r in rows:
        albums.append(
            {
                "id": r["id"],
                "year": r["year"],
                "name": r["name"],
                "rel_path": r["rel_path"],
                "photo_count": r["photo_count"],
                "published": bool(r["published"]),
                "cover_photo_id": r["cover_photo_id"],
                "last_indexed_at": r["last_indexed_at"],
                "updated_at": r["updated_at"],
            }
        )

    return jsonify({"albums": albums})


@app.route("/api/photos")
@login_required
def api_photos():
    try:
        album_id = int(request.args.get("album_id", "0"))
    except ValueError:
        return jsonify({"error": "invalid_album_id"}), 400

    try:
        page = max(1, int(request.args.get("page", "1") or 1))
    except (TypeError, ValueError):
        return jsonify({"error": "invalid_page"}), 400

    try:
        page_size = max(1, min(500, int(request.args.get("page_size", "200") or 200)))
    except (TypeError, ValueError):
        return jsonify({"error": "invalid_page_size"}), 400

    folder = normalize_album_folder_path(request.args.get("folder", ""))
    if folder is None:
        return jsonify({"error": "invalid_folder"}), 400

    album = get_album_for_user(album_id)
    if not album:
        return jsonify({"error": "album_not_accessible"}), 403

    album_rel = str(album["rel_path"] or "").strip().strip("/")
    base_prefix = f"{album_rel}/" if album_rel else ""
    if folder:
        base_prefix = f"{base_prefix}{folder}/"
    like_pattern = sql_like_prefix(base_prefix)
    start_pos = len(base_prefix) + 1

    offset = (page - 1) * page_size
    parent_folder = ""
    if folder:
        parent_folder = folder.rsplit("/", 1)[0] if "/" in folder else ""

    with get_db_conn() as conn:
        if folder:
            folder_exists = conn.execute(
                "SELECT 1 FROM photos WHERE album_id = ? AND rel_path LIKE ? ESCAPE '\\' LIMIT 1",
                (album_id, like_pattern),
            ).fetchone()
            if not folder_exists:
                return jsonify({"error": "folder_not_found"}), 404

        total_recursive = conn.execute(
            "SELECT COUNT(*) AS c FROM photos WHERE album_id = ? AND rel_path LIKE ? ESCAPE '\\'",
            (album_id, like_pattern),
        ).fetchone()["c"]

        folder_rows = conn.execute(
            """
            SELECT
              SUBSTR(SUBSTR(rel_path, ?), 1, INSTR(SUBSTR(rel_path, ?), '/') - 1) AS folder_name,
              COUNT(*) AS photo_count,
              MIN(id) AS cover_photo_id
            FROM photos
            WHERE album_id = ?
              AND rel_path LIKE ?
              ESCAPE '\\'
              AND INSTR(SUBSTR(rel_path, ?), '/') > 0
            GROUP BY folder_name
            ORDER BY folder_name ASC
            """,
            (start_pos, start_pos, album_id, like_pattern, start_pos),
        ).fetchall()

        total = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM photos
            WHERE album_id = ?
              AND rel_path LIKE ?
              ESCAPE '\\'
              AND INSTR(SUBSTR(rel_path, ?), '/') = 0
            """,
            (album_id, like_pattern, start_pos),
        ).fetchone()["c"]

        rows = conn.execute(
            """
            SELECT id, filename, size, mtime
            FROM photos
            WHERE album_id = ?
              AND rel_path LIKE ?
              ESCAPE '\\'
              AND INSTR(SUBSTR(rel_path, ?), '/') = 0
            ORDER BY filename ASC
            LIMIT ? OFFSET ?
            """,
            (album_id, like_pattern, start_pos, page_size, offset),
        ).fetchall()

    folders = []
    for r in folder_rows:
        folder_name = str(r["folder_name"] or "").strip()
        if not folder_name:
            continue
        next_path = f"{folder}/{folder_name}" if folder else folder_name
        folders.append(
            {
                "name": folder_name,
                "path": next_path,
                "photo_count": int(r["photo_count"] or 0),
                "cover_photo_id": r["cover_photo_id"],
            }
        )

    photos = [
        {
            "id": r["id"],
            "filename": r["filename"],
            "size_mb": round(r["size"] / (1024 * 1024), 2),
            "mtime": r["mtime"],
        }
        for r in rows
    ]

    return jsonify(
        {
            "album": {
                "id": album["id"],
                "year": album["year"],
                "name": album["name"],
                "photo_count": album["photo_count"],
                "rel_path": album["rel_path"],
            },
            "folder": folder,
            "parent_folder": parent_folder,
            "folders": folders,
            "folder_count": len(folders),
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_recursive": int(total_recursive or 0),
            "photos": photos,
            "has_more": offset + len(photos) < total,
        }
    )


@app.route("/api/cover/<int:album_id>")
@login_required
def api_cover(album_id):
    album = get_album_for_user(album_id)
    if not album:
        abort(403)

    local_cover = ensure_album_cover(album)
    if not local_cover:
        abort(404)

    rel_for_accel = f"{album['rel_path']}{CACHE_EXT}"
    internal = f"{ACCEL_CACHE_PREFIX}/cover/{rel_for_accel}"
    return accel_or_send(local_cover, internal, "image/webp")


@app.route("/api/thumb/<int:photo_id>")
@login_required
def api_thumb(photo_id):
    photo = get_photo_for_user(photo_id)
    if not photo:
        abort(403)

    local, generated = ensure_cached_variant(photo["rel_path"], photo["mtime"], "thumb")
    if generated or photo["cached_thumb_mtime"] < photo["mtime"]:
        update_photo_cache_marker(photo_id, photo["mtime"], "thumb")

    rel_for_accel = cache_rel_without_ext(photo["rel_path"])
    internal = f"{ACCEL_CACHE_PREFIX}/thumb/{rel_for_accel}"
    return accel_or_send(local, internal, "image/webp")


@app.route("/api/preview/<int:photo_id>")
@login_required
def api_preview(photo_id):
    photo = get_photo_for_user(photo_id)
    if not photo:
        abort(403)

    local, generated = ensure_cached_variant(photo["rel_path"], photo["mtime"], "preview")
    if generated or photo["cached_preview_mtime"] < photo["mtime"]:
        update_photo_cache_marker(photo_id, photo["mtime"], "preview")

    rel_for_accel = cache_rel_without_ext(photo["rel_path"])
    internal = f"{ACCEL_CACHE_PREFIX}/preview/{rel_for_accel}"
    return accel_or_send(local, internal, "image/webp")


@app.route("/api/download/<int:photo_id>")
@login_required
def api_download(photo_id):
    photo = get_photo_for_user(photo_id)
    if not photo:
        abort(403)

    settings = get_runtime_settings()
    src = safe_join(settings["photo_root"], photo["rel_path"])
    if not src.exists():
        abort(404)

    inline = parse_bool(request.args.get("inline", False))
    guessed_mime, _ = mimetypes.guess_type(photo["filename"])
    mime_type = guessed_mime or "application/octet-stream"
    internal = f"{ACCEL_ORIG_PREFIX}/{photo['rel_path']}"
    return accel_or_send(
        str(src),
        internal,
        mime_type if inline else "application/octet-stream",
        as_attachment=not inline,
        download_name=None if inline else photo["filename"],
    )


# ---------------------------------------------------------------------------
# Admin APIs
# ---------------------------------------------------------------------------


@app.route("/api/admin/scan-status")
@admin_required
def admin_scan_status():
    status = snapshot_scan_status()
    lock_state = get_json_runtime_state(RUNTIME_SCAN_LOCK_KEY, DEFAULT_LOCK_STATE)
    control_state = get_json_runtime_state(RUNTIME_SCAN_CONTROL_KEY, DEFAULT_SCAN_CONTROL)

    raw = get_setting("last_scan_summary", "")
    if raw:
        try:
            status["last_scan_summary"] = json.loads(raw)
        except json.JSONDecodeError:
            status["last_scan_summary"] = {}
    else:
        status["last_scan_summary"] = {}

    scan_total = int(status.get("scan_target_albums") or 0)
    scan_done = int(status.get("visited_albums") or 0)
    if scan_done <= 0:
        scan_done = int(status.get("processed_albums") or 0) + int(status.get("skipped_albums") or 0)
    scan_progress_pct = 0
    if scan_total > 0:
        scan_progress_pct = min(100, int((scan_done / scan_total) * 100))

    trans_total = int(status.get("total_cache_tasks") or 0)
    trans_done = int(status.get("completed_cache_tasks") or 0)
    transcode_progress_pct = 0
    if trans_total > 0:
        transcode_progress_pct = min(100, int((trans_done / trans_total) * 100))

    status["scan_done_albums"] = scan_done
    status["scan_progress_pct"] = scan_progress_pct
    status["transcode_done_tasks"] = trans_done
    status["transcode_progress_pct"] = transcode_progress_pct
    status["scan_lock_running"] = bool(lock_state.get("running"))
    status["pause_requested"] = bool(control_state.get("pause_requested")) or bool(
        status.get("pause_requested")
    )
    status["abort_requested"] = bool(control_state.get("abort_requested")) or bool(
        status.get("abort_requested")
    )

    if status.get("running") and status["abort_requested"]:
        abort_requested_at = int(control_state.get("updated_at") or 0)
        if abort_requested_at <= 0:
            abort_requested_at = int(status.get("started_at") or 0)
        if abort_requested_at > 0 and (now_ts() - abort_requested_at) >= max(2, SCAN_ABORT_FORCE_SECONDS):
            logger.warning("Abort request timed out, forcing scan runtime finish.")
            force_finish_scan_runtime("aborted_by_user_forced")
            status = snapshot_scan_status()
            lock_state = get_json_runtime_state(RUNTIME_SCAN_LOCK_KEY, DEFAULT_LOCK_STATE)
            control_state = get_json_runtime_state(RUNTIME_SCAN_CONTROL_KEY, DEFAULT_SCAN_CONTROL)
            status["pause_requested"] = bool(control_state.get("pause_requested")) or bool(
                status.get("pause_requested")
            )
            status["abort_requested"] = bool(control_state.get("abort_requested")) or bool(
                status.get("abort_requested")
            )

    if status.get("running"):
        if status["abort_requested"]:
            status["message"] = "aborting"
        elif status["pause_requested"] and not status.get("paused"):
            status["message"] = "pausing"

    cfg = get_runtime_settings()
    photo_root = cfg.get("photo_root", "")
    _, photo_root_err, photo_root_detail = check_photo_root_access(photo_root, must_be_absolute=False)
    status["photo_root"] = photo_root
    status["photo_root_ok"] = photo_root_err == ""
    if photo_root_err:
        status["photo_root_error"] = photo_root_err
        if photo_root_detail:
            status["photo_root_error_detail"] = photo_root_detail

    return jsonify(status)


@app.route("/api/admin/albums/scan", methods=["POST"])
@admin_required
def admin_scan_albums():
    data = request.get_json(silent=True) or {}

    years_filter = normalize_years_filter(data.get("years"))
    album_filter = normalize_album_filter(data.get("album_paths"))

    cfg = get_runtime_settings()
    _, photo_root_err, photo_root_detail = check_photo_root_access(cfg.get("photo_root"), must_be_absolute=False)
    if photo_root_err:
        payload = {"error": photo_root_err}
        if photo_root_detail:
            payload["detail"] = photo_root_detail
        return jsonify(payload), 400

    force = parse_bool(data.get("force", False))

    if not start_scan(trigger="manual", years_filter=years_filter, album_filter=album_filter, force=force):
        return jsonify({"error": "scan_running"}), 409

    return jsonify({"ok": True, "status": "started"})


@app.route("/api/admin/albums/scan/control", methods=["POST"])
@admin_required
def admin_scan_control():
    data = request.get_json(silent=True) or {}
    action = str(data.get("action", "")).strip().lower()
    if action not in {"pause", "resume", "abort"}:
        return jsonify({"error": "scan_control_invalid_action"}), 400

    status = snapshot_scan_status()
    lock_state = get_json_runtime_state(RUNTIME_SCAN_LOCK_KEY, DEFAULT_LOCK_STATE)
    running = bool(status.get("running")) or bool(lock_state.get("running"))
    if not running:
        return jsonify({"error": "scan_not_running"}), 409

    control = snapshot_scan_control_state()
    paused = bool(status.get("paused"))
    pause_requested = bool(control.get("pause_requested"))
    abort_requested = bool(control.get("abort_requested"))

    if action == "pause":
        if abort_requested:
            return jsonify({"error": "scan_abort_already_requested"}), 409
        if paused or pause_requested:
            return jsonify({"ok": True, "status": "already_pausing"})
        update_scan_control_state(pause_requested=True, abort_requested=False)
        set_scan_status(pause_requested=True, abort_requested=False, message="pausing")
        return jsonify({"ok": True, "status": "pausing"})

    if action == "resume":
        if abort_requested:
            return jsonify({"error": "scan_abort_already_requested"}), 409
        if not paused and not pause_requested:
            return jsonify({"error": "scan_not_paused"}), 409
        update_scan_control_state(pause_requested=False, abort_requested=False)
        set_scan_status(paused=False, pause_requested=False, abort_requested=False, message="running")
        return jsonify({"ok": True, "status": "resuming"})

    # action == "abort"
    if abort_requested:
        if wait_scan_stopped(timeout_seconds=0.8):
            return jsonify({"ok": True, "status": "aborted"})
        force_finish_scan_runtime("aborted_by_user_forced")
        return jsonify({"ok": True, "status": "aborted_forced"})
    update_scan_control_state(pause_requested=False, abort_requested=True)
    set_scan_status(paused=False, pause_requested=False, abort_requested=True, message="aborting")
    if wait_scan_stopped(timeout_seconds=3.0):
        return jsonify({"ok": True, "status": "aborted"})
    force_finish_scan_runtime("aborted_by_user_forced")
    return jsonify({"ok": True, "status": "aborted_forced"})


@app.route("/api/admin/panel-update/status")
@admin_required
def admin_panel_update_status():
    status = snapshot_panel_update_status()
    if not status.get("running"):
        message = str(status.get("message") or "")
        finished_at = int(status.get("finished_at") or 0)
        if (
            message in {"completed", "failed"}
            and finished_at > 0
            and now_ts() - finished_at >= PANEL_UPDATE_TERMINAL_TTL_SECONDS
        ):
            # Prevent stale terminal status from repeatedly triggering
            # frontend auto-actions after page refresh.
            set_panel_update_status(
                message="idle",
                stage="idle",
                current_step="",
                progress_pct=0,
                steps_total=0,
                steps_done=0,
                needs_reload=False,
            )
            status = snapshot_panel_update_status()
    return jsonify(status)


@app.route("/api/admin/panel-update/start", methods=["POST"])
@admin_required
def admin_panel_update_start():
    if not start_panel_update(trigger="manual"):
        return jsonify({"error": "panel_update_running"}), 409
    return jsonify({"ok": True, "status": "started"})


@app.route("/api/admin/settings", methods=["GET", "PUT"])
@admin_required
def admin_settings():
    if request.method == "GET":
        cfg = get_runtime_settings()
        raw = get_setting("last_scan_summary", "")
        last_scan_summary = {}
        if raw:
            try:
                last_scan_summary = json.loads(raw)
            except json.JSONDecodeError:
                pass

        return jsonify(
            {
                "photo_root": cfg["photo_root"],
                "cache_root": cfg["cache_root"],
                "max_scan_albums_per_run": get_int_setting(cfg, "max_scan_albums_per_run", 80, 1, 50000),
                "max_scan_files_per_album": get_int_setting(cfg, "max_scan_files_per_album", 5000, 10, 1000000),
                "max_new_thumbs_per_run": get_int_setting(cfg, "max_new_thumbs_per_run", 3000, 1, 1000000),
                "workers": get_int_setting(cfg, "workers", 4, 1, 16),
                "time_budget_seconds": get_int_setting(cfg, "time_budget_seconds", 900, 10, 86400),
                "preheat_count": get_int_setting(cfg, "preheat_count", 200, 0, 10000),
                "thumb_max_edge": get_int_setting(cfg, "thumb_max_edge", THUMB_MAX_EDGE_DEFAULT, 240, 1024),
                "preview_max_edge": get_int_setting(
                    cfg, "preview_max_edge", PREVIEW_MAX_EDGE_DEFAULT, 960, 3840
                ),
                "thumb_quality": get_int_setting(cfg, "thumb_quality", THUMB_QUALITY_DEFAULT, 50, 92),
                "preview_quality": get_int_setting(cfg, "preview_quality", PREVIEW_QUALITY_DEFAULT, 55, 95),
                "thumb_target_kb": get_int_setting(cfg, "thumb_target_kb", THUMB_TARGET_KB_DEFAULT, 40, 600),
                "preview_target_kb": get_int_setting(
                    cfg, "preview_target_kb", PREVIEW_TARGET_KB_DEFAULT, 200, 6000
                ),
                "webp_method": get_int_setting(cfg, "webp_method", WEBP_METHOD_DEFAULT, 0, 6),
                "allowed_extensions": cfg["allowed_extensions"],
                "exclude_dirs": cfg["exclude_dirs"],
                "last_scan_summary": last_scan_summary,
            }
        )

    data = request.get_json(silent=True) or {}
    warnings = []

    if "photo_root" in data:
        p, err, detail = check_photo_root_access(data["photo_root"], must_be_absolute=True)
        if err:
            payload = {"error": err}
            if detail:
                payload["detail"] = detail
            return jsonify(payload), 400
        set_setting("photo_root", str(p))

    if "cache_root" in data:
        requested_cache_root = str(data["cache_root"] or "").strip()
        p, err, detail, used_fallback = resolve_cache_root_path(
            requested_cache_root,
            must_be_absolute=True,
            allow_fallback=True,
        )
        if err:
            payload = {"error": err}
            if detail:
                payload["detail"] = detail
            return jsonify(payload), 400
        set_setting("cache_root", str(p))
        if used_fallback:
            warnings.append(f"缓存目录不可写，已自动回退到: {p}")

    int_fields = [
        ("max_scan_albums_per_run", 1, 50000),
        ("max_scan_files_per_album", 10, 1000000),
        ("max_new_thumbs_per_run", 1, 1000000),
        ("workers", 1, 16),
        ("time_budget_seconds", 10, 86400),
        ("preheat_count", 0, 10000),
        ("thumb_max_edge", 240, 1024),
        ("preview_max_edge", 960, 3840),
        ("thumb_quality", 50, 92),
        ("preview_quality", 55, 95),
        ("thumb_target_kb", 40, 600),
        ("preview_target_kb", 200, 6000),
        ("webp_method", 0, 6),
    ]

    for key, min_v, max_v in int_fields:
        if key in data:
            try:
                value = int(data[key])
            except (TypeError, ValueError):
                return jsonify({"error": f"{key}_must_be_integer"}), 400
            if value < min_v or value > max_v:
                return jsonify({"error": f"{key}_out_of_range", "min": min_v, "max": max_v}), 400
            set_setting(key, str(value))

    if "allowed_extensions" in data:
        exts = ",".join(parse_csv(data["allowed_extensions"]))
        if not exts:
            return jsonify({"error": "allowed_extensions_empty"}), 400
        set_setting("allowed_extensions", exts)

    if "exclude_dirs" in data:
        excludes = ",".join(parse_csv(data["exclude_dirs"]))
        set_setting("exclude_dirs", excludes)

    try:
        ensure_runtime_dirs()
    except OSError as exc:
        return jsonify({"error": "cache_root_prepare_failed", "detail": str(exc)}), 400

    effective = get_runtime_settings(force_refresh=True)
    return jsonify(
        {
            "ok": True,
            "warnings": warnings,
            "effective": {
                "photo_root": effective.get("photo_root", ""),
                "cache_root": effective.get("cache_root", ""),
            },
        }
    )


@app.route("/api/admin/users", methods=["GET", "POST"])
@admin_required
def admin_users():
    if request.method == "GET":
        with get_db_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, username, role, active, created_at, updated_at
                FROM users
                ORDER BY id ASC
                """
            ).fetchall()

        return jsonify(
            {
                "users": [
                    {
                        "id": r["id"],
                        "username": r["username"],
                        "role": r["role"],
                        "active": bool(r["active"]),
                        "created_at": r["created_at"],
                        "updated_at": r["updated_at"],
                    }
                    for r in rows
                ]
            }
        )

    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = (data.get("role") or "user").strip().lower()
    active = parse_bool(data.get("active", True))

    if not username:
        return jsonify({"error": "username_required"}), 400
    if len(password) < 6:
        return jsonify({"error": "password_too_short", "min": 6}), 400
    if role not in {"admin", "user"}:
        return jsonify({"error": "invalid_role"}), 400

    try:
        with get_db_conn() as conn:
            conn.execute(
                """
                INSERT INTO users(username, password_hash, role, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (username, generate_password_hash(password), role, 1 if active else 0),
            )
    except sqlite3.IntegrityError:
        return jsonify({"error": "username_exists"}), 409

    return jsonify({"ok": True})


@app.route("/api/admin/users/<int:user_id>", methods=["PUT"])
@admin_required
def admin_update_user(user_id):
    data = request.get_json(silent=True) or {}

    with get_db_conn() as conn:
        row = conn.execute(
            "SELECT id, username, role, active FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not row:
            return jsonify({"error": "user_not_found"}), 404

        username = (data.get("username") or row["username"]).strip()
        role = (data.get("role") or row["role"]).strip().lower()
        if role not in {"admin", "user"}:
            return jsonify({"error": "invalid_role"}), 400

        active = parse_bool(data["active"]) if "active" in data else bool(row["active"])

        # Prevent removing last active admin
        if row["role"] == "admin" and row["active"]:
            will_be_admin = role == "admin" and active
            if not will_be_admin:
                others = conn.execute(
                    "SELECT COUNT(*) AS c FROM users WHERE role = 'admin' AND active = 1 AND id != ?",
                    (user_id,),
                ).fetchone()["c"]
                if others == 0:
                    return jsonify({"error": "last_active_admin"}), 400

        password = data.get("password")
        update_password = isinstance(password, str) and password != ""
        if update_password and len(password) < 6:
            return jsonify({"error": "password_too_short", "min": 6}), 400

        try:
            if update_password:
                conn.execute(
                    """
                    UPDATE users
                    SET username = ?, role = ?, active = ?, password_hash = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (username, role, 1 if active else 0, generate_password_hash(password), user_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE users
                    SET username = ?, role = ?, active = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (username, role, 1 if active else 0, user_id),
                )
        except sqlite3.IntegrityError:
            return jsonify({"error": "username_exists"}), 409

    return jsonify({"ok": True})


@app.route("/api/admin/albums", methods=["GET"])
@admin_required
def admin_albums():
    year = (request.args.get("year") or "").strip()
    keyword = (request.args.get("q") or "").strip()

    where = []
    params = []

    if year:
        where.append("a.year = ?")
        params.append(year)

    if keyword:
        where.append("(a.name LIKE ? OR a.rel_path LIKE ?)")
        like = f"%{keyword}%"
        params.extend([like, like])

    sql = """
        SELECT a.id, a.year, a.name, a.rel_path, a.photo_count, a.published,
               a.cover_photo_id, a.last_indexed_at,
               GROUP_CONCAT(acl.user_id) AS acl_users
        FROM albums a
        LEFT JOIN user_album_acl acl ON acl.album_id = a.id
    """

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " GROUP BY a.id ORDER BY a.year DESC, a.name ASC"

    with get_db_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    albums = []
    for r in rows:
        acl_users = []
        if r["acl_users"]:
            acl_users = [int(x) for x in str(r["acl_users"]).split(",") if x]

        albums.append(
            {
                "id": r["id"],
                "year": r["year"],
                "name": r["name"],
                "rel_path": r["rel_path"],
                "photo_count": r["photo_count"],
                "published": bool(r["published"]),
                "cover_photo_id": r["cover_photo_id"],
                "last_indexed_at": r["last_indexed_at"],
                "acl_user_ids": acl_users,
            }
        )

    return jsonify({"albums": albums})


@app.route("/api/admin/albums/<int:album_id>/publish", methods=["POST"])
@admin_required
def admin_album_publish(album_id):
    data = request.get_json(silent=True) or {}
    published = parse_bool(data.get("published", False))

    with get_db_conn() as conn:
        row = conn.execute("SELECT id FROM albums WHERE id = ?", (album_id,)).fetchone()
        if not row:
            return jsonify({"error": "album_not_found"}), 404

        conn.execute(
            "UPDATE albums SET published = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (1 if published else 0, album_id),
        )

    # preheat on publish
    if published:
        settings = get_runtime_settings()
        preheat_count = get_int_setting(settings, "preheat_count", 200, 0, 10000)
        if preheat_count > 0:
            def _publish_preheat():
                with get_db_conn() as conn:
                    rows = conn.execute(
                        """
                        SELECT id, rel_path, mtime
                        FROM photos
                        WHERE album_id = ?
                        ORDER BY filename ASC
                        LIMIT ?
                        """,
                        (album_id, preheat_count),
                    ).fetchall()

                    tasks = [(r["id"], r["rel_path"], r["mtime"]) for r in rows]

                workers = get_int_setting(settings, "workers", 4, 1, 16)
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = [pool.submit(preheat_worker, t) for t in tasks]
                    for fut in as_completed(futs):
                        pid, src_mtime, _ = fut.result()
                        update_photo_cache_marker(pid, src_mtime, "thumb")
                        update_photo_cache_marker(pid, src_mtime, "preview")

            threading.Thread(target=_publish_preheat, daemon=True).start()

    return jsonify({"ok": True, "published": published})


@app.route("/api/admin/albums/<int:album_id>/acl", methods=["POST"])
@admin_required
def admin_album_acl(album_id):
    data = request.get_json(silent=True) or {}
    user_ids = data.get("user_ids")

    if not isinstance(user_ids, list):
        return jsonify({"error": "user_ids_must_be_array"}), 400

    normalized_ids = []
    for uid in user_ids:
        try:
            normalized_ids.append(int(uid))
        except (TypeError, ValueError):
            continue

    with get_db_conn() as conn:
        album = conn.execute("SELECT id FROM albums WHERE id = ?", (album_id,)).fetchone()
        if not album:
            return jsonify({"error": "album_not_found"}), 404

        if normalized_ids:
            placeholders = ",".join("?" for _ in normalized_ids)
            valid_users = conn.execute(
                f"SELECT id FROM users WHERE id IN ({placeholders}) AND active = 1",
                normalized_ids,
            ).fetchall()
            valid_ids = {r["id"] for r in valid_users}
        else:
            valid_ids = set()

        conn.execute("DELETE FROM user_album_acl WHERE album_id = ?", (album_id,))

        for uid in sorted(valid_ids):
            conn.execute(
                "INSERT INTO user_album_acl(user_id, album_id, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (uid, album_id),
            )

    return jsonify({"ok": True, "assigned_user_ids": sorted(list(valid_ids))})


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------


@app.errorhandler(403)
def handle_403(_e):
    if is_api_request():
        return jsonify({"error": "forbidden"}), 403
    return "Forbidden", 403


@app.errorhandler(404)
def handle_404(_e):
    if is_api_request():
        return jsonify({"error": "not_found"}), 404
    return "Not Found", 404


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


init_db()
reconcile_runtime_states()

if __name__ == "__main__":
    logger.info("PhotoNest running on %s:%s", HOST, PORT)
    logger.info("DB path: %s", DB_PATH)
    app.run(host=HOST, port=PORT)
