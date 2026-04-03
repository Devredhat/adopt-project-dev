# ============================================================
# ADOPT Infrastructure Monitor — BINARY LOG (CDC) Edition v7
# KEY FIXES in v7:
#
#  PROBLEM 1 — Supabase 109 MB full of historical replay data
#   The old position was never saved so every restart replayed
#   ALL binlogs from scratch → 66K rows, 109 MB used.
#   FIX: On startup, if Supabase has NO saved position (or
#   position <= 4), we call SHOW MASTER STATUS and immediately
#   write the CURRENT live position BEFORE starting the stream.
#   The stream then starts at NOW, not at history.
#   Also: sb_insert_event now SKIPS inserting if the event was
#   replayed (detected by comparing event time vs startup time).
#
#  PROBLEM 2 — /api/ping returning 404
#   Old code still deployed. v7 has the ping route.
#
#  PROBLEM 3 — Supabase filling up
#   FIX: Only events from the CURRENT SESSION (after startup)
#   are written to Supabase. Historical replay events are shown
#   in RAM for context but NOT stored again. Weekly prune also
#   kept as safety net.
#
#  All v6 fixes retained:
#   - Queue-based Supabase writer (no thread flood)
#   - Periodic position flush every 30s
#   - 3-layer position safety system
#   - Restore-to-DB button
#   - Weekly RAM+Supabase reset
# ─────────────────────────────────────────────────────────────

from flask import Flask, render_template_string, jsonify, request as freq, Response
import pymysql
import threading
import time
import csv
import io
import os
import json
import hashlib
import requests
from datetime import datetime

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent, RotateEvent

app = Flask(__name__)

# Timestamp when this process started — used to skip storing
# historical/replay events into Supabase (only NEW events get stored)
_STARTUP_TIME = time.time()

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
EMAIL_CONFIG = {
    "enabled":    True,
    "api_key":    "re_QyFu18A9_5mQtDHEUWSrJFHjd5ca3vkuh",
    "from_email": "onboarding@resend.dev",
    "to_email":   "mbsuthar32@gmail.com",
}

SLACK_CONFIG = {
    "enabled":            True,
    "bot_token":          "xoxb-9982558646354-10727308612931-ZJX4mbArZbiVJctSwE17dmm0",
    "channel":            "#db-alerts",
    "on_insert_customer": True,
    "on_delete":          True,
    "on_soft_delete":     True,
    "on_restore":         False,
    "on_update":          False,
}

WHATSAPP_CONFIG = {
    "enabled":            True,
    "id_instance":        "7103531926",
    "api_token":          "8577c63fab924f8a8b8eacefb44a8fae701a844527f84e22b4",
    "api_url":            "https://7103.api.greenapi.com",
    "recipient_phone":    "919510251732",
    "on_insert_customer": True,
    "on_delete":          True,
    "on_soft_delete":     True,
    "on_restore":         False,
    "on_update":          False,
}

MYSQL_SETTINGS = {
    "host":   "102.209.31.227",
    "port":   3306,
    "user":   "clusteradmin",
    "passwd": "ADOPT@2024#WIOCC@2023",
}

WATCH_DATABASES = {
    "adoptconvergebss", "adoptnotification", "adoptcommonapigateway",
    "adoptintegrationsystem", "adoptinventorymanagement", "adoptrevenuemanagement",
    "adoptsalesscrms", "adopttaskmanagement", "adoptticketmanagement", "adoptradiusbss",
}

CUSTOMER_TABLES = {"tblcustomers", "tblmcustomer", "tblmcustomers"}

EXCLUDE_TABLES = {
    "databasechangelog", "databasechangeloglock",
    "jv_commit", "jv_commit_property", "jv_global_id", "jv_snapshot", "schedulerlock",
    "tblmscheduleraudit", "tblscheduleraudit", "tblaudit",
    "tblauditlog", "tblaudit_log", "tblactivitylog", "tblactivity_log",
    "tbllog", "tblsystemlog", "tblaccesslog", "tbllogin_log",
}

# ─────────────────────────────────────────────────────────────
#  SUPABASE CONFIG
#  Env vars on Render: SUPABASE_URL  and  SUPABASE_KEY
#
#  Run ONCE in Supabase SQL editor:
#  ───────────────────────────────────────────────────────────
#  -- Events table (already created in v4)
#  create table if not exists adopt_events (
#    id          bigint primary key,
#    time        text,
#    event       text,
#    db          text,
#    tbl         text,
#    details     text,
#    meta        jsonb,
#    record      jsonb,
#    diff        jsonb,
#    created_at  timestamptz default now()
#  );
#  create index if not exists adopt_events_created_at_idx
#    on adopt_events(created_at);
#
#  -- NEW in v5: binlog position table
#  create table if not exists adopt_binlog_pos (
#    id         int primary key default 1,
#    log_file   text not null,
#    log_pos    bigint not null,
#    updated_at timestamptz default now()
#  );
#  -- seed one row so upsert always works
#  insert into adopt_binlog_pos (id, log_file, log_pos)
#  values (1, '', 0)
#  on conflict (id) do nothing;
#  ───────────────────────────────────────────────────────────
SUPABASE_URL   = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY", "")
SUPABASE_TABLE = "adopt_events"
SUPABASE_POS_TABLE = "adopt_binlog_pos"
SUPABASE_ENABLED = bool(SUPABASE_URL and SUPABASE_KEY)

def _sb_headers(prefer="return=minimal"):
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        prefer,
    }

# ── Supabase: binlog position ────────────────────────────────
def sb_save_position(log_file: str, log_pos: int):
    """Upsert the current binlog position into Supabase."""
    if not SUPABASE_ENABLED:
        return
    try:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_POS_TABLE}",
            headers=_sb_headers("resolution=merge-duplicates,return=minimal"),
            json={"id": 1, "log_file": log_file, "log_pos": log_pos,
                  "updated_at": datetime.utcnow().isoformat()},
            timeout=8,
        )
    except Exception as e:
        print(f"  ⚠  sb_save_position error: {e}")

def sb_load_position():
    """Load last saved binlog position from Supabase."""
    if not SUPABASE_ENABLED:
        return None, None
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_POS_TABLE}",
            headers=_sb_headers(),
            params={"select": "log_file,log_pos", "id": "eq.1"},
            timeout=10,
        )
        if resp.status_code == 200:
            rows = resp.json()
            if rows and rows[0].get("log_file") and rows[0].get("log_pos"):
                lf  = rows[0]["log_file"]
                lp  = int(rows[0]["log_pos"])
                if lf and lp > 4:          # 4 = binlog start sentinel, ignore
                    print(f"  ✔  Supabase binlog position: {lf} @ {lp}")
                    return lf, lp
    except Exception as e:
        print(f"  ⚠  sb_load_position error: {e}")
    return None, None

# ── Supabase: events ─────────────────────────────────────────

# ── Supabase event write queue ────────────────────────────────
# Instead of spawning a thread per event (causes thread flood on
# busy DBs), we push events into a queue and drain it in one
# dedicated worker thread with a small batch window.
import queue as _queue
_sb_event_queue: _queue.Queue = _queue.Queue(maxsize=10_000)

def _sb_event_worker():
    """Drains the Supabase insert queue — one worker, no flood."""
    while True:
        item = _sb_event_queue.get()
        ev, is_new = item  # tuple: (event_dict, is_new_event)
        if not SUPABASE_ENABLED or not is_new:
            # Skip storing replayed/historical events
            _sb_event_queue.task_done()
            continue
        payload = {
            "id":      ev["id"],
            "time":    ev["time"],
            "event":   ev["event"],
            "db":      ev["db"],
            "tbl":     ev["table"],
            "details": ev["details"],
            "meta":    ev.get("meta", {}),
            "record":  ev.get("record", {}),
            "diff":    ev.get("diff", []),
        }
        try:
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                headers=_sb_headers("resolution=merge-duplicates,return=minimal"),
                json=payload, timeout=10,
            )
            if resp.status_code not in (200, 201):
                print(f"  ⚠  Supabase insert failed [{resp.status_code}]: {resp.text[:200]}")
        except Exception as e:
            print(f"  ⚠  Supabase insert error: {e}")
        finally:
            _sb_event_queue.task_done()

def sb_insert_event(ev: dict, is_new: bool = True):
    """Enqueue event for Supabase insert (non-blocking)."""
    if not SUPABASE_ENABLED:
        return
    try:
        _sb_event_queue.put_nowait((ev, is_new))
    except _queue.Full:
        print("  ⚠  Supabase queue full — dropping oldest")
        try:
            _sb_event_queue.get_nowait()
            _sb_event_queue.put_nowait((ev, is_new))
        except Exception:
            pass

def sb_load_events(limit: int = 5000) -> list:
    """Load the most recent events from Supabase on startup."""
    if not SUPABASE_ENABLED:
        return []
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
            headers=_sb_headers(),
            params={"select": "*", "order": "id.desc", "limit": str(limit)},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"  ⚠  Supabase load failed [{resp.status_code}]: {resp.text[:200]}")
            return []
        rows = resp.json()
        events = []
        for r in rows:
            events.append({
                "id":      r["id"],
                "time":    r["time"],
                "event":   r["event"],
                "db":      r["db"],
                "table":   r["tbl"],
                "details": r["details"],
                "meta":    r.get("meta") or {},
                "record":  r.get("record") or {},
                "diff":    r.get("diff") or [],
            })
        print(f"  ✔  Loaded {len(events)} events from Supabase")
        return events
    except Exception as e:
        print(f"  ⚠  Supabase load error: {e}")
        return []

def sb_weekly_prune():
    if not SUPABASE_ENABLED:
        return
    try:
        resp = requests.delete(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
            headers=_sb_headers(),
            params={"created_at": "lt.now()-interval '7 days'"},
            timeout=15,
        )
        print(f"  🗑  Supabase weekly prune: HTTP {resp.status_code}")
    except Exception as e:
        print(f"  ⚠  Supabase prune error: {e}")

def sb_clear_all():
    if not SUPABASE_ENABLED:
        return
    try:
        resp = requests.delete(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
            headers={**_sb_headers(), "Prefer": "return=minimal"},
            params={"id": "gte.0"},
            timeout=15,
        )
        print(f"  🗑  Supabase cleared all rows: HTTP {resp.status_code}")
    except Exception as e:
        print(f"  ⚠  Supabase clear error: {e}")


# ─────────────────────────────────────────────────────────────
#  BINLOG POSITION — 3-LAYER SYSTEM
#  Layer 1: Supabase  (survives Render sleep/restart)
#  Layer 2: /tmp file (fast, wiped on sleep — secondary cache)
#  Layer 3: SHOW MASTER STATUS (true first run only)
# ─────────────────────────────────────────────────────────────
POSITION_FILE = "/tmp/adopt_binlog_position.json"


# ── Pending position for the periodic Supabase flush ─────────
_pending_pos_lock = threading.Lock()
_pending_pos = {"log_file": None, "log_pos": None}

def save_binlog_position(log_file: str, log_pos: int):
    """
    Save position to /tmp immediately (fast, local).
    Mark as pending for Supabase — the dedicated flush thread
    will actually write to Supabase every 30 s.
    DO NOT spawn a thread here — this is called on EVERY binlog
    event and spawning thousands of threads caused the freeze.
    """
    # Layer 2: /tmp — always write synchronously (trivially fast)
    try:
        with open(POSITION_FILE, "w") as f:
            json.dump({"log_file": log_file, "log_pos": log_pos}, f)
    except Exception as e:
        print(f"  ⚠  Could not save /tmp binlog position: {e}")
    # Layer 1: just mark as pending — flushed by _position_flush_worker
    with _pending_pos_lock:
        _pending_pos["log_file"] = log_file
        _pending_pos["log_pos"]  = log_pos


def _position_flush_worker():
    """
    Dedicated thread: flushes the latest binlog position to Supabase
    every 30 seconds.  Only ONE HTTP call per 30 s regardless of
    how many binlog events fire — eliminates the thread flood.
    """
    last_flushed_pos = None
    while True:
        time.sleep(30)
        with _pending_pos_lock:
            lf = _pending_pos.get("log_file")
            lp = _pending_pos.get("log_pos")
        if lf and lp and lp != last_flushed_pos:
            sb_save_position(lf, lp)
            last_flushed_pos = lp
            print(f"  💾  Position flushed to Supabase: {lf} @ {lp}")

def load_binlog_position():
    """
    Try all 3 layers in order. Returns (log_file, log_pos) or (None, None).
    IMPORTANT: (None, None) means caller MUST fetch SHOW MASTER STATUS.
    """
    # ── Layer 1: Supabase ────────────────────────────────────
    lf, lp = sb_load_position()
    if lf and lp and lp > 4:
        # Also update local cache so next call is instant
        try:
            with open(POSITION_FILE, "w") as f:
                json.dump({"log_file": lf, "log_pos": lp}, f)
        except:
            pass
        return lf, lp

    # ── Layer 2: /tmp file ───────────────────────────────────
    try:
        if os.path.exists(POSITION_FILE):
            with open(POSITION_FILE, "r") as f:
                data = json.load(f)
                lf2  = data.get("log_file")
                lp2  = data.get("log_pos")
                if lf2 and lp2 and int(lp2) > 4:
                    print(f"  ✔  /tmp binlog position: {lf2} @ {lp2}")
                    return lf2, int(lp2)
    except Exception as e:
        print(f"  ⚠  Could not load /tmp binlog position: {e}")

    return None, None


def get_current_master_position():
    """
    Fetch SHOW MASTER STATUS from MySQL.
    Returns (log_file, log_pos) or raises on failure.
    This is the LAST RESORT — only called when no saved position exists.
    """
    conn = pymysql.connect(**{**MYSQL_SETTINGS, "cursorclass": pymysql.cursors.DictCursor,
                               "connect_timeout": 10})
    cur  = conn.cursor()
    cur.execute("SHOW MASTER STATUS")
    master = cur.fetchone()
    cur.close(); conn.close()
    if not master:
        raise RuntimeError("SHOW MASTER STATUS returned empty — binlog may be disabled")
    vals = list(master.values())
    lf, lp = vals[0], int(vals[1])
    print(f"  ✔  SHOW MASTER STATUS → {lf} @ {lp}  (true live position)")
    # Immediately persist so next restart uses this
    save_binlog_position(lf, lp)
    return lf, lp


# ─────────────────────────────────────────────────────────────
#  EVENT DEDUPLICATION (content-hash based)
# ─────────────────────────────────────────────────────────────
_seen_events: dict = {}
_seen_events_lock = threading.Lock()
MAX_SEEN_EVENTS = 20_000

def _content_key(db: str, table: str, event_type: str, row: dict) -> str:
    try:
        row_hash = hashlib.md5(
            json.dumps(sorted(row.items()), default=str).encode()
        ).hexdigest()[:16]
    except Exception:
        row_hash = "?"
    minute_bucket = int(time.time()) // 60
    return f"{minute_bucket}:{db}:{table}:{event_type}:{row_hash}"

def is_duplicate_event(db: str, table: str, event_type: str, row: dict) -> bool:
    key = _content_key(db, table, event_type, row)
    with _seen_events_lock:
        if key in _seen_events:
            return True
        _seen_events[key] = int(time.time())
        if len(_seen_events) > MAX_SEEN_EVENTS:
            cutoff = int(time.time()) - 120
            expired = [k for k, t in _seen_events.items() if t < cutoff]
            for k in expired:
                del _seen_events[k]
    return False


# ─────────────────────────────────────────────────────────────
#  STATE
# ─────────────────────────────────────────────────────────────
STATE = {
    "events":          [],
    "ready":           False,
    "last_event":      None,
    "start_time":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "binlog_file":     "—",
    "binlog_position": 0,
    "mysql_user":      MYSQL_SETTINGS["user"],
    "mysql_host":      MYSQL_SETTINGS["host"],
    "stats": {
        "total_inserts":        0,
        "total_updates":        0,
        "total_deletes":        0,
        "total_soft_deletes":   0,
        "total_restores":       0,
        "fake_deletes_blocked": 0,
        "whatsapp_sent":        0,
        "whatsapp_failed":      0,
        "email_sent":           0,
        "email_failed":         0,
        "slack_sent":           0,
        "slack_failed":         0,
        "per_table":            {},
    },
    "customer_counts": {db: 0 for db in WATCH_DATABASES},
}

_col_cache  = {}
_event_id   = 0
_state_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def fmt_val(v):
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)

def format_row(row: dict) -> str:
    return " | ".join(f"{k}: {fmt_val(v)}" for k, v in row.items() if v is not None and str(v).strip())

def resolve_columns(db: str, table: str, row: dict) -> dict:
    if not any(k.startswith("UNKNOWN_COL") for k in row):
        return row
    cache_key = f"{db}.{table}"
    mapping = _col_cache.get(cache_key, {})
    return {mapping.get(k, k): v for k, v in row.items()}

def build_diff(before: dict, after: dict):
    changes = []
    all_keys = set(list(before.keys()) + list(after.keys()))
    for k in all_keys:
        bv = fmt_val(before.get(k))
        av = fmt_val(after.get(k))
        if bv != av:
            changes.append({"field": k, "before": bv, "after": av})
    return changes

def uptime_str():
    try:
        start = datetime.strptime(STATE["start_time"], "%Y-%m-%d %H:%M:%S")
        diff  = datetime.now() - start
        h, rem = divmod(int(diff.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        return f"{h}h {m}m {s}s"
    except:
        return "—"


# ── WEEKLY RESET (every Monday 00:00) ────────────────────────
def reset_weekly_state():
    while True:
        from datetime import timedelta
        now = datetime.now()
        days_ahead = (7 - now.weekday()) % 7
        if days_ahead == 0 and (now.hour > 0 or now.minute > 0):
            days_ahead = 7
        next_monday = (now + timedelta(days=days_ahead)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        time.sleep((next_monday - now).total_seconds())
        global _event_id
        with _state_lock:
            STATE["events"].clear()
            STATE["last_event"] = None
            STATE["start_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            STATE["stats"] = {
                "total_inserts": 0, "total_updates": 0,
                "total_deletes": 0, "total_soft_deletes": 0,
                "total_restores": 0, "fake_deletes_blocked": 0,
                "whatsapp_sent": 0, "whatsapp_failed": 0,
                "email_sent": 0, "email_failed": 0,
                "slack_sent": 0, "slack_failed": 0, "per_table": {},
            }
            STATE["customer_counts"] = {db: 0 for db in WATCH_DATABASES}
            _event_id = 0
        with _seen_events_lock:
            _seen_events.clear()
        sb_clear_all()
        print(f"  🔄  Weekly reset! {datetime.now().strftime('%Y-%m-%d')}")


# ─────────────────────────────────────────────────────────────
#  SLACK
# ─────────────────────────────────────────────────────────────
def send_slack(event_type, db, table, record=None, diff=None, event_id=None):
    if not SLACK_CONFIG.get("enabled"):
        return
    bot_token = SLACK_CONFIG.get("bot_token", "")
    channel   = SLACK_CONFIG.get("channel", "#db-alerts")
    if not bot_token or not channel:
        return

    COLOR_MAP = {
        "INSERT": "#22d87a", "UPDATE": "#f59e0b",
        "DELETE": "#f43f5e", "SOFT_DELETE": "#a78bfa", "RESTORE": "#38bdf8",
    }
    ICON_MAP = {
        "INSERT": "🎉", "UPDATE": "✏️", "DELETE": "🗑️",
        "SOFT_DELETE": "🚫", "RESTORE": "♻️",
    }
    color = COLOR_MAP.get(event_type, "#6366f1")
    icon  = ICON_MAP.get(event_type, "📢")

    fields = [
        {"title": "Database",  "value": db,                           "short": True},
        {"title": "Table",     "value": f"`{table}`",                  "short": True},
        {"title": "Event",     "value": event_type.replace("_", " "),  "short": True},
        {"title": "Event ID",  "value": f"#{event_id}",                "short": True},
        {"title": "Time",      "value": now_str(),                      "short": True},
    ]
    if record:
        rec_lines = []
        count = 0
        for k, v in record.items():
            v_str = fmt_val(v)
            if v_str and count < 6:
                rec_lines.append(f"*{k}:* {v_str}")
                count += 1
        if rec_lines:
            fields.append({"title": "Record Details", "value": "\n".join(rec_lines), "short": False})
    if diff:
        diff_lines = []
        for d in diff[:5]:
            diff_lines.append(f"*{d['field']}:* `{d['before'] or '—'}` → `{d['after'] or '—'}`")
        if diff_lines:
            fields.append({"title": "Changes (Before → After)", "value": "\n".join(diff_lines), "short": False})
    if event_type == "DELETE":
        fields.append({"title": "⚠️ Warning", "value": "This record was *PERMANENTLY DELETED*", "short": False})

    payload = {
        "channel": channel,
        "text":    f"{icon} *ADOPT DB ALERT* — {event_type.replace('_', ' ')} on `{db}.{table}`",
        "attachments": [{
            "color":  color, "fields": fields,
            "footer": "ADOPT Database Monitor v7",
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png",
            "ts":     int(datetime.now().timestamp()),
        }],
    }
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"},
            json=payload, timeout=15,
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("ok"):
            STATE["stats"]["slack_sent"] += 1
            print(f"  💬  Slack sent [{event_type}] -> {db}.{table}")
        else:
            STATE["stats"]["slack_failed"] += 1
            print(f"  ✗  Slack failed: {data.get('error', resp.text[:200])}")
    except Exception as e:
        STATE["stats"]["slack_failed"] += 1
        print(f"  ✗  Slack error: {e}")

def should_send_slack(event_type, table):
    cfg = SLACK_CONFIG
    if event_type == "INSERT" and table in CUSTOMER_TABLES:
        return cfg.get("on_insert_customer", True)
    if event_type == "DELETE":      return cfg.get("on_delete", True)
    if event_type == "SOFT_DELETE": return cfg.get("on_soft_delete", True)
    if event_type == "RESTORE":     return cfg.get("on_restore", False)
    if event_type == "UPDATE":      return cfg.get("on_update", False)
    return False


# ─────────────────────────────────────────────────────────────
#  WHATSAPP
# ─────────────────────────────────────────────────────────────
def send_whatsapp(event_type, db, table, record=None, diff=None, event_id=None):
    if not WHATSAPP_CONFIG.get("enabled"):
        return
    id_instance = WHATSAPP_CONFIG.get("id_instance", "")
    api_token   = WHATSAPP_CONFIG.get("api_token", "")
    api_url     = WHATSAPP_CONFIG.get("api_url", "").rstrip("/")
    recipient   = WHATSAPP_CONFIG.get("recipient_phone", "")
    if not all([id_instance, api_token, api_url, recipient]):
        return

    ICON_MAP = {"INSERT": "🎉", "UPDATE": "✏️", "DELETE": "🗑️", "SOFT_DELETE": "🚫", "RESTORE": "♻️"}
    icon = ICON_MAP.get(event_type, "📢")
    lines = [
        f"{icon} *ADOPT DB ALERT*", "━━━━━━━━━━━━━━━━━━",
        f"*Event:* {event_type.replace('_', ' ')}", f"*Database:* {db}",
        f"*Table:* {table}", f"*Time:* {now_str()}", f"*Event ID:* #{event_id}",
    ]
    if record:
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("*Record Details:*")
        count = 0
        for k, v in record.items():
            v_str = fmt_val(v)
            if v_str and count < 5:
                lines.append(f"• {k}: {v_str}")
                count += 1
    if diff:
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("*Changes:*")
        for d in diff[:5]:
            lines.append(f"• {d['field']}: {d['before'] or '-'} -> {d['after'] or '-'}")
    if event_type == "DELETE":
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("⚠️ *PERMANENTLY DELETED*")
    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("_ADOPT Database Monitor_")

    try:
        resp = requests.post(
            f"{api_url}/waInstance{id_instance}/sendMessage/{api_token}",
            json={"chatId": f"{recipient}@c.us", "message": "\n".join(lines)},
            timeout=15,
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("idMessage"):
            STATE["stats"]["whatsapp_sent"] += 1
            print(f"  📱  WhatsApp sent [{event_type}] -> {db}.{table}")
        else:
            STATE["stats"]["whatsapp_failed"] += 1
            print(f"  ✗  WhatsApp failed [{resp.status_code}]: {resp.text[:200]}")
    except Exception as e:
        STATE["stats"]["whatsapp_failed"] += 1
        print(f"  ✗  WhatsApp error: {e}")

def should_send_whatsapp(event_type, table):
    cfg = WHATSAPP_CONFIG
    if event_type == "INSERT" and table in CUSTOMER_TABLES:
        return cfg.get("on_insert_customer", True)
    if event_type == "DELETE":      return cfg.get("on_delete", True)
    if event_type == "SOFT_DELETE": return cfg.get("on_soft_delete", True)
    if event_type == "RESTORE":     return cfg.get("on_restore", False)
    if event_type == "UPDATE":      return cfg.get("on_update", False)
    return False


# ─────────────────────────────────────────────────────────────
#  EMAIL
# ─────────────────────────────────────────────────────────────
def send_email_notification(event_type, db, table, record_data, diff_data=None, meta=None):
    if not EMAIL_CONFIG.get("enabled"):
        return
    api_key    = EMAIL_CONFIG.get("api_key", "")
    from_email = EMAIL_CONFIG.get("from_email", "")
    to_email   = EMAIL_CONFIG.get("to_email", "")
    if not all([api_key, from_email, to_email]):
        return

    meta = meta or {}
    COLOR_MAP = {
        "INSERT": ("#1b5e20", "#e8f5e9", "🎉 New Record Inserted"),
        "DELETE": ("#b71c1c", "#ffebee", "🗑️ Record Permanently Deleted"),
    }
    accent, bg, subject_label = COLOR_MAP.get(event_type, ("#333", "#fff", "DB Event"))

    rows_html = ""
    if record_data:
        for key, value in record_data.items():
            if value is not None:
                rows_html += f"<tr><td><strong>{key}</strong></td><td>{fmt_val(value)}</td></tr>"

    diff_html = ""
    if diff_data:
        diff_rows = ""
        for d in diff_data:
            diff_rows += f"""<tr>
              <td style='font-weight:700;padding:8px 10px'>{d['field']}</td>
              <td style='background:#fff3f3;color:#c62828;padding:8px 10px'>{d['before'] or '—'}</td>
              <td style='background:#f3fff3;color:#2e7d32;padding:8px 10px'>{d['after'] or '—'}</td>
            </tr>"""
        if diff_rows:
            diff_html = f"""<h3 style="color:#555;margin:20px 0 8px;font-size:13px">🔄 Changes</h3>
            <table style="width:100%;border-collapse:collapse"><thead><tr>
                <th style="background:#555;color:#fff;padding:8px 10px;text-align:left;font-size:11px">Field</th>
                <th style="background:#c62828;color:#fff;padding:8px 10px;text-align:left;font-size:11px">Before</th>
                <th style="background:#2e7d32;color:#fff;padding:8px 10px;text-align:left;font-size:11px">After</th>
            </tr></thead><tbody>{diff_rows}</tbody></table>"""

    record_section = ""
    if rows_html:
        record_section = (
            "<h3 style='color:#555;margin:20px 0 8px;font-size:13px'>📋 Record Details</h3>"
            "<table style='width:100%;border-collapse:collapse'><thead><tr>"
            f"<th style='background:{accent};color:#fff;padding:8px 12px;text-align:left;font-size:11px'>Field</th>"
            f"<th style='background:{accent};color:#fff;padding:8px 12px;text-align:left;font-size:11px'>Value</th>"
            f"</tr></thead><tbody>{rows_html}</tbody></table>"
        )

    html_body = f"""<html><body style="font-family:Arial,sans-serif;background:#f0f2f5;padding:20px">
    <div style="max-width:650px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,.12)">
      <div style="background:{accent};color:#fff;padding:24px 28px;text-align:center">
        <h1 style="margin:0;font-size:22px">{subject_label}</h1>
        <p style="margin:6px 0 0;opacity:.85;font-size:12px">ADOPT Database Monitor — Automated Alert</p>
      </div>
      <div style="padding:24px 28px">
        <p style="background:{bg};border-left:4px solid {accent};padding:10px 14px;font-size:12px">
          <strong>Detected at:</strong> {now_str()} &nbsp;|&nbsp;
          <strong>Event:</strong> {event_type} &nbsp;|&nbsp;
          <strong>DB:</strong> {db} &nbsp;|&nbsp;
          <strong>Table:</strong> {table}
        </p>
        {diff_html}{record_section}
      </div>
      <div style="background:#f8f9fa;padding:14px;text-align:center;color:#aaa;font-size:11px;border-top:1px solid #eee">
        ADOPT Database Monitor v5 — Event #{meta.get('Event ID', '')}
      </div>
    </div></body></html>"""

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"from": from_email, "to": [to_email],
                  "subject": f"{subject_label} — {db} / {table}", "html": html_body},
            timeout=15,
        )
        if resp.status_code == 200:
            STATE["stats"]["email_sent"] += 1
            print(f"  ✉  Email sent [{event_type}] -> {db}.{table}")
        else:
            STATE["stats"]["email_failed"] += 1
            print(f"  ✗  Email failed [{resp.status_code}]: {resp.text[:200]}")
    except Exception as e:
        STATE["stats"]["email_failed"] += 1
        print(f"  ✗  Email error: {e}")


# ─────────────────────────────────────────────────────────────
#  DELETE DOUBLE-VERIFICATION
# ─────────────────────────────────────────────────────────────
DELETE_VERIFY_DELAY = 4

def _find_primary_key(db: str, table: str, record: dict):
    for candidate in ("id", "ID", "Id", f"{table}id", f"{table}Id",
                      f"{table}_id", "uuid", "UUID"):
        if candidate in record and record[candidate]:
            return candidate, record[candidate]
    try:
        conn = pymysql.connect(**{**MYSQL_SETTINGS, "db": db,
                                   "cursorclass": pymysql.cursors.DictCursor,
                                   "connect_timeout": 5})
        cur = conn.cursor()
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
            "ORDER BY ORDINAL_POSITION LIMIT 1",
            (db, table)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            pk_col = row["COLUMN_NAME"]
            pk_val = record.get(pk_col)
            if pk_val:
                return pk_col, pk_val
    except Exception as e:
        print(f"  ⚠  PK lookup error for {db}.{table}: {e}")
    return None, None


def verify_delete_and_push(db: str, table: str, details: str, record: dict, meta: dict, is_new: bool = True):
    pk_col, pk_val = _find_primary_key(db, table, record)
    time.sleep(DELETE_VERIFY_DELAY)

    if pk_col and pk_val:
        try:
            conn = pymysql.connect(**{**MYSQL_SETTINGS, "db": db,
                                       "cursorclass": pymysql.cursors.DictCursor,
                                       "connect_timeout": 5})
            cur = conn.cursor()
            cur.execute(f"SELECT 1 FROM `{table}` WHERE `{pk_col}` = %s LIMIT 1", (pk_val,))
            still_exists = cur.fetchone() is not None
            cur.close(); conn.close()

            if still_exists:
                print(f"  ✋  FAKE DELETE blocked — {db}.{table} [{pk_col}={pk_val}] still in DB")
                STATE["stats"]["fake_deletes_blocked"] = \
                    STATE["stats"].get("fake_deletes_blocked", 0) + 1
                return

            print(f"  ✔  CONFIRMED DELETE — {db}.{table} [{pk_col}={pk_val}]")
            meta["Verification"] = f"✅ Confirmed — {pk_col}={pk_val} not found in DB"

        except Exception as e:
            print(f"  ⚠  Delete verification failed ({e}), pushing as UNVERIFIED")
            meta["Verification"] = f"⚠️ Unverified — DB check failed: {e}"
    else:
        print(f"  ⚠  Delete verification skipped — no PK for {db}.{table}")
        meta["Verification"] = "⚠️ Unverified — no primary key found"

    push_event("DELETE", db, table, details, record=record, meta=meta, _is_new=is_new)


# ─────────────────────────────────────────────────────────────
#  PUSH EVENT
# ─────────────────────────────────────────────────────────────
def push_event(event_type, db, table, details, record=None, diff=None, meta=None, _is_new=True):
    global _event_id
    with _state_lock:
        _event_id += 1
        eid = _event_id

    meta = meta or {}
    meta["Event ID"]        = str(eid)
    meta["Binlog File"]     = STATE["binlog_file"]
    meta["Binlog Position"] = str(STATE["binlog_position"])
    meta["MySQL User"]      = STATE["mysql_user"]
    meta["MySQL Host"]      = STATE["mysql_host"]
    meta["Detected At"]     = now_str()

    ev = {
        "id": eid, "time": now_str(), "event": event_type,
        "db": db, "table": table, "details": details, "meta": meta,
        "record": {k: fmt_val(v) for k, v in (record or {}).items()},
        "diff": diff or [],
    }

    with _state_lock:
        STATE["events"].insert(0, ev)
        if len(STATE["events"]) > 5000:
            STATE["events"] = STATE["events"][:5000]

    # ── Enqueue for Supabase — only store NEW events, not replays ──
    sb_insert_event(ev, is_new=_is_new)

    tk = f"{db}.{table}"
    if tk not in STATE["stats"]["per_table"]:
        STATE["stats"]["per_table"][tk] = {"insert": 0, "update": 0, "delete": 0}
    etype_lower = event_type.lower()
    if etype_lower == "insert":
        STATE["stats"]["per_table"][tk]["insert"] += 1
    elif etype_lower in ("update", "soft_delete", "restore"):
        STATE["stats"]["per_table"][tk]["update"] += 1
    elif etype_lower == "delete":
        STATE["stats"]["per_table"][tk]["delete"] += 1

    cmap = {
        "INSERT": "total_inserts", "UPDATE": "total_updates",
        "DELETE": "total_deletes", "SOFT_DELETE": "total_soft_deletes", "RESTORE": "total_restores",
    }
    if event_type in cmap:
        STATE["stats"][cmap[event_type]] += 1

    if table in CUSTOMER_TABLES and db in STATE["customer_counts"]:
        if event_type == "INSERT":
            STATE["customer_counts"][db] += 1
        elif event_type == "DELETE":
            STATE["customer_counts"][db] = max(0, STATE["customer_counts"][db] - 1)

    STATE["last_event"] = now_str()

    is_customer  = table in CUSTOMER_TABLES
    should_email = (event_type == "INSERT" and is_customer) or event_type == "DELETE"
    if should_email and EMAIL_CONFIG.get("enabled"):
        threading.Thread(target=send_email_notification,
            args=(event_type, db, table, record or {}, diff, meta), daemon=True).start()

    if should_send_whatsapp(event_type, table) and WHATSAPP_CONFIG.get("enabled"):
        threading.Thread(target=send_whatsapp,
            args=(event_type, db, table, record, diff, eid), daemon=True).start()

    if should_send_slack(event_type, table) and SLACK_CONFIG.get("enabled"):
        threading.Thread(target=send_slack,
            args=(event_type, db, table, record, diff, eid), daemon=True).start()


# ─────────────────────────────────────────────────────────────
#  COLUMN CACHE
# ─────────────────────────────────────────────────────────────
def warm_column_cache():
    print("  Pre-loading column names (bulk query)...")
    try:
        conn = pymysql.connect(**{**MYSQL_SETTINGS, "cursorclass": pymysql.cursors.DictCursor})
        cur  = conn.cursor()
        db_list = "', '".join(WATCH_DATABASES)
        cur.execute(
            f"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION "
            f"FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA IN ('{db_list}') "
            f"ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
        )
        for r in cur.fetchall():
            key = f"{r['TABLE_SCHEMA']}.{r['TABLE_NAME']}"
            if key not in _col_cache:
                _col_cache[key] = {}
            _col_cache[key][f"UNKNOWN_COL{r['ORDINAL_POSITION']-1}"] = r["COLUMN_NAME"]
        cur.close(); conn.close()
        print(f"  ✔  Column cache: {len(_col_cache)} tables loaded")
    except Exception as e:
        print(f"  ✗  Column cache error: {e}")


# ─────────────────────────────────────────────────────────────
#  DIAGNOSTIC
# ─────────────────────────────────────────────────────────────
def check_binlog_status():
    print("\n" + "="*70)
    print("  BINLOG DIAGNOSTIC CHECK")
    print("="*70)
    try:
        conn   = pymysql.connect(**{**MYSQL_SETTINGS, "cursorclass": pymysql.cursors.DictCursor})
        cursor = conn.cursor()
        all_ok = True
        for var, expected, label in [
            ("log_bin",          "ON",   "Binlog enabled"),
            ("binlog_format",    "ROW",  "Binlog format = ROW"),
            ("binlog_row_image", "FULL", "Binlog row image = FULL"),
        ]:
            cursor.execute(f"SHOW VARIABLES LIKE '{var}'")
            row   = cursor.fetchone()
            value = row["Value"] if row else "NOT FOUND"
            ok    = value.upper() == expected.upper()
            if not ok: all_ok = False
            print(f"  {'✔' if ok else '✗'}  {label}: {value}")
        cursor.execute("SHOW MASTER STATUS")
        master = cursor.fetchone()
        if master:
            vals = list(master.values())
            STATE["binlog_file"]     = vals[0]
            STATE["binlog_position"] = vals[1]
            print(f"  ✔  Current MySQL binlog: {vals[0]}  position: {vals[1]}")
        else:
            all_ok = False
        cursor.execute("SELECT VERSION() as v, USER() as u")
        info = cursor.fetchone()
        if info:
            print(f"  ℹ  MySQL version: {info['v']}  |  Connected as: {info['u']}")
            STATE["mysql_user"] = info["u"]
        cursor.close(); conn.close()
        print(f"\n  {'✔  All checks passed!' if all_ok else '✗  Some checks failed'}")
    except Exception as e:
        print(f"  ✗  Cannot connect: {e}")
    print("="*70 + "\n")


# ─────────────────────────────────────────────────────────────
#  SUPABASE STARTUP LOAD
# ─────────────────────────────────────────────────────────────
def load_events_from_supabase():
    global _event_id
    events = sb_load_events(limit=5000)
    if not events:
        return
    with _state_lock:
        STATE["events"] = events
        if events:
            _event_id = max(e["id"] for e in events)
        for ev in events:
            tk = f"{ev['db']}.{ev['table']}"
            if tk not in STATE["stats"]["per_table"]:
                STATE["stats"]["per_table"][tk] = {"insert": 0, "update": 0, "delete": 0}
            etype = ev["event"].lower()
            if etype == "insert":
                STATE["stats"]["per_table"][tk]["insert"] += 1
                STATE["stats"]["total_inserts"] += 1
            elif etype in ("update", "soft_delete", "restore"):
                STATE["stats"]["per_table"][tk]["update"] += 1
                if etype == "update":       STATE["stats"]["total_updates"] += 1
                if etype == "soft_delete":  STATE["stats"]["total_soft_deletes"] += 1
                if etype == "restore":      STATE["stats"]["total_restores"] += 1
            elif etype == "delete":
                STATE["stats"]["per_table"][tk]["delete"] += 1
                STATE["stats"]["total_deletes"] += 1
    print(f"  ✔  RAM hydrated with {len(events)} events from Supabase (max id={_event_id})")


# ─────────────────────────────────────────────────────────────
#  BINLOG MONITOR THREAD
# ─────────────────────────────────────────────────────────────
def binlog_monitor():
    print("\n" + "="*70)
    print("  ADOPT DATABASE MONITOR v7 — BINARY LOG CDC")
    print("="*70)

    while True:
        stream = None
        try:
            # ── Resolve start position (3-layer) ─────────────
            log_file, log_pos = load_binlog_position()

            if log_file and log_pos and log_pos > 4:
                print(f"  ↺  Resuming from saved position: {log_file} @ {log_pos}")
            else:
                print("  ℹ  No saved position — fetching current live position from MySQL...")
                try:
                    log_file, log_pos = get_current_master_position()
                except Exception as e:
                    print(f"  ✗  Could not get MASTER STATUS: {e} — retrying in 10s")
                    time.sleep(10)
                    continue

            if not log_file or not log_pos or log_pos <= 4:
                print(f"  ✗  Invalid position ({log_file} @ {log_pos}) — retrying in 10s")
                time.sleep(10)
                continue

            # ── Get current LIVE position to know when we've caught up ──
            try:
                conn_ms = pymysql.connect(**{**MYSQL_SETTINGS,
                                              "cursorclass": pymysql.cursors.DictCursor,
                                              "connect_timeout": 5})
                cur_ms  = conn_ms.cursor()
                cur_ms.execute("SHOW MASTER STATUS")
                ms = cur_ms.fetchone()
                cur_ms.close(); conn_ms.close()
                live_file = list(ms.values())[0] if ms else log_file
                live_pos  = int(list(ms.values())[1]) if ms else log_pos
            except Exception:
                live_file = log_file
                live_pos  = log_pos

            # Events are NEW (store to Supabase) only if we've already
            # caught up to the live position at startup time.
            _caught_up = (log_file == live_file and log_pos >= live_pos)
            if _caught_up:
                print(f"  ✔  Starting AT live position: {log_file} @ {log_pos}")
            else:
                print(f"  ⏩  Catching up: {log_file}@{log_pos} → {live_file}@{live_pos}")
                print(f"     (replay events shown in UI but NOT stored to Supabase)")

            STATE["binlog_file"]     = log_file
            STATE["binlog_position"] = log_pos

            stream = BinLogStreamReader(
                connection_settings     = MYSQL_SETTINGS,
                ctl_connection_settings = MYSQL_SETTINGS,
                server_id               = 100,
                log_file                = log_file,
                log_pos                 = log_pos,
                only_events             = [WriteRowsEvent, UpdateRowsEvent,
                                           DeleteRowsEvent, RotateEvent],
                only_schemas            = list(WATCH_DATABASES),
                blocking                = True,
                freeze_schema           = False,
                resume_stream           = False,
            )
            print("  ✔  Connected to binlog stream\n")
            STATE["ready"] = True

            for binlog_event in stream:
                if isinstance(binlog_event, RotateEvent):
                    STATE["binlog_file"]     = binlog_event.next_binlog
                    STATE["binlog_position"] = binlog_event.position
                    save_binlog_position(STATE["binlog_file"], STATE["binlog_position"])
                    # After rotation we are always at live
                    _caught_up = True
                    print(f"  ↻  Binlog rotated → {binlog_event.next_binlog} @ {binlog_event.position}")
                    continue

                db    = binlog_event.schema
                table = binlog_event.table
                if table in EXCLUDE_TABLES or table.startswith("vw"):
                    continue

                current_log_pos = binlog_event.packet.log_pos
                STATE["binlog_position"] = current_log_pos
                save_binlog_position(STATE["binlog_file"], current_log_pos)

                # Check if we've caught up to live position
                if not _caught_up:
                    if STATE["binlog_file"] == live_file and current_log_pos >= live_pos:
                        _caught_up = True
                        print(f"  ✅  Caught up to live position! New events will now be stored.")

                is_new = _caught_up  # only store truly NEW events

                if isinstance(binlog_event, WriteRowsEvent):
                    for row in binlog_event.rows:
                        values = resolve_columns(db, table, row["values"])
                        if is_duplicate_event(db, table, "INSERT", values):
                            continue
                        detail = format_row(values)
                        if is_new:
                            print(f"  [INSERT] {db}.{table}")
                        push_event("INSERT", db, table, detail, record=values, _is_new=is_new)

                elif isinstance(binlog_event, DeleteRowsEvent):
                    for row in binlog_event.rows:
                        values = resolve_columns(db, table, row["values"])
                        if is_duplicate_event(db, table, "DELETE", values):
                            continue
                        detail = format_row(values)
                        if is_new:
                            print(f"  [DELETE?] {db}.{table} — queuing double-verification...")
                        meta_snapshot = {
                            "Binlog File":     STATE["binlog_file"],
                            "Binlog Position": str(current_log_pos),
                            "MySQL User":      STATE["mysql_user"],
                            "MySQL Host":      STATE["mysql_host"],
                        }
                        threading.Thread(
                            target=verify_delete_and_push,
                            args=(db, table, detail, dict(values), meta_snapshot, is_new),
                            daemon=True
                        ).start()

                elif isinstance(binlog_event, UpdateRowsEvent):
                    for row in binlog_event.rows:
                        before = resolve_columns(db, table, row["before_values"])
                        after  = resolve_columns(db, table, row["after_values"])
                        if is_duplicate_event(db, table, "UPDATE", after):
                            continue
                        diff = build_diff(before, after)

                        is_del_before = str(before.get("is_delete", "")).strip()
                        is_del_after  = str(after.get("is_delete",  "")).strip()

                        if is_del_before != "1" and is_del_after == "1":
                            detail = "SOFT DELETE — " + format_row(after)
                            if is_new: print(f"  [SOFT_DELETE] {db}.{table}")
                            push_event("SOFT_DELETE", db, table, detail,
                                       record=after, diff=diff, _is_new=is_new)
                        elif is_del_before == "1" and is_del_after == "0":
                            detail = "RESTORED — " + format_row(after)
                            if is_new: print(f"  [RESTORE] {db}.{table}")
                            push_event("RESTORE", db, table, detail,
                                       record=after, diff=diff, _is_new=is_new)
                        else:
                            if diff:
                                detail = "Changed: " + " | ".join(
                                    f"{d['field']}: {d['before']} -> {d['after']}" for d in diff)
                            else:
                                detail = "No column changes"
                            if is_new: print(f"  [UPDATE] {db}.{table}")
                            push_event("UPDATE", db, table, detail,
                                       record=after, diff=diff, _is_new=is_new)

        except Exception as e:
            print(f"\n  ✗  Binlog error: {e}")
            STATE["ready"] = False
        finally:
            try:
                if stream: stream.close()
            except: pass
        time.sleep(5)
        print("  ↺  Reconnecting to binlog stream...")


# ─────────────────────────────────────────────────────────────
#  API ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.route("/api/ping")
def api_ping():
    return jsonify({"status": "ok", "time": now_str(), "uptime": uptime_str(),
                    "binlog_file": STATE["binlog_file"],
                    "binlog_position": STATE["binlog_position"]})


@app.route("/api/events")
def api_events():
    limit  = int(freq.args.get("limit",  100))
    offset = int(freq.args.get("offset",   0))
    db_f   = freq.args.get("db",    "").strip().lower()
    ev_f   = freq.args.get("event", "").strip().upper()
    search = freq.args.get("search","").strip().lower()
    with _state_lock:
        evs = list(STATE["events"])
    if db_f:   evs = [e for e in evs if e["db"] == db_f]
    if ev_f and ev_f != "ALL": evs = [e for e in evs if e["event"] == ev_f]
    if search: evs = [e for e in evs if search in e["db"].lower() or
                      search in e["table"].lower() or search in e["details"].lower()]
    total = len(evs)
    paged = evs[offset: offset + limit]
    return jsonify({"events": paged, "total": total, "offset": offset, "limit": limit})


@app.route("/api/event/<int:eid>")
def api_event_detail(eid):
    with _state_lock:
        evs = list(STATE["events"])
    for e in evs:
        if e["id"] == eid:
            return jsonify(e)
    return jsonify({"error": "not found"}), 404


@app.route("/api/counts")
def api_counts():
    return jsonify(STATE["customer_counts"])


@app.route("/api/stats")
def api_stats():
    return jsonify({
        "ready":                 STATE["ready"],
        "databases":             len(WATCH_DATABASES),
        "last_event":            STATE["last_event"],
        "email_enabled":         EMAIL_CONFIG.get("enabled"),
        "email_sent":            STATE["stats"]["email_sent"],
        "email_failed":          STATE["stats"]["email_failed"],
        "whatsapp_enabled":      WHATSAPP_CONFIG.get("enabled", False),
        "whatsapp_sent":         STATE["stats"]["whatsapp_sent"],
        "whatsapp_failed":       STATE["stats"]["whatsapp_failed"],
        "slack_enabled":         SLACK_CONFIG.get("enabled", False),
        "slack_sent":            STATE["stats"]["slack_sent"],
        "slack_failed":          STATE["stats"]["slack_failed"],
        "slack_channel":         SLACK_CONFIG.get("channel", ""),
        "total_events":          len(STATE["events"]),
        "fake_deletes_blocked":  STATE["stats"].get("fake_deletes_blocked", 0),
        "binlog_file":           STATE["binlog_file"],
        "binlog_position":       STATE["binlog_position"],
        "mysql_user":            STATE["mysql_user"],
        "mysql_host":            STATE["mysql_host"],
        "uptime":                uptime_str(),
        "start_time":            STATE["start_time"],
        "supabase_enabled":      SUPABASE_ENABLED,
        **STATE["stats"],
    })


@app.route("/api/table_stats")
def api_table_stats():
    rows = []
    for key, counts in STATE["stats"]["per_table"].items():
        db, table = key.split(".", 1)
        rows.append({"db": db, "table": table, **counts,
                     "total": counts["insert"] + counts["update"] + counts["delete"]})
    rows.sort(key=lambda x: x["total"], reverse=True)
    return jsonify(rows[:50])


@app.route("/api/export/csv", methods=["GET"])
def api_export_csv():
    db_f   = freq.args.get("db",    "").strip().lower()
    ev_f   = freq.args.get("event", "").strip().upper()
    search = freq.args.get("search","").strip().lower()

    with _state_lock:
        evs = list(STATE["events"])

    if db_f:
        evs = [e for e in evs if e["db"] == db_f]
    if ev_f and ev_f not in ("", "ALL"):
        evs = [e for e in evs if e["event"] == ev_f]
    if search:
        evs = [e for e in evs if (
            search in e.get("db","").lower() or
            search in e.get("table","").lower() or
            search in e.get("details","").lower()
        )]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["ID","Time","Event","Database","Table","Details",
                     "Binlog File","Binlog Position","MySQL User","Verification"])
    for e in evs:
        writer.writerow([
            e.get("id",""), e.get("time",""), e.get("event",""),
            e.get("db",""), e.get("table",""), e.get("details",""),
            e.get("meta",{}).get("Binlog File",""),
            e.get("meta",{}).get("Binlog Position",""),
            e.get("meta",{}).get("MySQL User",""),
            e.get("meta",{}).get("Verification",""),
        ])

    csv_bytes = buf.getvalue().encode("utf-8")
    fname = "adopt_events"
    if ev_f and ev_f not in ("", "ALL"): fname += f"_{ev_f.lower()}"
    if db_f: fname += f"_{db_f}"
    fname += ".csv"

    return Response(csv_bytes, status=200, mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition":    f"attachment; filename=\"{fname}\"",
            "Content-Length":         str(len(csv_bytes)),
            "Cache-Control":          "no-store",
            "X-Content-Type-Options": "nosniff",
        })


# ─────────────────────────────────────────────────────────────
#  RESTORE-TO-DB ENDPOINT
# ─────────────────────────────────────────────────────────────
@app.route("/api/restore/<int:eid>", methods=["POST"])
def api_restore(eid):
    with _state_lock:
        evs = list(STATE["events"])
    ev = next((e for e in evs if e["id"] == eid), None)
    if not ev:
        return jsonify({"success": False, "error": f"Event #{eid} not found in log"}), 404

    if ev["event"] not in ("DELETE", "SOFT_DELETE"):
        return jsonify({"success": False,
                        "error": f"Restore only works on DELETE / SOFT_DELETE, not {ev['event']}"}), 400

    db     = ev["db"]
    table  = ev["table"]
    record = ev.get("record", {})

    if not record:
        return jsonify({"success": False, "error": "No record data stored for this event"}), 400

    if ev["event"] == "SOFT_DELETE":
        pk_col, pk_val = _find_primary_key(db, table, record)
        if not pk_col:
            return jsonify({"success": False,
                            "error": "Cannot find primary key for soft-delete restore"}), 400
        try:
            conn = pymysql.connect(**{**MYSQL_SETTINGS, "db": db,
                                       "cursorclass": pymysql.cursors.DictCursor,
                                       "connect_timeout": 5})
            cur = conn.cursor()
            cur.execute(f"UPDATE `{table}` SET `is_delete` = 0 WHERE `{pk_col}` = %s", (pk_val,))
            conn.commit()
            cur.close(); conn.close()
            msg = f"Soft-delete reversed: {table}[{pk_col}={pk_val}] is_delete → 0"
            print(f"  ♻  RESTORE (soft) {db}.{table} [{pk_col}={pk_val}]")
            return jsonify({"success": True, "message": msg})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    clean = {k: v for k, v in record.items() if v is not None and str(v).strip() != ""}
    if not clean:
        return jsonify({"success": False, "error": "Record data is empty — cannot restore"}), 400

    cols         = ", ".join(f"`{c}`" for c in clean.keys())
    placeholders = ", ".join(["%s"] * len(clean))
    values       = list(clean.values())

    try:
        conn = pymysql.connect(**{**MYSQL_SETTINGS, "db": db,
                                   "cursorclass": pymysql.cursors.DictCursor,
                                   "connect_timeout": 5})
        cur = conn.cursor()
        sql = f"INSERT IGNORE INTO `{table}` ({cols}) VALUES ({placeholders})"
        cur.execute(sql, values)
        conn.commit()
        affected = cur.rowcount
        cur.close(); conn.close()
        if affected > 0:
            msg = f"Record restored to {db}.{table} ({len(clean)} columns)"
            print(f"  ♻  RESTORED {db}.{table} — {len(clean)} cols")
        else:
            msg = f"Record already exists in {db}.{table} (INSERT IGNORE skipped — duplicate PK)"
        return jsonify({"success": True, "message": msg, "rows_affected": affected})
    except Exception as e:
        print(f"  ✗  Restore error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ─────────────────────────────────────────────────────────────
#  DASHBOARD HTML  (unchanged from v4)
# ─────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ADOPT DB Monitor v7</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Syne:wght@600;800&display=swap" rel="stylesheet">
<style>
:root{--bg:#070b14;--surf:#0f1623;--surf2:#151e2e;--border:#1a2540;--text:#c0cfe8;
      --muted:#4a5a75;--ins:#22d87a;--upd:#f59e0b;--del:#f43f5e;
      --rest:#38bdf8;--soft:#a78bfa;--acc:#6366f1;--wa:#25d366;--sl:#e01e5a}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'JetBrains Mono',monospace;min-height:100vh}
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
header{display:flex;align-items:center;justify-content:space-between;padding:14px 24px;background:var(--surf);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100}
.logo{font-family:'Syne',sans-serif;font-weight:800;font-size:18px;color:#fff}
.logo span{color:var(--acc)}
.logo sub{font-size:10px;color:var(--muted);font-weight:400;margin-left:4px}
.hbadges{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.badge{padding:3px 10px;border-radius:999px;font-size:10px;font-weight:700}
.b-live{background:#22d87a18;color:#22d87a;border:1px solid #22d87a44}
.b-bl{background:#6366f118;color:#818cf8;border:1px solid #6366f144}
.b-em{background:#f59e0b18;color:#f59e0b;border:1px solid #f59e0b44}
.b-up{background:#38bdf818;color:#38bdf8;border:1px solid #38bdf844}
.b-wa{background:#25d36618;color:#25d366;border:1px solid #25d36644}
.b-sl{background:#e01e5a18;color:#e01e5a;border:1px solid #e01e5a44}
.b-sb{background:#3ecf8e18;color:#3ecf8e;border:1px solid #3ecf8e44}
.pulse{display:inline-block;width:6px;height:6px;border-radius:50%;background:#22d87a;margin-right:4px;animation:blink 1.4s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
#loading{display:flex;flex-direction:column;align-items:center;justify-content:center;height:70vh;gap:14px;color:var(--muted)}
.spinner{width:36px;height:36px;border:3px solid var(--border);border-top-color:var(--acc);border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
#content{display:none}
.info-bar{display:flex;gap:10px;padding:12px 24px;background:var(--surf2);border-bottom:1px solid var(--border);flex-wrap:wrap;font-size:10px;color:var(--muted)}
.info-item{display:flex;gap:5px;align-items:center}
.info-item span{color:var(--text)}
.metrics{display:flex;gap:12px;padding:16px 24px;flex-wrap:wrap}
.metric{background:var(--surf);border:1px solid var(--border);border-radius:8px;padding:12px 16px;flex:1;min-width:110px;cursor:default;transition:border-color .2s;overflow:hidden}
.metric:hover{border-color:var(--acc)}
.metric-label{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.metric-val{font-weight:700;font-family:'Syne',sans-serif;font-size:clamp(13px,2.4vw,22px);word-break:break-all;line-height:1.1;}
.c-ins{color:var(--ins)}.c-upd{color:var(--upd)}.c-del{color:var(--del)}
.c-soft{color:var(--soft)}.c-rest{color:var(--rest)}.c-w{color:#fff}.c-wa{color:var(--wa)}
.c-em{color:#f59e0b}.c-sl{color:var(--sl)}
.notif-row{display:flex;gap:12px;margin:0 24px 16px;flex-wrap:wrap}
.notif-box{flex:1;min-width:260px;border-radius:10px;padding:14px 18px;display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.wa-box{background:var(--surf);border:1px solid #25d36644}
.sl-box{background:var(--surf);border:1px solid #e01e5a44}
.notif-icon{font-size:22px}
.notif-info{flex:1}
.notif-title{font-size:12px;font-weight:700;margin-bottom:3px}
.wa-title{color:#25d366}.sl-title{color:#e01e5a}
.notif-sub{font-size:10px;color:var(--muted)}
.notif-stats{display:flex;gap:14px}
.notif-stat{text-align:center}
.notif-stat-val{font-size:clamp(13px,2.2vw,18px);font-weight:700;font-family:'Syne',sans-serif;word-break:break-all}
.wa-stat-val{color:#25d366}.sl-stat-val{color:#e01e5a}
.notif-stat-lbl{font-size:9px;color:var(--muted)}
.tabs-bar{display:flex;gap:0;padding:0 24px;border-bottom:1px solid var(--border);overflow-x:auto;background:var(--surf)}
.tab{padding:10px 16px;font-size:11px;cursor:pointer;color:var(--muted);border-bottom:2px solid transparent;white-space:nowrap;transition:all .15s}
.tab:hover{color:var(--text)}.tab.active{color:#fff;border-bottom-color:var(--acc)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:10px;padding:16px 24px}
.db-card{background:var(--surf);border:1px solid var(--border);border-radius:8px;padding:12px 14px;transition:border-color .2s;cursor:pointer;overflow:hidden}
.db-card:hover{border-color:var(--acc)}.db-card.active-db{border-color:var(--acc);background:var(--surf2)}
.db-name{font-size:9px;color:var(--muted);margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.db-count{font-size:clamp(16px,3vw,26px);font-weight:700;font-family:'Syne',sans-serif;color:#fff;word-break:break-all}
.db-label{font-size:9px;color:var(--muted);margin-top:2px}
.log-wrap{margin:0 24px 24px;background:var(--surf);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.log-header{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap;gap:8px}
.log-header h3{font-family:'Syne',sans-serif;font-size:13px;font-weight:700;color:#fff}
.controls{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.flt{background:transparent;border:1px solid var(--border);color:var(--muted);font-family:inherit;font-size:10px;padding:4px 9px;border-radius:6px;cursor:pointer;transition:all .15s}
.flt:hover,.flt.active{border-color:var(--acc);color:#fff}
.flt.fi.active{border-color:var(--ins);color:var(--ins);background:#22d87a0d}
.flt.fu.active{border-color:var(--upd);color:var(--upd);background:#f59e0b0d}
.flt.fd.active{border-color:var(--del);color:var(--del);background:#f43f5e0d}
.flt.fs.active{border-color:var(--soft);color:var(--soft);background:#a78bfa0d}
.flt.fr.active{border-color:var(--rest);color:var(--rest);background:#38bdf80d}
.search-box{background:var(--surf2);border:1px solid var(--border);color:var(--text);font-family:inherit;font-size:11px;padding:4px 10px;border-radius:6px;outline:none;width:180px;transition:border-color .15s}
.search-box:focus{border-color:var(--acc)}
.btn-csv{background:var(--acc);color:#fff;border:none;font-family:inherit;font-size:10px;padding:4px 12px;border-radius:6px;cursor:pointer;transition:opacity .15s}
.btn-csv:hover{opacity:.85}
.btn-sound{background:transparent;border:1px solid var(--border);color:var(--muted);font-family:inherit;font-size:10px;padding:4px 9px;border-radius:6px;cursor:pointer}
.btn-sound.on{border-color:var(--ins);color:var(--ins)}
.log-body{max-height:500px;overflow-y:auto}
.ev{display:grid;grid-template-columns:130px 100px 190px 1fr 110px;gap:10px;align-items:start;padding:9px 16px;border-bottom:1px solid #ffffff06;transition:background .1s}
.ev:hover{background:#ffffff05}
.ev-time{color:var(--muted);font-size:10px;line-height:1.4}
.ev-type{font-weight:700;font-size:10px;letter-spacing:.4px;padding:2px 7px;border-radius:4px;width:fit-content;white-space:nowrap}
.ev-type.INSERT{background:#22d87a14;color:var(--ins);border:1px solid #22d87a30}
.ev-type.UPDATE{background:#f59e0b14;color:var(--upd);border:1px solid #f59e0b30}
.ev-type.DELETE{background:#f43f5e14;color:var(--del);border:1px solid #f43f5e30}
.ev-type.SOFT_DELETE{background:#a78bfa14;color:var(--soft);border:1px solid #a78bfa30}
.ev-type.RESTORE{background:#38bdf814;color:var(--rest);border:1px solid #38bdf830}
.ev-loc{color:#7a94b8;font-size:10px;word-break:break-all;line-height:1.5}
.ev-detail{color:var(--muted);font-size:10px;word-break:break-word;line-height:1.5;cursor:pointer}
.ev-actions{display:flex;flex-direction:column;gap:4px;align-items:flex-end}
.btn-detail{background:transparent;border:1px solid var(--border);color:var(--muted);font-family:inherit;font-size:9px;padding:3px 8px;border-radius:4px;cursor:pointer;white-space:nowrap;transition:all .15s}
.btn-detail:hover{border-color:var(--acc);color:#fff}
.btn-restore{background:#f43f5e14;border:1px solid #f43f5e44;color:#f43f5e;font-family:inherit;font-size:9px;padding:3px 8px;border-radius:4px;cursor:pointer;white-space:nowrap;transition:all .15s;font-weight:700}
.btn-restore:hover{background:#f43f5e28;border-color:#f43f5e88}
.btn-restore.soft{background:#a78bfa14;border-color:#a78bfa44;color:#a78bfa}
.btn-restore.soft:hover{background:#a78bfa28;border-color:#a78bfa88}
.btn-restore:disabled{opacity:.4;cursor:not-allowed}
.empty{text-align:center;padding:40px;color:var(--muted);font-size:12px}
.pager{padding:10px 16px;border-top:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;font-size:10px}
.pager-info{color:var(--muted)}.pager-btns{display:flex;gap:6px}
.tbl-stats{margin:0 24px 16px;background:var(--surf);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.tbl-stats-hdr{padding:10px 16px;border-bottom:1px solid var(--border);font-family:'Syne',sans-serif;font-size:12px;font-weight:700;color:#fff}
.tbl-stats-body{max-height:200px;overflow-y:auto}
.tbl-row{display:grid;grid-template-columns:200px 1fr 60px 60px 60px 60px;gap:8px;padding:7px 16px;border-bottom:1px solid #ffffff05;font-size:10px;align-items:center}
.tbl-row:hover{background:#ffffff04}
.tbl-bar-wrap{height:4px;background:var(--border);border-radius:2px;overflow:hidden}
.tbl-bar{height:4px;background:var(--acc);border-radius:2px;transition:width .3s}
.modal-overlay{display:none;position:fixed;inset:0;background:#000000bb;z-index:200;align-items:center;justify-content:center;padding:20px}
.modal-overlay.open{display:flex}
.modal{background:var(--surf);border:1px solid var(--border);border-radius:12px;max-width:800px;width:100%;max-height:90vh;overflow-y:auto;padding:24px;position:relative}
.modal-close{position:absolute;top:14px;right:16px;background:none;border:none;color:var(--muted);font-size:20px;cursor:pointer;line-height:1}
.modal-close:hover{color:#fff}
.modal h2{font-family:'Syne',sans-serif;font-size:16px;color:#fff;margin-bottom:16px}
.m-section{margin-bottom:18px}
.m-section h4{font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);margin-bottom:8px;padding-bottom:5px;border-bottom:1px solid var(--border)}
.m-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.m-item{background:var(--surf2);border-radius:6px;padding:8px 10px}
.m-item .lbl{font-size:9px;color:var(--muted);margin-bottom:3px}
.m-item .val{font-size:12px;color:#fff;word-break:break-all}
.diff-table{width:100%;border-collapse:collapse;font-size:11px}
.diff-table th{background:var(--surf2);padding:6px 10px;text-align:left;color:var(--muted);font-size:9px;text-transform:uppercase}
.diff-table td{padding:6px 10px;border-bottom:1px solid var(--border)}
.diff-table .before{color:#f87171}.diff-table .after{color:#4ade80}
.rec-table{width:100%;border-collapse:collapse;font-size:11px}
.rec-table td{padding:5px 10px;border-bottom:1px solid var(--border);vertical-align:top}
.rec-table td:first-child{color:var(--muted);width:180px;white-space:nowrap}
.rec-table td:last-child{color:#fff;word-break:break-word}
.flow-wrap{margin-top:10px;overflow-x:auto;border-radius:8px;background:var(--surf2);padding:16px}
.flow-wrap svg{display:block;margin:0 auto}
.restore-bar{background:#f43f5e0d;border:1px solid #f43f5e33;border-radius:8px;padding:14px 18px;margin-bottom:16px;display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.restore-bar.soft{background:#a78bfa0d;border-color:#a78bfa33}
.restore-info{flex:1;font-size:11px;color:var(--text)}
.restore-info strong{display:block;font-size:13px;margin-bottom:4px}
.restore-info.del strong{color:#f43f5e}
.restore-info.soft strong{color:#a78bfa}
.restore-info p{color:var(--muted);font-size:10px}
.btn-restore-modal{padding:8px 20px;border-radius:6px;font-family:inherit;font-size:11px;font-weight:700;cursor:pointer;border:none;transition:opacity .15s}
.btn-restore-modal.del{background:#f43f5e;color:#fff}
.btn-restore-modal.soft{background:#a78bfa;color:#fff}
.btn-restore-modal:hover{opacity:.85}
.btn-restore-modal:disabled{opacity:.4;cursor:not-allowed}
.restore-result{margin-top:8px;padding:8px 12px;border-radius:6px;font-size:11px;display:none}
.restore-result.ok{background:#22d87a14;color:#22d87a;border:1px solid #22d87a33}
.restore-result.err{background:#f43f5e14;color:#f43f5e;border:1px solid #f43f5e33}
.footer-bar{padding:10px 24px;font-size:10px;color:var(--muted);border-top:1px solid var(--border);display:flex;justify-content:space-between}
</style>
</head>
<body>
<header>
  <div class="logo">ADOPT<span>.</span>MONITOR<sub>v6</sub></div>
  <div class="hbadges">
    <span class="badge b-live"><span class="pulse"></span>LIVE</span>
    <span class="badge b-bl">⚡ BINLOG CDC</span>
    <span class="badge b-em" id="email-badge">📧 EMAIL ON</span>
    <span class="badge b-wa" id="wa-badge">📱 WA ON</span>
    <span class="badge b-sl" id="sl-badge">💬 SLACK ON</span>
    <span class="badge b-sb" id="sb-badge">🗄 SUPABASE ON</span>
    <span class="badge b-up" id="uptime-badge">⏱ 0h 0m</span>
  </div>
</header>

<div id="loading">
  <div class="spinner"></div>
  <div>Connecting to MySQL binlog stream...</div>
</div>

<div id="content">

  <div class="info-bar">
    <div class="info-item">📁 Binlog: <span id="i-binlog">—</span></div>
    <div class="info-item">📍 Position: <span id="i-pos">—</span></div>
    <div class="info-item">👤 MySQL User: <span id="i-user">—</span></div>
    <div class="info-item">🌐 Host: <span id="i-host">—</span></div>
    <div class="info-item">🕐 Started: <span id="i-start">—</span></div>
  </div>

  <div class="metrics">
    <div class="metric"><div class="metric-label">Databases</div><div class="metric-val c-w" id="m-dbs">-</div></div>
    <div class="metric"><div class="metric-label">Total Events</div><div class="metric-val c-w" id="m-total">0</div></div>
    <div class="metric"><div class="metric-label">Inserts</div><div class="metric-val c-ins" id="m-ins">0</div></div>
    <div class="metric"><div class="metric-label">Updates</div><div class="metric-val c-upd" id="m-upd">0</div></div>
    <div class="metric"><div class="metric-label">Deletes</div><div class="metric-val c-del" id="m-del">0</div></div>
    <div class="metric"><div class="metric-label">Soft Deletes</div><div class="metric-val c-soft" id="m-soft">0</div></div>
    <div class="metric"><div class="metric-label">Restores</div><div class="metric-val c-rest" id="m-rest">0</div></div>
    <div class="metric"><div class="metric-label">📱 WA Sent</div><div class="metric-val c-wa" id="m-wa">0</div></div>
    <div class="metric"><div class="metric-label">📧 Email Sent</div><div class="metric-val c-em" id="m-email">0</div></div>
    <div class="metric"><div class="metric-label">💬 Slack Sent</div><div class="metric-val c-sl" id="m-slack">0</div></div>
    <div class="metric"><div class="metric-label">🛡️ Fake Del Blocked</div><div class="metric-val" style="color:#f97316" id="m-fake">0</div></div>
  </div>

  <div class="notif-row">
    <div class="notif-box wa-box">
      <div class="notif-icon">📱</div>
      <div class="notif-info">
        <div class="notif-title wa-title">WhatsApp (Green API)</div>
        <div class="notif-sub" id="wa-status-text">Checking...</div>
      </div>
      <div class="notif-stats">
        <div class="notif-stat"><div class="notif-stat-val wa-stat-val" id="wa-sent-big">0</div><div class="notif-stat-lbl">Sent</div></div>
        <div class="notif-stat"><div class="notif-stat-val" style="color:#f43f5e" id="wa-fail-big">0</div><div class="notif-stat-lbl">Failed</div></div>
      </div>
    </div>
    <div class="notif-box sl-box">
      <div class="notif-icon">💬</div>
      <div class="notif-info">
        <div class="notif-title sl-title">Slack Notifications</div>
        <div class="notif-sub" id="sl-status-text">Checking...</div>
      </div>
      <div class="notif-stats">
        <div class="notif-stat"><div class="notif-stat-val sl-stat-val" id="sl-sent-big">0</div><div class="notif-stat-lbl">Sent</div></div>
        <div class="notif-stat"><div class="notif-stat-val" style="color:#f43f5e" id="sl-fail-big">0</div><div class="notif-stat-lbl">Failed</div></div>
      </div>
    </div>
  </div>

  <div class="tabs-bar" id="tabs-bar">
    <div class="tab active" onclick="switchTab('all')" id="tab-all">🌐 All Databases</div>
  </div>

  <div class="grid" id="cards"></div>

  <div class="tbl-stats">
    <div class="tbl-stats-hdr">🔥 Most Active Tables</div>
    <div class="tbl-stats-body" id="tbl-stats-body">
      <div style="padding:16px;color:var(--muted);font-size:11px;text-align:center">Waiting for events...</div>
    </div>
  </div>

  <div class="log-wrap">
    <div class="log-header">
      <h3>Live Binlog Event Stream</h3>
      <div class="controls">
        <input class="search-box" id="search-box" placeholder="🔍 Search table, db, details..." oninput="onSearch()">
        <button class="flt f-all active" onclick="setFilter('ALL')">All</button>
        <button class="flt fi" onclick="setFilter('INSERT')">Insert</button>
        <button class="flt fu" onclick="setFilter('UPDATE')">Update</button>
        <button class="flt fd" onclick="setFilter('DELETE')">Delete</button>
        <button class="flt fs" onclick="setFilter('SOFT_DELETE')">Soft Del</button>
        <button class="flt fr" onclick="setFilter('RESTORE')">Restore</button>
        <button class="btn-sound" id="sound-btn" onclick="toggleSound()">🔔 Sound OFF</button>
        <button class="btn-csv" onclick="exportCSV()">⬇ CSV</button>
      </div>
    </div>
    <div class="log-body" id="events"></div>
    <div class="pager" id="pagination"></div>
  </div>

  <div class="footer-bar">
    <span id="last-event">No events yet</span>
    <span>⚡ Real-time via MySQL Binlog CDC — Persisted to Supabase — v7</span>
  </div>
</div>

<!-- MODAL -->
<div class="modal-overlay" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <button class="modal-close" onclick="closeModal()">✕</button>
    <h2 id="modal-title">Event Detail</h2>
    <div id="modal-body"></div>
  </div>
</div>

<script>
let activeFilter='ALL',activeDB='all',searchTerm='',pageOffset=0,pageSize=100;
let totalEvents=0,allEvents=[],initialized=false,soundEnabled=false,lastEventId=0,maxTableTotal=1;
const AudioCtx=window.AudioContext||window.webkitAudioContext;

function toggleSound(){soundEnabled=!soundEnabled;const b=document.getElementById('sound-btn');b.textContent=soundEnabled?'🔔 Sound ON':'🔔 Sound OFF';b.classList.toggle('on',soundEnabled);}
function playBeep(f=440,d=0.12,v=0.15){try{const c=new AudioCtx(),o=c.createOscillator(),g=c.createGain();o.connect(g);g.connect(c.destination);o.frequency.value=f;g.gain.setValueAtTime(v,c.currentTime);g.gain.exponentialRampToValueAtTime(0.001,c.currentTime+d);o.start();o.stop(c.currentTime+d);}catch(e){}}
function switchTab(db){activeDB=db;pageOffset=0;document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.getElementById('tab-'+(db==='all'?'all':db))?.classList.add('active');document.querySelectorAll('.db-card').forEach(c=>{c.classList.toggle('active-db',c.dataset.db===db)});load();}
function setFilter(f){activeFilter=f;pageOffset=0;document.querySelectorAll('.flt').forEach(b=>b.classList.remove('active'));const map={ALL:'f-all',INSERT:'fi',UPDATE:'fu',DELETE:'fd',SOFT_DELETE:'fs',RESTORE:'fr'};document.querySelector('.'+map[f])?.classList.add('active');load();}
function onSearch(){searchTerm=document.getElementById('search-box').value;pageOffset=0;load();}
function changePage(dir){pageOffset=Math.max(0,pageOffset+dir*pageSize);load();}

async function exportCSV(){
  const params=new URLSearchParams();
  if(activeDB!=='all') params.append('db',activeDB);
  if(activeFilter!=='ALL') params.append('event',activeFilter);
  if(searchTerm) params.append('search',searchTerm);
  const pathParts=window.location.pathname.split('/').filter(Boolean);
  let apiBase='';
  if(pathParts.length>=2&&pathParts[0]==='tool') apiBase='/'+pathParts[0]+'/'+pathParts[1];
  const query=params.toString();
  const url=apiBase+'/api/export/csv'+(query?'?'+query:'');
  try{
    const resp=await fetch(url);
    if(!resp.ok){alert('Export failed: '+resp.status);return;}
    const blob=await resp.blob();
    const objUrl=URL.createObjectURL(blob);
    const a=document.createElement('a');
    a.href=objUrl;a.download='adopt_events.csv';
    document.body.appendChild(a);a.click();
    setTimeout(()=>{URL.revokeObjectURL(objUrl);document.body.removeChild(a);},2000);
  }catch(err){alert('Export error: '+err);}
}

async function restoreRecord(eid,btn){
  if(!confirm('Restore this deleted record back to MySQL?\n\nThis will re-INSERT the row. Are you sure?')) return;
  btn.disabled=true;btn.textContent='⏳ Restoring...';
  try{
    const resp=await fetch('/api/restore/'+eid,{method:'POST'});
    const data=await resp.json();
    if(data.success){
      btn.textContent='✅ Restored!';
      btn.style.background='#22d87a14';btn.style.borderColor='#22d87a44';btn.style.color='#22d87a';
      const rr=document.getElementById('restore-result-'+eid);
      if(rr){rr.textContent='✅ '+data.message;rr.className='restore-result ok';rr.style.display='block';}
      const mb=document.getElementById('modal-restore-btn-'+eid);
      if(mb){mb.disabled=true;mb.textContent='✅ Restored!';}
    }else{
      btn.textContent='❌ Failed';btn.disabled=false;
      alert('Restore failed: '+data.error);
    }
  }catch(err){btn.textContent='❌ Error';btn.disabled=false;alert('Error: '+err);}
}

async function restoreFromModal(eid){
  const btn=document.getElementById('modal-restore-btn-'+eid);
  if(!btn) return;
  if(!confirm('Restore this deleted record back to MySQL?\n\nThis will re-INSERT the row. Are you sure?')) return;
  btn.disabled=true;btn.textContent='⏳ Restoring...';
  try{
    const resp=await fetch('/api/restore/'+eid,{method:'POST'});
    const data=await resp.json();
    const rr=document.getElementById('restore-result-'+eid);
    if(data.success){
      btn.textContent='✅ Restored!';
      if(rr){rr.textContent='✅ '+data.message;rr.className='restore-result ok';rr.style.display='block';}
      const inlineBtn=document.getElementById('inline-restore-'+eid);
      if(inlineBtn){inlineBtn.textContent='✅ Restored';inlineBtn.disabled=true;}
    }else{
      btn.textContent='↩ Restore to DB';btn.disabled=false;
      if(rr){rr.textContent='❌ Error: '+data.error;rr.className='restore-result err';rr.style.display='block';}
    }
  }catch(err){btn.textContent='↩ Restore to DB';btn.disabled=false;alert('Error: '+err);}
}

function buildFlowchart(e){
  const COLOR={INSERT:'#22d87a',UPDATE:'#f59e0b',DELETE:'#f43f5e',SOFT_DELETE:'#a78bfa',RESTORE:'#38bdf8'};
  const c=COLOR[e.event]||'#6366f1';
  function recordId(rec){
    if(!rec) return '';
    const idKeys=['id','ID','name','Name','email','Email','username','Username','mobile','phone','customerId','customer_id','accountId','account_id','firstName','first_name','fullName','full_name'];
    for(const k of idKeys){if(rec[k]&&String(rec[k]).trim()) return String(rec[k]).trim().slice(0,40);}
    const first=Object.values(rec).find(v=>v&&String(v).trim());
    return first?String(first).trim().slice(0,40):'';
  }
  function changedSummary(diff){
    if(!diff||!diff.length) return 'No fields changed';
    const parts=diff.slice(0,3).map(d=>`${d.field}: "${d.before||'—'}" → "${d.after||'—'}"`);
    return parts.join(' | ')+(diff.length>3?` (+${diff.length-3} more)`:'');
  }
  function trunc(s,n=42){return s&&s.length>n?s.slice(0,n-1)+'…':(s||'');}
  const db=e.db,tbl=e.table,rid=recordId(e.record);
  const ridLine=rid?`Record: ${trunc(rid)}`:'';
  let steps=[];
  if(e.event==='INSERT'){
    steps=[
      {icon:'🗄️',label:'Where it happened',line1:`Database: ${db}`,line2:`Table: ${tbl}`,color:'#22d87a18',border:'#22d87a'},
      {icon:'➕',label:'What happened',line1:'A NEW record was added to the table.',line2:ridLine||'New row created successfully.',color:'#22d87a18',border:'#22d87a'},
      {icon:'📋',label:'What data was saved',line1:trunc(e.details||'Record fields stored in database.'),line2:'All columns written to the table.',color:'#0f162388',border:'#2a3a55'},
      {icon:'📣',label:'Who was notified',line1:'Email alert sent to the team.',line2:'WhatsApp + Slack message fired.',color:'#6366f118',border:'#6366f1'},
      {icon:'✅',label:'Final result',line1:'Record is now live in the database.',line2:'Visible here in the monitor dashboard.',color:'#22d87a18',border:'#22d87a'},
    ];
  }else if(e.event==='UPDATE'){
    const chg=changedSummary(e.diff);
    steps=[
      {icon:'🗄️',label:'Where it happened',line1:`Database: ${db}`,line2:`Table: ${tbl}`,color:'#f59e0b18',border:'#f59e0b'},
      {icon:'✏️',label:'What happened',line1:'An existing record was MODIFIED.',line2:ridLine||'Existing row updated.',color:'#f59e0b18',border:'#f59e0b'},
      {icon:'🔄',label:'What changed',line1:trunc(chg,50),line2:`${e.diff?e.diff.length:0} field(s) were updated.`,color:'#0f162388',border:'#2a3a55'},
      {icon:'📣',label:'Who was notified',line1:'Slack channel was notified.',line2:'(Email/WA only if configured for updates)',color:'#6366f118',border:'#6366f1'},
      {icon:'✅',label:'Final result',line1:'Record now holds the new values.',line2:'Old values are logged above in diff.',color:'#f59e0b18',border:'#f59e0b'},
    ];
  }else if(e.event==='SOFT_DELETE'){
    steps=[
      {icon:'🗄️',label:'Where it happened',line1:`Database: ${db}`,line2:`Table: ${tbl}`,color:'#a78bfa18',border:'#a78bfa'},
      {icon:'🚫',label:'What happened',line1:'A record was marked as DELETED',line2:'(but NOT physically removed from DB).',color:'#a78bfa18',border:'#a78bfa'},
      {icon:'🔍',label:'Which record',line1:ridLine||trunc(e.details||''),line2:'is_delete flag set to 1 (hidden from app).',color:'#0f162388',border:'#2a3a55'},
      {icon:'📣',label:'Who was notified',line1:'Email + WhatsApp + Slack notified.',line2:'Team alerted about the soft-deletion.',color:'#6366f118',border:'#6366f1'},
      {icon:'💡',label:'What this means',line1:'Record still exists — can be restored.',line2:'Click RESTORE below to bring it back.',color:'#a78bfa18',border:'#a78bfa'},
    ];
  }else if(e.event==='RESTORE'){
    steps=[
      {icon:'🗄️',label:'Where it happened',line1:`Database: ${db}`,line2:`Table: ${tbl}`,color:'#38bdf818',border:'#38bdf8'},
      {icon:'♻️',label:'What happened',line1:'A previously deleted record was RESTORED.',line2:'It is active and visible in the app again.',color:'#38bdf818',border:'#38bdf8'},
      {icon:'🔍',label:'Which record',line1:ridLine||trunc(e.details||''),line2:'is_delete flag changed from 1 back to 0.',color:'#0f162388',border:'#2a3a55'},
      {icon:'📣',label:'Who was notified',line1:'Slack / WhatsApp notified (if enabled).',line2:'Team alerted about the restoration.',color:'#6366f118',border:'#6366f1'},
      {icon:'✅',label:'Final result',line1:'Record is now fully active again.',line2:'Visible to users in the application.',color:'#38bdf818',border:'#38bdf8'},
    ];
  }else{
    const verified=e.meta&&e.meta['Verification']||'';
    const verLine=verified?trunc(verified,48):'Confirmed — record gone from database.';
    steps=[
      {icon:'🗄️',label:'Where it happened',line1:`Database: ${db}`,line2:`Table: ${tbl}`,color:'#f43f5e18',border:'#f43f5e'},
      {icon:'🗑️',label:'What happened',line1:'A record was PERMANENTLY DELETED.',line2:'Use RESTORE below to undo this.',color:'#f43f5e18',border:'#f43f5e'},
      {icon:'🔍',label:'Which record was deleted',line1:ridLine||trunc(e.details||''),line2:'Row removed from the table completely.',color:'#0f162388',border:'#2a3a55'},
      {icon:'🛡️',label:'Deletion verified',line1:verLine,line2:'System double-checked before alerting.',color:'#f59e0b18',border:'#f59e0b'},
      {icon:'📣',label:'Who was notified',line1:'⚠️ Email + WhatsApp + Slack alerted.',line2:'High-priority alert sent to the team.',color:'#f43f5e18',border:'#f43f5e'},
      {icon:'❌',label:'Final result',line1:'Record gone — click RESTORE to recover.',line2:'All original column data is stored here.',color:'#f43f5e18',border:'#f43f5e'},
    ];
  }
  const BOX_W=580,BOX_H=62,GAP_Y=16,START_X=40,HEADER_H=70;
  const SVG_W=660;
  const SVG_H=HEADER_H+steps.length*(BOX_H+GAP_Y)+50;
  let svg=`<svg width="${SVG_W}" viewBox="0 0 ${SVG_W} ${SVG_H}" xmlns="http://www.w3.org/2000/svg" style="font-family:JetBrains Mono,monospace;max-width:100%">`;
  svg+=`<defs><marker id="arrd" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1L8 5L2 9" fill="none" stroke="#2a3a55" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>`;
  const LABEL={INSERT:'New Record Added',UPDATE:'Record Modified',DELETE:'Record Permanently Deleted',SOFT_DELETE:'Record Hidden (Soft Delete)',RESTORE:'Record Restored'};
  svg+=`<rect x="0" y="0" width="${SVG_W}" height="${HEADER_H}" rx="0" fill="${c}18"/>`;
  svg+=`<rect x="0" y="${HEADER_H-2}" width="${SVG_W}" height="2" fill="${c}44"/>`;
  svg+=`<text x="30" y="28" fill="${c}" font-size="15" font-weight="700">${LABEL[e.event]||e.event}</text>`;
  svg+=`<text x="30" y="46" fill="#7a94b8" font-size="11">Database: ${db}   |   Table: ${tbl}   |   Time: ${e.time}   |   Event #${e.id}</text>`;
  svg+=`<text x="30" y="62" fill="#4a5a75" font-size="10">Click RESTORE button below to undo this deletion</text>`;
  steps.forEach((step,i)=>{
    const bx=START_X,by=HEADER_H+8+i*(BOX_H+GAP_Y);
    const bc=step.border||'#2a3a55';
    const mx=bx+BOX_W/2;
    svg+=`<rect x="${bx}" y="${by}" width="${BOX_W}" height="${BOX_H}" rx="8" fill="${step.color||'#0f162388'}" stroke="${bc}" stroke-width="1"/>`;
    svg+=`<circle cx="${bx+20}" cy="${by+BOX_H/2}" r="12" fill="${bc}33" stroke="${bc}" stroke-width="1"/>`;
    svg+=`<text x="${bx+20}" y="${by+BOX_H/2+1}" text-anchor="middle" dominant-baseline="middle" fill="${bc}" font-size="10" font-weight="700">${i+1}</text>`;
    svg+=`<text x="${bx+44}" y="${by+BOX_H/2-6}" dominant-baseline="middle" font-size="16">${step.icon}</text>`;
    svg+=`<text x="${bx+68}" y="${by+20}" fill="#ffffff" font-size="11" font-weight="700">${step.label}</text>`;
    svg+=`<text x="${bx+68}" y="${by+36}" fill="#c0cfe8" font-size="10">${step.line1}</text>`;
    svg+=`<text x="${bx+68}" y="${by+50}" fill="#4a5a75" font-size="9">${step.line2}</text>`;
    if(i<steps.length-1){
      const ay=by+BOX_H+2,ay2=by+BOX_H+GAP_Y-2;
      svg+=`<line x1="${mx}" y1="${ay}" x2="${mx}" y2="${ay2}" stroke="#2a3a55" stroke-width="1.5" marker-end="url(#arrd)"/>`;
    }
  });
  const footY=HEADER_H+8+steps.length*(BOX_H+GAP_Y)+4;
  svg+=`<text x="${SVG_W/2}" y="${footY+12}" text-anchor="middle" fill="#2a3a55" font-size="9">ADOPT Database Monitor v5  —  ${e.meta&&e.meta['Binlog File']?e.meta['Binlog File']:''}</text>`;
  svg+=`</svg>`;
  return svg;
}

async function openModal(id){
  const resp=await fetch('/api/event/'+id);
  if(!resp.ok) return;
  const e=await resp.json();
  document.getElementById('modal-title').innerHTML=`<span class="ev-type ${e.event}" style="margin-right:10px">${e.event.replace('_',' ')}</span> ${e.table}`;
  let html='';
  if(e.event==='DELETE'||e.event==='SOFT_DELETE'){
    const isSoft=e.event==='SOFT_DELETE';
    const cls=isSoft?'soft':'del';
    const title=isSoft?'🚫 This record was soft-deleted — it still exists in MySQL':'🗑️ This record was permanently deleted from MySQL';
    const desc=isSoft?'Clicking Restore will set <code>is_delete = 0</code> on this row, making it active again.':'Clicking Restore will re-INSERT this row with all its original column values.';
    html+=`<div class="restore-bar ${isSoft?'soft':''}">
      <div class="notif-icon">${isSoft?'🚫':'🗑️'}</div>
      <div class="restore-info ${cls}"><strong>${title}</strong><p>${desc}</p></div>
      <button class="btn-restore-modal ${cls}" id="modal-restore-btn-${e.id}" onclick="restoreFromModal(${e.id})">↩ Restore to DB</button>
    </div>
    <div class="restore-result" id="restore-result-${e.id}"></div>`;
  }
  html+=`<div class="m-section"><h4>📊 Event Flow Diagram — ${e.event.replace('_',' ')} Lifecycle</h4><div class="flow-wrap">${buildFlowchart(e)}</div></div>`;
  if(e.meta){
    html+='<div class="m-section"><h4>⚙️ Event Metadata</h4><div class="m-grid">';
    for(const[k,v]of Object.entries(e.meta)){if(v)html+=`<div class="m-item"><div class="lbl">${k}</div><div class="val">${v}</div></div>`;}
    html+='</div></div>';
  }
  if(e.diff&&e.diff.length){
    html+='<div class="m-section"><h4>🔄 Before → After</h4><table class="diff-table"><thead><tr><th>Field</th><th>Before</th><th>After</th></tr></thead><tbody>';
    for(const d of e.diff){html+=`<tr><td>${d.field}</td><td class="before">${d.before||'—'}</td><td class="after">${d.after||'—'}</td></tr>`;}
    html+='</tbody></table></div>';
  }
  if(e.record&&Object.keys(e.record).length){
    html+='<div class="m-section"><h4>📋 Full Record</h4><table class="rec-table">';
    for(const[k,v]of Object.entries(e.record)){if(v!==null&&v!==undefined&&v!=='')html+=`<tr><td>${k}</td><td>${v}</td></tr>`;}
    html+='</table></div>';
  }
  document.getElementById('modal-body').innerHTML=html;
  document.getElementById('modal').classList.add('open');
}
function closeModal(){document.getElementById('modal').classList.remove('open');}

function renderEvents(){
  const el=document.getElementById('events');
  if(!allEvents.length){
    el.innerHTML='<div class="empty">Waiting for binlog events...<br><small>Changes appear instantly as they happen in MySQL</small></div>';
    return;
  }
  el.innerHTML=allEvents.map(x=>{
    const canRestore=x.event==='DELETE'||x.event==='SOFT_DELETE';
    const isSoft=x.event==='SOFT_DELETE';
    const restoreBtn=canRestore
      ?`<button class="btn-restore ${isSoft?'soft':''}" id="inline-restore-${x.id}" onclick="event.stopPropagation();restoreRecord(${x.id},this)" title="Restore this record back to MySQL">↩ Restore</button>`
      :'';
    return `<div class="ev" onclick="openModal(${x.id})">
      <span class="ev-time">${x.time}</span>
      <span class="ev-type ${x.event}">${x.event.replace('_',' ')}</span>
      <span class="ev-loc">${x.db}<br><strong>${x.table}</strong></span>
      <span class="ev-detail">${x.details||''}</span>
      <div class="ev-actions">
        <button class="btn-detail" onclick="event.stopPropagation();openModal(${x.id})">› Details</button>
        ${restoreBtn}
      </div>
    </div>`;
  }).join('');
}

function renderPagination(){
  const el=document.getElementById('pagination');
  const tot=totalEvents;
  const cur=Math.floor(pageOffset/pageSize)+1;
  const max=Math.max(1,Math.ceil(tot/pageSize));
  el.innerHTML=`<span class="pager-info">Showing ${Math.min(pageOffset+1,tot)}–${Math.min(pageOffset+pageSize,tot)} of ${tot} events</span><div class="pager-btns"><button class="flt" onclick="changePage(-1)" ${pageOffset===0?'disabled style="opacity:.3"':''}>← Prev</button><span style="color:#fff;font-size:10px;padding:4px 8px">Page ${cur}/${max}</span><button class="flt" onclick="changePage(1)" ${pageOffset+pageSize>=tot?'disabled style="opacity:.3"':''}>Next →</button></div>`;
}

function renderTableStats(rows){
  if(!rows.length) return;
  maxTableTotal=Math.max(...rows.map(r=>r.total),1);
  document.getElementById('tbl-stats-body').innerHTML=rows.map(r=>`<div class="tbl-row"><span style="color:#94a3b8;font-size:10px" title="${r.db}">${r.table}</span><div class="tbl-bar-wrap"><div class="tbl-bar" style="width:${Math.round(r.total/maxTableTotal*100)}%"></div></div><span style="color:var(--ins)">${r.insert}</span><span style="color:var(--upd)">${r.update}</span><span style="color:var(--del)">${r.delete}</span><span style="color:#fff;font-weight:700">${r.total}</span></div>`).join('');
}

async function load(){
  try{
    const s=await fetch('/api/stats').then(r=>r.json());
    if(!s.ready) return;
    if(!initialized){document.getElementById('loading').style.display='none';document.getElementById('content').style.display='block';initialized=true;}
    document.getElementById('email-badge').textContent=s.email_enabled?'📧 EMAIL ON':'📧 EMAIL OFF';
    document.getElementById('wa-badge').textContent=s.whatsapp_enabled?'📱 WA ON':'📱 WA OFF';
    document.getElementById('sl-badge').textContent=s.slack_enabled?'💬 SLACK ON':'💬 SLACK OFF';
    document.getElementById('sb-badge').textContent=s.supabase_enabled?'🗄 SUPABASE ON':'🗄 SUPABASE OFF';
    document.getElementById('uptime-badge').textContent=`⏱ ${s.uptime}`;
    document.getElementById('i-binlog').textContent=s.binlog_file||'—';
    document.getElementById('i-pos').textContent=s.binlog_position||'—';
    document.getElementById('i-user').textContent=s.mysql_user||'—';
    document.getElementById('i-host').textContent=s.mysql_host||'—';
    document.getElementById('i-start').textContent=s.start_time||'—';
    document.getElementById('m-dbs').textContent=s.databases;
    document.getElementById('m-total').textContent=s.total_events;
    document.getElementById('m-ins').textContent=s.total_inserts;
    document.getElementById('m-upd').textContent=s.total_updates;
    document.getElementById('m-del').textContent=s.total_deletes;
    document.getElementById('m-soft').textContent=s.total_soft_deletes;
    document.getElementById('m-rest').textContent=s.total_restores;
    document.getElementById('m-wa').textContent=s.whatsapp_sent||0;
    document.getElementById('m-email').textContent=s.email_sent||0;
    document.getElementById('m-slack').textContent=s.slack_sent||0;
    document.getElementById('m-fake').textContent=s.fake_deletes_blocked||0;
    document.getElementById('wa-sent-big').textContent=s.whatsapp_sent||0;
    document.getElementById('wa-fail-big').textContent=s.whatsapp_failed||0;
    document.getElementById('sl-sent-big').textContent=s.slack_sent||0;
    document.getElementById('sl-fail-big').textContent=s.slack_failed||0;
    document.getElementById('wa-status-text').textContent=s.whatsapp_enabled?'Active — INSERT(customer), DELETE, SOFT DELETE':'Disabled';
    document.getElementById('sl-status-text').textContent=s.slack_enabled?`Active — Channel: ${s.slack_channel||'#db-alerts'}`:'Disabled';
    if(s.last_event) document.getElementById('last-event').textContent='Last event: '+s.last_event;

    const counts=await fetch('/api/counts').then(r=>r.json());
    const tabsBar=document.getElementById('tabs-bar');
    const existing=new Set([...tabsBar.querySelectorAll('.tab')].map(t=>t.dataset.db||'all'));
    for(const db of Object.keys(counts)){
      if(!existing.has(db)){
        const t=document.createElement('div');
        t.className='tab';t.dataset.db=db;
        t.textContent=db.replace('adopt','');
        t.onclick=()=>switchTab(db);t.id='tab-'+db;
        tabsBar.appendChild(t);
      }
    }
    document.getElementById('cards').innerHTML=Object.entries(counts).map(([db,cnt])=>`<div class="db-card ${activeDB===db?'active-db':''}" data-db="${db}" onclick="switchTab('${db}')"><div class="db-name">${db}</div><div class="db-count">${cnt}</div><div class="db-label">customers</div></div>`).join('');

    const db_p=activeDB!=='all'?`&db=${encodeURIComponent(activeDB)}`:'';
    const ev_p=activeFilter!=='ALL'?`&event=${encodeURIComponent(activeFilter)}`:'';
    const sr_p=searchTerm?`&search=${encodeURIComponent(searchTerm)}`:'';
    const evResp=await fetch(`/api/events?limit=${pageSize}&offset=${pageOffset}${db_p}${ev_p}${sr_p}`).then(r=>r.json());
    if(evResp.events.length&&evResp.events[0].id>lastEventId){
      if(soundEnabled) playBeep(660,0.1,0.1);
      lastEventId=evResp.events[0].id;
    }
    allEvents=evResp.events;totalEvents=evResp.total;
    renderEvents();renderPagination();

    const tStats=await fetch('/api/table_stats').then(r=>r.json());
    renderTableStats(tStats);
  }catch(e){console.error(e);}
}
setInterval(load,1000);load();
</script>
</body>
</html>"""


@app.route("/")
def dashboard():
    return render_template_string(HTML)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    check_binlog_status()
    warm_column_cache()
    if SUPABASE_ENABLED:
        print("\n  🗄  Loading events from Supabase...")
        load_events_from_supabase()
    else:
        print("\n  ⚠  Supabase not configured — events in RAM only")
        print("     Set SUPABASE_URL + SUPABASE_KEY env vars to enable\n")
    threading.Thread(target=binlog_monitor, daemon=True).start()
    threading.Thread(target=reset_weekly_state, daemon=True).start()
    # Supabase workers — single queue consumer + periodic position flush
    threading.Thread(target=_sb_event_worker, daemon=True).start()
    threading.Thread(target=_position_flush_worker, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    print(f"\n  Dashboard:   http://0.0.0.0:{port}")
    print(f"  Keepalive:   http://0.0.0.0:{port}/api/ping\n")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
