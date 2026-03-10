#!/usr/bin/env python3
# ============================================================
#  ADOPT Binlog Web Server v5
#  ✔ Full browser UI — no terminal interaction needed
#  ✔ Landing page lists all binlog files
#  ✔ Click file → opens NEW TAB → downloads + shows live
#  ✔ Rich inline details (field: before → after)
#  ✔ Full-text search across everything
#  ✔ Beautiful dark terminal aesthetic
#  ✔ Popup modal with complete record details
#  Usage: pip install flask pymysql pymysqlreplication
#         python adopt_binlog_server.py
#         Open http://localhost:7777
# ============================================================

import os, sys, csv, json, time, threading, traceback, queue
from datetime import datetime
from flask import Flask, Response, jsonify, send_file, request

# ─────────────────────────────────────────────────────────────
#  CONFIG — edit these
# ─────────────────────────────────────────────────────────────
MYSQL_SETTINGS = {
    "host":   "102.209.31.227",
    "port":   3306,
    "user":   "clusteradmin",
    "passwd": "ADOPT@2024#WIOCC@2023",
}

WATCH_DATABASES = {
    "adoptconvergebss","adoptnotification","adoptcommonapigateway",
    "adoptintegrationsystem","adoptinventorymanagement","adoptrevenuemanagement",
    "adoptsalesscrms","adopttaskmanagement","adoptticketmanagement","adoptradiusbss",
}

EXCLUDE_TABLES = {
    "databasechangelog","databasechangeloglock",
    "jv_commit","jv_commit_property","jv_global_id","jv_snapshot","schedulerlock",
    "tblmscheduleraudit","tblscheduleraudit","tblaudit","tblauditlog",
    "tblaudit_log","tblactivitylog","tblactivity_log","tbllog",
    "tblsystemlog","tblaccesslog","tbllogin_log",
}

OUTPUT_DIR = "./adopt_output"   # where CSV files are saved
PORT       = 7777

# ─────────────────────────────────────────────────────────────
#  FLASK APP
# ─────────────────────────────────────────────────────────────
app = Flask(__name__)

_col_cache   = {}
_cache_loaded = False
_binlog_list  = []   # [(name, size), ...]

# Per-session job tracking  {fname: {status, events, error, progress}}
_jobs = {}
_jobs_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────
def now(): return datetime.now().strftime("%H:%M:%S")

def fmt_size(b):
    try:
        if not b: return "?"
        if b >= 1024**3: return f"{b/1024**3:.2f} GB"
        if b >= 1024**2: return f"{b/1024**2:.2f} MB"
        return f"{b/1024:.1f} KB"
    except: return "?"

def safe_str(v):
    try:
        if v is None: return ""
        if isinstance(v, datetime): return v.strftime("%Y-%m-%d %H:%M:%S")
        return str(v)
    except: return ""

def safe_dict(d):
    try: return {k: safe_str(v) for k, v in d.items()}
    except: return {}

def ts_str(raw_ts):
    try: return datetime.utcfromtimestamp(int(raw_ts)).strftime("%Y-%m-%d %H:%M:%S")
    except: return ""

def log(msg):
    print(f"[{now()}] {msg}", flush=True)

# ─────────────────────────────────────────────────────────────
#  COLUMN CACHE
# ─────────────────────────────────────────────────────────────
def load_column_cache():
    global _cache_loaded
    try:
        import pymysql
        log("Loading column names from information_schema...")
        conn = pymysql.connect(**MYSQL_SETTINGS, cursorclass=pymysql.cursors.DictCursor)
        cur  = conn.cursor()
        db_list = "','".join(WATCH_DATABASES)
        cur.execute(
            f"SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION "
            f"FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA IN ('{db_list}') "
            f"ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION"
        )
        for r in cur.fetchall():
            key = f"{r['TABLE_SCHEMA']}.{r['TABLE_NAME']}"
            if key not in _col_cache: _col_cache[key] = {}
            _col_cache[key][f"UNKNOWN_COL{r['ORDINAL_POSITION']-1}"] = r["COLUMN_NAME"]
        cur.close(); conn.close()
        _cache_loaded = True
        log(f"Column cache: {len(_col_cache)} tables loaded")
    except Exception as e:
        log(f"Column cache failed (continuing): {e}")
        _cache_loaded = True

def resolve(db, table, row):
    try:
        if not any(k.startswith("UNKNOWN_COL") for k in row): return row
        m = _col_cache.get(f"{db}.{table}", {})
        return {m.get(k, k): v for k, v in row.items()}
    except: return row

# ─────────────────────────────────────────────────────────────
#  BINLOG LIST
# ─────────────────────────────────────────────────────────────
def fetch_binlog_list():
    global _binlog_list
    try:
        import pymysql
        conn = pymysql.connect(**MYSQL_SETTINGS, cursorclass=pymysql.cursors.DictCursor)
        cur  = conn.cursor()
        cur.execute("SHOW BINARY LOGS;")
        rows = cur.fetchall()
        cur.close(); conn.close()
        _binlog_list = [(r["Log_name"], r["File_size"]) for r in rows]
        log(f"Binlog list: {len(_binlog_list)} files")
    except Exception as e:
        log(f"Cannot fetch binlog list: {e}")
        _binlog_list = []

# ─────────────────────────────────────────────────────────────
#  BUILD RICH DETAIL STRING (inline display)
# ─────────────────────────────────────────────────────────────
def build_inline_detail(event_type, record, diff):
    """Build human-readable inline detail for table row display"""
    parts = []
    try:
        if event_type in ("UPDATE", "SOFT_DELETE", "RESTORE") and diff:
            for d in diff[:8]:   # show up to 8 fields inline
                f = d.get("field","")
                bv = d.get("before","") or "—"
                av = d.get("after","")  or "—"
                parts.append(f"{f}: {bv} → {av}")
            if len(diff) > 8:
                parts.append(f"... +{len(diff)-8} more")
            return " | ".join(parts)
        elif event_type == "INSERT" and record:
            # Show key fields
            priority = ["id","name","status","type","email","phone","description"]
            shown = []
            rec = record
            for pk in priority:
                for k in rec:
                    if pk.lower() in k.lower() and rec[k]:
                        shown.append(f"{k}: {rec[k]}")
                        break
            if not shown:
                items = [(k,v) for k,v in rec.items() if v][:6]
                shown = [f"{k}: {v}" for k,v in items]
            return " | ".join(shown)
        elif event_type == "DELETE" and record:
            items = [(k,v) for k,v in record.items() if v][:5]
            return " | ".join(f"{k}: {v}" for k,v in items)
    except: pass
    return ""

# ─────────────────────────────────────────────────────────────
#  BINLOG READER (runs in background thread)
# ─────────────────────────────────────────────────────────────
def read_binlog_thread(fname, fsize, csv_path, job_key):
    """Background thread: reads binlog, appends to CSV, updates job status."""
    try:
        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
        from pymysqlreplication.event import RotateEvent
    except ImportError:
        with _jobs_lock:
            _jobs[job_key]["error"] = "pymysqlreplication not installed. Run: pip install pymysqlreplication"
            _jobs[job_key]["status"] = "error"
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with _jobs_lock:
        _jobs[job_key].update({"status": "reading", "events": [], "total": 0,
                                "ins":0,"upd":0,"dlt":0,"soft":0,"rst":0,
                                "progress": 0, "error": None, "done": False})

    event_id  = 0
    last_pos  = 4
    stream    = None
    all_events = []

    # Also write to CSV
    is_new = not os.path.exists(csv_path)
    csv_fields = ["event_id","timestamp","event_type","database","table",
                  "binlog_file","binlog_pos","details","record_json","diff_json","inline_detail"]

    try:
        csv_fh = open(csv_path, "a", newline="", encoding="utf-8", buffering=1)
        csv_w  = csv.DictWriter(csv_fh, fieldnames=csv_fields)
        if is_new: csv_w.writeheader(); csv_fh.flush()
    except Exception as e:
        with _jobs_lock:
            _jobs[job_key]["error"] = f"Cannot open CSV: {e}"
            _jobs[job_key]["status"] = "error"
        return

    try:
        stream = BinLogStreamReader(
            connection_settings     = MYSQL_SETTINGS,
            ctl_connection_settings = MYSQL_SETTINGS,
            server_id               = 505,
            only_events             = [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, RotateEvent],
            only_schemas            = list(WATCH_DATABASES) if WATCH_DATABASES else None,
            log_file=fname, log_pos=4,
            resume_stream=False, blocking=False, freeze_schema=False,
        )

        for ev in stream:
            try:
                if isinstance(ev, RotateEvent): continue
                db    = ev.schema
                table = ev.table
                if table in EXCLUDE_TABLES or table.startswith("vw"): continue

                last_pos = ev.packet.log_pos
                t_str    = ts_str(getattr(ev, "timestamp", None))

                # Progress
                if fsize and last_pos > 4:
                    pct = min(int((last_pos / fsize) * 100), 99)
                    with _jobs_lock:
                        _jobs[job_key]["progress"] = pct

                rows_to_process = []

                if isinstance(ev, WriteRowsEvent):
                    for row in ev.rows:
                        try:
                            vals = resolve(db, table, row["values"])
                            rec  = safe_dict(vals)
                            inline = build_inline_detail("INSERT", rec, [])
                            event_id += 1
                            erow = {"event_id": event_id, "timestamp": t_str,
                                    "event_type": "INSERT", "database": db, "table": table,
                                    "binlog_file": fname, "binlog_pos": last_pos,
                                    "details": f"New row inserted",
                                    "record_json": json.dumps(rec, ensure_ascii=False),
                                    "diff_json": "", "inline_detail": inline}
                            rows_to_process.append(erow)
                            with _jobs_lock:
                                _jobs[job_key]["ins"] = _jobs[job_key].get("ins",0)+1
                        except: pass

                elif isinstance(ev, DeleteRowsEvent):
                    for row in ev.rows:
                        try:
                            vals = resolve(db, table, row["values"])
                            rec  = safe_dict(vals)
                            inline = build_inline_detail("DELETE", rec, [])
                            event_id += 1
                            erow = {"event_id": event_id, "timestamp": t_str,
                                    "event_type": "DELETE", "database": db, "table": table,
                                    "binlog_file": fname, "binlog_pos": last_pos,
                                    "details": "Row deleted",
                                    "record_json": json.dumps(rec, ensure_ascii=False),
                                    "diff_json": "", "inline_detail": inline}
                            rows_to_process.append(erow)
                            with _jobs_lock:
                                _jobs[job_key]["dlt"] = _jobs[job_key].get("dlt",0)+1
                        except: pass

                elif isinstance(ev, UpdateRowsEvent):
                    for row in ev.rows:
                        try:
                            before = resolve(db, table, row["before_values"])
                            after  = resolve(db, table, row["after_values"])
                            bd = safe_str(before.get("is_delete","")); ad = safe_str(after.get("is_delete",""))
                            if   bd != "1" and ad == "1": etype = "SOFT_DELETE"
                            elif bd == "1" and ad == "0": etype = "RESTORE"
                            else:                          etype = "UPDATE"
                            diff = []
                            for k in set(list(before) + list(after)):
                                bv = safe_str(before.get(k)); av = safe_str(after.get(k))
                                if bv != av:
                                    diff.append({"field": k, "before": bv, "after": av})
                            inline = build_inline_detail(etype, safe_dict(after), diff)
                            event_id += 1
                            erow = {"event_id": event_id, "timestamp": t_str,
                                    "event_type": etype, "database": db, "table": table,
                                    "binlog_file": fname, "binlog_pos": last_pos,
                                    "details": f"{len(diff)} field(s) changed",
                                    "record_json": json.dumps(safe_dict(after), ensure_ascii=False),
                                    "diff_json": json.dumps(diff, ensure_ascii=False),
                                    "inline_detail": inline}
                            rows_to_process.append(erow)
                            k2 = "soft" if etype=="SOFT_DELETE" else "rst" if etype=="RESTORE" else "upd"
                            with _jobs_lock:
                                _jobs[job_key][k2] = _jobs[job_key].get(k2,0)+1
                        except: pass

                for erow in rows_to_process:
                    try:
                        csv_w.writerow(erow)
                        csv_fh.flush()
                        # Store compact version for API
                        compact = {
                            "id":   erow["event_id"],
                            "ts":   erow["timestamp"],
                            "ev":   erow["event_type"],
                            "db":   erow["database"],
                            "tbl":  erow["table"],
                            "file": erow["binlog_file"],
                            "pos":  erow["binlog_pos"],
                            "det":  erow["details"],
                            "inline": erow["inline_detail"],
                            "rec":  erow["record_json"],
                            "diff": erow["diff_json"],
                        }
                        with _jobs_lock:
                            _jobs[job_key]["events"].append(compact)
                            _jobs[job_key]["total"] = event_id
                    except: pass

            except: continue

        with _jobs_lock:
            _jobs[job_key]["progress"] = 100
            _jobs[job_key]["status"]   = "done"
            _jobs[job_key]["done"]     = True

    except Exception as e:
        with _jobs_lock:
            _jobs[job_key]["error"]  = str(e)
            _jobs[job_key]["status"] = "error"
    finally:
        try:
            if stream: stream.close()
            csv_fh.close()
        except: pass

# ─────────────────────────────────────────────────────────────
#  ROUTES
# ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Landing page — lists all binlog files"""
    return Response(LANDING_HTML, mimetype="text/html")

@app.route("/api/binlogs")
def api_binlogs():
    """Return list of binlog files"""
    data = [{"name": n, "size": s, "size_fmt": fmt_size(s)} for n, s in _binlog_list]
    return jsonify({"files": data, "total": len(data), "db_host": MYSQL_SETTINGS["host"]})

@app.route("/api/start/<path:fname>")
def api_start(fname):
    """Start reading a binlog file in background"""
    matched = [(n, s) for n, s in _binlog_list if n == fname]
    if not matched:
        return jsonify({"error": f"File not found: {fname}"}), 404

    fsize = matched[0][1]
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, fname.replace("/", "_") + ".csv")
    job_key  = fname

    with _jobs_lock:
        if job_key in _jobs and _jobs[job_key].get("status") in ("reading", "done"):
            return jsonify({"ok": True, "cached": True, "job": job_key})
        _jobs[job_key] = {"status": "starting", "events": [], "total": 0,
                          "progress": 0, "error": None, "done": False,
                          "fname": fname, "fsize": fsize, "fsize_fmt": fmt_size(fsize)}

    t = threading.Thread(target=read_binlog_thread, args=(fname, fsize, csv_path, job_key), daemon=True)
    t.start()
    return jsonify({"ok": True, "cached": False, "job": job_key})

@app.route("/api/status/<path:fname>")
def api_status(fname):
    """Return job status + new events since offset"""
    offset = int(request.args.get("offset", 0))
    with _jobs_lock:
        job = _jobs.get(fname)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        new_events = job["events"][offset:]
        return jsonify({
            "status":   job["status"],
            "total":    job["total"],
            "progress": job["progress"],
            "ins":      job.get("ins", 0),
            "upd":      job.get("upd", 0),
            "dlt":      job.get("dlt", 0),
            "soft":     job.get("soft", 0),
            "rst":      job.get("rst", 0),
            "error":    job.get("error"),
            "done":     job.get("done", False),
            "new_events": new_events,
            "offset":   offset + len(new_events),
        })

@app.route("/viewer/<path:fname>")
def viewer(fname):
    """Full viewer page for a specific binlog file"""
    return Response(VIEWER_HTML.replace("__FNAME__", fname)
                               .replace("__HOST__", MYSQL_SETTINGS["host"]),
                    mimetype="text/html")

@app.route("/api/download_csv/<path:fname>")
def download_csv(fname):
    csv_path = os.path.join(OUTPUT_DIR, fname.replace("/", "_") + ".csv")
    if os.path.exists(csv_path):
        return send_file(csv_path, as_attachment=True,
                         download_name=fname + ".csv", mimetype="text/csv")
    return jsonify({"error": "CSV not ready yet"}), 404

# ─────────────────────────────────────────────────────────────
#  LANDING PAGE HTML
# ─────────────────────────────────────────────────────────────
LANDING_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ADOPT · Binlog Monitor</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;800&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #060a10;
  --surface: #0c1220;
  --surface2: #111927;
  --border: #1c2a42;
  --accent: #00e5ff;
  --accent2: #7c3aed;
  --green: #00ff9d;
  --yellow: #fbbf24;
  --red: #ff4560;
  --text: #b0c4de;
  --muted: #3d5269;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg);
  color: var(--text);
  font-family: 'JetBrains Mono', monospace;
  min-height: 100vh;
  overflow-x: hidden;
}
/* Scanline effect */
body::before {
  content: '';
  position: fixed; inset: 0;
  background: repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,0,0,.03) 2px, rgba(0,0,0,.03) 4px);
  pointer-events: none; z-index: 999;
}

/* Top bar */
.topbar {
  display: flex; align-items: center; justify-content: space-between;
  padding: 18px 40px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  position: sticky; top: 0; z-index: 100;
}
.logo {
  font-family: 'Syne', sans-serif;
  font-size: 22px; font-weight: 800; color: #fff;
  letter-spacing: 2px;
}
.logo .dot { color: var(--accent); }
.status-dot {
  display: flex; align-items: center; gap: 8px;
  font-size: 11px; color: var(--green);
}
.pulse {
  width: 8px; height: 8px; background: var(--green);
  border-radius: 50%;
  animation: pulse 2s infinite;
  box-shadow: 0 0 8px var(--green);
}
@keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.5;transform:scale(1.3)} }

/* Hero */
.hero {
  padding: 60px 40px 40px;
  text-align: center;
}
.hero h1 {
  font-family: 'Syne', sans-serif;
  font-size: clamp(28px, 5vw, 52px);
  font-weight: 800; color: #fff;
  line-height: 1.1; margin-bottom: 14px;
}
.hero h1 .hl { 
  color: transparent;
  -webkit-text-stroke: 1px var(--accent);
}
.hero p {
  font-size: 12px; color: var(--muted); letter-spacing: 1px;
  max-width: 500px; margin: 0 auto 40px;
}

/* Stats row */
.stats-row {
  display: flex; gap: 16px; justify-content: center;
  flex-wrap: wrap; margin-bottom: 50px;
}
.stat-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 16px 24px; text-align: center;
  min-width: 130px;
  position: relative; overflow: hidden;
}
.stat-card::before {
  content: '';
  position: absolute; top: 0; left: 0; right: 0; height: 2px;
  background: var(--accent);
}
.stat-val { font-size: 28px; font-weight: 800; color: #fff; }
.stat-lbl { font-size: 9px; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; margin-top: 4px; }

/* File list */
.section { max-width: 900px; margin: 0 auto; padding: 0 40px 60px; }
.section-title {
  font-size: 10px; color: var(--muted);
  text-transform: uppercase; letter-spacing: 2px;
  margin-bottom: 16px;
  display: flex; align-items: center; gap: 12px;
}
.section-title::after {
  content: ''; flex: 1; height: 1px; background: var(--border);
}

.file-grid { display: flex; flex-direction: column; gap: 8px; }

.file-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 16px 20px;
  display: flex; align-items: center; gap: 16px;
  cursor: pointer;
  text-decoration: none;
  color: inherit;
  transition: all 0.15s;
  position: relative; overflow: hidden;
}
.file-card::before {
  content: ''; position: absolute; left: 0; top: 0; bottom: 0;
  width: 3px; background: var(--accent2);
  transform: scaleY(0); transition: transform 0.15s; transform-origin: bottom;
}
.file-card:hover { border-color: var(--accent2); background: var(--surface2); }
.file-card:hover::before { transform: scaleY(1); }

.file-icon {
  width: 40px; height: 40px;
  background: #7c3aed18;
  border: 1px solid #7c3aed44;
  border-radius: 8px;
  display: flex; align-items: center; justify-content: center;
  font-size: 18px; flex-shrink: 0;
}
.file-info { flex: 1; }
.file-name { font-size: 13px; font-weight: 600; color: #fff; margin-bottom: 3px; }
.file-meta { font-size: 10px; color: var(--muted); }
.file-size {
  font-size: 11px; color: var(--accent);
  font-weight: 600;
}
.file-arrow {
  font-size: 18px; color: var(--muted);
  transition: transform 0.15s, color 0.15s;
}
.file-card:hover .file-arrow { transform: translateX(4px); color: var(--accent); }
.open-tag {
  font-size: 9px; color: var(--accent);
  border: 1px solid var(--accent);
  border-radius: 4px; padding: 2px 8px;
  text-transform: uppercase; letter-spacing: 1px;
}

.loading-msg {
  text-align: center; padding: 60px;
  font-size: 12px; color: var(--muted);
}
.spinner {
  display: inline-block; width: 24px; height: 24px;
  border: 2px solid var(--border);
  border-top-color: var(--accent);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
  margin-bottom: 12px;
}
@keyframes spin { to { transform: rotate(360deg); } }

.error-msg {
  background: #ff456018; border: 1px solid #ff456044;
  border-radius: 8px; padding: 16px 20px;
  color: var(--red); font-size: 11px;
}

footer {
  text-align: center; padding: 20px;
  font-size: 10px; color: var(--muted);
  border-top: 1px solid var(--border);
}
</style>
</head>
<body>
<div class="topbar">
  <div class="logo">ADOPT<span class="dot">.</span>BINLOG</div>
  <div class="status-dot"><span class="pulse"></span> LIVE MONITOR</div>
</div>

<div class="hero">
  <h1>Database <span class="hl">Change</span> Explorer</h1>
  <p>SELECT A BINLOG FILE BELOW — OPENS IN NEW TAB WITH LIVE STREAMING</p>

  <div class="stats-row" id="stats-row">
    <div class="stat-card">
      <div class="stat-val" id="s-files">—</div>
      <div class="stat-lbl">Binlog Files</div>
    </div>
    <div class="stat-card">
      <div class="stat-val" id="s-total">—</div>
      <div class="stat-lbl">Total Size</div>
    </div>
    <div class="stat-card">
      <div class="stat-val" id="s-host" style="font-size:11px;padding-top:8px">—</div>
      <div class="stat-lbl">MySQL Host</div>
    </div>
  </div>
</div>

<div class="section">
  <div class="section-title">Available Binlog Files</div>
  <div class="file-grid" id="file-grid">
    <div class="loading-msg">
      <div class="spinner"></div><br>Connecting to MySQL...
    </div>
  </div>
</div>

<footer>ADOPT Binlog Monitor v5 · Browser-based · Zero terminal interaction</footer>

<script>
async function loadFiles() {
  try {
    const res  = await fetch('/tool/fetch/api/binlogs');
    const data = await res.json();
    const grid = document.getElementById('file-grid');

    document.getElementById('s-files').textContent = data.files.length;
    let totalBytes = data.files.reduce((a,f) => a + f.size, 0);
    document.getElementById('s-total').textContent = fmtSize(totalBytes);
    document.getElementById('s-host').textContent  = data.db_host;

    if (!data.files.length) {
      grid.innerHTML = '<div class="error-msg">No binlog files found. Check MySQL connection.</div>';
      return;
    }

    grid.innerHTML = data.files.map((f, i) => `
      <a class="file-card" href="/viewer/${encodeURIComponent(f.name)}" target="_blank" rel="noopener">
        <div class="file-icon">📂</div>
        <div class="file-info">
          <div class="file-name">${f.name}</div>
          <div class="file-meta">Binary log file · Click to open in new tab</div>
        </div>
        <div class="file-size">${f.size_fmt}</div>
        <span class="open-tag">Open ↗</span>
        <span class="file-arrow">›</span>
      </a>
    `).join('');

  } catch(e) {
    document.getElementById('file-grid').innerHTML =
      `<div class="error-msg">❌ Cannot connect to server: ${e.message}<br><br>Make sure the Python server is running on port """ + str(PORT) + r"""</div>`;
  }
}

function fmtSize(b) {
  if (!b) return '?';
  if (b >= 1073741824) return (b/1073741824).toFixed(2)+' GB';
  if (b >= 1048576)    return (b/1048576).toFixed(2)+' MB';
  return (b/1024).toFixed(1)+' KB';
}

loadFiles();
</script>
</body>
</html>"""

# ─────────────────────────────────────────────────────────────
#  VIEWER PAGE HTML — original UI preserved, + CSV/PDF buttons
# ─────────────────────────────────────────────────────────────
VIEWER_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ADOPT Binlog Viewer — __FNAME__</title>
<style>
:root{--bg:#070b14;--surf:#0f1623;--surf2:#151e2e;--border:#1a2540;
      --text:#c0cfe8;--muted:#4a5a75;
      --ins:#22d87a;--upd:#f59e0b;--del:#f43f5e;
      --soft:#a78bfa;--rest:#38bdf8;--acc:#6366f1}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Courier New',monospace;min-height:100vh}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
header{display:flex;align-items:center;justify-content:space-between;
        padding:14px 24px;background:var(--surf);border-bottom:1px solid var(--border);
        position:sticky;top:0;z-index:100;flex-wrap:wrap;gap:8px}
.logo{font-size:18px;font-weight:900;color:#fff;letter-spacing:1px}
.logo span{color:var(--acc)}
.hbadges{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.badge{padding:3px 10px;border-radius:999px;font-size:10px;font-weight:700}
.b-blue{background:#6366f118;color:#818cf8;border:1px solid #6366f144}
.b-green{background:#22d87a18;color:#22d87a;border:1px solid #22d87a44}
.b-yellow{background:#f59e0b18;color:#f59e0b;border:1px solid #f59e0b44}
/* download buttons in header */
.btn-dl{background:var(--acc);color:#fff;border:none;font-family:inherit;
        font-size:10px;padding:5px 14px;border-radius:6px;cursor:pointer;
        opacity:0;pointer-events:none;transition:opacity .3s}
.btn-dl.ready{opacity:1;pointer-events:auto}
.btn-dl-pdf{background:#f43f5e22;color:#f43f5e;border:1px solid #f43f5e44;font-family:inherit;
        font-size:10px;padding:5px 14px;border-radius:6px;cursor:pointer;
        opacity:0;pointer-events:none;transition:opacity .3s}
.btn-dl-pdf.ready{opacity:1;pointer-events:auto}
/* progress */
.prog-wrap{flex:1;min-width:120px;max-width:260px;height:4px;
           background:var(--surf2);border-radius:2px;overflow:hidden}
.prog-bar{height:100%;background:var(--acc);border-radius:2px;transition:width .4s;width:0%}
.metrics{display:flex;gap:10px;padding:14px 24px;flex-wrap:wrap}
.metric{background:var(--surf);border:1px solid var(--border);border-radius:8px;
        padding:10px 14px;flex:1;min-width:100px;text-align:center}
.metric-label{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:4px}
.metric-val{font-size:22px;font-weight:900}
.c-w{color:#fff}.c-ins{color:var(--ins)}.c-upd{color:var(--upd)}
.c-del{color:var(--del)}.c-soft{color:var(--soft)}.c-rest{color:var(--rest)}
.controls{display:flex;gap:8px;padding:12px 24px;flex-wrap:wrap;align-items:center;
          background:var(--surf2);border-bottom:1px solid var(--border)}
.search-box{background:var(--surf);border:1px solid var(--border);color:var(--text);
            font-family:inherit;font-size:11px;padding:6px 12px;border-radius:6px;
            outline:none;width:260px}
.search-box:focus{border-color:var(--acc)}
.flt{background:transparent;border:1px solid var(--border);color:var(--muted);
     font-family:inherit;font-size:10px;padding:5px 10px;border-radius:6px;cursor:pointer}
.flt:hover,.flt.active{border-color:var(--acc);color:#fff}
.pg-btn{background:transparent;border:1px solid var(--border);color:var(--muted);
        font-family:inherit;font-size:10px;padding:5px 10px;border-radius:6px;cursor:pointer}
.pg-btn:hover{border-color:var(--acc);color:#fff}
.flt.fi.active{border-color:var(--ins);color:var(--ins)}
.flt.fu.active{border-color:var(--upd);color:var(--upd)}
.flt.fd.active{border-color:var(--del);color:var(--del)}
.flt.fs.active{border-color:var(--soft);color:var(--soft)}
.flt.fr.active{border-color:var(--rest);color:var(--rest)}
select.flt{padding:5px 8px}
.tbl-wrap{margin:0 24px 24px;background:var(--surf);border:1px solid var(--border);
          border-radius:10px;overflow:hidden}
.tbl-hdr{display:grid;grid-template-columns:60px 140px 110px 180px 160px 1fr 30px;
         gap:8px;padding:8px 14px;background:var(--surf2);border-bottom:1px solid var(--border);
         font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px}
.tbl-body{max-height:calc(100vh - 360px);overflow-y:auto}
.ev-row{display:grid;grid-template-columns:60px 140px 110px 180px 160px 1fr 30px;
        gap:8px;padding:8px 14px;border-bottom:1px solid #ffffff05;
        cursor:pointer;transition:background .1s;align-items:center;font-size:10px}
.ev-row:hover{background:#ffffff06}
.ev-id{color:var(--muted)}
.ev-ts{color:var(--muted);line-height:1.4}
.ev-type{font-weight:700;font-size:9px;padding:2px 7px;border-radius:4px;width:fit-content;white-space:nowrap}
.ev-type.INSERT{background:#22d87a14;color:var(--ins);border:1px solid #22d87a30}
.ev-type.UPDATE{background:#f59e0b14;color:var(--upd);border:1px solid #f59e0b30}
.ev-type.DELETE{background:#f43f5e14;color:var(--del);border:1px solid #f43f5e30}
.ev-type.SOFT_DELETE{background:#a78bfa14;color:var(--soft);border:1px solid #a78bfa30}
.ev-type.RESTORE{background:#38bdf814;color:var(--rest);border:1px solid #38bdf830}
.ev-loc{color:#7a94b8;line-height:1.5}
.ev-loc strong{color:#a0b4cc}
.ev-det{color:var(--muted);word-break:break-word;line-height:1.6;font-size:10px}
.ev-det .fn{color:#7c9ab8}
.ev-det .bv{color:#f87171}
.ev-det .av{color:#4ade80}
.ev-det .sep{color:#1a2540;margin:0 4px}
.ev-det .rv{color:#8ab4d4}
.ev-arrow{color:var(--muted);text-align:center}
.pager{padding:10px 14px;border-top:1px solid var(--border);display:flex;
       align-items:center;justify-content:space-between;font-size:10px;flex-wrap:wrap;gap:8px}
.pager-info{color:var(--muted)}
.pager-btns{display:flex;gap:6px}
.empty{text-align:center;padding:40px;color:var(--muted);font-size:12px}
.loading-row{text-align:center;padding:30px;color:var(--muted);font-size:11px}
/* Modal */
.modal-overlay{display:none;position:fixed;inset:0;background:#000000bb;z-index:200;
               align-items:center;justify-content:center;padding:20px}
.modal-overlay.open{display:flex}
.modal{background:var(--surf);border:1px solid var(--border);border-radius:12px;
       max-width:800px;width:100%;max-height:88vh;overflow-y:auto;padding:24px;position:relative}
.modal-close{position:absolute;top:14px;right:16px;background:none;border:none;
             color:var(--muted);font-size:22px;cursor:pointer}
.modal-close:hover{color:#fff}
.modal h2{font-size:15px;color:#fff;margin-bottom:16px}
.m-section{margin-bottom:18px}
.m-section h4{font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);
              margin-bottom:8px;padding-bottom:5px;border-bottom:1px solid var(--border)}
.m-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.m-item{background:var(--surf2);border-radius:6px;padding:8px 10px}
.m-item .lbl{font-size:9px;color:var(--muted);margin-bottom:3px}
.m-item .val{font-size:11px;color:#fff;word-break:break-all}
.diff-tbl{width:100%;border-collapse:collapse;font-size:11px}
.diff-tbl th{background:var(--surf2);padding:6px 10px;text-align:left;
             color:var(--muted);font-size:9px;text-transform:uppercase}
.diff-tbl td{padding:6px 10px;border-bottom:1px solid var(--border)}
.diff-tbl .bf{color:#f87171}.diff-tbl .af{color:#4ade80}
.rec-tbl{width:100%;border-collapse:collapse;font-size:11px}
.rec-tbl td{padding:5px 10px;border-bottom:1px solid var(--border);vertical-align:top}
.rec-tbl td:first-child{color:var(--muted);width:200px;white-space:nowrap}
.rec-tbl td:last-child{color:#fff;word-break:break-word}
/* stream badge */
.stream-badge{display:flex;align-items:center;gap:6px;font-size:10px;color:var(--upd)}
.stream-dot{width:6px;height:6px;background:var(--upd);border-radius:50%;
            animation:pulse 1s infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(1.3)}}
</style>
</head>
<body>

<header>
  <div class="logo">ADOPT<span>.</span>VIEWER</div>
  <div class="hbadges">
    <span class="badge b-blue" id="fname-badge">📂 __FNAME__</span>
    <span class="badge b-green" id="total-badge">⚡ 0 events</span>
    <span class="stream-badge" id="stream-badge" style="display:none">
      <span class="stream-dot"></span> STREAMING
    </span>
    <div class="prog-wrap" id="prog-wrap"><div class="prog-bar" id="prog-bar"></div></div>
    <button class="btn-dl" id="btn-csv" onclick="downloadCSV()">⬇ Export CSV</button>
    <button class="btn-dl-pdf" id="btn-pdf" onclick="downloadPDF()">📄 Export PDF</button>
  </div>
</header>

<div class="metrics">
  <div class="metric"><div class="metric-label">Total</div><div class="metric-val c-w" id="m-total">0</div></div>
  <div class="metric"><div class="metric-label">Inserts</div><div class="metric-val c-ins" id="m-ins">0</div></div>
  <div class="metric"><div class="metric-label">Updates</div><div class="metric-val c-upd" id="m-upd">0</div></div>
  <div class="metric"><div class="metric-label">Deletes</div><div class="metric-val c-del" id="m-del">0</div></div>
  <div class="metric"><div class="metric-label">Soft Del</div><div class="metric-val c-soft" id="m-soft">0</div></div>
  <div class="metric"><div class="metric-label">Restores</div><div class="metric-val c-rest" id="m-rst">0</div></div>
</div>

<div class="controls">
  <input class="search-box" id="search" placeholder="🔍 Search table / database / field / value..." oninput="applyFilters()">
  <button class="flt f-all active" onclick="setType('ALL')">All</button>
  <button class="flt fi" onclick="setType('INSERT')">Insert</button>
  <button class="flt fu" onclick="setType('UPDATE')">Update</button>
  <button class="flt fd" onclick="setType('DELETE')">Delete</button>
  <button class="flt fs" onclick="setType('SOFT_DELETE')">Soft Del</button>
  <button class="flt fr" onclick="setType('RESTORE')">Restore</button>
  <select class="flt" id="db-filter" onchange="applyFilters()">
    <option value="">All Databases</option>
  </select>
  <select class="flt" id="date-filter" onchange="applyFilters()">
    <option value="">All Dates</option>
  </select>
</div>

<div class="tbl-wrap">
  <div class="tbl-hdr">
    <span>#</span><span>Timestamp</span><span>Type</span>
    <span>Database</span><span>Table</span><span>Details / Changes</span><span></span>
  </div>
  <div class="tbl-body" id="tbl-body">
    <div class="loading-row">⟳ Loading binlog — please wait...</div>
  </div>
  <div class="pager" id="pager"></div>
</div>

<div class="modal-overlay" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <button class="modal-close" onclick="closeModal()">✕</button>
    <h2 id="modal-title">Event Detail</h2>
    <div id="modal-body"></div>
  </div>
</div>

<script>
const FNAME    = '__FNAME__';
const PAGE     = 200;
let allEvents  = [];
let filtered   = [];
let page       = 0;
let activeType = 'ALL';
let offset     = 0;
let polling    = null;
let isDone     = false;
const dbSet    = new Set();
const dateSet  = new Set();

// ── Start ────────────────────────────────────────────────────
async function startJob() {
  try {
    const r = await fetch(`/tool/fetch/api/start/${encodeURIComponent(FNAME)}`);
    const d = await r.json();
    if (d.error) { showError(d.error); return; }
    startPolling();
  } catch(e) { showError(e.message); }
}

function startPolling() {
  polling = setInterval(poll, 1500);
  poll();
}

async function poll() {
  try {
    const r = await fetch(`/tool/fetch/api/status/${encodeURIComponent(FNAME)}?offset=${offset}`);
    const d = await r.json();
    if (d.error) { clearInterval(polling); showError(d.error); return; }

    // metrics
    document.getElementById('m-total').textContent = fmt(d.total);
    document.getElementById('m-ins').textContent   = fmt(d.ins);
    document.getElementById('m-upd').textContent   = fmt(d.upd);
    document.getElementById('m-del').textContent   = fmt(d.dlt);
    document.getElementById('m-soft').textContent  = fmt(d.soft);
    document.getElementById('m-rst').textContent   = fmt(d.rst);
    document.getElementById('total-badge').textContent = `⚡ ${fmt(d.total)} events`;
    document.getElementById('prog-bar').style.width = d.progress + '%';

    if (d.status === 'reading') {
      document.getElementById('stream-badge').style.display = 'flex';
    }

    if (d.new_events && d.new_events.length) {
      d.new_events.forEach(e => {
        allEvents.push(e);
        if (e.db) dbSet.add(e.db);
        if (e.ts) dateSet.add(e.ts.slice(0,10));
      });
      offset = d.offset;
      updateDropdowns();
      applyFilters(false);
    }

    if (d.done && !isDone) {
      isDone = true;
      clearInterval(polling);
      document.getElementById('stream-badge').style.display = 'none';
      document.getElementById('btn-csv').classList.add('ready');
      document.getElementById('btn-pdf').classList.add('ready');
      applyFilters();
    }
  } catch(e) { /* retry */ }
}

// ── Dropdowns ─────────────────────────────────────────────────
function updateDropdowns() {
  const dbSel = document.getElementById('db-filter');
  const dtSel = document.getElementById('date-filter');
  const curDb = dbSel.value, curDt = dtSel.value;
  dbSel.innerHTML = '<option value="">All Databases</option>';
  [...dbSet].sort().forEach(d => {
    const o = document.createElement('option');
    o.value = o.textContent = d;
    if (d === curDb) o.selected = true;
    dbSel.appendChild(o);
  });
  dtSel.innerHTML = '<option value="">All Dates</option>';
  [...dateSet].sort().reverse().forEach(d => {
    const o = document.createElement('option');
    o.value = o.textContent = d;
    if (d === curDt) o.selected = true;
    dtSel.appendChild(o);
  });
}

// ── Filters ───────────────────────────────────────────────────
function setType(t) {
  activeType = t; page = 0;
  // Only remove active from the type-filter buttons, not selects or download buttons
  document.querySelectorAll('button.flt').forEach(b => b.classList.remove('active'));
  const map = {ALL:'f-all',INSERT:'fi',UPDATE:'fu',DELETE:'fd',SOFT_DELETE:'fs',RESTORE:'fr'};
  const btn = document.querySelector('button.' + map[t]);
  if (btn) btn.classList.add('active');
  applyFilters();
}

function applyFilters(resetPage = true) {
  if (resetPage) page = 0;
  const q  = document.getElementById('search').value.trim().toLowerCase();
  const db = document.getElementById('db-filter').value;
  const dt = document.getElementById('date-filter').value;
  filtered = allEvents.filter(r => {
    if (activeType !== 'ALL' && r.ev !== activeType) return false;
    if (db && r.db !== db) return false;
    if (dt && !r.ts.startsWith(dt)) return false;
    if (q) {
      // Search across everything including event id, all field names and values
      const hay = [
        String(r.id), '#' + String(r.id),
        r.db, r.tbl, r.ts, r.ev, r.det || '',
        r.inline || '', r.rec || '', r.diff || ''
      ].join(' ').toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });
  render();
}

// ── Render ────────────────────────────────────────────────────
function render() {
  const body  = document.getElementById('tbl-body');
  const slice = filtered.slice(page * PAGE, (page+1) * PAGE);

  if (!allEvents.length) {
    body.innerHTML = '<div class="loading-row">⟳ Waiting for data...</div>';
    renderPager(); return;
  }
  if (!slice.length) {
    body.innerHTML = '<div class="empty">No events match the current filters</div>';
    renderPager(); return;
  }

  body.innerHTML = slice.map(r => {
    const det = buildDetail(r);
    return `<div class="ev-row" onclick="openModal(${r.id})">
      <span class="ev-id">#${r.id}</span>
      <span class="ev-ts">${r.ts}</span>
      <span class="ev-type ${r.ev}">${r.ev.replace('_',' ')}</span>
      <span class="ev-loc">${r.db}</span>
      <span class="ev-loc"><strong>${r.tbl}</strong></span>
      <span class="ev-det">${det}</span>
      <span class="ev-arrow">›</span>
    </div>`;
  }).join('');
  renderPager();
}

function buildDetail(r) {
  try {
    // For UPDATE/SOFT_DELETE/RESTORE — show before→after diff
    if (r.diff) {
      let diff = [];
      try { diff = JSON.parse(r.diff); } catch {}
      if (diff.length) {
        const parts = diff.slice(0,8).map(d =>
          `<span class="fn">${esc(d.field)}</span>: ` +
          `<span class="bv">${esc(d.before||'—')}</span>` +
          ` <span style="color:var(--muted)">→</span> ` +
          `<span class="av">${esc(d.after||'—')}</span>`
        );
        let html = parts.join('<span class="sep"> | </span>');
        if (diff.length > 8) html += `<span class="sep"> | </span><span style="color:var(--muted)">+${diff.length-8} more</span>`;
        return html;
      }
    }
    // For INSERT/DELETE and fallback — show record fields
    if (r.rec) {
      let rec = {};
      try { rec = JSON.parse(r.rec); } catch {}
      const keys = Object.keys(rec).filter(k => rec[k] !== null && rec[k] !== undefined && String(rec[k]).trim() !== '');
      if (keys.length) {
        return keys.slice(0,8).map(k =>
          `<span class="fn">${esc(k)}</span>: <span class="rv">${esc(rec[k])}</span>`
        ).join('<span class="sep"> | </span>');
      }
    }
    return esc(r.det || '');
  } catch { return esc(r.det || ''); }
}

function renderPager() {
  const total = filtered.length;
  const pages = Math.max(1, Math.ceil(total / PAGE));
  const s = page * PAGE + 1, e = Math.min((page+1)*PAGE, total);
  document.getElementById('pager').innerHTML = `
    <span class="pager-info">Showing ${fmt(s)}–${fmt(e)} of ${fmt(total)} events</span>
    <div class="pager-btns">
      <button class="pg-btn" onclick="goPage(-1)" ${page===0?'disabled style="opacity:.3"':''}>← Prev</button>
      <span style="color:#fff;font-size:10px;padding:0 8px">Page ${page+1}/${pages}</span>
      <button class="pg-btn" onclick="goPage(1)" ${page>=pages-1?'disabled style="opacity:.3"':''}>Next →</button>
    </div>`;
}

function goPage(d) { page = Math.max(0, page+d); render(); window.scrollTo(0,0); }

// ── Modal ─────────────────────────────────────────────────────
function openModal(id) {
  const r = allEvents.find(x => x.id === id);
  if (!r) return;

  document.getElementById('modal-title').innerHTML =
    `<span class="ev-type ${r.ev}" style="margin-right:8px">${r.ev.replace('_',' ')}</span>${esc(r.tbl)}`;

  let html = '';
  html += `<div class="m-section"><h4>⚙️ Event Info</h4><div class="m-grid">
    <div class="m-item"><div class="lbl">Event ID</div><div class="val">#${r.id}</div></div>
    <div class="m-item"><div class="lbl">Timestamp</div><div class="val">${r.ts}</div></div>
    <div class="m-item"><div class="lbl">Database</div><div class="val">${esc(r.db)}</div></div>
    <div class="m-item"><div class="lbl">Table</div><div class="val">${esc(r.tbl)}</div></div>
    <div class="m-item"><div class="lbl">Binlog File</div><div class="val">${esc(r.file)}</div></div>
    <div class="m-item"><div class="lbl">Position</div><div class="val">${r.pos}</div></div>
  </div></div>`;

  let diff = [];
  try { diff = JSON.parse(r.diff || '[]'); } catch {}
  if (diff.length) {
    html += `<div class="m-section"><h4>🔄 Before → After</h4>
    <table class="diff-tbl"><thead><tr><th>Field</th><th>Before</th><th>After</th></tr></thead><tbody>`;
    diff.forEach(d => {
      html += `<tr><td>${esc(d.field)}</td><td class="bf">${esc(d.before||'—')}</td><td class="af">${esc(d.after||'—')}</td></tr>`;
    });
    html += `</tbody></table></div>`;
  }

  let rec = {};
  try { rec = JSON.parse(r.rec || '{}'); } catch {}
  const recKeys = Object.keys(rec).filter(k => rec[k] !== null && rec[k] !== undefined && rec[k] !== '');
  if (recKeys.length) {
    html += `<div class="m-section"><h4>📋 Full Record</h4>
    <table class="rec-tbl"><tbody>`;
    recKeys.forEach(k => {
      html += `<tr><td>${esc(k)}</td><td>${esc(rec[k])}</td></tr>`;
    });
    html += `</tbody></table></div>`;
  }

  document.getElementById('modal-body').innerHTML = html;
  document.getElementById('modal').classList.add('open');
}

function closeModal() { document.getElementById('modal').classList.remove('open'); }
document.addEventListener('keydown', e => { if(e.key==='Escape') closeModal(); });

// ── Downloads ─────────────────────────────────────────────────
function downloadCSV() {
  // Export currently filtered data as CSV
  const fields = ['id','ts','ev','db','tbl','file','pos','det','rec','diff'];
  const header = ['event_id','timestamp','event_type','database','table','binlog_file','binlog_pos','details','record_json','diff_json'];
  const rows = filtered.map(r => header.map((_,i) =>
    `"${String(r[fields[i]]||'').replace(/"/g,'""')}"`
  ).join(','));
  const blob = new Blob([header.join(',') + '\n' + rows.join('\n')], {type:'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = FNAME.replace(/\//g,'_') + '_filtered.csv';
  a.style.display = 'none';
  document.body.appendChild(a);
  a.click();
  setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(a.href); }, 3000);
}

function downloadPDF() {
  const mTotal = document.getElementById('m-total').textContent;
  const mIns   = document.getElementById('m-ins').textContent;
  const mUpd   = document.getElementById('m-upd').textContent;
  const mDel   = document.getElementById('m-del').textContent;
  const mSoft  = document.getElementById('m-soft').textContent;
  const mRst   = document.getElementById('m-rst').textContent;
  const rows   = filtered;  // ALL events — no limit
  const now    = new Date().toLocaleString();

  // HTML escape helper for PDF content
  const pe = s => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

  const tableRows = rows.map(r => {
    let det = '';
    try {
      if (r.diff && r.diff !== '[]' && r.diff !== '') {
        const d = JSON.parse(r.diff);
        det = d.slice(0,5).map(x => pe(x.field) + ': <span style="color:#dc2626">' + pe(x.before||'—') + '</span> &rarr; <span style="color:#16a34a">' + pe(x.after||'—') + '</span>').join(' &nbsp;|&nbsp; ');
        if (d.length > 5) det += ' &nbsp;|&nbsp; <em>+' + (d.length-5) + ' more</em>';
      } else if (r.rec && r.rec !== '{}') {
        const rc = JSON.parse(r.rec);
        det = Object.keys(rc).filter(k => rc[k]).slice(0,5).map(k => '<b>' + pe(k) + '</b>: ' + pe(rc[k])).join(' &nbsp;|&nbsp; ');
      }
    } catch(e) { det = pe(r.det || ''); }
    const ev = r.ev.replace('_',' ');
    const clr = {INSERT:'#16a34a',UPDATE:'#b45309',DELETE:'#be123c',SOFT_DELETE:'#7c3aed',RESTORE:'#0369a1'}[r.ev]||'#333';
    return '<tr><td>#' + r.id + '</td><td>' + pe(r.ts) + '</td>' +
      '<td style="color:' + clr + ';font-weight:700">' + ev + '</td>' +
      '<td>' + pe(r.db) + '</td><td><b>' + pe(r.tbl) + '</b></td>' +
      '<td style="max-width:280px;word-break:break-word;line-height:1.6">' + det + '</td></tr>';
  });  // keep as array for chunked blob

  const html = '<!DOCTYPE html><html><head><meta charset="UTF-8">' +
    '<title>ADOPT Binlog Report</title><style>' +
    'body{font-family:Courier New,monospace;font-size:9px;color:#111;margin:16px;background:#fff}' +
    'h1{font-size:16px;margin:0 0 2px}' +
    '.sub{color:#666;font-size:9px;margin-bottom:14px}' +
    '.stats{display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap}' +
    '.sc{background:#f5f5f5;border:1px solid #ddd;border-radius:5px;padding:8px 14px;text-align:center;min-width:80px}' +
    '.sv{font-size:18px;font-weight:900}' +
    '.sl{font-size:8px;color:#888;text-transform:uppercase;letter-spacing:.7px}' +
    'table{width:100%;border-collapse:collapse}' +
    'th{background:#1e293b;color:#fff;padding:5px 7px;text-align:left;font-size:8px;text-transform:uppercase;letter-spacing:.5px}' +
    'td{padding:4px 7px;border-bottom:1px solid #eee;vertical-align:top}' +
    'tr:nth-child(even) td{background:#f9f9f9}' +
    '.footer{margin-top:12px;font-size:8px;color:#aaa;text-align:center}' +
    '@page{margin:1cm}' +
    '</style></head><body>' +
    '<h1>ADOPT Binlog Report</h1>' +
    '<div class="sub">File: ' + FNAME + ' &nbsp;|&nbsp; Generated: ' + now + ' &nbsp;|&nbsp; ' + rows.length.toLocaleString() + ' events</div>' +
    '<div class="stats">' +
    '<div class="sc"><div class="sv">' + mTotal + '</div><div class="sl">Total</div></div>' +
    '<div class="sc"><div class="sv" style="color:#16a34a">' + mIns + '</div><div class="sl">Inserts</div></div>' +
    '<div class="sc"><div class="sv" style="color:#b45309">' + mUpd + '</div><div class="sl">Updates</div></div>' +
    '<div class="sc"><div class="sv" style="color:#be123c">' + mDel + '</div><div class="sl">Deletes</div></div>' +
    '<div class="sc"><div class="sv" style="color:#7c3aed">' + mSoft + '</div><div class="sl">Soft Del</div></div>' +
    '<div class="sc"><div class="sv" style="color:#0369a1">' + mRst + '</div><div class="sl">Restores</div></div>' +
    '</div>' +
    '<table><thead><tr><th>#</th><th>Timestamp</th><th>Type</th><th>Database</th><th>Table</th><th>Changes / Data</th></tr></thead>' +
    '<tbody>';  // tbody content added separately via chunked blob
  const tableRows_arr = tableRows;

  // Build blob in chunks to avoid freezing browser on large datasets
  const parts = [];
  parts.push(html.split('<tbody>')[0] + '<tbody>');
  const CHUNK = 2000;
  for (let i = 0; i < tableRows_arr.length; i += CHUNK) {
    parts.push(tableRows_arr.slice(i, i + CHUNK).join(''));
  }
  parts.push('</tbody></table><div class="footer">ADOPT Binlog Monitor &nbsp;|&nbsp; ' + FNAME + '</div></body></html>');
  const blob = new Blob(parts, {type: 'text/html;charset=utf-8'});
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = FNAME.replace(/\//g,'_') + '_report.html';
  a.style.display = 'none';
  document.body.appendChild(a);
  a.click();
  setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 5000);
}

// ── Utils ─────────────────────────────────────────────────────
function esc(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function fmt(n) { return Number(n).toLocaleString(); }
function showError(msg) {
  document.getElementById('tbl-body').innerHTML =
    `<div class="empty" style="color:#f43f5e">❌ Error: ${esc(msg)}</div>`;
}

startJob();
</script>
</body>
</html>"""

# ─────────────────────────────────────────────────────────────
#  STARTUP (placeholder to trigger deletion of old block)

# ─────────────────────────────────────────────────────────────
#  STARTUP
# ─────────────────────────────────────────────────────────────
def startup():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Load column cache and binlog list in background
    t1 = threading.Thread(target=load_column_cache, daemon=True)
    t2 = threading.Thread(target=fetch_binlog_list, daemon=True)
    t1.start(); t2.start()
    t1.join(); t2.join()
    log(f"Server ready: http://localhost:{PORT}")

if __name__ == "__main__":
    print("=" * 60)
    print("  ADOPT Binlog Web Server v5")
    print("=" * 60)
    print(f"  Installing dependencies if needed...")

    # Try to install if missing
    missing = []
    try: import pymysql
    except: missing.append("pymysql")
    try: import pymysqlreplication
    except: missing.append("pymysqlreplication")

    if missing:
        import subprocess
        print(f"  Installing: {', '.join(missing)}")
        subprocess.run([sys.executable, "-m", "pip", "install"] + missing, check=False)

    startup()
    print(f"\n  ✅  Open in browser: http://localhost:{PORT}")
    print(f"  ⚡  Press Ctrl+C to stop\n")

    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)

