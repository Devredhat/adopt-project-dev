# ============================================================
# ADOPT Infrastructure — User Activity Dashboard (Tool 4)
# Source: adoptconvergebss.tblauditlog + tblstaffuser
# READ-ONLY — No INSERT/UPDATE/DELETE
# Port: 6000
# ============================================================

from flask import Flask, render_template_string, jsonify, request as freq
import pymysql
import threading
import time
from datetime import datetime, timedelta

app = Flask(__name__)

MYSQL_SETTINGS = {
    "host":   "102.209.31.227",
    "port":   3306,
    "user":   "clusteradmin",
    "passwd": "ADOPT@2024#WIOCC@2023",
}

def get_conn():
    return pymysql.connect(
        **MYSQL_SETTINGS,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10
    )

# ─────────────────────────────────────────────────────────────
#  API ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")

        cur.execute("SELECT COUNT(*) as cnt FROM adoptconvergebss.tblstaffuser WHERE is_delete=0")
        total_staff = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT COUNT(DISTINCT user_id) as cnt
            FROM adoptconvergebss.tblauditlog
            WHERE DATE(auditdate) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        """)
        active_7d = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT COUNT(*) as cnt
            FROM adoptconvergebss.tblauditlog
            WHERE DATE(auditdate) = %s
        """, (today,))
        today_events = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT user_name, COUNT(*) as cnt
            FROM adoptconvergebss.tblauditlog
            WHERE DATE(auditdate) = %s
            GROUP BY user_name ORDER BY cnt DESC LIMIT 1
        """, (today,))
        top_user = cur.fetchone()

        cur.execute("""
            SELECT module, COUNT(*) as cnt
            FROM adoptconvergebss.tblauditlog
            WHERE DATE(auditdate) = %s
            GROUP BY module ORDER BY cnt DESC LIMIT 8
        """, (today,))
        modules = cur.fetchall()

        cur.execute("""
            SELECT operation, COUNT(*) as cnt
            FROM adoptconvergebss.tblauditlog
            WHERE DATE(auditdate) = %s
            GROUP BY operation ORDER BY cnt DESC
        """, (today,))
        operations = cur.fetchall()

        cur.execute("""
            SELECT HOUR(auditdate) as hr, COUNT(*) as cnt
            FROM adoptconvergebss.tblauditlog
            WHERE auditdate >= %s AND auditdate < DATE_ADD(%s, INTERVAL 1 DAY)
            GROUP BY HOUR(auditdate) ORDER BY hr
        """, (today, today))
        hourly = {str(r["hr"]): r["cnt"] for r in cur.fetchall()}

        cur.close(); conn.close()
        return jsonify({
            "total_staff": total_staff,
            "active_7d": active_7d,
            "today_events": today_events,
            "top_user": top_user,
            "modules": modules,
            "operations": operations,
            "hourly": hourly,
            "today": today,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/users")
def api_users():
    search = freq.args.get("search", "").strip()
    status = freq.args.get("status", "").strip()
    try:
        conn = get_conn()
        cur  = conn.cursor()

        where = "WHERE s.is_delete=0"
        params = []
        if search:
            where += " AND (s.firstname LIKE %s OR s.lastname LIKE %s OR s.username LIKE %s OR s.email LIKE %s)"
            params += [f"%{search}%"] * 4
        if status:
            where += " AND s.sstatus = %s"
            params.append(status)

        cur.execute(f"""
            SELECT
                s.staffid, s.firstname, s.lastname, s.username,
                s.email, s.phone, s.sstatus, s.last_login_time,
                s.createdate, s.department,
                COALESCE(t.today_actions, 0) as today_actions,
                COALESCE(w.week_actions, 0) as week_actions,
                la.last_activity, la.last_module
            FROM adoptconvergebss.tblstaffuser s
            LEFT JOIN (
                SELECT user_id, COUNT(*) as today_actions
                FROM adoptconvergebss.tblauditlog
                WHERE DATE(auditdate) = CURDATE()
                GROUP BY user_id
            ) t ON t.user_id = s.staffid
            LEFT JOIN (
                SELECT user_id, COUNT(*) as week_actions
                FROM adoptconvergebss.tblauditlog
                WHERE DATE(auditdate) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                GROUP BY user_id
            ) w ON w.user_id = s.staffid
            LEFT JOIN (
                SELECT user_id,
                       MAX(auditdate) as last_activity,
                       SUBSTRING_INDEX(GROUP_CONCAT(module ORDER BY auditdate DESC), ',', 1) as last_module
                FROM adoptconvergebss.tblauditlog
                GROUP BY user_id
            ) la ON la.user_id = s.staffid
            {where}
            ORDER BY today_actions DESC, last_activity DESC
        """, params)

        users = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({"users": users, "total": len(users)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/user/<int:uid>/activity")
def api_user_activity(uid):
    date_from = freq.args.get("from", (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"))
    date_to   = freq.args.get("to",   datetime.now().strftime("%Y-%m-%d"))
    limit     = int(freq.args.get("limit", 100))
    offset    = int(freq.args.get("offset", 0))
    module_f  = freq.args.get("module", "").strip()

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # ── Get staff info first ──────────────────────────────
        cur.execute("""
            SELECT staffid, firstname, lastname, username, email,
                   phone, sstatus, last_login_time, department, createdate
            FROM adoptconvergebss.tblstaffuser
            WHERE staffid = %s
        """, [uid])
        user_info = cur.fetchone()

        # ── Find correct audit user_id by matching full name ──
        # tblauditlog.user_id may differ from tblstaffuser.staffid
        # Match by CONCAT(firstname, ' ', lastname) = user_name in audit
        audit_uid = uid  # fallback
        if user_info:
            full_name = f"{user_info['firstname']} {user_info['lastname']}".strip()
            cur.execute("""
                SELECT DISTINCT user_id FROM adoptconvergebss.tblauditlog
                WHERE user_name = %s
                ORDER BY user_id LIMIT 1
            """, [full_name])
            row = cur.fetchone()
            if row:
                audit_uid = row["user_id"]
            else:
                # fallback: try employee_name match
                cur.execute("""
                    SELECT DISTINCT user_id FROM adoptconvergebss.tblauditlog
                    WHERE employee_name = %s
                    ORDER BY user_id LIMIT 1
                """, [full_name])
                row2 = cur.fetchone()
                if row2:
                    audit_uid = row2["user_id"]

        # ── Build where clause using resolved audit_uid ───────
        where = "WHERE user_id = %s AND DATE(auditdate) BETWEEN %s AND %s"
        params = [audit_uid, date_from, date_to]
        if module_f:
            where += " AND module = %s"
            params.append(module_f)

        # Total count
        cur.execute(f"SELECT COUNT(*) as cnt FROM adoptconvergebss.tblauditlog {where}", params)
        total = cur.fetchone()["cnt"]

        # Events — with full datetime
        cur.execute(f"""
            SELECT audit_id,
                   DATE_FORMAT(auditdate, '%Y-%m-%d %H:%i:%s') as auditdate,
                   user_name, module, operation,
                   ip_address, remark, entity_ref_id
            FROM adoptconvergebss.tblauditlog
            {where}
            ORDER BY auditdate DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        events = cur.fetchall()

        # Module breakdown
        cur.execute("""
            SELECT module, COUNT(*) as cnt
            FROM adoptconvergebss.tblauditlog
            WHERE user_id = %s AND DATE(auditdate) BETWEEN %s AND %s
            GROUP BY module ORDER BY cnt DESC
        """, [audit_uid, date_from, date_to])
        modules = cur.fetchall()

        # Operation breakdown
        cur.execute("""
            SELECT operation, COUNT(*) as cnt
            FROM adoptconvergebss.tblauditlog
            WHERE user_id = %s AND DATE(auditdate) BETWEEN %s AND %s
            GROUP BY operation ORDER BY cnt DESC
        """, [audit_uid, date_from, date_to])
        operations = cur.fetchall()

        cur.close(); conn.close()
        return jsonify({
            "user": user_info,
            "events": events,
            "total": total,
            "modules": modules,
            "operations": operations,
            "date_from": date_from,
            "date_to": date_to,
            "audit_uid": audit_uid,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/live_feed")
def api_live_feed():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT audit_id,
                   DATE_FORMAT(auditdate, '%Y-%m-%d %H:%i:%s') as auditdate,
                   user_name, user_id,
                   module, operation, ip_address, remark
            FROM adoptconvergebss.tblauditlog
            ORDER BY auditdate DESC, audit_id DESC
            LIMIT 50
        """)
        events = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({"events": events})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/modules")
def api_modules():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT DISTINCT module FROM adoptconvergebss.tblauditlog WHERE module IS NOT NULL ORDER BY module")
        mods = [r["module"] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(mods)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── NEW: lookup staffid by audit user_id (for live feed clicks) ──
@app.route("/api/staffid_by_audit_uid/<int:audit_uid>")
def staffid_by_audit_uid(audit_uid):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # Get user_name from audit log
        cur.execute("""
            SELECT DISTINCT user_name FROM adoptconvergebss.tblauditlog
            WHERE user_id = %s LIMIT 1
        """, [audit_uid])
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({"staffid": audit_uid})

        user_name = row["user_name"]
        parts = user_name.strip().split(" ", 1)
        firstname = parts[0] if parts else ""
        lastname  = parts[1] if len(parts) > 1 else ""

        cur.execute("""
            SELECT staffid FROM adoptconvergebss.tblstaffuser
            WHERE firstname = %s AND lastname = %s AND is_delete = 0
            LIMIT 1
        """, [firstname, lastname])
        staff = cur.fetchone()
        cur.close(); conn.close()

        return jsonify({"staffid": staff["staffid"] if staff else audit_uid, "user_name": user_name})
    except Exception as e:
        return jsonify({"staffid": audit_uid, "error": str(e)})


# ─────────────────────────────────────────────────────────────
#  DASHBOARD HTML
# ─────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ADOPT · User Activity</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Syne:wght@600;700;800&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#060a10;--surf:#0b1119;--surf2:#101820;--surf3:#141f2a;
  --border:#192232;--border2:#1f2e40;
  --text:#7a9ab8;--bright:#d4e8f8;--muted:#2a3d52;--muted2:#3a5068;
  --green:#00e676;--blue:#38bdf8;--orange:#fb923c;--red:#f43f5e;
  --purple:#a78bfa;--yellow:#fbbf24;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'IBM Plex Mono',monospace;min-height:100vh}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
header{display:flex;align-items:center;justify-content:space-between;padding:0 24px;height:52px;
  background:var(--surf);border-bottom:1px solid var(--border2);position:sticky;top:0;z-index:100}
.logo{font-family:'Syne',sans-serif;font-weight:800;font-size:16px;color:#fff;
  display:flex;align-items:center;gap:10px;letter-spacing:1px}
.logo-icon{width:28px;height:28px;background:linear-gradient(135deg,var(--green),#0ea5e9);
  border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:13px}
.logo-sub{font-size:9px;color:var(--muted2);font-weight:400;font-family:'IBM Plex Mono',monospace;letter-spacing:2px}
.hbadges{display:flex;gap:8px}
.badge{padding:3px 10px;border-radius:20px;font-size:9px;font-weight:600;letter-spacing:.5px}
.b-live{background:#00e67615;color:var(--green);border:1px solid #00e67630;display:flex;align-items:center;gap:5px}
.b-ro{background:#f43f5e15;color:var(--red);border:1px solid #f43f5e30}
.pulse{width:5px;height:5px;border-radius:50%;background:var(--green);animation:blink 1.4s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
.layout{display:flex;height:calc(100vh - 52px);overflow:hidden}
.sidebar{width:320px;flex-shrink:0;background:var(--surf);border-right:1px solid var(--border);
  display:flex;flex-direction:column;overflow:hidden}
.main{flex:1;overflow-y:auto;background:var(--bg)}
.sb-head{padding:14px 16px;border-bottom:1px solid var(--border);flex-shrink:0}
.sb-title{font-family:'Syne',sans-serif;font-size:11px;font-weight:700;color:#fff;
  text-transform:uppercase;letter-spacing:1px;margin-bottom:10px}
.sb-search{width:100%;background:var(--surf2);border:1px solid var(--border2);
  color:var(--bright);font-family:inherit;font-size:10px;padding:7px 10px;
  border-radius:6px;outline:none;transition:border-color .15s}
.sb-search:focus{border-color:var(--green)}
.sb-filters{display:flex;gap:5px;margin-top:8px}
.sf{font-size:9px;padding:3px 8px;border-radius:4px;border:1px solid var(--border2);
  background:transparent;color:var(--muted2);cursor:pointer;font-family:inherit;transition:all .15s}
.sf:hover,.sf.active{border-color:var(--green);color:var(--green);background:#00e67610}
.sb-list{flex:1;overflow-y:auto}
.user-row{padding:10px 16px;border-bottom:1px solid var(--border);cursor:pointer;
  transition:background .1s;display:flex;align-items:center;gap:10px}
.user-row:hover{background:var(--surf2)}
.user-row.active{background:var(--surf3);border-left:2px solid var(--green)}
.user-avatar{width:32px;height:32px;border-radius:8px;display:flex;align-items:center;
  justify-content:center;font-size:11px;font-weight:700;flex-shrink:0;font-family:'Syne',sans-serif}
.user-info{flex:1;min-width:0}
.user-name{font-size:11px;color:var(--bright);font-weight:600;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.user-role{font-size:9px;color:var(--muted2);margin-top:1px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.user-badge{flex-shrink:0;font-size:9px;padding:2px 6px;border-radius:4px;font-weight:600}
.ub-today{background:#38bdf815;color:var(--blue)}
.ub-none{background:#2a3d5230;color:var(--muted2)}
.main-inner{padding:20px 24px}
.stats-row{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;margin-bottom:20px}
.stat-card{background:var(--surf);border:1px solid var(--border);border-radius:10px;padding:14px 16px}
.stat-label{font-size:9px;color:var(--muted2);text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px}
.stat-val{font-size:26px;font-weight:700;font-family:'Syne',sans-serif;line-height:1}
.stat-sub{font-size:9px;color:var(--muted2);margin-top:4px}
.panel{background:var(--surf);border:1px solid var(--border);border-radius:10px;margin-bottom:16px;overflow:hidden}
.panel-head{padding:12px 16px;border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
.panel-title{font-family:'Syne',sans-serif;font-size:12px;font-weight:700;color:#fff}
.panel-body{max-height:420px;overflow-y:auto}
.user-detail-head{background:var(--surf2);padding:18px 20px;border-bottom:1px solid var(--border);
  display:flex;align-items:flex-start;gap:16px;flex-wrap:wrap}
.udh-avatar{width:52px;height:52px;border-radius:12px;display:flex;align-items:center;
  justify-content:center;font-size:18px;font-weight:800;font-family:'Syne',sans-serif;flex-shrink:0}
.udh-info{flex:1}
.udh-name{font-family:'Syne',sans-serif;font-size:18px;font-weight:800;color:#fff;line-height:1.2}
.udh-username{font-size:10px;color:var(--muted2);margin-top:3px}
.udh-meta{display:flex;gap:10px;margin-top:8px;flex-wrap:wrap}
.udh-tag{font-size:9px;padding:2px 8px;border-radius:4px;border:1px solid}
.act-table{width:100%;border-collapse:collapse;font-size:10px}
.act-table th{padding:8px 12px;text-align:left;color:var(--muted2);font-size:9px;
  text-transform:uppercase;letter-spacing:.6px;background:var(--surf2);
  border-bottom:1px solid var(--border);position:sticky;top:0}
.act-table td{padding:8px 12px;border-bottom:1px solid var(--border);vertical-align:top}
.act-table tr:hover td{background:#ffffff04}
.op-badge{padding:2px 7px;border-radius:4px;font-size:9px;font-weight:600}
.op-view{background:#38bdf815;color:var(--blue)}
.op-add{background:#00e67615;color:var(--green)}
.op-update{background:#fbbf2415;color:var(--yellow)}
.op-delete{background:#f43f5e15;color:var(--red)}
.op-login{background:#a78bfa15;color:var(--purple)}
.op-default{background:#2a3d52;color:var(--muted2)}
.mod-tag{padding:2px 7px;border-radius:4px;font-size:9px;background:var(--surf3);
  color:var(--text);border:1px solid var(--border2)}
.mod-bar-row{display:flex;align-items:center;gap:10px;padding:8px 16px;
  border-bottom:1px solid var(--border);font-size:10px}
.mod-bar-name{width:130px;color:var(--bright);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mod-bar-wrap{flex:1;height:6px;background:var(--border);border-radius:3px;overflow:hidden}
.mod-bar-fill{height:6px;border-radius:3px;transition:width .4s}
.mod-bar-cnt{width:50px;text-align:right;color:var(--muted2)}
.hour-chart{display:flex;align-items:flex-end;gap:3px;height:60px;padding:8px 16px 4px}
.hour-bar-wrap{flex:1;display:flex;flex-direction:column;align-items:center;gap:2px}
.hour-bar{width:100%;background:var(--green);border-radius:2px 2px 0 0;
  min-height:2px;transition:height .3s;opacity:.7}
.hour-bar:hover{opacity:1}
.hour-lbl{font-size:7px;color:var(--muted);margin-top:3px}
.feed-row{display:flex;gap:10px;padding:8px 16px;border-bottom:1px solid var(--border);
  font-size:10px;align-items:flex-start;cursor:pointer;transition:background .1s}
.feed-row:hover{background:var(--surf2)}
.feed-time{color:var(--muted2);white-space:nowrap;font-size:9px;margin-top:1px;min-width:135px}
.feed-user{color:var(--bright);font-weight:600;white-space:nowrap;overflow:hidden;
  text-overflow:ellipsis;min-width:120px;max-width:140px}
.feed-detail{color:var(--text);flex:1;min-width:0}
.pager{padding:8px 16px;display:flex;align-items:center;justify-content:space-between;
  border-top:1px solid var(--border);font-size:9px;color:var(--muted2)}
.pager-btns{display:flex;gap:6px}
.pbtn{background:transparent;border:1px solid var(--border2);color:var(--muted2);
  font-family:inherit;font-size:9px;padding:3px 8px;border-radius:4px;cursor:pointer;transition:all .15s}
.pbtn:hover:not(:disabled){border-color:var(--green);color:var(--green)}
.pbtn:disabled{opacity:.3;cursor:default}
.flt-row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.flt-input{background:var(--surf2);border:1px solid var(--border2);color:var(--bright);
  font-family:inherit;font-size:10px;padding:5px 10px;border-radius:6px;outline:none}
.flt-input:focus{border-color:var(--green)}
.home-view{padding:0}
.ops-grid{display:grid;grid-template-columns:1fr 1fr;gap:0}
.ops-item{padding:10px 16px;border-bottom:1px solid var(--border);
  border-right:1px solid var(--border);display:flex;align-items:center;
  justify-content:space-between;font-size:10px}
.ops-item:nth-child(2n){border-right:none}
.empty{padding:40px;text-align:center;color:var(--muted2);font-size:11px}
.loading{display:flex;align-items:center;justify-content:center;padding:30px;gap:10px;color:var(--muted2);font-size:11px}
.spin{width:20px;height:20px;border:2px solid var(--border);border-top-color:var(--green);
  border-radius:50%;animation:spin .7s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
/* detail info box */
.detail-box{background:var(--surf3);border:1px solid var(--border2);border-radius:8px;
  padding:10px 14px;font-size:9px;color:var(--muted2);margin-top:6px}
.detail-box span{color:var(--bright)}
</style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-icon">👤</div>
    <div>
      <div>ADOPT<span style="color:var(--green)">.</span>USERS</div>
      <div class="logo-sub">ACTIVITY DASHBOARD</div>
    </div>
  </div>
  <div class="hbadges">
    <span class="badge b-live"><span class="pulse"></span>LIVE</span>
    <span class="badge b-ro">READ ONLY</span>
    <span class="badge" style="background:#fbbf2415;color:var(--yellow);border:1px solid #fbbf2430" id="total-badge">— STAFF</span>
  </div>
</header>

<div class="layout">
  <div class="sidebar">
    <div class="sb-head">
      <div class="sb-title">Staff Users</div>
      <input class="sb-search" id="user-search" placeholder="🔍 Search name, username..." oninput="filterUsers()">
      <div class="sb-filters">
        <button class="sf active" onclick="setStatusFilter(event,'')">All</button>
        <button class="sf" onclick="setStatusFilter(event,'ACTIVE')">Active</button>
        <button class="sf" onclick="setStatusFilter(event,'INACTIVE')">Inactive</button>
      </div>
    </div>
    <div class="sb-list" id="user-list">
      <div class="loading"><div class="spin"></div>Loading users...</div>
    </div>
  </div>

  <div class="main">
    <!-- HOME VIEW -->
    <div id="view-home" class="main-inner home-view">
      <div class="stats-row" id="stats-row">
        <div class="loading"><div class="spin"></div></div>
      </div>

      <div class="panel">
        <div class="panel-head">
          <div class="panel-title">📊 Today's Activity by Hour</div>
          <span style="font-size:9px;color:var(--muted2)" id="today-lbl">—</span>
        </div>
        <div id="hour-chart-wrap"></div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">
        <div class="panel">
          <div class="panel-head"><div class="panel-title">🗂 Module Activity (Today)</div></div>
          <div id="module-bars"></div>
        </div>
        <div class="panel">
          <div class="panel-head"><div class="panel-title">⚡ Operations (Today)</div></div>
          <div class="ops-grid" id="ops-grid"></div>
        </div>
      </div>

      <div class="panel">
        <div class="panel-head">
          <div class="panel-title">🔴 Live Activity Feed</div>
          <button class="pbtn" onclick="loadLiveFeed()">↺ Refresh</button>
        </div>
        <div class="panel-body" id="live-feed">
          <div class="loading"><div class="spin"></div>Loading...</div>
        </div>
      </div>
    </div>

    <!-- USER DETAIL VIEW -->
    <div id="view-user" style="display:none">
      <div id="user-detail-head"></div>
      <div class="main-inner" style="padding-top:16px">
        <div class="stats-row" id="user-stats-row"></div>

        <div class="panel" style="margin-bottom:16px">
          <div class="panel-head">
            <div class="panel-title">🔍 Filter Activity</div>
            <div class="flt-row">
              <span style="font-size:9px;color:var(--muted2)">From:</span>
              <input type="date" class="flt-input" id="date-from">
              <span style="font-size:9px;color:var(--muted2)">To:</span>
              <input type="date" class="flt-input" id="date-to">
              <select class="flt-input" id="mod-filter" onchange="loadUserActivity()">
                <option value="">All Modules</option>
              </select>
              <button class="pbtn" onclick="loadUserActivity()" style="border-color:var(--green);color:var(--green)">Apply</button>
              <button class="pbtn" onclick="showHome()">← Back</button>
            </div>
          </div>
        </div>

        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">
          <div class="panel">
            <div class="panel-head"><div class="panel-title">🗂 Modules Used</div></div>
            <div id="user-module-bars"></div>
          </div>
          <div class="panel">
            <div class="panel-head"><div class="panel-title">⚡ Operations</div></div>
            <div class="ops-grid" id="user-ops-grid"></div>
          </div>
        </div>

        <div class="panel">
          <div class="panel-head">
            <div class="panel-title">📋 Activity Log</div>
            <span style="font-size:9px;color:var(--muted2)" id="act-total-lbl"></span>
          </div>
          <div class="panel-body">
            <table class="act-table">
              <thead>
                <tr>
                  <th>Date &amp; Time</th>
                  <th>Module</th>
                  <th>Operation</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody id="act-tbody"></tbody>
            </table>
          </div>
          <div class="pager" id="act-pager"></div>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
let allUsers = [];
let filteredUsers = [];
let statusFilter = '';
let activeUserId = null;
let actOffset = 0;
const actLimit = 100;
let actTotal = 0;

const COLORS = ['#00e676','#38bdf8','#fb923c','#a78bfa','#fbbf24','#f43f5e','#34d399','#818cf8'];
function avatarColor(id){ return COLORS[Math.abs(id) % COLORS.length]; }
function initials(fn, ln){ return ((fn||'?')[0]+(ln||'?')[0]).toUpperCase(); }

function fmtTime(v){
  // handles both "2026-03-11 12:34:56" and "2026-03-11T12:34:56" and Date objects
  if(!v) return '—';
  return String(v).replace('T',' ').slice(0,19);
}

function opClass(op){
  if(!op) return 'op-default';
  const o = op.toLowerCase();
  if(o.includes('view')) return 'op-view';
  if(o.includes('add')||o.includes('creat')||o.includes('insert')) return 'op-add';
  if(o.includes('update')||o.includes('edit')||o.includes('modif')) return 'op-update';
  if(o.includes('delet')||o.includes('remov')) return 'op-delete';
  if(o.includes('login')||o.includes('logout')) return 'op-login';
  return 'op-default';
}

// ── Stats ──────────────────────────────────────────────────
async function loadStats(){
  try{
    const s = await fetch('/api/stats').then(r=>r.json());
    if(s.error){ console.error(s.error); return; }
    document.getElementById('today-lbl').textContent = s.today;
    document.getElementById('total-badge').textContent = `${s.total_staff} STAFF`;
    document.getElementById('stats-row').innerHTML = `
      <div class="stat-card">
        <div class="stat-label">Total Staff</div>
        <div class="stat-val" style="color:#fff">${s.total_staff}</div>
        <div class="stat-sub">registered users</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Active (7 days)</div>
        <div class="stat-val" style="color:var(--green)">${s.active_7d}</div>
        <div class="stat-sub">unique users</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Today's Events</div>
        <div class="stat-val" style="color:var(--blue)">${s.today_events.toLocaleString()}</div>
        <div class="stat-sub">${s.today}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Most Active Today</div>
        <div class="stat-val" style="color:var(--orange);font-size:13px;margin-top:4px">${s.top_user?s.top_user.user_name:'—'}</div>
        <div class="stat-sub">${s.top_user?s.top_user.cnt+' actions':''}</div>
      </div>`;

    const maxH = Math.max(...Object.values(s.hourly||{}), 1);
    let hhtml = '<div class="hour-chart">';
    for(let i=0;i<24;i++){
      const cnt = s.hourly[String(i)]||0;
      const pct = Math.max(4, Math.round(cnt/maxH*100));
      hhtml += `<div class="hour-bar-wrap" title="${i}:00 — ${cnt} events">
        <div class="hour-bar" style="height:${pct}%"></div>
        <div class="hour-lbl">${i%4===0?i:''}</div>
      </div>`;
    }
    hhtml += '</div>';
    document.getElementById('hour-chart-wrap').innerHTML = hhtml;

    const maxM = Math.max(...(s.modules||[]).map(m=>m.cnt), 1);
    document.getElementById('module-bars').innerHTML =
      (s.modules||[]).map((m,i)=>`
        <div class="mod-bar-row">
          <div class="mod-bar-name">${m.module||'Unknown'}</div>
          <div class="mod-bar-wrap"><div class="mod-bar-fill" style="width:${Math.round(m.cnt/maxM*100)}%;background:${COLORS[i%COLORS.length]}"></div></div>
          <div class="mod-bar-cnt">${m.cnt}</div>
        </div>`).join('')||'<div class="empty">No data</div>';

    document.getElementById('ops-grid').innerHTML =
      (s.operations||[]).map(o=>`
        <div class="ops-item">
          <span class="op-badge ${opClass(o.operation)}">${o.operation||'Unknown'}</span>
          <span style="color:var(--bright);font-weight:600">${o.cnt}</span>
        </div>`).join('')||'<div class="empty">No data</div>';

  } catch(e){ console.error(e); }
}

// ── Users ──────────────────────────────────────────────────
async function loadUsers(){
  try{
    const resp = await fetch('/api/users').then(r=>r.json());
    allUsers = resp.users || [];
    filterUsers();
  } catch(e){ console.error(e); }
}

function filterUsers(){
  const q = document.getElementById('user-search').value.toLowerCase();
  filteredUsers = allUsers.filter(u => {
    const matchStatus = !statusFilter || u.sstatus === statusFilter;
    const matchSearch = !q ||
      (u.firstname||'').toLowerCase().includes(q) ||
      (u.lastname||'').toLowerCase().includes(q) ||
      (u.username||'').toLowerCase().includes(q) ||
      (u.email||'').toLowerCase().includes(q);
    return matchStatus && matchSearch;
  });
  renderUserList();
}

function setStatusFilter(ev, s){
  statusFilter = s;
  document.querySelectorAll('.sf').forEach(b=>b.classList.remove('active'));
  ev.target.classList.add('active');
  filterUsers();
}

function renderUserList(){
  const el = document.getElementById('user-list');
  if(!filteredUsers.length){
    el.innerHTML = '<div class="empty">No users found</div>';
    return;
  }
  el.innerHTML = filteredUsers.map(u => {
    const color = avatarColor(u.staffid);
    const name = `${u.firstname||''} ${u.lastname||''}`.trim();
    const todayBadge = u.today_actions > 0
      ? `<span class="user-badge ub-today">${u.today_actions}</span>`
      : `<span class="user-badge ub-none">0</span>`;
    return `
      <div class="user-row ${activeUserId===u.staffid?'active':''}" onclick="openUser(${u.staffid})">
        <div class="user-avatar" style="background:${color}20;color:${color};border:1px solid ${color}40">
          ${initials(u.firstname, u.lastname)}
        </div>
        <div class="user-info">
          <div class="user-name">${name||u.username}</div>
          <div class="user-role">${u.username}</div>
        </div>
        ${todayBadge}
      </div>`;
  }).join('');
}

// ── Live Feed ──────────────────────────────────────────────
async function loadLiveFeed(){
  try{
    const resp = await fetch('/api/live_feed').then(r=>r.json());
    const el = document.getElementById('live-feed');
    if(!resp.events||!resp.events.length){
      el.innerHTML = '<div class="empty">No recent activity</div>';
      return;
    }
    el.innerHTML = resp.events.map(e => `
      <div class="feed-row" onclick="openUserByAuditUid(${e.user_id})">
        <div class="feed-time">${fmtTime(e.auditdate)}</div>
        <div class="feed-user">${e.user_name||'—'}</div>
        <div class="feed-detail">
          <span class="mod-tag">${e.module||'—'}</span>
          <span class="op-badge ${opClass(e.operation)}" style="margin-left:5px">${e.operation||'—'}</span>
          <span style="color:var(--muted2);margin-left:6px;font-size:9px">${(e.remark||'').slice(0,70)}</span>
        </div>
      </div>`).join('');
  } catch(e){ console.error(e); }
}

// ── Open user from live feed (audit_uid → staffid) ─────────
async function openUserByAuditUid(audit_uid){
  try{
    const r = await fetch(`/api/staffid_by_audit_uid/${audit_uid}`).then(r=>r.json());
    openUser(r.staffid);
  } catch(e){
    openUser(audit_uid);
  }
}

// ── User Detail ────────────────────────────────────────────
async function openUser(uid){
  activeUserId = uid;
  renderUserList();
  document.getElementById('view-home').style.display = 'none';
  document.getElementById('view-user').style.display = 'block';

  const today = new Date().toISOString().split('T')[0];
  const week  = new Date(Date.now()-7*86400000).toISOString().split('T')[0];
  document.getElementById('date-from').value = week;
  document.getElementById('date-to').value   = today;

  try{
    const mods = await fetch('/api/modules').then(r=>r.json());
    const sel = document.getElementById('mod-filter');
    sel.innerHTML = '<option value="">All Modules</option>' +
      mods.map(m=>`<option value="${m}">${m}</option>`).join('');
  } catch(e){}

  actOffset = 0;
  await loadUserActivity();
}

async function loadUserActivity(){
  const uid  = activeUserId;
  const from = document.getElementById('date-from').value;
  const to   = document.getElementById('date-to').value;
  const mod  = document.getElementById('mod-filter').value;

  // Show loading in table
  document.getElementById('act-tbody').innerHTML =
    `<tr><td colspan="4"><div class="loading"><div class="spin"></div>Loading...</div></td></tr>`;

  try{
    const resp = await fetch(
      `/api/user/${uid}/activity?from=${from}&to=${to}&module=${encodeURIComponent(mod)}&limit=${actLimit}&offset=${actOffset}`
    ).then(r=>r.json());

    if(resp.error){ console.error(resp.error); return; }
    actTotal = resp.total;

    const u = resp.user || {};
    const color = avatarColor(u.staffid||0);
    const name  = `${u.firstname||''} ${u.lastname||''}`.trim();

    // Header
    document.getElementById('user-detail-head').innerHTML = `
      <div class="user-detail-head">
        <div class="udh-avatar" style="background:${color}20;color:${color};border:1px solid ${color}40">
          ${initials(u.firstname, u.lastname)}
        </div>
        <div class="udh-info">
          <div class="udh-name">${name||u.username||'—'}</div>
          <div class="udh-username">@${u.username||'—'} · ${u.email||'—'}</div>
          <div class="udh-meta">
            <span class="udh-tag" style="color:${u.sstatus==='ACTIVE'?'var(--green)':'var(--red)'};border-color:${u.sstatus==='ACTIVE'?'#00e67640':'#f43f5e40'}">${u.sstatus||'—'}</span>
            ${u.department?`<span class="udh-tag" style="color:var(--blue);border-color:#38bdf840">${u.department}</span>`:''}
            ${u.phone?`<span class="udh-tag" style="color:var(--muted2);border-color:var(--border2)">📞 ${u.phone}</span>`:''}
            ${u.last_login_time?`<span class="udh-tag" style="color:var(--yellow);border-color:#fbbf2440">Last Login: ${fmtTime(u.last_login_time)}</span>`:''}
          </div>
          <div class="detail-box" style="margin-top:8px">
            DB: <span>adoptconvergebss</span> · Table: <span>tblauditlog</span> ·
            Audit UID: <span>${resp.audit_uid}</span> · Staff ID: <span>${u.staffid||'—'}</span>
          </div>
        </div>
        <div>
          <div class="stat-card" style="min-width:120px;text-align:center">
            <div class="stat-label">This Period</div>
            <div class="stat-val" style="color:var(--green)">${actTotal.toLocaleString()}</div>
            <div class="stat-sub">actions</div>
          </div>
        </div>
      </div>`;

    // Mini stats
    document.getElementById('user-stats-row').innerHTML = `
      <div class="stat-card">
        <div class="stat-label">Total in Range</div>
        <div class="stat-val" style="color:var(--green)">${actTotal.toLocaleString()}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Modules Used</div>
        <div class="stat-val" style="color:var(--blue)">${resp.modules.length}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Operation Types</div>
        <div class="stat-val" style="color:var(--orange)">${resp.operations.length}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Date Range</div>
        <div class="stat-val" style="color:var(--text);font-size:11px;margin-top:4px">${from}</div>
        <div class="stat-sub">→ ${to}</div>
      </div>`;

    // Module bars
    const maxM = Math.max(...(resp.modules||[]).map(m=>m.cnt),1);
    document.getElementById('user-module-bars').innerHTML =
      (resp.modules||[]).map((m,i)=>`
        <div class="mod-bar-row">
          <div class="mod-bar-name">${m.module||'—'}</div>
          <div class="mod-bar-wrap"><div class="mod-bar-fill" style="width:${Math.round(m.cnt/maxM*100)}%;background:${COLORS[i%COLORS.length]}"></div></div>
          <div class="mod-bar-cnt">${m.cnt}</div>
        </div>`).join('')||'<div class="empty">No data</div>';

    // Ops
    document.getElementById('user-ops-grid').innerHTML =
      (resp.operations||[]).map(o=>`
        <div class="ops-item">
          <span class="op-badge ${opClass(o.operation)}">${o.operation||'—'}</span>
          <span style="color:var(--bright);font-weight:600">${o.cnt}</span>
        </div>`).join('')||'<div class="empty">No data</div>';

    // Activity label
    document.getElementById('act-total-lbl').textContent =
      `Showing ${actOffset+1}–${Math.min(actOffset+actLimit,actTotal)} of ${actTotal.toLocaleString()}`;

    // Table
    const tbody = document.getElementById('act-tbody');
    if(!resp.events.length){
      tbody.innerHTML = `<tr><td colspan="4" class="empty">No activity found for this date range</td></tr>`;
    } else {
      tbody.innerHTML = resp.events.map(e => `
        <tr>
          <td style="color:var(--muted2);white-space:nowrap;font-size:9px">${fmtTime(e.auditdate)}</td>
          <td><span class="mod-tag">${e.module||'—'}</span></td>
          <td><span class="op-badge ${opClass(e.operation)}">${e.operation||'—'}</span></td>
          <td style="color:var(--muted2);font-size:9px;max-width:300px;word-break:break-word">${(e.remark||'').slice(0,100)}</td>
        </tr>`).join('');
    }

    // Pager
    const maxPage = Math.ceil(actTotal/actLimit)||1;
    const curPage = Math.floor(actOffset/actLimit)+1;
    document.getElementById('act-pager').innerHTML = `
      <span>${actTotal.toLocaleString()} total events</span>
      <div class="pager-btns">
        <button class="pbtn" onclick="changePage(-1)" ${actOffset===0?'disabled':''}>← Prev</button>
        <span style="color:var(--bright);padding:0 8px">Page ${curPage}/${maxPage}</span>
        <button class="pbtn" onclick="changePage(1)" ${actOffset+actLimit>=actTotal?'disabled':''}>Next →</button>
      </div>`;

  } catch(e){ console.error(e); }
}

function changePage(dir){
  actOffset = Math.max(0, actOffset + dir*actLimit);
  loadUserActivity();
}

function showHome(){
  activeUserId = null;
  renderUserList();
  document.getElementById('view-home').style.display = 'block';
  document.getElementById('view-user').style.display = 'none';
}

// ── Init ───────────────────────────────────────────────────
loadStats();
loadUsers();
loadLiveFeed();
setInterval(loadLiveFeed, 15000);
setInterval(loadStats, 60000);
</script>
</body>
</html>"""

@app.route("/")
def dashboard():
    return render_template_string(HTML)

if __name__ == "__main__":
    print("\n  [Tool 4] User Activity Dashboard → port 6000")
    app.run(host="0.0.0.0", port=6000, debug=False, use_reloader=False, threaded=True)
