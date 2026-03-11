# ============================================================
# ADOPT Infrastructure — User Activity Dashboard (Tool 4)
# Source: adoptconvergebss.tblauditlog + tblstaffuser
# READ-ONLY — No INSERT/UPDATE/DELETE
# Port: 6000
# ============================================================

from flask import Flask, render_template_string, jsonify, request as freq
import pymysql
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
#  API
# ─────────────────────────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    try:
        conn = get_conn(); cur = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")

        cur.execute("SELECT COUNT(*) as cnt FROM adoptconvergebss.tblstaffuser WHERE is_delete=0")
        total_staff = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(DISTINCT user_id) as cnt FROM adoptconvergebss.tblauditlog WHERE DATE(auditdate) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)")
        active_7d = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM adoptconvergebss.tblauditlog WHERE DATE(auditdate) = %s", (today,))
        today_events = cur.fetchone()["cnt"]

        cur.execute("SELECT user_name, COUNT(*) as cnt FROM adoptconvergebss.tblauditlog WHERE DATE(auditdate) = %s GROUP BY user_name ORDER BY cnt DESC LIMIT 1", (today,))
        top_user = cur.fetchone()

        cur.execute("SELECT module, COUNT(*) as cnt FROM adoptconvergebss.tblauditlog WHERE DATE(auditdate) = %s GROUP BY module ORDER BY cnt DESC LIMIT 8", (today,))
        modules = cur.fetchall()

        cur.execute("SELECT operation, COUNT(*) as cnt FROM adoptconvergebss.tblauditlog WHERE DATE(auditdate) = %s GROUP BY operation ORDER BY cnt DESC", (today,))
        operations = cur.fetchall()

        # check if time data exists
        cur.execute("SELECT DATE_FORMAT(MAX(auditdate),'%%H:%%i:%%s') as maxtime FROM adoptconvergebss.tblauditlog WHERE DATE(auditdate) = %s", (today,))
        trow = cur.fetchone()
        has_time = trow and trow["maxtime"] and trow["maxtime"] != "00:00:00"

        cur.execute("""
            SELECT HOUR(auditdate) as hr, COUNT(*) as cnt
            FROM adoptconvergebss.tblauditlog
            WHERE DATE(auditdate) = %s
            GROUP BY HOUR(auditdate) ORDER BY hr
        """, (today,))
        hourly = {str(r["hr"]): r["cnt"] for r in cur.fetchall()}

        cur.close(); conn.close()
        return jsonify({
            "total_staff": total_staff, "active_7d": active_7d,
            "today_events": today_events, "top_user": top_user,
            "modules": modules, "operations": operations,
            "hourly": hourly, "today": today, "has_time": has_time,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/users")
def api_users():
    search = freq.args.get("search","").strip()
    status = freq.args.get("status","").strip()
    try:
        conn = get_conn(); cur = conn.cursor()
        where = "WHERE s.is_delete=0"
        params = []
        if search:
            where += " AND (s.firstname LIKE %s OR s.lastname LIKE %s OR s.username LIKE %s OR s.email LIKE %s)"
            params += [f"%{search}%"]*4
        if status:
            where += " AND s.sstatus = %s"
            params.append(status)

        cur.execute(f"""
            SELECT s.staffid, s.firstname, s.lastname, s.username,
                s.email, s.phone, s.sstatus, s.last_login_time,
                s.createdate, s.department,
                COALESCE(t.today_actions,0) as today_actions,
                COALESCE(w.week_actions,0) as week_actions,
                la.last_activity, la.last_module
            FROM adoptconvergebss.tblstaffuser s
            LEFT JOIN (
                SELECT user_id, COUNT(*) as today_actions
                FROM adoptconvergebss.tblauditlog
                WHERE DATE(auditdate)=CURDATE() GROUP BY user_id
            ) t ON t.user_id=s.staffid
            LEFT JOIN (
                SELECT user_id, COUNT(*) as week_actions
                FROM adoptconvergebss.tblauditlog
                WHERE DATE(auditdate)>=DATE_SUB(CURDATE(),INTERVAL 7 DAY) GROUP BY user_id
            ) w ON w.user_id=s.staffid
            LEFT JOIN (
                SELECT user_id, MAX(auditdate) as last_activity,
                    SUBSTRING_INDEX(GROUP_CONCAT(module ORDER BY auditdate DESC),',',1) as last_module
                FROM adoptconvergebss.tblauditlog GROUP BY user_id
            ) la ON la.user_id=s.staffid
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
    date_from = freq.args.get("from", (datetime.now()-timedelta(days=7)).strftime("%Y-%m-%d"))
    date_to   = freq.args.get("to", datetime.now().strftime("%Y-%m-%d"))
    limit     = int(freq.args.get("limit", 100))
    offset    = int(freq.args.get("offset", 0))
    module_f  = freq.args.get("module","").strip()

    try:
        conn = get_conn(); cur = conn.cursor()

        # Get staff info
        cur.execute("""
            SELECT staffid, firstname, lastname, username, email,
                   phone, sstatus, last_login_time, department, createdate
            FROM adoptconvergebss.tblstaffuser WHERE staffid=%s
        """, [uid])
        user_info = cur.fetchone()

        # Resolve audit_uid by matching full name in audit log
        audit_uid = uid
        if user_info:
            full_name = f"{user_info['firstname']} {user_info['lastname']}".strip()
            cur.execute("SELECT DISTINCT user_id FROM adoptconvergebss.tblauditlog WHERE user_name=%s LIMIT 1", [full_name])
            row = cur.fetchone()
            if row:
                audit_uid = row["user_id"]
            else:
                # try employee_name fallback
                cur.execute("SELECT DISTINCT user_id FROM adoptconvergebss.tblauditlog WHERE employee_name=%s LIMIT 1", [full_name])
                row2 = cur.fetchone()
                if row2:
                    audit_uid = row2["user_id"]
                # else keep staffid as-is

        where = "WHERE user_id=%s AND DATE(auditdate) BETWEEN %s AND %s"
        params = [audit_uid, date_from, date_to]
        if module_f:
            where += " AND module=%s"
            params.append(module_f)

        cur.execute(f"SELECT COUNT(*) as cnt FROM adoptconvergebss.tblauditlog {where}", params)
        total = cur.fetchone()["cnt"]

        cur.execute(f"""
            SELECT audit_id,
                   DATE_FORMAT(auditdate,'%%Y-%%m-%%d %%H:%%i:%%s') as auditdate,
                   user_name, module, operation, ip_address, remark, entity_ref_id
            FROM adoptconvergebss.tblauditlog
            {where}
            ORDER BY audit_id DESC
            LIMIT %s OFFSET %s
        """, params+[limit, offset])
        events = cur.fetchall()

        cur.execute("SELECT module, COUNT(*) as cnt FROM adoptconvergebss.tblauditlog WHERE user_id=%s AND DATE(auditdate) BETWEEN %s AND %s GROUP BY module ORDER BY cnt DESC", [audit_uid, date_from, date_to])
        modules = cur.fetchall()

        cur.execute("SELECT operation, COUNT(*) as cnt FROM adoptconvergebss.tblauditlog WHERE user_id=%s AND DATE(auditdate) BETWEEN %s AND %s GROUP BY operation ORDER BY cnt DESC", [audit_uid, date_from, date_to])
        operations = cur.fetchall()

        cur.close(); conn.close()
        return jsonify({
            "user": user_info, "events": events, "total": total,
            "modules": modules, "operations": operations,
            "date_from": date_from, "date_to": date_to,
            "audit_uid": audit_uid,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/live_feed")
def api_live_feed():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT audit_id,
                   DATE_FORMAT(auditdate,'%%Y-%%m-%%d %%H:%%i:%%s') as auditdate,
                   user_name, user_id, module, operation, ip_address, remark
            FROM adoptconvergebss.tblauditlog
            ORDER BY audit_id DESC
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
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT DISTINCT module FROM adoptconvergebss.tblauditlog WHERE module IS NOT NULL ORDER BY module")
        mods = [r["module"] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(mods)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/staffid_by_audit_uid/<int:audit_uid>")
def staffid_by_audit_uid(audit_uid):
    try:
        conn = get_conn(); cur = conn.cursor()
        # get user_name from audit
        cur.execute("SELECT DISTINCT user_name FROM adoptconvergebss.tblauditlog WHERE user_id=%s LIMIT 1", [audit_uid])
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({"staffid": audit_uid, "user_name": ""})
        user_name = row["user_name"]
        parts = user_name.strip().split(" ", 1)
        fn = parts[0] if parts else ""
        ln = parts[1] if len(parts) > 1 else ""
        cur.execute("SELECT staffid FROM adoptconvergebss.tblstaffuser WHERE firstname=%s AND lastname=%s AND is_delete=0 LIMIT 1", [fn, ln])
        staff = cur.fetchone()
        cur.close(); conn.close()
        return jsonify({"staffid": staff["staffid"] if staff else audit_uid, "user_name": user_name})
    except Exception as e:
        return jsonify({"staffid": audit_uid, "error": str(e)})


# ─────────────────────────────────────────────────────────────
#  HTML
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
::-webkit-scrollbar{width:4px;height:4px}::-webkit-scrollbar-track{background:var(--bg)}::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
header{display:flex;align-items:center;justify-content:space-between;padding:0 24px;height:52px;
  background:var(--surf);border-bottom:1px solid var(--border2);position:sticky;top:0;z-index:100}
.logo{font-family:'Syne',sans-serif;font-weight:800;font-size:16px;color:#fff;display:flex;align-items:center;gap:10px;letter-spacing:1px}
.logo-icon{width:28px;height:28px;background:linear-gradient(135deg,var(--green),#0ea5e9);border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:13px}
.logo-sub{font-size:9px;color:var(--muted2);font-weight:400;letter-spacing:2px}
.hbadges{display:flex;gap:8px}
.badge{padding:3px 10px;border-radius:20px;font-size:9px;font-weight:600;letter-spacing:.5px}
.b-live{background:#00e67615;color:var(--green);border:1px solid #00e67630;display:flex;align-items:center;gap:5px}
.b-ro{background:#f43f5e15;color:var(--red);border:1px solid #f43f5e30}
.pulse{width:5px;height:5px;border-radius:50%;background:var(--green);animation:blink 1.4s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
.layout{display:flex;height:calc(100vh - 52px);overflow:hidden}
.sidebar{width:300px;flex-shrink:0;background:var(--surf);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.main{flex:1;overflow-y:auto;background:var(--bg)}
.sb-head{padding:14px 16px;border-bottom:1px solid var(--border);flex-shrink:0}
.sb-title{font-family:'Syne',sans-serif;font-size:11px;font-weight:700;color:#fff;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px}
.sb-search{width:100%;background:var(--surf2);border:1px solid var(--border2);color:var(--bright);font-family:inherit;font-size:10px;padding:7px 10px;border-radius:6px;outline:none}
.sb-search:focus{border-color:var(--green)}
.sb-filters{display:flex;gap:5px;margin-top:8px}
.sf{font-size:9px;padding:3px 8px;border-radius:4px;border:1px solid var(--border2);background:transparent;color:var(--muted2);cursor:pointer;font-family:inherit;transition:all .15s}
.sf:hover,.sf.active{border-color:var(--green);color:var(--green);background:#00e67610}
.sb-list{flex:1;overflow-y:auto}
.user-row{padding:10px 16px;border-bottom:1px solid var(--border);cursor:pointer;transition:background .1s;display:flex;align-items:center;gap:10px}
.user-row:hover{background:var(--surf2)}
.user-row.active{background:var(--surf3);border-left:2px solid var(--green)}
.ua{width:32px;height:32px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;flex-shrink:0;font-family:'Syne',sans-serif}
.ui{flex:1;min-width:0}
.un{font-size:11px;color:var(--bright);font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.uu{font-size:9px;color:var(--muted2);margin-top:1px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ub{flex-shrink:0;font-size:9px;padding:2px 6px;border-radius:4px;font-weight:600}
.ub-t{background:#38bdf815;color:var(--blue)}.ub-n{background:#2a3d5230;color:var(--muted2)}
.mi{padding:20px 24px}
.sr{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:12px;margin-bottom:20px}
.sc{background:var(--surf);border:1px solid var(--border);border-radius:10px;padding:14px 16px}
.sl{font-size:9px;color:var(--muted2);text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px}
.sv{font-size:26px;font-weight:700;font-family:'Syne',sans-serif;line-height:1}
.ss{font-size:9px;color:var(--muted2);margin-top:4px}
.panel{background:var(--surf);border:1px solid var(--border);border-radius:10px;margin-bottom:16px;overflow:hidden}
.ph{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
.pt{font-family:'Syne',sans-serif;font-size:12px;font-weight:700;color:#fff}
.pb{max-height:400px;overflow-y:auto}
.udh{background:var(--surf2);padding:18px 20px;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;gap:16px;flex-wrap:wrap}
.udha{width:52px;height:52px;border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:18px;font-weight:800;font-family:'Syne',sans-serif;flex-shrink:0}
.udhi{flex:1}
.udhn{font-family:'Syne',sans-serif;font-size:18px;font-weight:800;color:#fff;line-height:1.2}
.udhu{font-size:10px;color:var(--muted2);margin-top:3px}
.udhm{display:flex;gap:8px;margin-top:8px;flex-wrap:wrap}
.udht{font-size:9px;padding:2px 8px;border-radius:4px;border:1px solid}
.at{width:100%;border-collapse:collapse;font-size:10px}
.at th{padding:8px 12px;text-align:left;color:var(--muted2);font-size:9px;text-transform:uppercase;letter-spacing:.6px;background:var(--surf2);border-bottom:1px solid var(--border);position:sticky;top:0}
.at td{padding:8px 12px;border-bottom:1px solid var(--border);vertical-align:top}
.at tr:hover td{background:#ffffff04}
.ob{padding:2px 7px;border-radius:4px;font-size:9px;font-weight:600}
.ov{background:#38bdf815;color:var(--blue)}
.oa{background:#00e67615;color:var(--green)}
.ou{background:#fbbf2415;color:var(--yellow)}
.od{background:#f43f5e15;color:var(--red)}
.ol{background:#a78bfa15;color:var(--purple)}
.ox{background:#2a3d52;color:var(--muted2)}
.mt{padding:2px 7px;border-radius:4px;font-size:9px;background:var(--surf3);color:var(--text);border:1px solid var(--border2)}
.mbr{display:flex;align-items:center;gap:10px;padding:8px 16px;border-bottom:1px solid var(--border);font-size:10px}
.mbn{width:130px;color:var(--bright);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mbw{flex:1;height:6px;background:var(--border);border-radius:3px;overflow:hidden}
.mbf{height:6px;border-radius:3px;transition:width .4s}
.mbc{width:50px;text-align:right;color:var(--muted2)}
.hc{display:flex;align-items:flex-end;gap:3px;height:60px;padding:8px 16px 4px}
.hbw{flex:1;display:flex;flex-direction:column;align-items:center;gap:2px}
.hb{width:100%;background:var(--green);border-radius:2px 2px 0 0;min-height:2px;opacity:.7;cursor:default}
.hb:hover{opacity:1}
.hl{font-size:7px;color:var(--muted);margin-top:3px}
.fr{display:flex;gap:10px;padding:8px 16px;border-bottom:1px solid var(--border);font-size:10px;align-items:flex-start;cursor:pointer;transition:background .1s}
.fr:hover{background:var(--surf2)}
.ft{color:var(--muted2);white-space:nowrap;font-size:9px;margin-top:1px;min-width:140px}
.fu{color:var(--bright);font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:110px;max-width:130px}
.fd{color:var(--text);flex:1;min-width:0}
.pg{padding:8px 16px;display:flex;align-items:center;justify-content:space-between;border-top:1px solid var(--border);font-size:9px;color:var(--muted2)}
.pgb{display:flex;gap:6px}
.pb2{background:transparent;border:1px solid var(--border2);color:var(--muted2);font-family:inherit;font-size:9px;padding:3px 8px;border-radius:4px;cursor:pointer;transition:all .15s}
.pb2:hover:not(:disabled){border-color:var(--green);color:var(--green)}
.pb2:disabled{opacity:.3;cursor:default}
.og{display:grid;grid-template-columns:1fr 1fr;gap:0}
.oi{padding:10px 16px;border-bottom:1px solid var(--border);border-right:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;font-size:10px}
.oi:nth-child(2n){border-right:none}
.empty{padding:40px;text-align:center;color:var(--muted2);font-size:11px}
.loading{display:flex;align-items:center;justify-content:center;padding:30px;gap:10px;color:var(--muted2);font-size:11px}
.spin{width:20px;height:20px;border:2px solid var(--border);border-top-color:var(--green);border-radius:50%;animation:spin .7s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.info-strip{background:var(--surf3);border:1px solid var(--border2);border-radius:6px;padding:6px 12px;font-size:9px;color:var(--muted2);margin-top:8px;line-height:1.8}
.info-strip b{color:var(--bright)}
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
  <!-- SIDEBAR -->
  <div class="sidebar">
    <div class="sb-head">
      <div class="sb-title">Staff Users</div>
      <input class="sb-search" id="user-search" placeholder="🔍 Search..." oninput="filterUsers()">
      <div class="sb-filters">
        <button class="sf active" onclick="setF(event,'')">All</button>
        <button class="sf" onclick="setF(event,'ACTIVE')">Active</button>
        <button class="sf" onclick="setF(event,'INACTIVE')">Inactive</button>
      </div>
    </div>
    <div class="sb-list" id="user-list">
      <div class="loading"><div class="spin"></div>Loading...</div>
    </div>
  </div>

  <!-- MAIN -->
  <div class="main">

    <!-- HOME -->
    <div id="view-home" class="mi" style="padding:0">
      <div class="sr" id="stats-row" style="padding:20px 24px 0">
        <div class="loading"><div class="spin"></div></div>
      </div>

      <div style="padding:0 24px">
        <div class="panel">
          <div class="ph">
            <div class="pt">📊 Today's Activity by Hour</div>
            <span style="font-size:9px;color:var(--muted2)" id="today-lbl">—</span>
          </div>
          <div id="hour-chart-wrap"></div>
        </div>

        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">
          <div class="panel">
            <div class="ph"><div class="pt">🗂 Module Activity (Today)</div></div>
            <div id="module-bars"></div>
          </div>
          <div class="panel">
            <div class="ph"><div class="pt">⚡ Operations (Today)</div></div>
            <div class="og" id="ops-grid"></div>
          </div>
        </div>

        <div class="panel">
          <div class="ph">
            <div class="pt">🔴 Live Activity Feed</div>
            <button class="pb2" onclick="loadLiveFeed()">↺ Refresh</button>
          </div>
          <div class="pb" id="live-feed">
            <div class="loading"><div class="spin"></div>Loading...</div>
          </div>
        </div>
      </div>
    </div>

    <!-- USER DETAIL -->
    <div id="view-user" style="display:none">
      <div id="user-detail-head"></div>
      <div class="mi" style="padding-top:16px">
        <div class="sr" id="user-stats-row"></div>

        <!-- Date filter -->
        <div class="panel" style="margin-bottom:16px">
          <div class="ph">
            <div class="pt">🔍 Filter Activity</div>
            <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
              <span style="font-size:9px;color:var(--muted2)">From:</span>
              <input type="date" style="background:var(--surf2);border:1px solid var(--border2);color:var(--bright);font-family:inherit;font-size:10px;padding:5px 10px;border-radius:6px;outline:none" id="date-from">
              <span style="font-size:9px;color:var(--muted2)">To:</span>
              <input type="date" style="background:var(--surf2);border:1px solid var(--border2);color:var(--bright);font-family:inherit;font-size:10px;padding:5px 10px;border-radius:6px;outline:none" id="date-to">
              <select style="background:var(--surf2);border:1px solid var(--border2);color:var(--bright);font-family:inherit;font-size:10px;padding:5px 10px;border-radius:6px;outline:none" id="mod-filter" onchange="loadUserActivity()">
                <option value="">All Modules</option>
              </select>
              <button class="pb2" onclick="loadUserActivity()" style="border-color:var(--green);color:var(--green)">Apply</button>
              <button class="pb2" onclick="showHome()">← Back</button>
            </div>
          </div>
        </div>

        <!-- Module + ops -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">
          <div class="panel">
            <div class="ph"><div class="pt">🗂 Modules Used</div></div>
            <div id="user-module-bars"></div>
          </div>
          <div class="panel">
            <div class="ph"><div class="pt">⚡ Operations</div></div>
            <div class="og" id="user-ops-grid"></div>
          </div>
        </div>

        <!-- Activity log -->
        <div class="panel">
          <div class="ph">
            <div class="pt">📋 Activity Log</div>
            <span style="font-size:9px;color:var(--muted2)" id="act-total-lbl"></span>
          </div>
          <div class="pb">
            <table class="at">
              <thead><tr><th>Date / Time</th><th>Module</th><th>Operation</th><th>Details</th></tr></thead>
              <tbody id="act-tbody"></tbody>
            </table>
          </div>
          <div class="pg" id="act-pager"></div>
        </div>
      </div>
    </div>

  </div>
</div>

<script>
let allUsers=[], filteredUsers=[], statusFilter='', activeUserId=null;
let actOffset=0; const actLimit=100; let actTotal=0;
const CL=['#00e676','#38bdf8','#fb923c','#a78bfa','#fbbf24','#f43f5e','#34d399','#818cf8'];
const ac=id=>CL[Math.abs(id)%CL.length];
const ini=(f,l)=>((f||'?')[0]+(l||'?')[0]).toUpperCase();

function fmt(v){
  if(!v) return '—';
  const s=String(v).replace('T',' ');
  // if time is 00:00:00 just show date
  if(s.endsWith('00:00:00')) return s.slice(0,10);
  return s.slice(0,19);
}

function oc(op){
  if(!op) return 'ox';
  const o=op.toLowerCase();
  if(o.includes('view')) return 'ov';
  if(o.includes('add')||o.includes('creat')||o.includes('insert')) return 'oa';
  if(o.includes('update')||o.includes('edit')||o.includes('modif')) return 'ou';
  if(o.includes('delet')||o.includes('remov')) return 'od';
  if(o.includes('login')||o.includes('logout')) return 'ol';
  return 'ox';
}

// ── Stats ──────────────────────────────────────────────────
async function loadStats(){
  try{
    const s=await fetch('/api/stats').then(r=>r.json());
    if(s.error){console.error(s.error);return;}
    document.getElementById('today-lbl').textContent=s.today;
    document.getElementById('total-badge').textContent=`${s.total_staff} STAFF`;
    document.getElementById('stats-row').innerHTML=`
      <div class="sc"><div class="sl">Total Staff</div><div class="sv" style="color:#fff">${s.total_staff}</div><div class="ss">registered users</div></div>
      <div class="sc"><div class="sl">Active (7 days)</div><div class="sv" style="color:var(--green)">${s.active_7d}</div><div class="ss">unique users</div></div>
      <div class="sc"><div class="sl">Today's Events</div><div class="sv" style="color:var(--blue)">${s.today_events.toLocaleString()}</div><div class="ss">${s.today}</div></div>
      <div class="sc"><div class="sl">Most Active Today</div><div class="sv" style="color:var(--orange);font-size:13px;margin-top:4px">${s.top_user?s.top_user.user_name:'—'}</div><div class="ss">${s.top_user?s.top_user.cnt+' actions':''}</div></div>`;

    const mxH=Math.max(...Object.values(s.hourly||{}),1);
    let hh='<div class="hc">';
    for(let i=0;i<24;i++){
      const cnt=s.hourly[String(i)]||0;
      const pct=Math.max(4,Math.round(cnt/mxH*100));
      hh+=`<div class="hbw" title="${i}:00 — ${cnt} events"><div class="hb" style="height:${pct}%"></div><div class="hl">${i%4===0?i:''}</div></div>`;
    }
    hh+='</div>';
    document.getElementById('hour-chart-wrap').innerHTML=hh;

    const mxM=Math.max(...(s.modules||[]).map(m=>m.cnt),1);
    document.getElementById('module-bars').innerHTML=
      (s.modules||[]).map((m,i)=>`<div class="mbr"><div class="mbn">${m.module||'?'}</div><div class="mbw"><div class="mbf" style="width:${Math.round(m.cnt/mxM*100)}%;background:${CL[i%CL.length]}"></div></div><div class="mbc">${m.cnt}</div></div>`).join('')||'<div class="empty">No data</div>';

    document.getElementById('ops-grid').innerHTML=
      (s.operations||[]).map(o=>`<div class="oi"><span class="ob ${oc(o.operation)}">${o.operation||'?'}</span><span style="color:var(--bright);font-weight:600">${o.cnt}</span></div>`).join('')||'<div class="empty">No data</div>';
  }catch(e){console.error(e);}
}

// ── Users ──────────────────────────────────────────────────
async function loadUsers(){
  try{
    const r=await fetch('/api/users').then(r=>r.json());
    allUsers=r.users||[];
    filterUsers();
  }catch(e){console.error(e);}
}

function filterUsers(){
  const q=document.getElementById('user-search').value.toLowerCase();
  filteredUsers=allUsers.filter(u=>{
    const ms=!statusFilter||u.sstatus===statusFilter;
    const mq=!q||(u.firstname||'').toLowerCase().includes(q)||(u.lastname||'').toLowerCase().includes(q)||(u.username||'').toLowerCase().includes(q)||(u.email||'').toLowerCase().includes(q);
    return ms&&mq;
  });
  renderUserList();
}

function setF(ev,s){
  statusFilter=s;
  document.querySelectorAll('.sf').forEach(b=>b.classList.remove('active'));
  ev.target.classList.add('active');
  filterUsers();
}

function renderUserList(){
  const el=document.getElementById('user-list');
  if(!filteredUsers.length){el.innerHTML='<div class="empty">No users found</div>';return;}
  el.innerHTML=filteredUsers.map(u=>{
    const c=ac(u.staffid);
    const nm=`${u.firstname||''} ${u.lastname||''}`.trim();
    const bd=u.today_actions>0?`<span class="ub ub-t">${u.today_actions}</span>`:`<span class="ub ub-n">0</span>`;
    return `<div class="user-row ${activeUserId===u.staffid?'active':''}" onclick="openUser(${u.staffid})">
      <div class="ua" style="background:${c}20;color:${c};border:1px solid ${c}40">${ini(u.firstname,u.lastname)}</div>
      <div class="ui"><div class="un">${nm||u.username}</div><div class="uu">${u.username}</div></div>
      ${bd}</div>`;
  }).join('');
}

// ── Live Feed ──────────────────────────────────────────────
async function loadLiveFeed(){
  try{
    const r=await fetch('/api/live_feed').then(r=>r.json());
    const el=document.getElementById('live-feed');
    if(!r.events||!r.events.length){el.innerHTML='<div class="empty">No recent activity</div>';return;}
    el.innerHTML=r.events.map(e=>`
      <div class="fr" onclick="openByAudit(${e.user_id})">
        <div class="ft">${fmt(e.auditdate)}</div>
        <div class="fu">${e.user_name||'—'}</div>
        <div class="fd">
          <span class="mt">${e.module||'—'}</span>
          <span class="ob ${oc(e.operation)}" style="margin-left:5px">${e.operation||'—'}</span>
          <span style="color:var(--muted2);margin-left:6px;font-size:9px">${(e.remark||'').slice(0,70)}</span>
        </div>
      </div>`).join('');
  }catch(e){console.error(e);}
}

async function openByAudit(audit_uid){
  try{
    const r=await fetch(`/api/staffid_by_audit_uid/${audit_uid}`).then(r=>r.json());
    openUser(r.staffid);
  }catch(e){ openUser(audit_uid); }
}

// ── User Detail ────────────────────────────────────────────
async function openUser(uid){
  activeUserId=uid;
  renderUserList();
  document.getElementById('view-home').style.display='none';
  document.getElementById('view-user').style.display='block';

  const today=new Date().toISOString().split('T')[0];
  const week=new Date(Date.now()-7*86400000).toISOString().split('T')[0];
  document.getElementById('date-from').value=week;
  document.getElementById('date-to').value=today;

  // Show loading immediately
  document.getElementById('user-detail-head').innerHTML='<div class="loading" style="padding:20px"><div class="spin"></div>Loading user...</div>';
  document.getElementById('act-tbody').innerHTML='<tr><td colspan="4"><div class="loading"><div class="spin"></div>Loading...</div></td></tr>';

  try{
    const mods=await fetch('/api/modules').then(r=>r.json());
    const sel=document.getElementById('mod-filter');
    sel.innerHTML='<option value="">All Modules</option>'+mods.map(m=>`<option value="${m}">${m}</option>`).join('');
  }catch(e){}

  actOffset=0;
  await loadUserActivity();
}

async function loadUserActivity(){
  const uid=activeUserId;
  const from=document.getElementById('date-from').value;
  const to=document.getElementById('date-to').value;
  const mod=document.getElementById('mod-filter').value;

  document.getElementById('act-tbody').innerHTML='<tr><td colspan="4"><div class="loading"><div class="spin"></div>Loading...</div></td></tr>';

  try{
    const resp=await fetch(`/api/user/${uid}/activity?from=${from}&to=${to}&module=${encodeURIComponent(mod)}&limit=${actLimit}&offset=${actOffset}`).then(r=>r.json());
    if(resp.error){console.error(resp.error);document.getElementById('act-tbody').innerHTML=`<tr><td colspan="4" class="empty">Error: ${resp.error}</td></tr>`;return;}

    actTotal=resp.total;
    const u=resp.user||{};
    const c=ac(u.staffid||0);
    const nm=`${u.firstname||''} ${u.lastname||''}`.trim();

    // Header
    document.getElementById('user-detail-head').innerHTML=`
      <div class="udh">
        <div class="udha" style="background:${c}20;color:${c};border:1px solid ${c}40">${ini(u.firstname,u.lastname)}</div>
        <div class="udhi">
          <div class="udhn">${nm||u.username||'—'}</div>
          <div class="udhu">@${u.username||'—'} · ${u.email||'—'}</div>
          <div class="udhm">
            <span class="udht" style="color:${u.sstatus==='ACTIVE'?'var(--green)':'var(--red)'};border-color:${u.sstatus==='ACTIVE'?'#00e67640':'#f43f5e40'}">${u.sstatus||'—'}</span>
            ${u.phone?`<span class="udht" style="color:var(--muted2);border-color:var(--border2)">📞 ${u.phone}</span>`:''}
            ${u.last_login_time?`<span class="udht" style="color:var(--yellow);border-color:#fbbf2440">Last Login: ${fmt(u.last_login_time)}</span>`:''}
          </div>
          <div class="info-strip">
            DB: <b>adoptconvergebss</b> · Table: <b>tblauditlog</b> ·
            Staff ID: <b>${u.staffid||'—'}</b> · Audit UID: <b>${resp.audit_uid}</b> ·
            Source: <b>tblstaffuser</b>
          </div>
        </div>
        <div><div class="sc" style="min-width:110px;text-align:center">
          <div class="sl">This Period</div>
          <div class="sv" style="color:var(--green)">${actTotal.toLocaleString()}</div>
          <div class="ss">actions</div>
        </div></div>
      </div>`;

    // Mini stats
    document.getElementById('user-stats-row').innerHTML=`
      <div class="sc"><div class="sl">Total in Range</div><div class="sv" style="color:var(--green)">${actTotal.toLocaleString()}</div></div>
      <div class="sc"><div class="sl">Modules Used</div><div class="sv" style="color:var(--blue)">${resp.modules.length}</div></div>
      <div class="sc"><div class="sl">Operation Types</div><div class="sv" style="color:var(--orange)">${resp.operations.length}</div></div>
      <div class="sc"><div class="sl">Date Range</div><div class="sv" style="color:var(--text);font-size:10px;margin-top:4px">${from}</div><div class="ss">→ ${to}</div></div>`;

    // Module bars
    const mxM=Math.max(...(resp.modules||[]).map(m=>m.cnt),1);
    document.getElementById('user-module-bars').innerHTML=
      (resp.modules||[]).map((m,i)=>`<div class="mbr"><div class="mbn">${m.module||'—'}</div><div class="mbw"><div class="mbf" style="width:${Math.round(m.cnt/mxM*100)}%;background:${CL[i%CL.length]}"></div></div><div class="mbc">${m.cnt}</div></div>`).join('')||'<div class="empty">No data</div>';

    // Ops
    document.getElementById('user-ops-grid').innerHTML=
      (resp.operations||[]).map(o=>`<div class="oi"><span class="ob ${oc(o.operation)}">${o.operation||'—'}</span><span style="color:var(--bright);font-weight:600">${o.cnt}</span></div>`).join('')||'<div class="empty">No data</div>';

    // Table label
    document.getElementById('act-total-lbl').textContent=`Showing ${actOffset+1}–${Math.min(actOffset+actLimit,actTotal)} of ${actTotal.toLocaleString()}`;

    // Table rows
    const tbody=document.getElementById('act-tbody');
    if(!resp.events.length){
      tbody.innerHTML=`<tr><td colspan="4" class="empty">No activity found for this date range</td></tr>`;
    } else {
      tbody.innerHTML=resp.events.map(e=>`
        <tr>
          <td style="color:var(--muted2);white-space:nowrap;font-size:9px">${fmt(e.auditdate)}</td>
          <td><span class="mt">${e.module||'—'}</span></td>
          <td><span class="ob ${oc(e.operation)}">${e.operation||'—'}</span></td>
          <td style="color:var(--muted2);font-size:9px;max-width:280px;word-break:break-word">${(e.remark||'').slice(0,100)}</td>
        </tr>`).join('');
    }

    // Pager
    const mxP=Math.ceil(actTotal/actLimit)||1;
    const cP=Math.floor(actOffset/actLimit)+1;
    document.getElementById('act-pager').innerHTML=`
      <span>${actTotal.toLocaleString()} total</span>
      <div class="pgb">
        <button class="pb2" onclick="changePage(-1)" ${actOffset===0?'disabled':''}>← Prev</button>
        <span style="color:var(--bright);padding:0 8px">Page ${cP}/${mxP}</span>
        <button class="pb2" onclick="changePage(1)" ${actOffset+actLimit>=actTotal?'disabled':''}>Next →</button>
      </div>`;

  }catch(e){
    console.error(e);
    document.getElementById('act-tbody').innerHTML=`<tr><td colspan="4" class="empty">Failed to load: ${e.message}</td></tr>`;
  }
}

function changePage(d){actOffset=Math.max(0,actOffset+d*actLimit);loadUserActivity();}

function showHome(){
  activeUserId=null;renderUserList();
  document.getElementById('view-home').style.display='block';
  document.getElementById('view-user').style.display='none';
}

loadStats();loadUsers();loadLiveFeed();
setInterval(loadLiveFeed,15000);
setInterval(loadStats,60000);
</script>
</body>
</html>"""

@app.route("/")
def dashboard():
    return render_template_string(HTML)

if __name__ == "__main__":
    print("\n  [Tool 4] User Activity Dashboard → port 6000")
    app.run(host="0.0.0.0", port=6000, debug=False, use_reloader=False, threaded=True)
