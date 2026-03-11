#!/usr/bin/env python3
# ============================================================
#  ADOPT Infrastructure Suite — Master Server (v3 Fixed)
#  Fix: binlog file not found via proxy
#  Fix: keep-alive no sleep
#  Fix: all API paths rewritten correctly
# ============================================================

import threading, os, sys, time, re
PORT = int(os.environ.get("PORT", 8080))

def start_tool_1():
    try:
        import binray_Log_db as t1
        print("  [Tool 1] Binlog Monitor → port 5000")
        t1.check_binlog_status()
        t1.warm_column_cache()
        threading.Thread(target=t1.binlog_monitor, daemon=True).start()
        t1.app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        print(f"  [Tool 1] Failed: {e}")

def start_tool_2():
    try:
        import fetch_binlog_files as t2
        print("  [Tool 2] Binlog File Reader → port 7777")
        t2.startup()
        t2.app.run(host="0.0.0.0", port=7777, debug=False, threaded=True)
    except Exception as e:
        print(f"  [Tool 2] Failed: {e}")

def start_tool_3():
    try:
        import database_tool as t3
        print("  [Tool 3] Database AI → port 7000")
        t3.load_schema()
        t3.app.run(host="0.0.0.0", port=7000, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        print(f"  [Tool 3] Failed: {e}")

def start_tool_4():
    try:
        import user_activity_tool as t4
        print("  [Tool 4] User Activity → port 6000")
        t4.app.run(host="0.0.0.0", port=6000, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        print(f"  [Tool 4] Failed: {e}")

def keep_alive():
    import requests
    time.sleep(30)
    while True:
        try:
            url = os.environ.get("RENDER_EXTERNAL_URL", f"http://localhost:{PORT}")
            requests.get(url + "/ping", timeout=10)
            print(f"  [KeepAlive] OK")
        except Exception as e:
            print(f"  [KeepAlive] {e}")
        time.sleep(600)

# ── Flask ──────────────────────────────────────────────────
from flask import Flask, Response, request
import requests as req


home_app = Flask(__name__)

TOOLS = {
    "binlog": "http://localhost:5000",
    "fetch":  "http://localhost:7777",
    "ai":     "http://localhost:7000",
    "users":  "http://localhost:6000",
}

HOME_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ADOPT · Infrastructure Suite</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
:root{--bg:#05080f;--bg2:#080d18;--surf:#0c1220;--border:#182035;--border2:#1e2d45;
      --text:#8ba3c4;--bright:#dce8f8;--muted:#2d3f58;--muted2:#3d5570;
      --binlog:#22d87a;--fetch:#6366f1;--ai:#f59e0b;--users:#00e676}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;overflow:hidden}
body{background:var(--bg);color:var(--text);font-family:'Space Mono',monospace;height:100vh;display:flex;flex-direction:column}
body::after{content:'';position:fixed;inset:0;pointer-events:none;z-index:9999;
  background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,.04) 2px,rgba(0,0,0,.04) 4px)}
.grid-bg{position:fixed;inset:0;pointer-events:none;z-index:0;
  background-image:linear-gradient(var(--border) 1px,transparent 1px),linear-gradient(90deg,var(--border) 1px,transparent 1px);
  background-size:60px 60px;opacity:.3}
.grid-bg::after{content:'';position:absolute;inset:0;background:radial-gradient(ellipse 80% 60% at 50% 30%,transparent 40%,var(--bg) 100%)}
nav{position:relative;z-index:100;display:flex;align-items:center;padding:0 24px;height:54px;
    background:rgba(8,13,24,.96);border-bottom:1px solid var(--border2);backdrop-filter:blur(12px);flex-shrink:0}
.nav-logo{font-family:'Syne',sans-serif;font-weight:800;font-size:15px;color:#fff;letter-spacing:3px;
          margin-right:32px;display:flex;align-items:center;gap:8px;white-space:nowrap}
.nav-logo-icon{width:24px;height:24px;background:linear-gradient(135deg,var(--binlog),#0ea5e9);
               border-radius:5px;display:flex;align-items:center;justify-content:center;font-size:11px}
.nav-links{display:flex;align-items:stretch;height:100%}
.nav-btn{position:relative;display:flex;align-items:center;gap:6px;padding:0 16px;font-size:10px;
         font-weight:700;letter-spacing:.5px;color:var(--muted2);cursor:pointer;border:none;
         background:none;font-family:'Space Mono',monospace;transition:color .2s;white-space:nowrap;
         border-bottom:2px solid transparent;margin-bottom:-1px}
.nav-btn:hover{color:var(--bright)}
.nav-btn::after{content:'';position:absolute;bottom:-1px;left:0;right:0;height:2px;
                border-radius:2px 2px 0 0;transform:scaleX(0);transition:transform .2s}
.btn-home::after{background:#dce8f8}.btn-home.active{color:var(--bright);border-bottom-color:#dce8f8}
.btn-binlog::after{background:var(--binlog)}.btn-binlog.active{color:var(--binlog);border-bottom-color:var(--binlog)}
.btn-fetch::after{background:var(--fetch)}.btn-fetch.active{color:var(--fetch);border-bottom-color:var(--fetch)}
.btn-ai::after{background:var(--ai)}.btn-ai.active{color:var(--ai);border-bottom-color:var(--ai)}
.nav-btn:hover::after{transform:scaleX(1)}
.nav-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.btn-binlog .nav-dot{background:var(--binlog);box-shadow:0 0 6px var(--binlog);animation:blink 2s infinite}
.btn-fetch  .nav-dot{background:var(--fetch)}.btn-ai .nav-dot{background:var(--ai)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
.nav-badge{font-size:8px;padding:1px 6px;border-radius:8px;font-weight:700}
.btn-binlog .nav-badge{background:#22d87a15;color:var(--binlog);border:1px solid #22d87a30}
.btn-fetch  .nav-badge{background:#6366f115;color:var(--fetch);border:1px solid #6366f130}
.btn-ai     .nav-badge{background:#f59e0b15;color:var(--ai);border:1px solid #f59e0b30}
.nav-spacer{flex:1}
.nav-status{display:flex;align-items:center;gap:6px;font-size:9px;color:var(--muted2);letter-spacing:.5px}
.live-dot{width:6px;height:6px;border-radius:50%;background:var(--binlog);box-shadow:0 0 8px var(--binlog);animation:blink 1.4s infinite}
.content{flex:1;overflow:hidden;position:relative}
.view{display:none;width:100%;height:100%}.view.active{display:block}
.tool-frame{width:100%;height:100%;border:none;display:block;background:var(--bg2)}
.home-scroll{width:100%;height:100%;overflow-y:auto;position:relative;z-index:1}
.home-scroll::-webkit-scrollbar{width:5px}
.home-scroll::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}
.hero{padding:60px 32px 40px;text-align:center;position:relative}
.hero-glow{position:absolute;top:-40px;left:50%;transform:translateX(-50%);width:600px;height:300px;
           background:radial-gradient(ellipse,#22d87a08 0%,transparent 70%);pointer-events:none}
.hero-eyebrow{display:inline-flex;align-items:center;gap:8px;font-size:9px;letter-spacing:2px;
              color:var(--muted2);text-transform:uppercase;margin-bottom:18px;padding:5px 14px;
              border:1px solid var(--border2);border-radius:20px;background:var(--surf)}
.hero h1{font-family:'Syne',sans-serif;font-size:clamp(26px,5vw,52px);font-weight:800;
         line-height:1.1;color:#fff;letter-spacing:-1px;margin-bottom:14px}
.hero h1 .line2{display:block;color:transparent;-webkit-text-stroke:1.5px rgba(139,163,196,.35)}
.hl-g{color:var(--binlog)!important;-webkit-text-stroke:0!important}
.hl-i{color:var(--fetch)!important;-webkit-text-stroke:0!important}
.hl-a{color:var(--ai)!important;-webkit-text-stroke:0!important}
.hero-sub{font-size:10px;color:var(--muted2);letter-spacing:.8px;max-width:440px;margin:0 auto 40px;line-height:1.9}
.ts-bar{display:flex;gap:10px;justify-content:center;padding:0 24px 24px;flex-wrap:wrap}
.ts-pill{display:flex;align-items:center;gap:7px;padding:6px 14px;background:var(--surf);
         border:1px solid var(--border2);border-radius:20px;font-size:9px;cursor:pointer;transition:all .15s}
.ts-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.ts-online .ts-dot{background:#22d87a;box-shadow:0 0 6px #22d87a;animation:blink 2s infinite}
.ts-offline .ts-dot{background:#f43f5e}
.ts-checking .ts-dot{background:var(--ai);animation:blink .8s infinite}
.ts-online{border-color:#22d87a30;color:#22d87a}
.ts-offline{border-color:#f43f5e30;color:#f43f5e}
.ts-checking{border-color:#f59e0b30;color:var(--ai)}
.stats-bar{display:flex;gap:12px;justify-content:center;flex-wrap:wrap;margin-bottom:40px;padding:0 24px}
.stat-pill{display:flex;align-items:center;gap:12px;background:var(--surf);border:1px solid var(--border2);
           border-radius:8px;padding:10px 16px;min-width:140px}
.stat-val{font-size:20px;font-weight:700;font-family:'Syne',sans-serif;line-height:1}
.stat-info{display:flex;flex-direction:column;gap:2px}
.stat-label{font-size:9px;color:var(--muted2);letter-spacing:.5px;text-transform:uppercase}
.stat-sub{font-size:8px;color:var(--muted)}
.tools-grid{max-width:1040px;margin:0 auto;padding:0 24px 56px;
            display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}
.tool-card{background:var(--surf);border:1px solid var(--border2);border-radius:14px;
           padding:26px 22px 22px;cursor:pointer;overflow:hidden;
           transition:border-color .25s,transform .2s,box-shadow .25s;
           display:flex;flex-direction:column;position:relative;animation:cardIn .5s ease both}
.tool-card:nth-child(1){animation-delay:.1s}.tool-card:nth-child(2){animation-delay:.2s}.tool-card:nth-child(3){animation-delay:.3s}
@keyframes cardIn{from{opacity:0;transform:translateY(18px)}to{opacity:1;transform:translateY(0)}}
.tool-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:14px 14px 0 0;transition:height .2s}
.tool-card::after{content:'';position:absolute;top:-40px;right:-40px;width:120px;height:120px;border-radius:50%;opacity:0;transition:opacity .3s}
.card-binlog::before{background:linear-gradient(90deg,var(--binlog),#0ea5e9)}
.card-fetch::before{background:linear-gradient(90deg,var(--fetch),#a855f7)}
.card-ai::before{background:linear-gradient(90deg,var(--ai),#f97316)}
.card-binlog::after{background:radial-gradient(circle,#22d87a1a,transparent 70%)}
.card-fetch::after{background:radial-gradient(circle,#6366f11a,transparent 70%)}
.card-ai::after{background:radial-gradient(circle,#f59e0b1a,transparent 70%)}
.card-binlog:hover{border-color:#22d87a55;box-shadow:0 0 32px #22d87a18;transform:translateY(-3px)}
.card-fetch:hover{border-color:#6366f155;box-shadow:0 0 32px #6366f118;transform:translateY(-3px)}
.card-ai:hover{border-color:#f59e0b55;box-shadow:0 0 32px #f59e0b18;transform:translateY(-3px)}
.tool-card:hover::before{height:5px}.tool-card:hover::after{opacity:1}
.card-num{position:absolute;top:14px;left:20px;font-size:60px;font-weight:800;
          font-family:'Syne',sans-serif;color:var(--border2);line-height:1;pointer-events:none}
.card-header{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px}
.card-icon{width:42px;height:42px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:18px}
.card-binlog .card-icon{background:#22d87a12;border:1px solid #22d87a30}
.card-fetch  .card-icon{background:#6366f112;border:1px solid #6366f130}
.card-ai     .card-icon{background:#f59e0b12;border:1px solid #f59e0b30}
.card-tag{font-size:8px;padding:3px 8px;border-radius:8px;font-weight:700}
.card-binlog .card-tag{background:#22d87a12;color:var(--binlog);border:1px solid #22d87a30}
.card-fetch  .card-tag{background:#6366f112;color:var(--fetch);border:1px solid #6366f130}
.card-ai     .card-tag{background:#f59e0b12;color:var(--ai);border:1px solid #f59e0b30}
.card-title{font-family:'Syne',sans-serif;font-size:15px;font-weight:700;color:#fff;margin-bottom:8px;line-height:1.3}
.card-desc{font-size:10px;color:var(--muted2);line-height:1.9;flex:1;margin-bottom:18px}
.card-features{display:flex;flex-direction:column;gap:5px;margin-bottom:20px}
.card-feat{display:flex;align-items:center;gap:8px;font-size:10px;color:var(--text)}
.card-feat::before{content:'';width:4px;height:4px;border-radius:50%;flex-shrink:0}
.card-binlog .card-feat::before{background:var(--binlog)}
.card-fetch  .card-feat::before{background:var(--fetch)}
.card-ai     .card-feat::before{background:var(--ai)}
.card-footer{display:flex;align-items:center;justify-content:space-between;padding-top:14px;border-top:1px solid var(--border)}
.card-port{font-size:9px;color:var(--muted2)}.card-port b{font-weight:700}
.card-binlog .card-port b{color:var(--binlog)}.card-fetch .card-port b{color:var(--fetch)}.card-ai .card-port b{color:var(--ai)}
.card-open{display:flex;align-items:center;gap:5px;font-size:10px;font-weight:700;padding:6px 14px;
           border-radius:7px;border:1px solid;cursor:pointer;font-family:'Space Mono',monospace;transition:all .2s;background:none}
.card-binlog .card-open{color:var(--binlog);border-color:#22d87a40;background:#22d87a0a}
.card-fetch  .card-open{color:var(--fetch);border-color:#6366f140;background:#6366f10a}
.card-ai     .card-open{color:var(--ai);border-color:#f59e0b40;background:#f59e0b0a}
.offline-overlay{display:none;position:absolute;top:0;left:0;right:0;bottom:0;background:var(--bg);
                 z-index:10;flex-direction:column;align-items:center;justify-content:center;
                 gap:12px;font-size:13px;color:#f43f5e;text-align:center}
.offline-overlay.show{display:flex}
.retry-btn{margin-top:8px;padding:6px 16px;border-radius:6px;cursor:pointer;
           font-family:'Space Mono',monospace;font-size:10px;font-weight:700;border:1px solid}
footer{border-top:1px solid var(--border);padding:14px 32px;display:flex;align-items:center;
       justify-content:space-between;font-size:9px;color:var(--muted);flex-wrap:wrap;gap:8px}
.footer-logo{font-family:'Syne',sans-serif;font-weight:700;font-size:11px;color:var(--muted2);letter-spacing:2px}
@media(max-width:650px){
  nav{padding:0 10px}.nav-logo{margin-right:10px;font-size:12px}
  .nav-btn{padding:0 7px;font-size:9px}.nav-badge,.nav-dot{display:none}
  .hero{padding:40px 12px 28px}.tools-grid,.stats-bar{padding:0 10px}.tools-grid{padding-bottom:40px}
}
.btn-users::after{background:var(--users)}.btn-users.active{color:var(--users);border-bottom-color:var(--users)}
.btn-users .nav-dot{background:var(--users)}
.btn-users .nav-badge{background:#00e67615;color:var(--users);border:1px solid #00e67630}
</style>
</head>
<body>
<div class="grid-bg"></div>
<nav>
  <div class="nav-logo"><div class="nav-logo-icon">⚡</div>ADOPT</div>
  <div class="nav-links">
    <button class="nav-btn btn-home active" id="nav-home" onclick="showView('home')">HOME</button>
    <button class="nav-btn btn-binlog" id="nav-binlog" onclick="showView('binlog')">
      <span class="nav-dot"></span>LIVE BINLOG<span class="nav-badge">LIVE</span>
    </button>
    <button class="nav-btn btn-fetch" id="nav-fetch" onclick="showView('fetch')">
      <span class="nav-dot"></span>FILE READER<span class="nav-badge">FILES</span>
    </button>
    <button class="nav-btn btn-ai" id="nav-ai" onclick="showView('ai')">
      <span class="nav-dot"></span>DATABASE AI<span class="nav-badge">AI</span>
    </button>
    <button class="nav-btn btn-users" id="nav-users" onclick="showView('users')">
      <span class="nav-dot"></span>USER ACTIVITY<span class="nav-badge">NEW</span>
    </button>
  </div>
  <div class="nav-spacer"></div>
  <div class="nav-status"><span class="live-dot"></span>SUITE v2</div>
</nav>
<div class="content">
  <div class="view active" id="view-home">
    <div class="home-scroll">
      <div class="hero">
        <div class="hero-glow"></div>
        <div class="hero-eyebrow"><span class="live-dot"></span>ADOPT INFRASTRUCTURE SUITE</div>
        <h1>Three Tools.<br>
          <span class="line2">One <span class="hl-g">Monitor</span>. One <span class="hl-i">Reader</span>. One <span class="hl-a">Brain</span>.</span>
        </h1>
        <p class="hero-sub">Real-time MySQL CDC · Historical binlog exploration · AI-powered database assistant</p>
      </div>
      <div class="ts-bar">
        <div class="ts-pill ts-checking" id="ts-binlog" onclick="showView('binlog')"><span class="ts-dot"></span>⚡ Live Monitor — checking...</div>
        <div class="ts-pill ts-checking" id="ts-fetch"  onclick="showView('fetch')"><span class="ts-dot"></span>📂 File Reader — checking...</div>
        <div class="ts-pill ts-checking" id="ts-ai"     onclick="showView('ai')"><span class="ts-dot"></span>🤖 Database AI — checking...</div>
      </div>
      <div class="stats-bar">
        <div class="stat-pill"><div class="stat-val" style="color:var(--binlog)">10</div><div class="stat-info"><div class="stat-label">Databases</div><div class="stat-sub">adoptconvergebss + 9</div></div></div>
        <div class="stat-pill"><div class="stat-val" style="color:var(--fetch)">3</div><div class="stat-info"><div class="stat-label">Tools</div><div class="stat-sub">Monitor · Reader · AI</div></div></div>
        <div class="stat-pill"><div class="stat-val" style="color:var(--ai)">CDC</div><div class="stat-info"><div class="stat-label">Change Capture</div><div class="stat-sub">Zero-latency stream</div></div></div>
        <div class="stat-pill"><div class="stat-val" style="color:#38bdf8">v2</div><div class="stat-info"><div class="stat-label">Version</div><div class="stat-sub">WhatsApp + Email</div></div></div>
      </div>
      <div class="tools-grid">
        <div class="tool-card card-binlog" onclick="showView('binlog')">
          <div class="card-num">01</div>
          <div class="card-header"><div class="card-icon">📡</div><div class="card-tag">● LIVE</div></div>
          <div class="card-title">Live Binlog MySQL Monitoring</div>
          <div class="card-desc">Real-time CDC from MySQL binary logs. Watch every INSERT, UPDATE, DELETE instantly.</div>
          <div class="card-features">
            <div class="card-feat">Real-time CDC via binlog stream</div>
            <div class="card-feat">Before / After diff for every UPDATE</div>
            <div class="card-feat">WhatsApp + Email notifications</div>
            <div class="card-feat">Per-database tab + CSV export</div>
          </div>
          <div class="card-footer"><div class="card-port">Port: <b>5000</b></div>
            <button class="card-open" onclick="event.stopPropagation();showView('binlog')">Open →</button></div>
        </div>
        <div class="tool-card card-fetch" onclick="showView('fetch')">
          <div class="card-num">02</div>
          <div class="card-header"><div class="card-icon">📂</div><div class="card-tag">BROWSE</div></div>
          <div class="card-title">Binlog File Reader &amp; Download</div>
          <div class="card-desc">Browse all historical binlog files. Stream, explore, and export full change history.</div>
          <div class="card-features">
            <div class="card-feat">Lists all binlog files with sizes</div>
            <div class="card-feat">Live streaming viewer per file</div>
            <div class="card-feat">Full-text search across all events</div>
            <div class="card-feat">CSV + HTML report export</div>
          </div>
          <div class="card-footer"><div class="card-port">Port: <b>7777</b></div>
            <button class="card-open" onclick="event.stopPropagation();showView('fetch')">Open →</button></div>
        </div>
        <div class="tool-card card-ai" onclick="showView('ai')">
          <div class="card-num">03</div>
          <div class="card-header"><div class="card-icon">🤖</div><div class="card-tag">AI</div></div>
          <div class="card-title">Database + AI Assistant</div>
          <div class="card-desc">Conversational AI explorer. Ask in plain English or write raw SQL.</div>
          <div class="card-features">
            <div class="card-feat">Natural language to SQL</div>
            <div class="card-feat">Smart SQL autocomplete</div>
            <div class="card-feat">Cross-database search</div>
            <div class="card-feat">Full schema explorer sidebar</div>
          </div>
          <div class="card-footer"><div class="card-port">Port: <b>7000</b></div>
            <button class="card-open" onclick="event.stopPropagation();showView('ai')">Open →</button></div>
        </div>
        <div class="tool-card card-binlog" onclick="showView('users')" style="--binlog:#00e676">
          <div class="card-num">04</div>
          <div class="card-header"><div class="card-icon">👤</div><div class="card-tag" style="background:#00e67612;color:#00e676;border:1px solid #00e67630">● NEW</div></div>
          <div class="card-title">User Activity Dashboard</div>
          <div class="card-desc">Track staff login history, module usage, and audit trail across all 125 staff users.</div>
          <div class="card-features">
            <div class="card-feat">938K+ audit events</div>
            <div class="card-feat">Per-user activity history</div>
            <div class="card-feat">Module & operation breakdown</div>
            <div class="card-feat">Live activity feed</div>
          </div>
          <div class="card-footer"><div class="card-port">Port: <b style="color:#00e676">6000</b></div>
            <button class="card-open" style="color:#00e676;border-color:#00e67640;background:#00e67608" onclick="event.stopPropagation();showView('users')">Open →</button></div>
        </div>
      </div>
      <footer>
        <span class="footer-logo">ADOPT · INFRASTRUCTURE SUITE</span>
        <span>MySQL CDC · Cohere AI · Flask · Render</span>
        <span>102.209.31.227</span>
      </footer>
    </div>
  </div>
  <div class="view" id="view-binlog" style="position:relative">
    <div class="offline-overlay" id="off-binlog">
      <div style="font-size:36px">⚡</div><b style="color:var(--binlog)">Live Monitor loading...</b>
      <div style="font-size:11px;color:#4a5a75">Please wait</div>
      <button class="retry-btn" onclick="reloadFrame('binlog')" style="color:var(--binlog);border-color:#22d87a40;background:#22d87a0a">↺ Retry</button>
    </div>
    <iframe class="tool-frame" id="frame-binlog" src="about:blank"></iframe>
  </div>
  <div class="view" id="view-fetch" style="position:relative">
    <div class="offline-overlay" id="off-fetch">
      <div style="font-size:36px">📂</div><b style="color:var(--fetch)">File Reader loading...</b>
      <div style="font-size:11px;color:#4a5a75">Please wait</div>
      <button class="retry-btn" onclick="reloadFrame('fetch')" style="color:var(--fetch);border-color:#6366f140;background:#6366f10a">↺ Retry</button>
    </div>
    <iframe class="tool-frame" id="frame-fetch" src="about:blank"></iframe>
  </div>
  <div class="view" id="view-ai" style="position:relative">
    <div class="offline-overlay" id="off-ai">
      <div style="font-size:36px">🤖</div><b style="color:var(--ai)">Database AI loading...</b>
      <div style="font-size:11px;color:#4a5a75">Please wait</div>
      <button class="retry-btn" onclick="reloadFrame('ai')" style="color:var(--ai);border-color:#f59e0b40;background:#f59e0b0a">↺ Retry</button>
    </div>
    <iframe class="tool-frame" id="frame-ai" src="about:blank"></iframe>
  </div>
  <div class="view" id="view-users" style="position:relative">
    <div class="offline-overlay" id="off-users">
      <div style="font-size:36px">👤</div><b style="color:#00e676">User Activity loading...</b>
      <div style="font-size:11px;color:#4a5a75">Please wait</div>
      <button class="retry-btn" onclick="reloadFrame('users')" style="color:#00e676;border-color:#00e67640;background:#00e6760a">↺ Retry</button>
    </div>
    <iframe class="tool-frame" id="frame-users" src="about:blank"></iframe>
  </div>
</div>
<script>
const loaded={binlog:false,fetch:false,ai:false,users:false};
const labels={binlog:'⚡ Live Monitor',fetch:'📂 File Reader',ai:'🤖 Database AI',users:'👤 User Activity'};
function showView(name){
  document.querySelectorAll('.view').forEach(v=>v.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('view-'+name).classList.add('active');
  document.getElementById('nav-'+name).classList.add('active');
  if(name!=='home'&&!loaded[name]) loadFrame(name);
  const t={home:'ADOPT · Suite',binlog:'ADOPT · Live Monitor',fetch:'ADOPT · File Reader',ai:'ADOPT · Database AI',users:'ADOPT · User Activity'};
  document.title=t[name]||'ADOPT';
}
function loadFrame(name){
  const frame=document.getElementById('frame-'+name);
  const overlay=document.getElementById('off-'+name);
  overlay.classList.add('show');
  frame.src='/tool/'+name+'/';
  loaded[name]=true;
  frame.onload=()=>{try{if(frame.contentDocument)overlay.classList.remove('show');}catch(e){overlay.classList.remove('show');}};
}
function reloadFrame(name){loaded[name]=false;loadFrame(name);}
async function checkStatus(){
  try{
    const res=await fetch('/api/status');
    const data=await res.json();
    for(const [name,info] of Object.entries(data)){
      const el=document.getElementById('ts-'+name);
      if(!el) continue;
      el.className='ts-pill '+(info.online?'ts-online':'ts-offline');
      el.innerHTML=`<span class="ts-dot"></span>${labels[name]} — ${info.online?'Online ✓':'Starting...'}`;
    }
  }catch(e){}
}
checkStatus();
setInterval(checkStatus,5000);
</script>
</body>
</html>"""

# ── Routes ─────────────────────────────────────────────────

@home_app.route("/")
def home():
    return Response(HOME_HTML, mimetype="text/html")

@home_app.route("/ping")
def ping():
    return Response("pong", mimetype="text/plain")

@home_app.route("/myip")
def myip():
    import json
    ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    all_headers = {k: v for k, v in request.headers}
    return Response(json.dumps({
        "remote_addr": request.remote_addr,
        "x_forwarded_for": request.headers.get("X-Forwarded-For"),
        "cf_connecting_ip": request.headers.get("CF-Connecting-IP"),
        "real_ip": ip
    }, indent=2), mimetype="application/json")

@home_app.route("/api/status")
def api_status():
    import json
    status = {}
    for name, url in TOOLS.items():
        try:
            req.get(url, timeout=2)
            status[name] = {"online": True}
        except:
            status[name] = {"online": False}
    return Response(json.dumps(status), mimetype="application/json")

@home_app.route("/inspect-login-tables")
def inspect_login_tables():
    import pymysql, json
    MYSQL = {"host":"102.209.31.227","port":3306,"user":"clusteradmin",
             "passwd":"ADOPT@2024#WIOCC@2023","cursorclass":pymysql.cursors.DictCursor}
    result = {}
    conn = pymysql.connect(**MYSQL)
    cur = conn.cursor()
    
    tables_to_check = [
        ("adoptconvergebss", "tbluseraudit"),
        ("adoptconvergebss", "tblauditlog"),
        ("adoptconvergebss", "tblstaffuser"),
        ("adoptradiusbss",   "tbltliveuser"),
        ("adoptcommonapigateway", "tblmstaffuser"),
    ]
    
    for db, table in tables_to_check:
        key = f"{db}.{table}"
        try:
            # columns
            cur.execute(f"DESCRIBE `{db}`.`{table}`")
            cols = [r['Field'] for r in cur.fetchall()]
            # sample rows
            cur.execute(f"SELECT * FROM `{db}`.`{table}` ORDER BY 1 DESC LIMIT 3")
            rows = cur.fetchall()
            # count
            cur.execute(f"SELECT COUNT(*) as cnt FROM `{db}`.`{table}`")
            cnt = cur.fetchone()['cnt']
            result[key] = {"columns": cols, "count": cnt, "sample": rows}
        except Exception as e:
            result[key] = {"error": str(e)}
    
    cur.close(); conn.close()
    return Response(json.dumps(result, indent=2, default=str), mimetype="application/json")


def rewrite_html(content, tool_name):
    """
    Rewrite internal URLs — skip already-prefixed /tool/ paths
    to avoid double rewriting.
    """
    prefix = f"/tool/{tool_name}"

    # Only rewrite paths NOT already starting with /tool/
    # Match: attr="/path  but NOT attr="/tool/
    # The path must start with / followed by a word char (not a quote or space)
    for attr in ["src", "href", "action"]:
        for q in ['"', "'"]:
            content = re.sub(
                rf'{attr}={q}/(?!tool/)(?=[a-zA-Z0-9_])',
                f'{attr}={q}{prefix}/',
                content
            )

    # fetch() calls
    content = re.sub(r"fetch\('/(?!tool/)", f"fetch('{prefix}/", content)
    content = re.sub(r'fetch\("/(?!tool/)', f'fetch("{prefix}/', content)
    content = re.sub(r"fetch\(`/(?!tool/)", f"fetch(`{prefix}/", content)

    # window.open()
    content = re.sub(r"window\.open\('/(?!tool/)", f"window.open('{prefix}/", content)
    content = re.sub(r'window\.open\("/(?!tool/)', f'window.open("{prefix}/', content)

    return content


def proxy_request(tool_name, path):
    base_url = TOOLS.get(tool_name)
    if not base_url:
        return Response(f"Unknown tool: {tool_name}", status=404)

    # Clean path — remove any accidental double slashes
    clean_path = path.lstrip("/")
    target_url = f"{base_url.rstrip('/')}/{clean_path}"
    if request.query_string:
        target_url += "?" + request.query_string.decode("utf-8")

    print(f"  [Proxy] {tool_name}: {request.method} {target_url}")

    try:
        headers = {k: v for k, v in request.headers
                   if k.lower() not in ("host", "content-length")}

        resp = req.request(
            method        = request.method,
            url           = target_url,
            headers       = headers,
            data          = request.get_data(),
            cookies       = request.cookies,
            allow_redirects = False,
            timeout       = 30,
            stream        = True,
        )

        excluded     = ["content-encoding", "content-length",
                        "transfer-encoding", "connection"]
        resp_headers = [(k, v) for k, v in resp.raw.headers.items()
                        if k.lower() not in excluded]
        content_type = resp.headers.get("Content-Type", "")

        if "text/html" in content_type:
            content = resp.content.decode("utf-8", errors="replace")
            content = rewrite_html(content, tool_name)
            return Response(content, status=resp.status_code,
                            headers=resp_headers, content_type=content_type)

        # JSON / static files — pass through unchanged
        return Response(
            resp.iter_content(chunk_size=8192),
            status       = resp.status_code,
            headers      = resp_headers,
            content_type = content_type,
        )

    except req.exceptions.ConnectionError:
        # Return a friendly retry page — NOT a JSON error
        return Response(
            f"""<!DOCTYPE html><html><body style="background:#05080f;color:#f43f5e;
            font-family:monospace;display:flex;align-items:center;justify-content:center;
            height:100vh;flex-direction:column;gap:12px;font-size:13px;text-align:center;padding:20px">
            <div style="font-size:40px">⚠️</div>
            <b>{tool_name} is starting up...</b>
            <div style="color:#4a5a75;font-size:11px">Auto-retrying in 5 seconds...</div>
            <script>setTimeout(()=>location.reload(),5000)</script>
            </body></html>""",
            status=503, content_type="text/html"
        )
    except Exception as e:
        print(f"  [Proxy] Error: {e}")
        return Response(f"Proxy error: {e}", status=500)


@home_app.route("/tool/binlog/", defaults={"path": ""})
@home_app.route("/tool/binlog/<path:path>", methods=["GET","POST","PUT","DELETE","PATCH"])
def proxy_binlog(path): return proxy_request("binlog", path)

@home_app.route("/tool/fetch/", defaults={"path": ""})
@home_app.route("/tool/fetch/<path:path>", methods=["GET","POST","PUT","DELETE","PATCH"])
def proxy_fetch(path): return proxy_request("fetch", path)

@home_app.route("/tool/ai/", defaults={"path": ""})
@home_app.route("/tool/ai/<path:path>", methods=["GET","POST","PUT","DELETE","PATCH"])
def proxy_ai(path): return proxy_request("ai", path)

@home_app.route("/tool/users/", defaults={"path": ""})
@home_app.route("/tool/users/<path:path>", methods=["GET","POST","PUT","DELETE","PATCH"])
def proxy_users(path): return proxy_request("users", path)


# ── Main ───────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  ADOPT Infrastructure Suite — Master Server v3")
    print("=" * 60 + "\n")

    threading.Thread(target=start_tool_1, daemon=True).start(); time.sleep(1)
    threading.Thread(target=start_tool_2, daemon=True).start(); time.sleep(1)
    threading.Thread(target=start_tool_3, daemon=True).start(); time.sleep(2)
    threading.Thread(target=start_tool_4, daemon=True).start(); time.sleep(1)
    threading.Thread(target=keep_alive,   daemon=True).start()

    print(f"\n  ✅  All tools started!")
    print(f"  🌐  Open: http://localhost:{PORT}\n")

    home_app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
