#!/usr/bin/env python3
"""
dashboard.py
────────────
CloudOps Infrastructure Health Dashboard.

Serves the web UI on  http://localhost:5000
Proxies Prometheus metrics from the collector on http://localhost:8000/metrics

Run AFTER (or alongside) main.py:
    python dashboard.py

Environment variables:
    PROM_URL        URL of the Prometheus /metrics endpoint
                    (default: http://localhost:8000/metrics)
    DASHBOARD_PORT  Port to serve the dashboard on (default: 5000)
    DASHBOARD_HOST  Host to bind to (default: 0.0.0.0)
"""
from __future__ import annotations

import os
import sqlite3
import json
from flask import Flask, render_template_string, jsonify, request
import requests

app = Flask(__name__)

PROM_URL       = os.getenv("PROM_URL",        "http://localhost:8000/metrics")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "5000"))
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST",   "0.0.0.0")
DB_PATH        = os.getenv("DB_PATH",          "observability_data/metrics.db")


@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


# ══════════════════════════════════════════════════════════════════════════════
# HTML DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

DASHBOARD_HTML = r'''
<!DOCTYPE html>
<html lang="en">
<head>
    <title>CloudOps — Infrastructure Health</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        *, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }
        :root {
            --bg:#f0f4f8; --surface:#fff; --surface2:#f8fafc; --border:#e2e8f0; --border2:#cbd5e1;
            --accent:#2563eb; --accent-l:#eff6ff;
            --green:#059669; --green-l:#ecfdf5;
            --yellow:#d97706; --yellow-l:#fffbeb;
            --red:#dc2626; --red-l:#fef2f2;
            --orange:#ea580c; --orange-l:#fff7ed;
            --text:#0f172a; --text2:#334155; --muted:#64748b; --muted2:#94a3b8;
            --sans:'Plus Jakarta Sans',sans-serif; --mono:'JetBrains Mono',monospace;
            --shadow:0 1px 3px rgba(0,0,0,.08),0 1px 2px rgba(0,0,0,.04);
            --shadow-md:0 4px 12px rgba(0,0,0,.08),0 2px 4px rgba(0,0,0,.04);
        }
        body { font-family:var(--sans); background:var(--bg); color:var(--text); min-height:100vh; }

        /* ── Topbar ── */
        .topbar { position:sticky; top:0; z-index:100; display:flex; align-items:center; justify-content:space-between; padding:0 32px; height:60px; background:var(--surface); border-bottom:1px solid var(--border); box-shadow:var(--shadow); }
        .brand { display:flex; align-items:center; gap:10px; font-weight:800; font-size:1.15rem; color:var(--text); letter-spacing:-.3px; }
        .brand-icon { width:32px; height:32px; border-radius:8px; background:linear-gradient(135deg,#2563eb,#7c3aed); display:flex; align-items:center; justify-content:center; font-size:.9rem; color:#fff; }
        .brand-tag { font-size:.65rem; font-weight:600; letter-spacing:1.5px; color:var(--muted); text-transform:uppercase; margin-left:4px; font-family:var(--mono); }
        .topbar-right { display:flex; align-items:center; gap:16px; }
        #last-ts { font-size:.75rem; color:var(--muted); font-family:var(--mono); }
        .live-badge { display:inline-flex; align-items:center; gap:6px; padding:5px 12px; border-radius:20px; font-size:.7rem; font-weight:700; letter-spacing:.5px; }
        .live-badge.live { background:var(--green-l); color:var(--green); }
        .live-badge.dead { background:var(--red-l); color:var(--red); }
        .live-badge .dot { width:7px; height:7px; border-radius:50%; background:currentColor; }
        .live-badge.live .dot { animation:pulse 1.4s infinite; }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }

        /* ── Nav ── */
        .nav-wrap { background:var(--surface); border-bottom:1px solid var(--border); padding:0 32px; overflow-x:auto; }
        .nav { display:flex; }
        .nav-tab { padding:14px 20px; font-size:.8rem; font-weight:600; color:var(--muted); background:none; border:none; border-bottom:2px solid transparent; cursor:pointer; white-space:nowrap; font-family:var(--sans); transition:color .15s,border-color .15s; }
        .nav-tab:hover { color:var(--text2); }
        .nav-tab.active { color:var(--accent); border-bottom-color:var(--accent); }
        .nav-tab .cnt { margin-left:7px; background:var(--bg); color:var(--muted); border-radius:10px; padding:1px 8px; font-size:.68rem; font-weight:700; }
        .nav-tab.active .cnt { background:var(--accent-l); color:var(--accent); }
        .nav-tab.anomaly-tab.active { color:var(--orange); border-bottom-color:var(--orange); }
        .nav-tab.anomaly-tab.active .cnt { background:var(--orange-l); color:var(--orange); }
        .nav-tab.anomaly-tab .cnt.has-data { background:var(--orange-l); color:var(--orange); }

        /* ── Layout ── */
        .main { padding:24px 32px 48px; max-width:1600px; margin:0 auto; }
        .tab-panel { display:none; }
        .tab-panel.active { display:block; }

        /* ── Summary chips ── */
        .strip { display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap; }
        .chip { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:12px 18px; box-shadow:var(--shadow); min-width:120px; }
        .chip .v { font-size:1.6rem; font-weight:800; line-height:1; letter-spacing:-1px; margin-bottom:3px; }
        .chip .l { font-size:.62rem; font-weight:600; letter-spacing:.8px; text-transform:uppercase; color:var(--muted); }
        .c-blue .v{color:var(--accent)} .c-green .v{color:var(--green)} .c-yellow .v{color:var(--yellow)} .c-red .v{color:var(--red)} .c-orange .v{color:var(--orange)}

        /* ── Table ── */
        .table-card { background:var(--surface); border:1px solid var(--border); border-radius:12px; box-shadow:var(--shadow); overflow:hidden; }
        .rt { width:100%; border-collapse:collapse; font-size:.8rem; }
        .rt thead tr { background:var(--surface2); border-bottom:1px solid var(--border); }
        .rt th { padding:11px 16px; text-align:left; font-weight:700; font-size:.65rem; letter-spacing:.8px; text-transform:uppercase; color:var(--muted); white-space:nowrap; }
        .rt tbody tr { border-bottom:1px solid var(--border); transition:background .12s; animation:rowIn .3s ease both; }
        @keyframes rowIn { from{opacity:0;transform:translateY(4px)} to{opacity:1;transform:none} }
        .rt tbody tr:last-child { border-bottom:none; }
        .rt tbody tr:hover { background:#f8fafc; }
        .rt td { padding:13px 16px; vertical-align:middle; white-space:nowrap; }
        .rname strong { display:block; color:var(--text); font-weight:700; font-size:.82rem; max-width:220px; overflow:hidden; text-overflow:ellipsis; }
        .rname small { display:block; color:var(--muted2); font-size:.68rem; font-family:var(--mono); max-width:220px; overflow:hidden; text-overflow:ellipsis; margin-top:1px; }
        .bdg { display:inline-flex; align-items:center; gap:5px; padding:4px 10px; border-radius:20px; font-size:.68rem; font-weight:700; white-space:nowrap; }
        .bdg-ok{background:var(--green-l);color:var(--green)} .bdg-warn{background:var(--yellow-l);color:var(--yellow)} .bdg-crit{background:var(--red-l);color:var(--red)} .bdg-unkn{background:var(--bg);color:var(--muted)}
        .bdg-dot{width:6px;height:6px;border-radius:50%;background:currentColor}
        .mv{color:var(--text2);font-weight:500;font-family:var(--mono);font-size:.78rem}
        .mv-hi{color:var(--red);font-weight:700;font-family:var(--mono);font-size:.78rem}
        .mv-mid{color:var(--yellow);font-weight:600;font-family:var(--mono);font-size:.78rem}
        .mv-lo{color:var(--green);font-weight:500;font-family:var(--mono);font-size:.78rem}
        .mv-na{color:var(--muted2);font-size:.72rem;font-family:var(--mono)}
        .mbar{display:flex;align-items:center;gap:8px}
        .mbar-t{width:64px;height:5px;background:var(--border);border-radius:3px;overflow:hidden;flex-shrink:0}
        .mbar-f{height:100%;border-radius:3px;transition:width 1s ease}
        .mbar-f.g{background:var(--green)} .mbar-f.y{background:var(--yellow)} .mbar-f.r{background:var(--red)}
        .rpill{display:inline-block;padding:3px 9px;border-radius:20px;font-size:.65rem;font-weight:600;font-family:var(--mono);background:var(--accent-l);color:var(--accent);letter-spacing:.3px}
        .xbtn{cursor:pointer;color:var(--muted);font-size:.72rem;background:var(--bg);border:1px solid var(--border);font-family:var(--mono);padding:3px 9px;border-radius:6px;transition:all .15s}
        .xbtn:hover{color:var(--accent);border-color:var(--accent);background:var(--accent-l)}
        .drow{display:none} .drow.open{display:table-row}
        .dcell{background:var(--surface2);border-bottom:1px solid var(--border);padding:0!important}
        .dinner{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:1px;background:var(--border)}
        .dkv{background:var(--surface2);padding:12px 18px}
        .dkv span{font-size:.62rem;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:var(--muted);display:block;margin-bottom:4px}
        .dkv b{font-size:.8rem;color:var(--text2);font-weight:600;font-family:var(--mono)}
        .err{background:var(--red-l);border:1px solid #fecaca;border-radius:10px;padding:12px 18px;font-size:.78rem;color:var(--red);margin-bottom:20px;display:none;font-weight:500}
        .err.show{display:block}
        .empty{text-align:center;padding:60px 20px}
        .empty-icon{font-size:2.5rem;margin-bottom:12px}
        .empty h3{font-weight:700;color:var(--text);margin-bottom:6px}
        .empty p{font-size:.8rem;color:var(--muted);line-height:1.7}
        @keyframes shimmer{0%,100%{opacity:.5}50%{opacity:1}}
        .sh{animation:shimmer 1.4s infinite;color:var(--muted2)}

        /* ── DB Stats panel ── */
        .db-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;margin-bottom:24px}
        .db-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px;box-shadow:var(--shadow)}
        .db-card h3{font-size:.7rem;font-weight:700;letter-spacing:.8px;text-transform:uppercase;color:var(--muted);margin-bottom:14px}
        .db-stat{display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid var(--border);font-size:.8rem}
        .db-stat:last-child{border-bottom:none}
        .db-stat .k{color:var(--text2);font-weight:500}
        .db-stat .v{color:var(--text);font-weight:700;font-family:var(--mono)}

        /* ── Anomaly panel ── */
        .an-timeline { display:flex; gap:6px; align-items:flex-end; padding:16px; background:var(--surface2); border-radius:10px; border:1px solid var(--border); margin-bottom:20px; overflow-x:auto; }
        .an-bar-wrap { display:flex; flex-direction:column; align-items:center; gap:4px; cursor:pointer; min-width:44px; flex-shrink:0; }
        .an-bar-count { font-size:9px; font-weight:700; font-family:var(--mono); min-height:14px; }
        .an-bar { width:36px; border-radius:4px; border:2px solid transparent; transition:all .2s; }
        .an-bar.ok { background:var(--border); min-height:18px; }
        .an-bar.has-anomaly { background:rgba(234,88,12,.25); border-color:rgba(234,88,12,.4); }
        .an-bar.selected { border-color:var(--orange) !important; box-shadow:0 0 10px rgba(234,88,12,.35); }
        .an-bar.ok.selected { background:var(--accent-l); border-color:var(--accent) !important; box-shadow:0 0 10px rgba(37,99,235,.3); }
        .an-bar-label { font-size:9px; font-family:var(--mono); color:var(--muted); }
        .an-legend { display:flex; gap:16px; padding:0 4px; }
        .an-legend-item { display:flex; align-items:center; gap:6px; font-size:10px; color:var(--muted); }
        .an-legend-dot { width:10px; height:10px; border-radius:2px; }

        .an-detail-header { display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }
        .an-detail-title { font-weight:700; font-size:.9rem; }
        .an-severity-badge { padding:4px 12px; border-radius:20px; font-size:.7rem; font-weight:700; }
        .sev-warning { background:var(--yellow-l); color:var(--yellow); }
        .sev-critical { background:var(--red-l); color:var(--red); }

        .an-card { background:var(--surface); border:1px solid var(--border); border-radius:10px; overflow:hidden; margin-bottom:10px; transition:box-shadow .15s; }
        .an-card:hover { box-shadow:var(--shadow-md); }
        .an-card-header { display:flex; align-items:center; gap:12px; padding:14px 16px; cursor:pointer; border-left:3px solid; }
        .an-card-header.sev-warning { border-left-color:var(--yellow); }
        .an-card-header.sev-critical { border-left-color:var(--red); }
        .an-card-icon { width:34px; height:34px; border-radius:8px; display:flex; align-items:center; justify-content:center; font-size:14px; flex-shrink:0; }
        .an-card-meta { flex:1; min-width:0; }
        .an-card-name { font-weight:700; font-size:.82rem; color:var(--text); margin-bottom:2px; }
        .an-card-metric { font-size:.72rem; font-family:var(--mono); color:var(--muted); }
        .an-card-val { text-align:right; }
        .an-card-val .cur { font-weight:800; font-size:.9rem; font-family:var(--mono); }
        .an-card-val .cur.warning { color:var(--yellow); }
        .an-card-val .cur.critical { color:var(--red); }
        .an-card-val .lim { font-size:.65rem; color:var(--muted); font-family:var(--mono); }
        .an-card-chevron { color:var(--muted); transition:transform .2s; margin-left:8px; }
        .an-card-chevron.open { transform:rotate(180deg); }

        .an-card-body { display:none; padding:14px 16px; border-top:1px solid var(--border); background:var(--surface2); }
        .an-card-body.open { display:block; }
        .an-stats-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:14px; }
        .an-stat { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:10px 12px; }
        .an-stat .lbl { font-size:.6rem; text-transform:uppercase; letter-spacing:.8px; color:var(--muted); margin-bottom:4px; }
        .an-stat .val { font-family:var(--mono); font-weight:700; font-size:.82rem; color:var(--text2); }

        .devbar-wrap { margin-bottom:12px; }
        .devbar-label { font-size:.65rem; text-transform:uppercase; letter-spacing:.8px; color:var(--muted); margin-bottom:6px; }
        .devbar { position:relative; height:28px; background:#e2e8f0; border-radius:6px; overflow:hidden; }
        .devbar-normal { position:absolute; top:0; bottom:0; background:rgba(5,150,105,.2); border-left:2px solid var(--green); border-right:2px solid var(--green); }
        .devbar-marker { position:absolute; top:15%; bottom:15%; width:3px; border-radius:2px; transition:left .6s ease; }
        .devbar-marker.warning { background:var(--yellow); box-shadow:0 0 6px var(--yellow); }
        .devbar-marker.critical { background:var(--red); box-shadow:0 0 6px var(--red); }
        .devbar-low { position:absolute; left:4px; top:50%; transform:translateY(-50%); font-size:9px; color:var(--muted); font-family:var(--mono); }
        .devbar-high { position:absolute; right:4px; top:50%; transform:translateY(-50%); font-size:9px; color:var(--muted); font-family:var(--mono); }

        .an-reason { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:10px 12px; font-size:.75rem; color:var(--text2); line-height:1.6; font-family:var(--mono); }

        .an-filters { display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; align-items:center; }
        .an-filter-select { padding:6px 12px; border:1px solid var(--border); border-radius:8px; font-size:.75rem; font-family:var(--sans); color:var(--text2); background:var(--surface); cursor:pointer; }
        .an-filter-select:focus { outline:none; border-color:var(--accent); }
        .an-hours-label { font-size:.75rem; color:var(--muted); }

        .an-empty { text-align:center; padding:48px 20px; }
        .an-empty-icon { font-size:2.5rem; margin-bottom:10px; }
        .an-empty h3 { font-weight:700; color:var(--green); margin-bottom:6px; }
        .an-empty p { font-size:.8rem; color:var(--muted); }

        ::-webkit-scrollbar{width:6px;height:6px}
        ::-webkit-scrollbar-track{background:var(--bg)}
        ::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}
        footer{text-align:center;font-size:.7rem;color:var(--muted);padding:20px;border-top:1px solid var(--border);background:var(--surface);margin-top:40px}
    </style>
</head>
<body>

<div class="topbar">
    <div class="brand">
        <div class="brand-icon">☁</div>
        CloudOps
        <span class="brand-tag">Infrastructure Health</span>
    </div>
    <div class="topbar-right">
        <span id="last-ts"></span>
        <span class="live-badge live" id="status"><span class="dot"></span> LIVE</span>
    </div>
</div>

<div class="nav-wrap">
    <div class="nav">
        <button class="nav-tab active" onclick="showTab('ec2',this)">EC2 <span class="cnt" id="tc-ec2">0</span></button>
        <button class="nav-tab" onclick="showTab('rds',this)">RDS <span class="cnt" id="tc-rds">0</span></button>
        <button class="nav-tab" onclick="showTab('lam',this)">Lambda <span class="cnt" id="tc-lam">0</span></button>
        <button class="nav-tab" onclick="showTab('logs',this)">Logs <span class="cnt" id="tc-logs">0</span></button>
        <button class="nav-tab anomaly-tab" onclick="showTab('anomalies',this)" id="anomaly-tab-btn">🔍 Anomalies <span class="cnt" id="tc-anomalies">0</span></button>
        <button class="nav-tab" onclick="showTab('db',this)">DB Stats <span class="cnt" id="tc-db">✦</span></button>
    </div>
</div>

<div class="main">
<div class="err" id="err-bar"></div>

<!-- EC2 -->
<div class="tab-panel active" id="panel-ec2">
    <div class="strip" id="s-ec2"></div>
    <div class="table-card">
    <table class="rt">
        <thead><tr><th>Instance</th><th>Region</th><th>Health</th><th>CPU %</th><th>Net In</th><th>Net Out</th><th>Disk Read</th><th>Disk Write</th><th>Status Check</th><th></th></tr></thead>
        <tbody id="tb-ec2"><tr><td colspan="10"><div class="empty sh">Loading EC2…</div></td></tr></tbody>
    </table></div>
</div>

<!-- RDS -->
<div class="tab-panel" id="panel-rds">
    <div class="strip" id="s-rds"></div>
    <div class="table-card">
    <table class="rt">
        <thead><tr><th>Database</th><th>Region</th><th>Health</th><th>CPU %</th><th>Connections</th><th>Read IOPS</th><th>Write IOPS</th><th>Free Storage</th><th>Replica Lag</th><th></th></tr></thead>
        <tbody id="tb-rds"><tr><td colspan="10"><div class="empty sh">Loading RDS…</div></td></tr></tbody>
    </table></div>
</div>

<!-- Lambda -->
<div class="tab-panel" id="panel-lam">
    <div class="strip" id="s-lam"></div>
    <div class="table-card">
    <table class="rt">
        <thead><tr><th>Function</th><th>Region</th><th>Health</th><th>Invocations</th><th>Errors</th><th>Error Rate</th><th>Throttles</th><th>Avg Duration</th><th>Max Duration</th><th></th></tr></thead>
        <tbody id="tb-lam"><tr><td colspan="10"><div class="empty sh">Loading Lambda…</div></td></tr></tbody>
    </table></div>
</div>

<!-- Logs -->
<div class="tab-panel" id="panel-logs">
    <div class="strip" id="s-logs"></div>
    <div class="table-card">
    <table class="rt">
        <thead><tr><th>Cloud / Region</th><th>Errors + Critical</th><th>Warnings</th><th>Info</th><th>Debug</th><th>Health</th></tr></thead>
        <tbody id="tb-logs"><tr><td colspan="6"><div class="empty sh">Loading logs…</div></td></tr></tbody>
    </table></div>
</div>

<!-- Anomalies -->
<div class="tab-panel" id="panel-anomalies">
    <div class="strip" id="s-anomalies"></div>

    <div class="an-filters">
        <span class="an-hours-label">Show last:</span>
        <select class="an-filter-select" id="an-hours" onchange="loadAnomalies()">
            <option value="1">1 hour</option>
            <option value="6">6 hours</option>
            <option value="24" selected>24 hours</option>
            <option value="72">3 days</option>
            <option value="168">7 days</option>
        </select>
        <select class="an-filter-select" id="an-severity" onchange="loadAnomalies()">
            <option value="">All severities</option>
            <option value="critical">Critical only</option>
            <option value="warning">Warning only</option>
        </select>
        <select class="an-filter-select" id="an-rtype" onchange="loadAnomalies()">
            <option value="">All resource types</option>
            <option value="ec2">EC2</option>
            <option value="rds">RDS</option>
            <option value="lambda">Lambda</option>
        </select>
        <button class="xbtn" onclick="loadAnomalies()" style="margin-left:auto">⟳ Refresh</button>
    </div>

    <!-- Timeline bar chart -->
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px 20px;box-shadow:var(--shadow);margin-bottom:20px;">
        <div style="font-size:.7rem;font-weight:700;letter-spacing:.8px;text-transform:uppercase;color:var(--muted);margin-bottom:4px;">Detection Timeline — click a bar to filter</div>
        <div style="font-size:.75rem;color:var(--muted2);margin-bottom:12px;" id="an-timeline-subtitle">Loading…</div>
        <div class="an-timeline" id="an-timeline-chart"></div>
        <div class="an-legend" style="margin-top:8px;">
            <div class="an-legend-item"><div class="an-legend-dot" style="background:rgba(234,88,12,.25);border:1px solid rgba(234,88,12,.4)"></div>Has anomalies</div>
            <div class="an-legend-item"><div class="an-legend-dot" style="background:var(--border)"></div>All normal</div>
            <div class="an-legend-item"><div class="an-legend-dot" style="background:var(--orange-l);border:1px solid var(--orange)"></div>Selected</div>
        </div>
    </div>

    <!-- Anomaly cards -->
    <div id="an-cards-header" class="an-detail-header" style="display:none;">
        <span class="an-detail-title" id="an-cards-title"></span>
        <button class="xbtn" onclick="clearAnFilter()">✕ Show all</button>
    </div>
    <div id="an-cards"></div>
</div>

<!-- DB Stats -->
<div class="tab-panel" id="panel-db">
    <div class="strip" id="s-db"></div>
    <div id="db-content"><div class="empty sh">Loading database stats…</div></div>
</div>

</div><!-- .main -->

<footer>CloudOps · Auto-refresh 10s · Prometheus → localhost:8000/metrics · DB → observability_data/metrics.db</footer>

<script>
// ── Prometheus parser ──────────────────────────────────────────────────────
function parseProm(text) {
    const out = {};
    for (const line of text.split('\n')) {
        if (!line || line.startsWith('#')) continue;
        const sp = line.lastIndexOf(' ');
        if (sp < 0) continue;
        const raw = line.slice(0, sp).trim();
        const v   = parseFloat(line.slice(sp + 1));
        if (isNaN(v)) continue;
        const bi   = raw.indexOf('{');
        const name = bi >= 0 ? raw.slice(0, bi) : raw;
        const lstr = bi >= 0 ? raw.slice(bi+1, raw.lastIndexOf('}')) : '';
        const lbls = {};
        for (const p of lstr.matchAll(/(\w+)="([^"]*)"/g)) lbls[p[1]] = p[2];
        if (!out[name]) out[name] = [];
        out[name].push({ lbls, value: v });
    }
    return out;
}

const G = (cloud, rtype, metric) => `cloud_${cloud}_${rtype}_${metric}`;

function V(m, gn, rid) {
    const s = (m[gn]||[]).find(x => x.lbls.resource_id === rid);
    return s ? s.value : null;
}

function resources(m, gn) {
    const map = new Map();
    for (const s of (m[gn]||[]))
        if (s.lbls.resource_id && !map.has(s.lbls.resource_id))
            map.set(s.lbls.resource_id, { id:s.lbls.resource_id, name:s.lbls.resource_name||s.lbls.resource_id, region:s.lbls.region||'--' });
    return [...map.values()];
}

function discoveredResources(m, cloud, rtype) {
    const map = new Map();
    for (const s of (m['cloud_resource_discovered']||[]))
        if (s.lbls.cloud===cloud && s.lbls.resource_type===rtype && s.value>0)
            if (!map.has(s.lbls.resource_id))
                map.set(s.lbls.resource_id, { id:s.lbls.resource_id, name:s.lbls.resource_name||s.lbls.resource_id, region:s.lbls.region||'--' });
    return [...map.values()];
}

function mergeResWithDiscovery(m, gauges, cloud, rtype) {
    const map = new Map();
    for (const gn of gauges)
        for (const r of resources(m, gn))
            if (!map.has(r.id)) map.set(r.id, r);
    for (const r of discoveredResources(m, cloud, rtype))
        if (!map.has(r.id)) map.set(r.id, r);
    return [...map.values()];
}

function mergeRes(m, gauges) {
    const map = new Map();
    for (const gn of gauges)
        for (const r of resources(m, gn))
            if (!map.has(r.id)) map.set(r.id, r);
    return [...map.values()];
}

// ── Formatters ─────────────────────────────────────────────────────────────
const N  = (v,d=0) => v==null?'--':(+v).toFixed(d).replace(/\B(?=(\d{3})+(?!\d))/g,',');
const P  = (v,d=1) => v==null?'--':(+v).toFixed(d)+'%';
const MS = v => v==null?'--':(+v).toFixed(1)+' ms';
const SC = v => v==null?'--':(+v).toFixed(3)+' s';
function B(v) {
    if (v==null) return '--'; v=+v;
    if (v>=1e12) return (v/1e12).toFixed(2)+' TB';
    if (v>=1e9)  return (v/1e9).toFixed(2)+' GB';
    if (v>=1e6)  return (v/1e6).toFixed(1)+' MB';
    if (v>=1e3)  return (v/1e3).toFixed(1)+' KB';
    return v.toFixed(0)+' B';
}

function badge(s) {
    const c={ok:'bdg-ok',warn:'bdg-warn',crit:'bdg-crit',unkn:'bdg-unkn'};
    const l={ok:'● Healthy',warn:'▲ Warning',crit:'✕ Critical',unkn:'? No Data'};
    return `<span class="bdg ${c[s]||c.unkn}"><span class="bdg-dot"></span>${l[s]||l.unkn}</span>`;
}

function bar(v) {
    if (v==null) return '<span class="mv-na">--</span>';
    const p=Math.min(100,Math.max(0,+v)), c=p>85?'r':p>65?'y':'g', tc=p>85?'mv-hi':p>65?'mv-mid':'mv-lo';
    return `<div class="mbar"><div class="mbar-t"><div class="mbar-f ${c}" style="width:${p}%"></div></div><span class="${tc}">${p.toFixed(1)}%</span></div>`;
}

function MV(v,{hi,mid,fmt=N}={}) {
    if (v==null) return '<span class="mv-na">--</span>';
    const cls=hi!=null&&+v>=hi?'mv-hi':mid!=null&&+v>=mid?'mv-mid':'mv';
    return `<span class="${cls}">${fmt(v)}</span>`;
}

function chip(v,l,color) { return `<div class="chip c-${color}"><div class="v">${v}</div><div class="l">${l}</div></div>`; }
function dkv(label,val) { return `<div class="dkv"><span>${label}</span><b>${val}</b></div>`; }

const _open={};
function toggle(uid) {
    _open[uid]=!_open[uid];
    document.getElementById('dr-'+uid).classList.toggle('open',_open[uid]);
    const b=document.getElementById('xb-'+uid);
    if(b) b.textContent=_open[uid]?'▲ Less':'▼ More';
}
function showTab(name,el) {
    document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
    document.querySelectorAll('.nav-tab').forEach(t=>t.classList.remove('active'));
    document.getElementById('panel-'+name).classList.add('active');
    el.classList.add('active');
    if (name==='db') loadDbStats();
    if (name==='anomalies') loadAnomalies();
}

// ══════════════════════════════════════════════════════════════════════════
// RENDERERS
// ══════════════════════════════════════════════════════════════════════════

function renderEC2(m) {
    const list=mergeRes(m,[G('aws','ec2','cpu_utilization_percent'),G('aws','ec2','network_in_bytes')]);
    document.getElementById('tc-ec2').textContent=list.length;
    const cpuVals=list.map(r=>V(m,G('aws','ec2','cpu_utilization_percent'),r.id)).filter(v=>v!=null);
    const avgCpu=cpuVals.length?cpuVals.reduce((a,b)=>a+b,0)/cpuVals.length:null;
    const nCrit=cpuVals.filter(v=>v>85).length, nWarn=cpuVals.filter(v=>v>65&&v<=85).length;
    document.getElementById('s-ec2').innerHTML=
        chip(list.length,'Instances','blue')+
        chip(avgCpu!=null?avgCpu.toFixed(1)+'%':'--','Avg CPU',avgCpu>85?'red':avgCpu>65?'yellow':'green')+
        chip(nCrit,'CPU Critical >85%',nCrit?'red':'green')+
        chip(nWarn,'CPU Warning >65%',nWarn?'yellow':'green');
    if (!list.length) { document.getElementById('tb-ec2').innerHTML=`<tr><td colspan="10"><div class="empty"><div class="empty-icon">🖥️</div><h3>No EC2 instances found</h3><p>Ensure credentials are valid and the collector has run.</p></div></td></tr>`; return; }
    document.getElementById('tb-ec2').innerHTML=list.map((r,i)=>{
        const cpu=V(m,G('aws','ec2','cpu_utilization_percent'),r.id);
        const nI=V(m,G('aws','ec2','network_in_bytes'),r.id), nO=V(m,G('aws','ec2','network_out_bytes'),r.id);
        const dR=V(m,G('aws','ec2','disk_read_bytes'),r.id), dW=V(m,G('aws','ec2','disk_write_bytes'),r.id);
        const sc=V(m,G('aws','ec2','status_check_failed'),r.id);
        const mem=V(m,G('aws','ec2','memory_used_percent'),r.id), dsk=V(m,G('aws','ec2','disk_used_percent'),r.id);
        const pI=V(m,G('aws','ec2','network_packets_in'),r.id), pO=V(m,G('aws','ec2','network_packets_out'),r.id);
        const scI=V(m,G('aws','ec2','status_check_failed_instance'),r.id), scS=V(m,G('aws','ec2','status_check_failed_system'),r.id);
        const rO=V(m,G('aws','ec2','disk_read_ops'),r.id), wO=V(m,G('aws','ec2','disk_write_ops'),r.id);
        const health=cpu==null?'unkn':(sc||0)>0?'crit':cpu>85?'crit':cpu>65?'warn':'ok';
        const uid='ec2'+i;
        return `<tr style="animation-delay:${i*20}ms">
            <td><div class="rname"><strong title="${r.id}">${r.name}</strong><small>${r.id}</small></div></td>
            <td><span class="rpill">${r.region}</span></td>
            <td>${badge(health)}</td><td>${bar(cpu)}</td>
            <td class="mv">${B(nI)}</td><td class="mv">${B(nO)}</td>
            <td class="mv">${B(dR)}</td><td class="mv">${B(dW)}</td>
            <td>${MV(sc,{hi:1,fmt:v=>+v>0?`⚠ ${N(v)} failed`:'✓ OK'})}</td>
            <td><button class="xbtn" id="xb-${uid}" onclick="toggle('${uid}')">▼ More</button></td>
        </tr>
        <tr class="drow" id="dr-${uid}"><td class="dcell" colspan="10"><div class="dinner">
            ${dkv('Memory %',mem!=null?P(mem):'N/A — CW Agent needed')}
            ${dkv('Disk %',dsk!=null?P(dsk):'N/A — CW Agent needed')}
            ${dkv('Packets In',N(pI))} ${dkv('Packets Out',N(pO))}
            ${dkv('Disk Read Ops',N(rO))} ${dkv('Disk Write Ops',N(wO))}
            ${dkv('Instance Check',N(scI))} ${dkv('System Check',N(scS))}
        </div></td></tr>`;
    }).join('');
}

function renderRDS(m) {
    const list=mergeResWithDiscovery(m,[G('aws','rds','cpu_utilization_percent'),G('aws','rds','database_connections')],'aws','rds');
    document.getElementById('tc-rds').textContent=list.length;

    const cpuVals=list.map(r=>V(m,G('aws','rds','cpu_utilization_percent'),r.id)).filter(v=>v!=null);
    const avgCpu=cpuVals.length?cpuVals.reduce((a,b)=>a+b,0)/cpuVals.length:null;
    const nC=cpuVals.filter(v=>v>85).length;
    const totalConn=list.reduce((s,r)=>s+(V(m,G('aws','rds','database_connections'),r.id)||0),0);
    document.getElementById('s-rds').innerHTML=
        chip(list.length,'Databases','blue')+
        chip(avgCpu!=null?avgCpu.toFixed(1)+'%':'--','Avg CPU',avgCpu>85?'red':avgCpu>65?'yellow':'green')+
        chip(nC,'CPU Critical >85%',nC?'red':'green')+
        chip(N(totalConn),'Total Connections',totalConn>0?'blue':'green');

    if (!list.length) { document.getElementById('tb-rds').innerHTML=`<tr><td colspan="10"><div class="empty"><div class="empty-icon">🗄️</div><h3>No RDS instances</h3><p>Enable rds_instances in config YAML.</p></div></td></tr>`; return; }

    document.getElementById('tb-rds').innerHTML=list.map((r,i)=>{
        const cpu  = V(m,G('aws','rds','cpu_utilization_percent'),r.id);
        const conn = V(m,G('aws','rds','database_connections'),r.id);
        const rI   = V(m,G('aws','rds','read_iops'),r.id);
        const wI   = V(m,G('aws','rds','write_iops'),r.id);
        const free = V(m,G('aws','rds','free_storage_bytes'),r.id);
        const rL   = V(m,G('aws','rds','read_latency_seconds'),r.id);
        const wL   = V(m,G('aws','rds','write_latency_seconds'),r.id);
        const fM   = V(m,G('aws','rds','freeable_memory_bytes'),r.id);
        const nR   = V(m,G('aws','rds','network_receive_bytes_per_sec'),r.id);
        const nT   = V(m,G('aws','rds','network_transmit_bytes_per_sec'),r.id);
        const bin  = V(m,G('aws','rds','binlog_disk_usage_bytes'),r.id);
        const swap = V(m,G('aws','rds','swap_usage_bytes'),r.id);
        const dqd  = V(m,G('aws','rds','disk_queue_depth'),r.id);
        const bb   = V(m,G('aws','rds','burst_balance_percent'),r.id);
        const lagStd    = V(m,G('aws','rds','replica_lag_seconds'),r.id);
        const lagAurora = V(m,G('aws','rds','replica_lag_aurora_seconds'),r.id);
        const lag = lagStd != null ? lagStd : lagAurora;

        const health = cpu==null ? 'unkn'
                     : cpu>85   ? 'crit'
                     : cpu>65   ? 'warn'
                     : (dqd||0)>5 ? 'warn'
                     : 'ok';
        const uid = 'rds'+i;

        const connDisplay = conn==null ? '<span class="mv-na">--</span>'
                          : conn===0   ? '<span class="mv-lo">0</span>'
                          : `<span class="mv">${N(conn,0)}</span>`;

        const lagDisplay = lag==null
            ? '<span class="mv-na">N/A (primary)</span>'
            : MV(lag,{hi:30,mid:5,fmt:SC});

        return `<tr style="animation-delay:${i*20}ms">
            <td><div class="rname"><strong>${r.name}</strong><small>${r.id}</small></div></td>
            <td><span class="rpill">${r.region}</span></td>
            <td>${badge(health)}</td>
            <td>${bar(cpu)}</td>
            <td>${connDisplay}</td>
            <td class="mv">${N(rI,4)}</td>
            <td class="mv">${N(wI,4)}</td>
            <td class="mv">${B(free)}</td>
            <td>${lagDisplay}</td>
            <td><button class="xbtn" id="xb-${uid}" onclick="toggle('${uid}')">▼ More</button></td>
        </tr>
        <tr class="drow" id="dr-${uid}"><td class="dcell" colspan="10"><div class="dinner">
            ${dkv('Read Latency',    rL!=null ? MS(rL*1000) : 'N/A')}
            ${dkv('Write Latency',   wL!=null ? MS(wL*1000) : 'N/A')}
            ${dkv('Freeable Mem',    B(fM))}
            ${dkv('Swap Usage',      B(swap))}
            ${dkv('Net Receive/s',   B(nR))}
            ${dkv('Net Transmit/s',  B(nT))}
            ${dkv('Disk Queue',      dqd!=null ? dqd.toFixed(4) : '--')}
            ${dkv('Burst Balance',   bb!=null ? bb.toFixed(1)+'%' : '--')}
            ${dkv('BinLog Disk',     B(bin))}
            ${dkv('Replica Lag',     lag!=null ? SC(lag) : 'N/A — not a replica')}
        </div></td></tr>`;
    }).join('');
}

function renderLambda(m) {
    const list=mergeResWithDiscovery(m,[G('aws','lambda','invocations_total'),G('aws','lambda','duration_avg_ms')],'aws','lambda');
    document.getElementById('tc-lam').textContent=list.length;
    const tI=list.reduce((s,r)=>s+(V(m,G('aws','lambda','invocations_total'),r.id)||0),0);
    const tE=list.reduce((s,r)=>s+(V(m,G('aws','lambda','errors_total'),r.id)||0),0);
    const eR=tI>0?(tE/tI*100):0;
    document.getElementById('s-lam').innerHTML=
        chip(list.length,'Functions','blue')+chip(N(tI),'Invocations','blue')+
        chip(N(tE),'Errors',tE?'red':'green')+chip(eR.toFixed(2)+'%','Error Rate',eR>5?'red':eR>1?'yellow':'green');
    if (!list.length) { document.getElementById('tb-lam').innerHTML=`<tr><td colspan="10"><div class="empty"><div class="empty-icon">λ</div><h3>No Lambda functions</h3><p>Enable lambda_functions in config YAML.</p></div></td></tr>`; return; }
    document.getElementById('tb-lam').innerHTML=list.map((r,i)=>{
        const inv=V(m,G('aws','lambda','invocations_total'),r.id);
        const err=V(m,G('aws','lambda','errors_total'),r.id);
        const thr=V(m,G('aws','lambda','throttles_total'),r.id);
        const dA=V(m,G('aws','lambda','duration_avg_ms'),r.id), dM=V(m,G('aws','lambda','duration_max_ms'),r.id);
        const cc=V(m,G('aws','lambda','concurrent_executions'),r.id);
        const ia=V(m,G('aws','lambda','iterator_age_ms'),r.id), id_=V(m,G('aws','lambda','init_duration_ms'),r.id);
        const ur=V(m,G('aws','lambda','unreserved_concurrent_executions'),r.id);
        const eRi=inv&&inv>0&&err!=null?err/inv*100:null;
        const health=inv==null?'unkn':eRi!=null&&eRi>10?'crit':eRi!=null&&eRi>2?'warn':(thr||0)>0?'warn':inv===0?'unkn':'ok';
        const uid='lam'+i;
        const shortId=r.id.includes(':')?r.id.split(':function:').pop()||r.id.split(':').pop():r.id;
        return `<tr style="animation-delay:${i*20}ms">
            <td><div class="rname"><strong title="${r.id}">${r.name}</strong><small>${shortId}</small></div></td>
            <td><span class="rpill">${r.region}</span></td>
            <td>${badge(health)}</td>
            <td class="mv">${inv==null?'<span class="mv-na">No invocations</span>':N(inv)}</td>
            <td>${MV(err,{hi:1,fmt:N})}</td>
            <td>${MV(eRi,{hi:10,mid:2,fmt:v=>v.toFixed(2)+'%'})}</td>
            <td>${MV(thr,{hi:1,fmt:N})}</td>
            <td class="mv">${MS(dA)}</td><td class="mv">${MS(dM)}</td>
            <td><button class="xbtn" id="xb-${uid}" onclick="toggle('${uid}')">▼ More</button></td>
        </tr>
        <tr class="drow" id="dr-${uid}"><td class="dcell" colspan="10"><div class="dinner">
            ${dkv('Concurrent',N(cc))} ${dkv('Iterator Age',MS(ia))}
            ${dkv('Init Duration',MS(id_))} ${dkv('Unreserved Conc',N(ur))}
        </div></td></tr>`;
    }).join('');
}

function renderLogs(m) {
    const raw=m['cloud_logs_collected']||[];
    const grp=new Map();
    for (const s of raw) {
        const k=`${s.lbls.cloud}|${s.lbls.region}`;
        if (!grp.has(k)) grp.set(k,{cloud:s.lbls.cloud,region:s.lbls.region,lv:{}});
        grp.get(k).lv[s.lbls.log_level]=(grp.get(k).lv[s.lbls.log_level]||0)+s.value;
    }
    const list=[...grp.values()];
    document.getElementById('tc-logs').textContent=list.length;
    const tE=list.reduce((s,g)=>s+(g.lv.ERROR||0)+(g.lv.CRITICAL||0),0);
    document.getElementById('s-logs').innerHTML=chip(list.length,'Log Sources','blue')+chip(N(tE),'Errors + Critical',tE?'red':'green');
    if (!list.length) { document.getElementById('tb-logs').innerHTML=`<tr><td colspan="6"><div class="empty"><div class="empty-icon">📜</div><h3>No log data yet</h3><p>Check IAM CloudWatch Logs permissions.</p></div></td></tr>`; return; }
    document.getElementById('tb-logs').innerHTML=list.map((g,i)=>{
        const err=(g.lv.ERROR||0)+(g.lv.CRITICAL||0), warn=g.lv.WARN||0;
        const health=err>0?'crit':warn>10?'warn':'ok';
        return `<tr style="animation-delay:${i*20}ms">
            <td><span class="rpill">${g.cloud}</span> &nbsp;${g.region}</td>
            <td>${MV(err,{hi:1,fmt:N})}</td><td>${MV(warn,{hi:50,mid:10,fmt:N})}</td>
            <td class="mv">${N(g.lv.INFO||0)}</td><td class="mv">${N(g.lv.DEBUG||0)}</td>
            <td>${badge(health)}</td>
        </tr>`;
    }).join('');
}

// ══════════════════════════════════════════════════════════════════════════
// ANOMALY TAB
// ══════════════════════════════════════════════════════════════════════════

let _anFilterTime = null;   // selected time-bucket key for click-filter
let _anData       = null;   // last fetched data

function fmtTs(ts) {
    if (!ts) return '--';
    try {
        return new Date(ts).toLocaleString(undefined, {
            year:'numeric', month:'short', day:'numeric',
            hour:'2-digit', minute:'2-digit', second:'2-digit',
            hour12: false
        });
    } catch { return ts; }
}

function devBar(current, lower, upper, severity) {
    const range  = upper - lower;
    const span   = range * 2.4 || Math.abs(current) * 0.5 || 1;
    const center = (lower + upper) / 2;
    const start  = center - span / 2;
    const lowPct = Math.max(1, ((lower - start) / span) * 100);
    const hiPct  = Math.min(99, ((upper - start) / span) * 100);
    const valPct = Math.min(98, Math.max(2, ((current - start) / span) * 100));
    const nw     = hiPct - lowPct;
    return `
    <div class="devbar-wrap">
        <div class="devbar-label">Deviation from normal range</div>
        <div class="devbar">
            <div class="devbar-normal" style="left:${lowPct}%;width:${nw}%"></div>
            <div class="devbar-marker ${severity}" style="left:${valPct}%"></div>
            <span class="devbar-low">${(+lower).toFixed(4)}</span>
            <span class="devbar-high">${(+upper).toFixed(4)}</span>
        </div>
    </div>`;
}

function anCard(a, idx) {
    const uid  = 'an' + idx;
    const icon = a.resource_type === 'ec2' ? '⚡' :
                 a.resource_type === 'rds' ? '🗄' :
                 a.resource_type === 'lambda' ? 'λ' : '☁';
    const sevLabel = a.severity === 'critical' ? 'CRITICAL' : 'WARNING';
    const overPct  = a.upper_bound > 0
        ? (((+a.current_value - +a.upper_bound) / +a.upper_bound) * 100).toFixed(1)
        : '--';

    return `
    <div class="an-card" style="animation-delay:${idx*30}ms">
        <div class="an-card-header sev-${a.severity}" onclick="toggleAnCard('${uid}')">
            <div class="an-card-icon" style="background:${a.severity==='critical'?'rgba(220,38,38,.12)':'rgba(217,119,6,.12)'}">
                ${icon}
            </div>
            <div class="an-card-meta">
                <div class="an-card-name">${a.resource_name} <small style="font-weight:400;color:var(--muted);font-size:.7rem">(${a.resource_type}/${a.region})</small></div>
                <div class="an-card-metric">${a.metric_name.replace(/_/g,' ')}</div>
            </div>
            <div class="an-card-val">
                <div class="cur ${a.severity}">${(+a.current_value).toFixed(4)} <small style="font-size:.65rem">${a.metric_unit||''}</small></div>
                <div class="lim">limit: ${(+a.upper_bound).toFixed(4)}</div>
            </div>
            <span class="an-severity-badge sev-${a.severity}" style="margin:0 8px;flex-shrink:0">${sevLabel}</span>
            <span class="an-card-chevron" id="achev-${uid}">▾</span>
        </div>
        <div class="an-card-body" id="acb-${uid}">
            <div class="an-stats-grid">
                <div class="an-stat"><div class="lbl">Current Value</div><div class="val" style="color:${a.severity==='critical'?'var(--red)':'var(--yellow)'}">${(+a.current_value).toFixed(6)}</div></div>
                <div class="an-stat"><div class="lbl">Normal Low</div><div class="val">${(+a.lower_bound).toFixed(6)}</div></div>
                <div class="an-stat"><div class="lbl">Normal High</div><div class="val">${(+a.upper_bound).toFixed(6)}</div></div>
                <div class="an-stat"><div class="lbl">Avg (baseline)</div><div class="val">${(+a.avg_value).toFixed(6)}</div></div>
                <div class="an-stat"><div class="lbl">Std Deviation</div><div class="val">${(+a.std_value).toFixed(6)}</div></div>
                <div class="an-stat"><div class="lbl">Over Limit</div><div class="val" style="color:var(--orange)">+${overPct}%</div></div>
                <div class="an-stat"><div class="lbl">Data Points</div><div class="val">${a.data_points}</div></div>
                <div class="an-stat"><div class="lbl">Detected At</div><div class="val" style="font-size:.7rem">${fmtTs(a.detected_at)}</div></div>
            </div>
            ${devBar(+a.current_value, +a.lower_bound, +a.upper_bound, a.severity)}
            <div class="an-reason">${a.reason}</div>
        </div>
    </div>`;
}

function toggleAnCard(uid) {
    const body = document.getElementById('acb-' + uid);
    const chev = document.getElementById('achev-' + uid);
    const open = body.classList.toggle('open');
    if (chev) chev.style.transform = open ? 'rotate(180deg)' : '';
}

function clearAnFilter() {
    _anFilterTime = null;
    document.getElementById('an-cards-header').style.display = 'none';
    if (_anData) renderAnomalyTimeline(_anData);
    if (_anData) renderAnomalyCards(_anData);
}

// ── Local-timezone timeline bucketing ──────────────────────────────────────
// All timestamps from the server are UTC (marked with Z).
// We bucket them here using the browser's local timezone so times match
// what the user sees on their clock (e.g. IST = UTC+5:30).

function localBucketKey(ms, bucketMin) {
    // Floor epoch ms to bucket boundary in LOCAL time
    const d   = new Date(ms);
    const off  = d.getTimezoneOffset() * 60000;          // ms offset from UTC
    const local = ms - off;                               // shift to local
    const floor = Math.floor(local / (bucketMin * 60000)) * (bucketMin * 60000);
    return floor + off;                                   // back to UTC epoch ms
}

function fmtLocalHHMM(ms) {
    const d = new Date(ms);
    return d.getHours().toString().padStart(2,'0') + ':' + d.getMinutes().toString().padStart(2,'0');
}

function buildLocalTimeline(anomalies, bucketMin) {
    if (!anomalies.length) return [];
    const counts = new Map();
    for (const a of anomalies) {
        const key = localBucketKey(a.detected_at_ms, bucketMin);
        counts.set(key, (counts.get(key) || 0) + 1);
        a._localBucket = key;   // attach for filtering
    }
    const keys = [...counts.keys()].sort((x,y)=>x-y);
    const step  = bucketMin * 60000;
    const start = keys[0], end = keys[keys.length-1];
    const result = [];
    for (let t = start; t <= end; t += step) {
        result.push({ bucket: t, label: fmtLocalHHMM(t), count: counts.get(t) || 0 });
    }
    return result;
}

function renderAnomalyTimeline(data) {
    const chart    = document.getElementById('an-timeline-chart');
    const subtitle = document.getElementById('an-timeline-subtitle');
    const anomalies = data.anomalies || [];

    if (!anomalies.length) {
        chart.innerHTML = '<span style="color:var(--muted);font-size:.8rem">No anomalies in this period</span>';
        subtitle.textContent = 'No anomalies recorded yet';
        return;
    }

    const buckets = buildLocalTimeline(anomalies, data.bucket_minutes || 5);
    const maxCount = Math.max(...buckets.map(b => b.count), 1);
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    subtitle.textContent = `${buckets.length} time buckets · ${data.total} total anomalies · last ${document.getElementById('an-hours').value}h · ${tz}`;

    chart.innerHTML = buckets.map(b => {
        const hasAnomaly = b.count > 0;
        const barH = hasAnomaly ? Math.max(24, Math.round((b.count / maxCount) * 80)) : 18;
        const selected = _anFilterTime === b.bucket;
        return `
        <div class="an-bar-wrap" onclick="filterByBucket(${b.bucket})">
            <div class="an-bar-count" style="color:${hasAnomaly ? 'var(--orange)' : 'transparent'}">${hasAnomaly ? b.count : ''}</div>
            <div class="an-bar ${hasAnomaly ? 'has-anomaly' : 'ok'} ${selected ? 'selected' : ''}"
                 style="height:${barH}px" title="${b.label}: ${b.count} anomalies"></div>
            <div class="an-bar-label">${b.label}</div>
        </div>`;
    }).join('');
}

function filterByBucket(bucketMs) {
    if (_anFilterTime === bucketMs) {
        clearAnFilter();
        return;
    }
    _anFilterTime = bucketMs;

    // Re-render timeline to update .selected highlight
    renderAnomalyTimeline(_anData);

    // Filter anomalies by local bucket
    const filtered = (_anData.anomalies || []).filter(a => a._localBucket === bucketMs);
    const header   = document.getElementById('an-cards-header');
    header.style.display = 'flex';
    const label = fmtLocalHHMM(bucketMs);
    document.getElementById('an-cards-title').textContent =
        `${filtered.length} anomal${filtered.length===1?'y':'ies'} at ${label} (local time)`;
    const cardsEl = document.getElementById('an-cards');
    cardsEl.innerHTML = filtered.length
        ? filtered.map((a, i) => anCard(a, i)).join('')
        : `<div class="an-empty"><div class="an-empty-icon">✅</div><h3>No anomalies in this period</h3></div>`;

    setTimeout(() => header.scrollIntoView({ behavior: 'smooth', block: 'start' }), 50);
}

function renderAnomalyCards(data) {
    const list = data.anomalies || [];
    const el   = document.getElementById('an-cards');
    if (!list.length) {
        el.innerHTML = `<div class="an-empty">
            <div class="an-empty-icon">✅</div>
            <h3>No anomalies detected</h3>
            <p>All metrics are within normal range for the selected period.<br>
               Make sure anomaly_detection.py is running alongside main.py.</p>
        </div>`;
        return;
    }
    el.innerHTML = list.map((a, i) => anCard(a, i)).join('');
}

async function loadAnomalies() {
    const hours    = document.getElementById('an-hours').value;
    const severity = document.getElementById('an-severity').value;
    const rtype    = document.getElementById('an-rtype').value;

    let url = `/api/anomalies?hours=${hours}`;
    if (severity) url += `&severity=${severity}`;
    if (rtype)    url += `&resource_type=${rtype}`;

    try {
        const r = await fetch(url);
        if (!r.ok) throw new Error(r.statusText);
        const data = await r.json();
        _anData = data;

        // Update tab badge
        const cnt = document.getElementById('tc-anomalies');
        cnt.textContent = data.total || 0;
        if (data.total > 0) cnt.classList.add('has-data');
        else cnt.classList.remove('has-data');

        // Summary chips
        const crits  = (data.anomalies||[]).filter(a => a.severity==='critical').length;
        const warns  = (data.anomalies||[]).filter(a => a.severity==='warning').length;
        const rtypes = [...new Set((data.anomalies||[]).map(a => a.resource_type))].length;
        document.getElementById('s-anomalies').innerHTML =
            chip(data.total || 0, 'Total Anomalies', data.total ? 'orange' : 'green') +
            chip(crits, 'Critical', crits ? 'red' : 'green') +
            chip(warns, 'Warnings', warns ? 'yellow' : 'green') +
            chip(rtypes || 0, 'Resource Types Affected', rtypes ? 'orange' : 'green');

        renderAnomalyTimeline(data);
        if (!_anFilterTime) renderAnomalyCards(data);

    } catch(e) {
        document.getElementById('an-cards').innerHTML =
            `<div class="err show">Could not load anomalies: ${e.message}<br>
             Make sure anomaly_detection.py has run at least once to create the anomalies table.</div>`;
    }
}

// ── DB Stats tab ───────────────────────────────────────────────────────────
async function loadDbStats() {
    try {
        const r = await fetch('/api/db-stats');
        if (!r.ok) throw new Error(r.statusText);
        const d = await r.json();

        document.getElementById('s-db').innerHTML=
            chip(d.metrics_count.toLocaleString(),'Total Metrics','blue')+
            chip(d.logs_count.toLocaleString(),'Total Logs','blue')+
            chip(d.resources.length,'Resource Types','green');

        let cards = `<div class="db-grid">`;

        cards += `<div class="db-card"><h3>Row Counts</h3>
            <div class="db-stat"><span class="k">Metrics rows</span><span class="v">${d.metrics_count.toLocaleString()}</span></div>
            <div class="db-stat"><span class="k">Log rows</span><span class="v">${d.logs_count.toLocaleString()}</span></div>
        </div>`;

        cards += `<div class="db-card"><h3>Time Range</h3>
            <div class="db-stat"><span class="k">Metrics oldest</span><span class="v">${d.metrics_range[0]||'--'}</span></div>
            <div class="db-stat"><span class="k">Metrics newest</span><span class="v">${d.metrics_range[1]||'--'}</span></div>
            <div class="db-stat"><span class="k">Logs oldest</span><span class="v">${d.logs_range[0]||'--'}</span></div>
            <div class="db-stat"><span class="k">Logs newest</span><span class="v">${d.logs_range[1]||'--'}</span></div>
        </div>`;

        cards += `<div class="db-card"><h3>Resources by Type</h3>`;
        for (const row of d.resources)
            cards += `<div class="db-stat"><span class="k">${row.cloud} / ${row.resource_type}</span><span class="v">${row.cnt}</span></div>`;
        cards += `</div>`;

        cards += `<div class="db-card"><h3>Log Levels</h3>`;
        for (const row of d.log_levels)
            cards += `<div class="db-stat"><span class="k">${row.cloud} / ${row.log_level}</span><span class="v">${row.cnt.toLocaleString()}</span></div>`;
        if (!d.log_levels.length) cards += `<div class="db-stat"><span class="k">No log data yet</span><span class="v">--</span></div>`;
        cards += `</div>`;

        cards += `</div>`;

        cards += `<div class="table-card" style="margin-top:16px"><table class="rt">
            <thead><tr><th>Cloud</th><th>Type</th><th>Resource</th><th>Metric</th><th>Value</th><th>Unit</th><th>Collected At</th></tr></thead>
            <tbody>`;
        for (const row of d.latest_metrics)
            cards += `<tr><td class="mv">${row.cloud}</td><td class="mv">${row.resource_type}</td>
                <td><div class="rname"><strong>${row.resource_name}</strong></div></td>
                <td class="mv">${row.metric_name}</td>
                <td class="mv-lo">${(+row.metric_value).toFixed(4)}</td>
                <td class="mv-na">${row.metric_unit}</td>
                <td class="mv-na" style="font-size:.7rem">${row.collected_at}</td></tr>`;
        if (!d.latest_metrics.length)
            cards += `<tr><td colspan="7"><div class="empty sh">No metrics in database yet</div></td></tr>`;
        cards += `</tbody></table></div>`;

        document.getElementById('db-content').innerHTML = cards;
    } catch(e) {
        document.getElementById('db-content').innerHTML =
            `<div class="err show">Could not load DB stats: ${e.message}</div>`;
    }
}

// ── Main loop ──────────────────────────────────────────────────────────────
async function update() {
    try {
        const r = await fetch('/api/metrics');
        if (!r.ok) throw new Error(r.statusText);
        const text = await r.text();
        document.getElementById('err-bar').classList.remove('show');
        document.getElementById('status').className='live-badge live';
        document.getElementById('status').innerHTML='<span class="dot"></span> LIVE';
        document.getElementById('last-ts').textContent='Updated '+new Date().toLocaleTimeString();
        const m = parseProm(text);
        renderEC2(m); renderRDS(m); renderLambda(m); renderLogs(m);
    } catch(e) {
        document.getElementById('err-bar').textContent=
            '⚠ Cannot reach collector at localhost:8000 — is main.py running? Error: '+e.message;
        document.getElementById('err-bar').classList.add('show');
        document.getElementById('status').className='live-badge dead';
        document.getElementById('status').innerHTML='✕ Offline';
        document.getElementById('last-ts').textContent=new Date().toLocaleTimeString();
    }
}

// Auto-refresh anomaly tab if it is active
async function updateAll() {
    await update();
    const panel = document.getElementById('panel-anomalies');
    if (panel && panel.classList.contains('active')) {
        loadAnomalies();
    }
}

update();
setInterval(updateAll, 10000);
</script>
</body>
</html>
'''


# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/')
def dashboard():
    return render_template_string(DASHBOARD_HTML)


@app.route('/api/metrics')
def proxy_metrics():
    """Proxy Prometheus /metrics so the browser avoids CORS issues."""
    try:
        r = requests.get(PROM_URL, timeout=5)
        return r.text, 200, {'Content-Type': 'text/plain; charset=utf-8'}
    except Exception as e:
        return f'# ERROR: Cannot reach {PROM_URL}\n# {e}\n', 503, {'Content-Type': 'text/plain'}


@app.route('/api/db-stats')
def db_stats():
    """Return summary stats from the SQLite database as JSON."""
    if not os.path.exists(DB_PATH):
        return jsonify({"error": f"Database not found at {DB_PATH}"}), 404

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    def q(sql):
        return [dict(r) for r in conn.execute(sql).fetchall()]

    def q1(sql):
        row = conn.execute(sql).fetchone()
        return list(row) if row else [None, None]

    data = {
        "metrics_count":  conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0],
        "logs_count":     conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0],
        "metrics_range":  q1("SELECT MIN(collected_at), MAX(collected_at) FROM metrics"),
        "logs_range":     q1("SELECT MIN(collected_at), MAX(collected_at) FROM logs"),
        "resources": q("""
            SELECT cloud, resource_type, COUNT(DISTINCT resource_id) AS cnt
            FROM   metrics
            GROUP  BY cloud, resource_type
            ORDER  BY cloud, resource_type
        """),
        "log_levels": q("""
            SELECT cloud, log_level, COUNT(*) AS cnt
            FROM   logs
            GROUP  BY cloud, log_level
            ORDER  BY cloud, cnt DESC
        """),
        "latest_metrics": q("""
            SELECT cloud, resource_type, resource_name, metric_name,
                   metric_value, metric_unit, collected_at
            FROM   metrics
            ORDER  BY collected_at DESC
            LIMIT  50
        """),
    }

    conn.close()
    return jsonify(data)


@app.route('/api/anomalies')
def get_anomalies():
    """
    Return anomalies from the anomalies table written by anomaly_detection.py.

    Query params:
        hours         int   — how many hours back to look (default 24)
        severity      str   — filter by 'critical' or 'warning' (default: all)
        resource_type str   — filter by resource type e.g. 'ec2' (default: all)
        limit         int   — max rows to return (default 200)
        bucket_minutes int  — size of timeline buckets in minutes (default: auto)
    """
    if not os.path.exists(DB_PATH):
        return jsonify({"error": f"Database not found at {DB_PATH}"}), 404

    hours         = int(request.args.get('hours', 24))
    severity      = request.args.get('severity', '').strip().lower()
    resource_type = request.args.get('resource_type', '').strip().lower()
    limit         = int(request.args.get('limit', 200))

    # Auto bucket size: keep ≈ 24–48 bars in the timeline
    if hours <= 2:
        bucket_minutes = 5
    elif hours <= 12:
        bucket_minutes = 15
    elif hours <= 48:
        bucket_minutes = 60
    else:
        bucket_minutes = 180

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Check the anomalies table exists
    tbl = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='anomalies'"
    ).fetchone()
    if not tbl:
        conn.close()
        return jsonify({
            "error": "anomalies table not found — run anomaly_detection.py first",
            "anomalies": [], "timeline": [], "total": 0
        })

    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    # Build WHERE clause
    where  = ["detected_at >= ?"]
    params = [cutoff]
    if severity:
        where.append("severity = ?")
        params.append(severity)
    if resource_type:
        where.append("LOWER(resource_type) = ?")
        params.append(resource_type)

    where_sql = " AND ".join(where)

    # Fetch anomaly rows
    rows = conn.execute(f"""
        SELECT id, detected_at, cloud, region, resource_type, resource_id,
               resource_name, metric_name, metric_unit, current_value,
               avg_value, std_value, upper_bound, lower_bound,
               severity, reason, data_points, acknowledged
        FROM   anomalies
        WHERE  {where_sql}
        ORDER  BY detected_at DESC
        LIMIT  ?
    """, params + [limit]).fetchall()

    anomalies = []
    for r in rows:
        d = dict(r)
        # Ensure timestamp is marked UTC so JS Date() parses correctly into local tz
        ts = d['detected_at']
        if not ts.endswith('Z') and '+' not in ts[10:]:
            ts = ts + 'Z'
        d['detected_at'] = ts
        # Send epoch ms — frontend uses this for local-tz bucketing
        d['detected_at_ms'] = int(
            datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp() * 1000
        )
        anomalies.append(d)

    # Timeline is built in the frontend using browser local timezone.
    timeline = []

    # Summary by resource
    resource_summary: dict = {}
    for a in anomalies:
        key = a['resource_id']
        if key not in resource_summary:
            resource_summary[key] = {
                "resource_id":   a['resource_id'],
                "resource_name": a['resource_name'],
                "resource_type": a['resource_type'],
                "region":        a['region'],
                "count":         0,
                "critical":      0,
                "warning":       0,
                "metrics":       set(),
            }
        resource_summary[key]['count']  += 1
        resource_summary[key][a['severity']] += 1
        resource_summary[key]['metrics'].add(a['metric_name'])

    for v in resource_summary.values():
        v['metrics'] = list(v['metrics'])

    conn.close()

    return jsonify({
        "total":            len(anomalies),
        "hours":            hours,
        "bucket_minutes":   bucket_minutes,
        "anomalies":        anomalies,
        "timeline":         timeline,
        "resource_summary": list(resource_summary.values()),
    })


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("=" * 70)
    print("  CloudOps — Infrastructure Health Dashboard")
    print("=" * 70)
    print(f"  Dashboard  : http://localhost:{DASHBOARD_PORT}")
    print(f"  Collector  : {PROM_URL}")
    print(f"  Database   : {DB_PATH}")
    print("=" * 70)
    print()
    print("  Make sure main.py is also running in a separate terminal.")
    print()
    app.run(host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=False)