#include "duckdb/common/tree_renderer/html_tree_renderer.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/json_document.hpp"

#include <cstdlib>

namespace duckdb {

string HTMLTreeRenderer::ToString(const LogicalOperator &op) {
	StringTreeRenderer ss;
	Render(op, ss);
	return ss.str();
}

string HTMLTreeRenderer::ToString(const PhysicalOperator &op) {
	StringTreeRenderer ss;
	Render(op, ss);
	return ss.str();
}

string HTMLTreeRenderer::ToString(const ProfilingNode &op) {
	StringTreeRenderer ss;
	Render(op, ss);
	return ss.str();
}

string HTMLTreeRenderer::ToString(const Pipeline &op) {
	StringTreeRenderer ss;
	Render(op, ss);
	return ss.str();
}

void HTMLTreeRenderer::Render(const LogicalOperator &op, BaseTreeRenderer &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void HTMLTreeRenderer::Render(const PhysicalOperator &op, BaseTreeRenderer &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void HTMLTreeRenderer::Render(const ProfilingNode &op, BaseTreeRenderer &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void HTMLTreeRenderer::Render(const Pipeline &op, BaseTreeRenderer &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

//! Single-page interactive viewer (CSS + JS). The query plan is injected as JSON in place of __PLAN_JSON__.
static const char *HTML_TEMPLATE = R"DUCKDBHTML(
<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DuckDB Query Plan</title>
<style>
:root {
    --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    --mono: "SF Mono", "JetBrains Mono", "Fira Code", Menlo, Consolas, monospace;
}
html[data-theme="light"] {
    --bg: #f4f5f7;
    --bg-grid: #e7e9ee;
    --panel: #ffffff;
    --panel-2: #f7f8fa;
    --border: #e2e5ea;
    --border-strong: #cbd1da;
    --text: #1c2127;
    --text-muted: #6b7382;
    --text-faint: #9aa1ad;
    --accent: #1a1a1a;
    --connector: #c2c8d2;
    --shadow: 0 1px 2px rgba(16,22,34,.06), 0 4px 12px rgba(16,22,34,.06);
    --shadow-hover: 0 2px 6px rgba(16,22,34,.10), 0 12px 28px rgba(16,22,34,.14);
    --duck: #fff000;
}
html[data-theme="dark"] {
    --bg: #16191f;
    --bg-grid: #1d2128;
    --panel: #232830;
    --panel-2: #1c2027;
    --border: #333a45;
    --border-strong: #424b59;
    --text: #e7eaee;
    --text-muted: #9aa3b1;
    --text-faint: #6b7382;
    --accent: #fff000;
    --connector: #3a414d;
    --shadow: 0 1px 2px rgba(0,0,0,.3), 0 6px 16px rgba(0,0,0,.35);
    --shadow-hover: 0 2px 8px rgba(0,0,0,.4), 0 16px 32px rgba(0,0,0,.5);
    --duck: #fff000;
}
* { box-sizing: border-box; }
html, body {
    margin: 0; padding: 0; height: 100%;
    font-family: var(--font);
    color: var(--text);
    background: var(--bg);
    overflow: hidden;
}

/* ---------- Toolbar ---------- */
#toolbar {
    position: fixed; top: 0; left: 0; right: 0; height: 52px; z-index: 20;
    display: flex; align-items: center; gap: 14px;
    padding: 0 16px;
    background: var(--panel);
    border-bottom: 1px solid var(--border);
    box-shadow: 0 1px 0 rgba(0,0,0,.02);
}
#brand { display: flex; align-items: center; gap: 9px; font-weight: 700; font-size: 15px; letter-spacing: -.2px; white-space: nowrap; }
#brand .logo {
    width: 22px; height: 22px; border-radius: 6px; background: var(--duck);
    display: flex; align-items: center; justify-content: center; font-size: 14px;
}
#brand .sub { color: var(--text-muted); font-weight: 500; }
#stats { display: flex; align-items: center; gap: 6px; flex-wrap: nowrap; overflow: hidden; }
.stat {
    display: inline-flex; align-items: baseline; gap: 6px;
    background: var(--panel-2); border: 1px solid var(--border);
    border-radius: 7px; padding: 4px 10px; white-space: nowrap;
}
.stat .k { font-size: 10.5px; text-transform: uppercase; letter-spacing: .5px; color: var(--text-faint); font-weight: 600; }
.stat .v { font-size: 13px; font-weight: 600; font-variant-numeric: tabular-nums; }
.spacer { flex: 1; }
.search-wrap { position: relative; }
#search {
    width: 200px; height: 32px; padding: 0 30px 0 30px;
    border: 1px solid var(--border-strong); border-radius: 8px;
    background: var(--panel-2); color: var(--text); font-size: 13px; font-family: var(--font);
    outline: none; transition: border-color .15s, box-shadow .15s;
}
#search:focus { border-color: var(--accent); box-shadow: 0 0 0 3px color-mix(in srgb, var(--accent) 18%, transparent); }
.search-wrap .icon { position: absolute; left: 9px; top: 50%; transform: translateY(-50%); color: var(--text-faint); pointer-events: none; }
#search-count { position: absolute; right: 8px; top: 50%; transform: translateY(-50%); font-size: 11px; color: var(--text-faint); font-variant-numeric: tabular-nums; }
.btn {
    height: 32px; min-width: 32px; padding: 0 10px;
    display: inline-flex; align-items: center; justify-content: center; gap: 6px;
    border: 1px solid var(--border-strong); border-radius: 8px;
    background: var(--panel-2); color: var(--text); cursor: pointer;
    font-size: 12.5px; font-weight: 500; font-family: var(--font);
    transition: background .12s, border-color .12s, color .12s;
    user-select: none;
}
.btn:hover { background: var(--bg-grid); border-color: var(--text-faint); }
.btn.active { background: var(--accent); color: var(--panel); border-color: var(--accent); }
html[data-theme="dark"] .btn.active { color: #16191f; }
.btn svg { display: block; }
.group { display: inline-flex; align-items: center; }
.group .btn { border-radius: 0; border-right-width: 0; }
.group .btn:first-child { border-top-left-radius: 8px; border-bottom-left-radius: 8px; }
.group .btn:last-child { border-radius: 0 8px 8px 0; border-right-width: 1px; }

/* ---------- Canvas / viewport ---------- */
#viewport {
    position: fixed; top: 52px; left: 0; right: 0; bottom: 0;
    overflow: hidden; cursor: grab;
    background-color: var(--bg);
    background-image: radial-gradient(var(--bg-grid) 1.1px, transparent 1.1px);
    background-size: 22px 22px;
}
#viewport.panning { cursor: grabbing; }
#canvas { position: absolute; top: 0; left: 0; transform-origin: 0 0; padding: 48px; will-change: transform; }

/* ---------- Tree (nested ul/li with connectors) ---------- */
.tree, .tree ul { position: relative; padding: 0; margin: 0; list-style: none; }
.tree ul { display: flex; padding-top: 28px; }
.tree li {
    position: relative; display: flex; flex-direction: column; align-items: center;
    padding: 28px 14px 0;
}
/* riser: vertical line from the sibling bus down into this node's card */
.tree li::after {
    content: ""; position: absolute; top: 0; left: 50%; transform: translateX(-50%);
    width: 2px; height: 28px; background: var(--connector);
}
/* bus: horizontal line spanning the siblings */
.tree li::before {
    content: ""; position: absolute; top: 0; height: 2px; background: var(--connector);
}
.tree li:first-child::before { left: 50%; right: 0; }
.tree li:last-child::before { left: 0; right: 50%; }
.tree li:not(:first-child):not(:last-child)::before { left: 0; right: 0; }
.tree li:only-child::before { display: none; }
/* drop: vertical line from a parent card down to its children's bus */
.tree ul::before {
    content: ""; position: absolute; top: 0; left: 50%; transform: translateX(-50%);
    width: 2px; height: 28px; background: var(--connector);
}
/* root node: no incoming connector */
.tree > li { padding-top: 0; }
.tree > li::before, .tree > li::after { display: none; }
/* collapsed node: hide its drop + children */
.tree li.collapsed > ul { display: none; }

/* ---------- Node card ---------- */
.node {
    position: relative; min-width: 168px; max-width: 320px;
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 10px; box-shadow: var(--shadow);
    transition: box-shadow .15s, border-color .15s, transform .12s, opacity .15s;
    overflow: hidden;
}
.node:hover { box-shadow: var(--shadow-hover); border-color: var(--border-strong); }
.node.selected { border-color: var(--accent); box-shadow: 0 0 0 2px color-mix(in srgb, var(--accent) 55%, transparent), var(--shadow-hover); }
.node.dim { opacity: .32; }
.node.match { border-color: #f5a623; box-shadow: 0 0 0 2px rgba(245,166,35,.6), var(--shadow); }

/* kind accent stripe on the left */
.node .accent { position: absolute; left: 0; top: 0; bottom: 0; width: 4px; background: var(--k-color, var(--border-strong)); }
.node-head {
    display: flex; align-items: center; gap: 8px;
    padding: 9px 11px 9px 14px; cursor: pointer; user-select: none;
}
.node-title { font-weight: 700; font-size: 13px; letter-spacing: -.1px; flex: 1; line-height: 1.25; }
.node-kind-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--k-color, var(--border-strong)); flex-shrink: 0; }
.chevron {
    flex-shrink: 0; width: 18px; height: 18px; border-radius: 5px;
    display: none; align-items: center; justify-content: center;
    color: var(--text-muted); background: var(--panel-2); border: 1px solid var(--border);
    font-size: 10px;
}
.node.has-children .chevron { display: inline-flex; }
.node-head:hover .chevron { color: var(--text); border-color: var(--border-strong); }
.chevron svg { transition: transform .15s; }
.tree li.collapsed > .node .chevron svg { transform: rotate(-90deg); }

/* metrics row */
.node-metrics { display: flex; gap: 0; padding: 0 12px 9px 14px; flex-wrap: wrap; }
.metric { display: flex; flex-direction: column; gap: 1px; margin-right: 14px; }
.metric .label { font-size: 9.5px; text-transform: uppercase; letter-spacing: .4px; color: var(--text-faint); font-weight: 600; }
.metric .num { font-size: 12.5px; font-weight: 650; font-variant-numeric: tabular-nums; color: var(--text); }
.metric .num.est { color: var(--text-muted); font-weight: 500; }
.metric.timing .num { color: var(--t-color, var(--text)); }

/* time share bar */
.timebar { height: 3px; background: var(--panel-2); border-radius: 2px; margin: 0 12px 9px 14px; overflow: hidden; }
.timebar > i { display: block; height: 100%; background: var(--t-color, var(--border-strong)); border-radius: 2px; }

/* details */
.node-details { border-top: 1px solid var(--border); background: var(--panel-2); display: none; }
.node.open-details .node-details { display: block; }
.detail { padding: 7px 14px; border-bottom: 1px solid var(--border); }
.detail:last-child { border-bottom: none; }
.detail .dk { font-size: 9.5px; text-transform: uppercase; letter-spacing: .4px; color: var(--text-faint); font-weight: 700; margin-bottom: 3px; }
.detail .dv { font-size: 12px; font-family: var(--mono); color: var(--text); word-break: break-word; line-height: 1.45; }
.detail .dv .row { display: block; }
.details-toggle {
    text-align: center; font-size: 11px; color: var(--text-muted); padding: 5px;
    cursor: pointer; user-select: none; font-weight: 600;
}
.details-toggle:hover { color: var(--text); }

/* heatmap mode tints the whole card */
.tree.heat .node.heat-critical { --k-color: #e5484d; background: color-mix(in srgb, #e5484d 9%, var(--panel)); }
.tree.heat .node.heat-high     { --k-color: #f76808; background: color-mix(in srgb, #f76808 8%, var(--panel)); }
.tree.heat .node.heat-moderate { --k-color: #ffb224; background: color-mix(in srgb, #ffb224 9%, var(--panel)); }

/* collapsed-subtree badge */
.hidden-badge {
    position: absolute; bottom: -9px; left: 50%; transform: translateX(-50%);
    font-size: 9.5px; font-weight: 700; color: var(--text-muted);
    background: var(--panel); border: 1px solid var(--border-strong);
    border-radius: 9px; padding: 1px 7px; white-space: nowrap; display: none;
}
.tree li.collapsed > .node .hidden-badge { display: block; }

/* ---------- Legend ---------- */
#legend {
    position: fixed; bottom: 14px; left: 14px; z-index: 15;
    background: var(--panel); border: 1px solid var(--border); border-radius: 10px;
    box-shadow: var(--shadow); padding: 9px 12px; font-size: 11px;
    display: flex; flex-direction: column; gap: 5px; max-width: 200px;
}
#legend .lg-title { font-size: 9.5px; text-transform: uppercase; letter-spacing: .5px; color: var(--text-faint); font-weight: 700; margin-bottom: 2px; }
#legend .lg-row { display: flex; align-items: center; gap: 7px; color: var(--text-muted); }
#legend .sw { width: 11px; height: 11px; border-radius: 3px; flex-shrink: 0; }

/* ---------- Zoom indicator ---------- */
#zoom-ind {
    position: fixed; bottom: 14px; right: 14px; z-index: 15;
    background: var(--panel); border: 1px solid var(--border); border-radius: 8px;
    box-shadow: var(--shadow); padding: 5px 10px; font-size: 12px; font-weight: 600;
    color: var(--text-muted); font-variant-numeric: tabular-nums;
}
.icon-btn-only { padding: 0; width: 32px; }
@media (max-width: 720px) {
    #stats { display: none; }
    #search { width: 130px; }
}
</style>
</head>
<body>
<div id="toolbar">
    <div id="brand"><span class="logo">🦆</span><span>DuckDB</span><span class="sub" id="brand-sub">Query Plan</span></div>
    <div id="stats"></div>
    <div class="spacer"></div>
    <div class="search-wrap">
        <span class="icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg></span>
        <input id="search" type="text" placeholder="Search nodes…" autocomplete="off" spellcheck="false">
        <span id="search-count"></span>
    </div>
    <div class="group">
        <button class="btn icon-btn-only" id="zoom-out" title="Zoom out">−</button>
        <button class="btn" id="zoom-fit" title="Fit to screen">Fit</button>
        <button class="btn icon-btn-only" id="zoom-in" title="Zoom in">+</button>
    </div>
    <div class="group">
        <button class="btn" id="expand-all" title="Expand all">Expand</button>
        <button class="btn" id="collapse-all" title="Collapse to top levels">Collapse</button>
    </div>
    <button class="btn" id="heat-toggle" title="Toggle time heatmap">Heatmap</button>
    <button class="btn icon-btn-only" id="theme-toggle" title="Toggle theme">◐</button>
</div>

<div id="viewport">
    <div id="canvas"><ul class="tree" id="tree"></ul></div>
</div>

<div id="legend"></div>
<div id="zoom-ind">100%</div>

<script id="plan-data" type="application/json">__PLAN_JSON__</script>
<script>
"use strict";
(function () {
    var PLAN = JSON.parse(document.getElementById("plan-data").textContent);
    var ANALYZE = !!PLAN.analyze;
    var TOTAL_TIME = PLAN.total_time || 0;

    var KINDS = {
        scan:      { color: "#30a46c", label: "Scan" },
        join:      { color: "#8e4ec6", label: "Join" },
        aggregate: { color: "#0091ff", label: "Aggregate" },
        order:     { color: "#f76808", label: "Order / Top-N" },
        generic:   { color: "#8b94a3", label: "Other" }
    };

    // ---------- number / time formatting ----------
    function fmtInt(n) {
        if (n === null || n === undefined) return "–";
        return n.toLocaleString("en-US");
    }
    function fmtCompact(n) {
        if (n === null || n === undefined) return "–";
        var a = Math.abs(n);
        if (a >= 1e9) return (n / 1e9).toFixed(a >= 1e10 ? 0 : 1) + "B";
        if (a >= 1e6) return (n / 1e6).toFixed(a >= 1e7 ? 0 : 1) + "M";
        if (a >= 1e3) return (n / 1e3).toFixed(a >= 1e4 ? 0 : 1) + "K";
        return String(n);
    }
    function fmtTime(s) {
        if (s === null || s === undefined) return "–";
        if (s >= 1) return s.toFixed(2) + "s";
        if (s >= 0.001) return (s * 1e3).toFixed(s >= 0.1 ? 0 : 1) + "ms";
        if (s > 0) return (s * 1e6).toFixed(0) + "µs";
        return "0";
    }
    function heatClass(frac) {
        if (frac >= 0.25) return "heat-critical";
        if (frac >= 0.10) return "heat-high";
        if (frac >= 0.01) return "heat-moderate";
        return "";
    }
    function heatColor(frac) {
        if (frac >= 0.25) return "#e5484d";
        if (frac >= 0.10) return "#f76808";
        if (frac >= 0.01) return "#ffb224";
        return "var(--text-muted)";
    }

    // ---------- build DOM ----------
    var allNodes = [];
    function makeNode(data, depth) {
        var li = document.createElement("li");
        var hasChildren = data.children && data.children.length > 0;

        var node = document.createElement("div");
        node.className = "node" + (hasChildren ? " has-children" : "");
        var kind = KINDS[data.kind] || KINDS.generic;
        node.style.setProperty("--k-color", kind.color);

        var frac = (ANALYZE && TOTAL_TIME > 0 && data.timing != null) ? data.timing / TOTAL_TIME : 0;
        if (ANALYZE && data.timing != null) {
            var hc = heatClass(frac);
            if (hc) node.classList.add(hc);
            node.style.setProperty("--t-color", heatColor(frac));
        }

        // accent stripe
        var accent = document.createElement("div");
        accent.className = "accent";
        node.appendChild(accent);

        // head
        var head = document.createElement("div");
        head.className = "node-head";
        var dot = document.createElement("span"); dot.className = "node-kind-dot";
        var title = document.createElement("span"); title.className = "node-title"; title.textContent = data.name;
        var chev = document.createElement("span"); chev.className = "chevron";
        chev.innerHTML = '<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><path d="m6 9 6 6 6-6"/></svg>';
        head.appendChild(dot); head.appendChild(title); head.appendChild(chev);
        node.appendChild(head);

        // metrics
        var metrics = document.createElement("div");
        metrics.className = "node-metrics";
        if (data.cardinality != null) {
            metrics.appendChild(metric("Rows", fmtCompact(data.cardinality), "", fmtInt(data.cardinality)));
        }
        if (data.estimated_cardinality != null) {
            metrics.appendChild(metric("Est. Rows", fmtCompact(data.estimated_cardinality), "est", fmtInt(data.estimated_cardinality)));
        }
        if (ANALYZE && data.timing != null) {
            var tm = metric("Time", fmtTime(data.timing), "", fmtTime(data.timing) + (TOTAL_TIME > 0 ? "  ·  " + (frac * 100).toFixed(1) + "%" : ""));
            tm.classList.add("timing");
            metrics.appendChild(tm);
        }
        if (metrics.children.length) node.appendChild(metrics);

        // time bar
        if (ANALYZE && data.timing != null && TOTAL_TIME > 0) {
            var bar = document.createElement("div"); bar.className = "timebar";
            var fill = document.createElement("i"); fill.style.width = Math.max(2, frac * 100).toFixed(1) + "%";
            bar.appendChild(fill); node.appendChild(bar);
        }

        // details
        var details = data.details || [];
        if (details.length) {
            var dwrap = document.createElement("div");
            dwrap.className = "node-details";
            details.forEach(function (d) {
                var dd = document.createElement("div"); dd.className = "detail";
                var dk = document.createElement("div"); dk.className = "dk"; dk.textContent = d.key;
                var dv = document.createElement("div"); dv.className = "dv";
                (d.values || []).forEach(function (v) {
                    var r = document.createElement("span"); r.className = "row"; r.textContent = v; dv.appendChild(r);
                });
                dd.appendChild(dk); dd.appendChild(dv); dwrap.appendChild(dd);
            });
            node.appendChild(dwrap);

            var dtoggle = document.createElement("div");
            dtoggle.className = "details-toggle";
            dtoggle.textContent = "Show details";
            dtoggle.addEventListener("click", function (e) {
                e.stopPropagation();
                var open = node.classList.toggle("open-details");
                dtoggle.textContent = open ? "Hide details" : "Show details";
            });
            node.appendChild(dtoggle);
        }

        // hidden-subtree badge
        var badge = document.createElement("span");
        badge.className = "hidden-badge";
        node.appendChild(badge);

        li.appendChild(node);

        var rec = { li: li, node: node, data: data, badge: badge, descendants: 0 };
        allNodes.push(rec);

        // children
        if (hasChildren) {
            var ul = document.createElement("ul");
            var count = 0;
            data.children.forEach(function (c) {
                var childRec = makeNode(c, depth + 1);
                ul.appendChild(childRec.li);
                count += 1 + childRec.descendants;
            });
            li.appendChild(ul);
            rec.descendants = count;
            badge.textContent = "+" + count + " hidden";

            chev.addEventListener("click", function (e) { e.stopPropagation(); toggleCollapse(li); });
        }

        // selection (clicking the header selects without collapsing)
        head.addEventListener("click", function () { select(node); });
        return rec;
    }

    function metric(label, value, cls, tooltip) {
        var m = document.createElement("div"); m.className = "metric";
        var l = document.createElement("span"); l.className = "label"; l.textContent = label;
        var n = document.createElement("span"); n.className = "num" + (cls ? " " + cls : ""); n.textContent = value;
        if (tooltip) m.title = tooltip;
        m.appendChild(l); m.appendChild(n);
        return m;
    }

    function toggleCollapse(li) { li.classList.toggle("collapsed"); }

    var selected = null;
    function select(node) {
        if (selected) selected.classList.remove("selected");
        selected = node; node.classList.add("selected");
    }

    var tree = document.getElementById("tree");
    var rootRec = makeNode(PLAN.root, 0);
    tree.appendChild(rootRec.li);

    // ---------- summary header ----------
    (function () {
        var stats = document.getElementById("stats");
        document.getElementById("brand-sub").textContent = ANALYZE ? "Query Profile" : "Query Plan";
        function addStat(k, v) {
            var s = document.createElement("div"); s.className = "stat";
            s.innerHTML = '<span class="k">' + k + '</span><span class="v">' + v + '</span>';
            stats.appendChild(s);
        }
        addStat("Operators", allNodes.length);
        if (ANALYZE) {
            addStat("Total Time", fmtTime(TOTAL_TIME));
            if (PLAN.root && PLAN.root.cardinality != null) addStat("Result Rows", fmtInt(PLAN.root.cardinality));
        }
    })();

    // ---------- legend ----------
    (function () {
        var lg = document.getElementById("legend");
        var html = '<div class="lg-title">Operators</div>';
        Object.keys(KINDS).forEach(function (k) {
            html += '<div class="lg-row"><span class="sw" style="background:' + KINDS[k].color + '"></span>' + KINDS[k].label + '</div>';
        });
        if (ANALYZE) {
            html += '<div class="lg-title" style="margin-top:6px">Time share</div>';
            html += '<div class="lg-row"><span class="sw" style="background:#e5484d"></span>≥ 25% (critical)</div>';
            html += '<div class="lg-row"><span class="sw" style="background:#f76808"></span>≥ 10% (high)</div>';
            html += '<div class="lg-row"><span class="sw" style="background:#ffb224"></span>≥ 1% (moderate)</div>';
        }
        lg.innerHTML = html;
    })();

    // ---------- pan & zoom ----------
    var viewport = document.getElementById("viewport");
    var canvas = document.getElementById("canvas");
    var scale = 1, tx = 0, ty = 0;
    var zoomInd = document.getElementById("zoom-ind");
    function applyTransform() {
        canvas.style.transform = "translate(" + tx + "px," + ty + "px) scale(" + scale + ")";
        zoomInd.textContent = Math.round(scale * 100) + "%";
    }
    function zoomAt(cx, cy, factor) {
        var ns = Math.min(2.5, Math.max(0.1, scale * factor));
        var k = ns / scale;
        tx = cx - (cx - tx) * k;
        ty = cy - (cy - ty) * k;
        scale = ns;
        applyTransform();
    }
    viewport.addEventListener("wheel", function (e) {
        e.preventDefault();
        var rect = viewport.getBoundingClientRect();
        var factor = e.deltaY < 0 ? 1.12 : 1 / 1.12;
        zoomAt(e.clientX - rect.left, e.clientY - rect.top, factor);
    }, { passive: false });

    var dragging = false, sx = 0, sy = 0, stx = 0, sty = 0;
    viewport.addEventListener("mousedown", function (e) {
        if (e.button !== 0) return;
        dragging = true; sx = e.clientX; sy = e.clientY; stx = tx; sty = ty;
        viewport.classList.add("panning");
    });
    window.addEventListener("mousemove", function (e) {
        if (!dragging) return;
        tx = stx + (e.clientX - sx); ty = sty + (e.clientY - sy);
        applyTransform();
    });
    window.addEventListener("mouseup", function () { dragging = false; viewport.classList.remove("panning"); });

    function fit() {
        // reset transform to measure natural size
        scale = 1; tx = 0; ty = 0; applyTransform();
        var cw = canvas.scrollWidth, ch = canvas.scrollHeight;
        var vw = viewport.clientWidth, vh = viewport.clientHeight;
        var s = Math.min(vw / cw, vh / ch, 1);
        s = Math.max(s, 0.1);
        scale = s;
        tx = (vw - cw * s) / 2;
        ty = Math.max(16, (vh - ch * s) / 2);
        applyTransform();
    }

    document.getElementById("zoom-in").addEventListener("click", function () {
        zoomAt(viewport.clientWidth / 2, viewport.clientHeight / 2, 1.2);
    });
    document.getElementById("zoom-out").addEventListener("click", function () {
        zoomAt(viewport.clientWidth / 2, viewport.clientHeight / 2, 1 / 1.2);
    });
    document.getElementById("zoom-fit").addEventListener("click", fit);

    // ---------- expand / collapse all ----------
    document.getElementById("expand-all").addEventListener("click", function () {
        allNodes.forEach(function (r) { r.li.classList.remove("collapsed"); });
    });
    document.getElementById("collapse-all").addEventListener("click", function () {
        // collapse everything below the root's direct children
        allNodes.forEach(function (r) {
            if (r.descendants > 0 && r !== rootRec) r.li.classList.add("collapsed");
        });
    });

    // ---------- search ----------
    var search = document.getElementById("search");
    var searchCount = document.getElementById("search-count");
    search.addEventListener("input", function () {
        var q = search.value.trim().toLowerCase();
        if (!q) {
            allNodes.forEach(function (r) { r.node.classList.remove("match", "dim"); });
            searchCount.textContent = "";
            return;
        }
        var hits = 0;
        allNodes.forEach(function (r) {
            var hay = r.data.name.toLowerCase();
            (r.data.details || []).forEach(function (d) {
                hay += " " + d.key.toLowerCase() + " " + (d.values || []).join(" ").toLowerCase();
            });
            var m = hay.indexOf(q) >= 0;
            r.node.classList.toggle("match", m);
            r.node.classList.toggle("dim", !m);
            if (m) {
                hits++;
                // reveal matches by expanding ancestors
                var p = r.li.parentElement;
                while (p && p !== tree) {
                    if (p.tagName === "LI") p.classList.remove("collapsed");
                    p = p.parentElement;
                }
            }
        });
        searchCount.textContent = hits ? hits : "0";
    });

    // ---------- heatmap toggle ----------
    var heatBtn = document.getElementById("heat-toggle");
    function setHeat(on) {
        tree.classList.toggle("heat", on);
        heatBtn.classList.toggle("active", on);
    }
    heatBtn.addEventListener("click", function () { setHeat(!tree.classList.contains("heat")); });

    // ---------- theme toggle ----------
    document.getElementById("theme-toggle").addEventListener("click", function () {
        var html = document.documentElement;
        html.setAttribute("data-theme", html.getAttribute("data-theme") === "dark" ? "light" : "dark");
    });

    // ---------- init ----------
    if (ANALYZE) setHeat(true);
    requestAnimationFrame(fit);
    window.addEventListener("resize", function () { /* keep current transform */ });
})();
</script>
</body>
</html>
)DUCKDBHTML";

//! Map an operator name (and, for leaves, its details) to a coarse "kind" used for colour-coding in the UI. Mirrors
//! the classification used by the text renderer.
static const char *ClassifyKind(const string &name, bool is_leaf, const InsertionOrderPreservingMap<string> &extra) {
	if (StringUtil::Contains(name, "SCAN") || StringUtil::Contains(name, "GET")) {
		return "scan";
	}
	if (StringUtil::Contains(name, "JOIN") || name == "CROSS_PRODUCT") {
		return "join";
	}
	if (StringUtil::Contains(name, "AGGREGATE") || StringUtil::Contains(name, "GROUP_BY") ||
	    StringUtil::Contains(name, "DISTINCT") || StringUtil::Contains(name, "WINDOW")) {
		return "aggregate";
	}
	if (StringUtil::Contains(name, "ORDER_BY") || StringUtil::Contains(name, "TOP_N")) {
		return "order";
	}
	if (is_leaf) {
		for (auto &entry : extra) {
			if (entry.first == "Table" || entry.first == "Function") {
				return "scan";
			}
		}
	}
	return "generic";
}

//! Prettify a leftover internal key (e.g. "__some_metric__" -> "Some Metric"). Regular keys are returned unchanged.
static string PrettifyKey(const string &key) {
	if (!StringUtil::StartsWith(key, "__")) {
		return key;
	}
	auto result = StringUtil::Replace(key, "__", "");
	result = StringUtil::Replace(result, "_", " ");
	return StringUtil::Title(result);
}

static JSONMutableValue BuildNodeJSON(JSONWriter &writer, RenderTree &tree, idx_t x, idx_t y, double &total_time,
                                      bool &has_timing) {
	auto node_p = tree.GetNode(x, y);
	D_ASSERT(node_p);
	auto &node = *node_p;

	auto obj = writer.CreateObject();
	obj.AddString("name", node.name);

	bool is_leaf = node.child_positions.empty();
	obj.AddString("kind", ClassifyKind(node.name, is_leaf, node.extra_text));

	auto details = writer.CreateArray();
	for (auto &entry : node.extra_text) {
		auto &key = entry.first;
		auto &value = entry.second;
		if (key == RenderTreeNode::CARDINALITY) {
			if (!value.empty()) {
				obj.Add("cardinality", writer.CreateSignedInteger(std::strtoll(value.c_str(), nullptr, 10)));
			}
			continue;
		}
		if (key == RenderTreeNode::ESTIMATED_CARDINALITY) {
			if (!value.empty()) {
				obj.Add("estimated_cardinality", writer.CreateSignedInteger(std::strtoll(value.c_str(), nullptr, 10)));
			}
			continue;
		}
		if (key == RenderTreeNode::TIMING) {
			double seconds = std::strtod(value.c_str(), nullptr);
			obj.Add("timing", writer.CreateDouble(seconds));
			total_time += seconds;
			has_timing = true;
			continue;
		}
		if (value.empty()) {
			continue;
		}
		auto detail = writer.CreateObject();
		detail.AddString("key", PrettifyKey(key));
		auto values = writer.CreateArray();
		for (auto &split : StringUtil::Split(value, "\n")) {
			values.AppendString(split);
		}
		detail.Add("values", values);
		details.Append(detail);
	}
	obj.Add("details", details);

	auto children = writer.CreateArray();
	for (auto &child_pos : node.child_positions) {
		children.Append(BuildNodeJSON(writer, tree, child_pos.x, child_pos.y, total_time, has_timing));
	}
	obj.Add("children", children);
	return obj;
}

void HTMLTreeRenderer::ToStreamInternal(RenderTree &root, BaseTreeRenderer &ss) {
	JSONWriter writer;
	double total_time = 0;
	bool has_timing = false;
	auto root_node = BuildNodeJSON(writer, root, 0, 0, total_time, has_timing);

	auto doc = writer.CreateObject();
	doc.Add("analyze", writer.CreateBoolean(has_timing));
	doc.Add("total_time", writer.CreateDouble(total_time));
	doc.Add("root", root_node);
	writer.SetRoot(doc);

	auto json = writer.ToString(JSONWriteFlags::ALLOW_INF_AND_NAN);
	// escape angle brackets so the JSON cannot break out of the <script> tag it is embedded in
	json = StringUtil::Replace(json, "<", "\\u003c");
	json = StringUtil::Replace(json, ">", "\\u003e");

	string html = HTML_TEMPLATE;
	html = StringUtil::Replace(html, "__PLAN_JSON__", json);
	ss << html;
}

string HTMLTreeRenderer::RenderProfilerDisabled() {
	return R"(<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>DuckDB Query Profile</title></head>
<body style="font-family: sans-serif; padding: 2rem; color: #1c2127;">
  Query profiling is disabled. Use <code>PRAGMA enable_profiling;</code> to enable profiling.
</body></html>)";
}

} // namespace duckdb
