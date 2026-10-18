"""Generate a self-contained HTML dashboard from platform health evidence."""

# Support sibling imports in script and package execution.
import sys as _src_import_sys
from pathlib import Path as _src_import_path
_src_import_dir = str(_src_import_path(__file__).resolve().parent)
if _src_import_dir not in _src_import_sys.path:
    _src_import_sys.path.insert(0, _src_import_dir)
import argparse
import html
import json
from pathlib import Path

try:
    from .platform_health_demo import build_platform_health
except ImportError:
    from platform_health_demo import build_platform_health
from pipeline_observability import build_observability_report
from data_quality_scorecard import build_scorecard

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "reports" / "platform_health_dashboard.html"


def _render_pipeline_reliability_breakdown():
    """Render per-pipeline run counts without hiding historical failures."""
    import html
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent
    report_path = root / "reports" / "pipeline_reliability_breakdown.json"
    if not report_path.exists():
        return (
            '<section><h2>Pipeline reliability breakdown</h2>'
            '<p>Evidence unavailable: run the reliability breakdown report first.</p></section>'
        )

    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return (
            '<section><h2>Pipeline reliability breakdown</h2>'
            '<p>Reliability evidence could not be read.</p></section>'
        )

    rows = []
    for name, metrics in report.get("pipeline_breakdown", {}).items():
        escaped_name = html.escape(str(name))
        total = metrics.get("total", 0)
        success = metrics.get("success", 0)
        failed = metrics.get("failed", 0)
        other = metrics.get("other", 0)
        rate = metrics.get("success_rate_percent")
        rate_text = f"{rate:.2f}%" if isinstance(rate, (int, float)) else "N/A"
        rows.append(
            "<tr>"
            f"<td>{escaped_name}</td>"
            f"<td>{total}</td><td>{success}</td><td>{failed}</td>"
            f"<td>{other}</td><td>{rate_text}</td></tr>"
        )

    overall = report.get("overall", {})
    overall_rate = overall.get("success_rate_percent")
    overall_text = f"{overall_rate:.2f}%" if isinstance(overall_rate, (int, float)) else "N/A"
    table_rows = "".join(rows) or '<tr><td colspan="6">No run records available</td></tr>'
    note = html.escape(report.get("classification_note", "Classification limitations not supplied."))

    return (
        '<section id="pipeline-reliability-breakdown">'
        '<h2>Pipeline reliability breakdown</h2>'
        '<p class="muted"><strong>Interpretation note:</strong> '
        'These figures include all recorded historical runs, including development '
        'and test activity where present. Pipeline names alone do not reliably '
        'identify production runs. Treat this success rate as historical recorded '
        'run performance, not as a verified production SLA.</p>'
        f'<p>All recorded runs: {overall.get("total", 0)} | '
        f'Successes: {overall.get("success", 0)} | '
        f'Failures: {overall.get("failed", 0)} | '
        f'Other statuses: {overall.get("other", 0)} | '
        f'Success rate: {overall_text}</p>'
        '<table><thead><tr><th>Pipeline</th><th>Total</th><th>Success</th>'
        '<th>Failed</th><th>Other</th><th>Success rate</th></tr></thead>'
        f'<tbody>{table_rows}</tbody></table>'
        f'<p><small>{note}</small></p>'
        '</section>'
    )

def render_dashboard(report):
    def esc(value):
        return html.escape(str(value if value is not None else "Unavailable"))

    pipeline = report["pipeline"]
    quality = report["data_quality"]
    recovery = report["recovery"]
    unfinished_details = recovery.get("unfinished_run_details", [])

    cards = [
        ("Pipeline success", f"{pipeline['success_rate_percent']}%", "Recorded run history"),
        ("Average duration", f"{pipeline['average_run_duration_seconds']} s", "Recorded duration data"),
        ("Quality score", f"{quality['overall_score_percent']}%" if quality["overall_score_percent"] is not None else "Unavailable", quality["scorecard_status"]),
        ("Quality regressions", quality["regression_count"], quality["regression_status"]),
        ("Lineage discrepancies", recovery["lineage_discrepancies"], recovery["assessment_status"]),
    ]

    card_html = "".join(
        f'<article class="card"><div class="muted">{esc(label)}</div>'
        f'<div class="metric">{esc(value)}</div><div class="muted">{esc(note)}</div></article>'
        for label, value, note in cards
    )

    observability_details = build_observability_report()
    hotspots = observability_details["failure_hotspots"]
    if hotspots:
        hotspot_html = "".join(
            '<article class="action">'
            f'<strong>{esc(item["stage_name"])}</strong>'
            f'<p>Failures: {esc(item["failures"])} / {esc(item["executions"])} executions '
            f'({esc(item["failure_rate_percent"])}%)</p>'
            f'<small>Average duration: {esc(item["average_duration_seconds"])} seconds</small>'
            '</article>'
            for item in hotspots
        )
    else:
        hotspot_html = '<p>No recorded stage failures were found in the available history.</p>'

    if unfinished_details:
        recovery_detail_html = "".join(
            '<article class="action recovery-run">'
            f'<strong>Run ID: {esc(item["run_id"])}</strong>'
            f'<p>Classification: <strong>{esc(item["recovery_classification"])}</strong></p>'
            f'<p>Evidence classification: <strong>{esc(item.get("evidence_classification", "UNAVAILABLE"))}</strong></p>'
            f'<p>Started: {esc(item["started_at_utc"])}</p>'
            f'<p>Age: {esc(item["age_minutes"])} minutes</p>'
            f'<p>Recorded status: {esc(item["status"])}</p>'
            f'<p>Stage records: {esc(item["stage_record_count"])} ? '
            f'Attempts: {esc(item["attempt_record_count"])}</p>'
            f'<p>Artifacts: {esc(item["artifact_record_count"])} ? '
            f'Artifact observations: {esc(item["artifact_observation_count"])}</p>'
            + (
                '<p><strong>No stage or artifact evidence is recorded for this run.</strong></p>'
                if not any(item.get(key, 0) for key in (
                    "stage_record_count", "attempt_record_count",
                    "artifact_record_count", "artifact_observation_count"
                ))
                else ""
            )
            + (
                f'<p>Timestamp parsing issue: {esc(item["timestamp_parse_error"])}</p>'
                if item.get("timestamp_parse_error") else ""
            )
            + '</article>'
            for item in unfinished_details
        )
    else:
        recovery_detail_html = (
            '<p>No unfinished runs were found in the available recovery evidence.</p>'
        )

    decision_events = recovery.get("decision_events", [])
    if decision_events:
        recovery_decision_html = "".join(
            '<article class="action recovery-decision">'
            f'<strong>{esc(item["decision"])}</strong>'
            f'<p>Run ID: {esc(item["run_id"])}</p>'
            f'<p>Reason: {esc(item["reason"])}</p>'
            f'<p>Recorded by: {esc(item["recorded_by"])}</p>'
            f'<p>Recorded at (UTC): {esc(item["recorded_at_utc"])}</p>'
            '</article>'
            for item in decision_events
        )
    else:
        recovery_decision_html = (
            '<p>No human recovery decisions have been recorded.</p>'
        )

    quality_dimensions = report["data_quality"]
    quality_detail_html = "".join(
        '<article class="action">'
        f'<strong>{esc(name.replace("_", " ").title())}</strong>'
        f'<p>Score: {esc(item["score_percent"]) if item["score_percent"] is not None else "Unavailable"}</p>'
        f'<small>Evidence source: {esc(item["source"] or "No matching recorded metric")}</small>'
        '</article>'
        for name, item in build_scorecard()["dimensions"].items()
    )

    actions = report["prioritized_actions"]
    if actions:
        action_html = "".join(
            '<article class="action">'
            f'<span class="priority {esc(item["priority"]).lower()}">{esc(item["priority"])}</span>'
            f'<strong>{esc(item["area"])}</strong>'
            f'<p>{esc(item["action"])}</p>'
            f'<small>{esc(item["evidence_count"])} evidence item(s)</small>'
            '</article>'
            for item in actions
        )
    else:
        action_html = '<p>No prioritized actions were generated from the available evidence.</p>'

    safe_json = json.dumps(report, indent=2, default=str).replace("</", "<\\/")

    script_html = r"""<script>
"use strict";
function filterDashboardItems(inputId, listId, countId) {
  const query = document.getElementById(inputId).value.trim().toLowerCase();
  const list = document.getElementById(listId);
  const items = Array.from(list.children).filter(el => el.tagName === "ARTICLE");
  let visible = 0;
  items.forEach(item => {
    const match = item.textContent.toLowerCase().includes(query);
    item.hidden = !match;
    if (match) visible++;
  });
  document.getElementById(countId).textContent =
    items.length ? `${visible} of ${items.length} items shown` : "No evidence items available.";
}
function filterHotspotRate() {
  const minimum = Number(document.getElementById("hotspot-min-rate").value);
  const query = document.getElementById("hotspot-search").value.trim().toLowerCase();
  const list = document.getElementById("hotspot-list");
  const items = Array.from(list.children).filter(el => el.tagName === "ARTICLE");
  let visible = 0;
  items.forEach(item => {
    const text = item.textContent.toLowerCase();
    const rateMatch = text.match(/\(([0-9]+(?:\.[0-9]+)?)%\)/);
    const rate = rateMatch ? Number(rateMatch[1]) : 0;
    const show = text.includes(query) && rate >= minimum;
    item.hidden = !show;
    if (show) visible++;
  });
  document.getElementById("hotspot-count").textContent =
    items.length ? `${visible} of ${items.length} hotspots shown` : "No failure hotspot evidence available.";
}
document.addEventListener("DOMContentLoaded", () => {
  filterDashboardItems("quality-search", "quality-list", "quality-count");
  filterDashboardItems("recovery-search", "recovery-list", "recovery-count");
  filterHotspotRate();
});
</script>"""
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Banking Data Platform | Health Dashboard</title>
<style>
:root {{ color-scheme: dark; font-family: Inter, Segoe UI, Arial, sans-serif; }}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: #101722; color: #edf2f7; }}
header {{ padding: 28px clamp(18px, 4vw, 52px); background: #182435; border-bottom: 1px solid #314156; }}
h1 {{ margin: 0 0 8px; font-size: clamp(24px, 3vw, 36px); }}
h2 {{ margin-top: 30px; font-size: 20px; }}
p {{ line-height: 1.55; }}
.muted, small {{ color: #aab9ca; }}
main {{ max-width: 1300px; margin: auto; padding: 24px clamp(18px, 4vw, 52px) 48px; }}
.status {{ display: inline-block; padding: 7px 11px; border: 1px solid #536a85; border-radius: 999px; font-size: 13px; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 14px; }}
.card, .panel, .action {{ background: #182435; border: 1px solid #314156; border-radius: 12px; padding: 18px; }}
.metric {{ font-size: 30px; font-weight: 700; margin: 13px 0; overflow-wrap: anywhere; }}
.columns {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px; }}
.action {{ margin: 10px 0; }}
.priority {{ display: inline-block; padding: 4px 8px; margin-right: 8px; border-radius: 6px; font-size: 11px; font-weight: 800; }}
.high {{ background: #5a2932; color: #ffbec7; }}
.medium {{ background: #57451f; color: #ffe0a0; }}
.low {{ background: #244738; color: #b5f0d0; }}
button {{ cursor: pointer; border: 1px solid #536a85; background: #263951; color: white; border-radius: 8px; padding: 10px 14px; }}
pre {{ white-space: pre-wrap; overflow-wrap: anywhere; max-height: 420px; overflow: auto; padding: 16px; background: #0c121b; border-radius: 8px; }}
footer {{ margin-top: 28px; color: #aab9ca; font-size: 13px; }}
main {{ max-width: 1440px; margin: 0 auto; padding: 28px clamp(16px, 4vw, 52px) 48px; }}
.card, .panel, .action {{ border: 1px solid #314156; box-shadow: 0 8px 24px #00000018; }}
.card:hover, .action:hover {{ border-color: #6d91bd; }}
.dashboard-nav {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 0 0 24px; padding: 12px; background: #101722; border: 1px solid #314156; border-radius: 12px; }}
.dashboard-nav a {{ color: #cfe2ff; text-decoration: none; border: 1px solid #405673; border-radius: 8px; padding: 8px 11px; font-size: 13px; }}
.dashboard-nav a:hover, .dashboard-nav a:focus-visible {{ background: #263951; outline: 2px solid #86b7ff; outline-offset: 2px; }}
.toolbar {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin: 12px 0 16px; }}
.toolbar input, .toolbar select {{ min-height: 40px; max-width: 100%; background: #0c121b; color: #edf2f7; border: 1px solid #536a85; border-radius: 8px; padding: 9px 12px; }}
.toolbar input {{ flex: 1 1 240px; }}
.toolbar label {{ display: flex; flex-wrap: wrap; gap: 8px; align-items: center; color: #b9c7d8; font-size: 13px; }}
.result-count {{ color: #aab9ca; font-size: 13px; margin: 8px 0; }}
[hidden] {{ display: none !important; }}
a:focus-visible, button:focus-visible {{ outline: 2px solid #86b7ff; outline-offset: 3px; }}
@media (max-width: 700px) {{
  header {{ padding: 22px 16px; }}
  main {{ padding: 20px 14px 32px; }}
  .grid, .columns {{ grid-template-columns: 1fr; }}
  .dashboard-nav a {{ flex: 1 1 auto; text-align: center; }}
}}
@media (prefers-reduced-motion: reduce) {{
  *, *::before, *::after {{ scroll-behavior: auto !important; transition: none !important; }}
}}
</style>
</head>
<body>
<header>
  <h1>Banking Data Platform</h1>
  <p class="muted">Pipeline observability · Data quality · Recovery intelligence</p>
  <span class="status">Platform status: {esc(report["status"])}</span>
</header>
<main>
  <nav class="dashboard-nav" aria-label="Dashboard sections">
    <a href="#executive-kpis">Overview</a>
    <a href="#pipeline-health">Pipeline health</a>
    <a href="#pipeline-reliability-breakdown">Reliability</a>
    <a href="#failure-hotspots">Failure hotspots</a>
    <a href="#recovery-evidence">Recovery evidence</a>
  <a href="#quality-evidence">Quality evidence</a>
    <a href="#prioritized-actions">Actions</a>
  </nav>
  <h2 id="executive-kpis">Executive KPIs</h2>
  <section class="grid">{card_html}</section>
  <section class="columns">
    <div>
      <h2 id="pipeline-health">Pipeline health</h2>
      <article class="panel">
        <p>Recorded runs: <strong>{esc(pipeline["total_runs"])}</strong></p>
        <p>Failure hotspots: <strong>{esc(pipeline["failure_hotspot_count"])}</strong></p>
        <p>Average run duration: <strong>{esc(pipeline["average_run_duration_seconds"])} seconds</strong></p>
      </article>
    </div>
    <div>
      <h2>Data quality and recovery</h2>
      <article class="panel">
        <p>Scorecard coverage: <strong>{esc(quality["scorecard_status"])}</strong></p>
        <p>Trend status: <strong>{esc(quality["trend_status"])}</strong></p>
        <p>Recovery assessment: <strong>{esc(recovery["assessment_status"])}</strong></p>
        <p>Failed stages: <strong>{esc(recovery["failed_stages"])}</strong></p>
        <p>Unfinished runs: <strong>{esc(recovery["unfinished_runs"])}</strong></p>
      </article>
    </div>
  </section>
  <h2 id="recovery-evidence">Unfinished run recovery evidence</h2>
  <p class="muted">Age-based classifications identify runs for investigation; they do not prove the cause of interruption. This section is read-only.</p>
  <div class="toolbar">
    <label for="recovery-search">Search recovery evidence
      <input id="recovery-search" type="search" placeholder="Run ID or classification" oninput="filterDashboardItems('recovery-search','recovery-list','recovery-count')">
    </label>
  </div>
  <p id="recovery-count" class="result-count" aria-live="polite"></p>
  <section id="recovery-list">{recovery_detail_html}</section>
  <h2 id="recovery-decisions">Human recovery decision history</h2>
  <p class="muted">Append-only audit history. Recording a decision does not execute a retry or change pipeline status.</p>
  <section id="recovery-decision-list">{recovery_decision_html}</section>
  {_render_pipeline_reliability_breakdown()}
<h2 id="failure-hotspots">Failure hotspot details</h2>
  <div class="toolbar">
    <label for="hotspot-search">Search stages
      <input id="hotspot-search" type="search" placeholder="e.g. Automated Tests" oninput="filterHotspotRate()">
    </label>
    <label for="hotspot-min-rate">Minimum failure rate
      <select id="hotspot-min-rate" onchange="filterHotspotRate()">
        <option value="0">All rates</option>
        <option value="1">At least 1%</option>
        <option value="10">At least 10%</option>
        <option value="25">At least 25%</option>
        <option value="40">At least 40%</option>
      </select>
    </label>
  </div>
  <p id="hotspot-count" class="result-count" aria-live="polite"></p>
  <section id="hotspot-list">{hotspot_html}</section>
  <h2 id="quality-evidence">Quality metric evidence</h2>
  <div class="toolbar">
    <label for="quality-search">Search quality evidence
      <input id="quality-search" type="search" placeholder="Metric or evidence filename" oninput="filterDashboardItems('quality-search','quality-list','quality-count')">
    </label>
  </div>
  <p id="quality-count" class="result-count" aria-live="polite"></p>
  <section id="quality-list">{quality_detail_html}</section>
  <h2 id="prioritized-actions">Prioritized actions</h2>
  <section>{action_html}</section>
  <h2>Evidence payload</h2>
  <p class="muted">The raw report is included for inspection and demonstration.</p>
  <button onclick="const p=document.getElementById('payload'); p.hidden=!p.hidden; this.textContent=p.hidden?'Show raw JSON':'Hide raw JSON'">Show raw JSON</button>
  <pre id="payload" hidden>{esc(safe_json)}</pre>
  <footer>Generated from recorded platform evidence. Read-only dashboard; no jobs are restarted and no pipeline data is modified. Metrics may be unavailable when source evidence is missing.</footer>
</main>
{script_html}
</body>
</html>"""


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output HTML file.")
    args = parser.parse_args(argv)

    report = build_platform_health()
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    document = render_dashboard(report)
    output.write_text(document, encoding="utf-8")

    if not output.is_file() or output.stat().st_size == 0:
        raise RuntimeError("Dashboard output was not created correctly.")

    print("Dashboard generated:", output)
    print("Platform status:", report["status"])
    print("HTML bytes:", output.stat().st_size)
    print("Source evidence is embedded; refresh by rerunning this script.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# PIPELINE_RELIABILITY_BREAKDOWN_V1

