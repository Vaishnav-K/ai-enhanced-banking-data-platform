"""Generate a self-contained HTML dashboard from platform health evidence."""
import argparse
import html
import json
from pathlib import Path

from platform_health_demo import build_platform_health

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "reports" / "platform_health_dashboard.html"


def render_dashboard(report):
    def esc(value):
        return html.escape(str(value if value is not None else "Unavailable"))

    pipeline = report["pipeline"]
    quality = report["data_quality"]
    recovery = report["recovery"]

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
</style>
</head>
<body>
<header>
  <h1>Banking Data Platform</h1>
  <p class="muted">Pipeline observability · Data quality · Recovery intelligence</p>
  <span class="status">Platform status: {esc(report["status"])}</span>
</header>
<main>
  <h2>Executive KPIs</h2>
  <section class="grid">{card_html}</section>
  <section class="columns">
    <div>
      <h2>Pipeline health</h2>
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
  <h2>Failure hotspot details</h2>
  <section>{hotspot_html}</section>
  <h2>Quality metric evidence</h2>
  <section>{quality_detail_html}</section>
  <h2>Prioritized actions</h2>
  <section>{action_html}</section>
  <h2>Evidence payload</h2>
  <p class="muted">The raw report is included for inspection and demonstration.</p>
  <button onclick="const p=document.getElementById('payload'); p.hidden=!p.hidden; this.textContent=p.hidden?'Show raw JSON':'Hide raw JSON'">Show raw JSON</button>
  <pre id="payload" hidden>{esc(safe_json)}</pre>
  <footer>Generated from recorded platform evidence. Read-only dashboard; no jobs are restarted and no pipeline data is modified. Metrics may be unavailable when source evidence is missing.</footer>
</main>
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
