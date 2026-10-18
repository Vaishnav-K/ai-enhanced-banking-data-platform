"""Read-only historical data-quality trend analysis."""
import argparse
import json
import sqlite3
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = ROOT / "data" / "processed" / "ingestion_manifest.db"
DEFAULT_REPORT_DIR = ROOT / "data" / "processed"


def _read_json(path):
    try:
        with path.open("r", encoding="utf-8-sig") as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _find_quality_reports(report_dir):
    """Discover quality report artifacts without assuming a history table."""
    candidates = sorted(report_dir.glob("**/*quality*report*.json"))
    candidates += sorted(report_dir.glob("**/*quality*summary*.json"))
    reports = []
    seen = set()

    for path in candidates:
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        payload = _read_json(path)
        if not isinstance(payload, dict):
            continue

        # Keep only JSON documents that resemble quality reports.
        normalized_keys = {str(key).lower() for key in payload}
        if not any(
            token in key
            for key in normalized_keys
            for token in ("quality", "violation", "invalid", "failed", "passed")
        ):
            continue

        reports.append({
            "path": str(path.relative_to(ROOT)),
            "modified_at": path.stat().st_mtime,
            "payload": payload,
        })

    return sorted(reports, key=lambda item: (item["modified_at"], item["path"]))


def _extract_counts(payload):
    """Extract only explicitly recorded numeric quality metrics."""
    result = {}
    aliases = {
        "records_checked": ("records_checked", "total_records", "rows_checked"),
        "records_passed": ("records_passed", "passed_records", "valid_records"),
        "records_failed": ("records_failed", "failed_records", "invalid_records"),
        "violations": ("violations", "violation_count", "total_violations"),
        "anomalies": ("anomalies", "anomaly_count", "total_anomalies"),
        "quality_score": ("quality_score", "score", "pass_rate_percent"),
    }

    for metric, keys in aliases.items():
        for key in keys:
            value = payload.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                result[metric] = value
                break

    nested = payload.get("summary")
    if isinstance(nested, dict):
        for metric, value in _extract_counts(nested).items():
            result.setdefault(metric, value)

    return result


def build_quality_trends(report_dir=DEFAULT_REPORT_DIR, db_path=DEFAULT_DB):
    """Build trends from distinct quality report files; never alters source data."""
    report_dir = Path(report_dir)
    reports = _find_quality_reports(report_dir)

    history = []
    for report in reports:
        history.append({
            "artifact": report["path"],
            "modified_at_epoch": report["modified_at"],
            "metrics": _extract_counts(report["payload"]),
        })

    metric_series = {}
    metric_changes = {}
    metric_names = sorted({
        metric
        for item in history
        for metric in item["metrics"]
    })

    for metric in metric_names:
        values = [
            {"artifact": item["artifact"], "value": item["metrics"][metric]}
            for item in history
            if metric in item["metrics"]
        ]
        metric_series[metric] = values
        if len(values) >= 2:
            first = values[0]["value"]
            last = values[-1]["value"]
            metric_changes[metric] = {
                "first_value": first,
                "latest_value": last,
                "absolute_change": round(last - first, 4),
                "direction": (
                    "INCREASED" if last > first
                    else "DECREASED" if last < first
                    else "UNCHANGED"
                ),
                "reports_compared": len(values),
            }

    issue_counts = Counter()
    issue_evidence = []
    for item in history:
        for metric in ("records_failed", "violations", "anomalies"):
            value = item["metrics"].get(metric)
            if isinstance(value, (int, float)) and value > 0:
                issue_counts[metric] += value
                issue_evidence.append({
                    "artifact": item["artifact"],
                    "metric": metric,
                    "count": value,
                })

    database_tables = []
    try:
        if Path(db_path).exists():
            with sqlite3.connect(str(db_path)) as conn:
                database_tables = [
                    row[0] for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                    )
                ]
    except sqlite3.Error:
        database_tables = []

    return {
        "report_type": "DATA_QUALITY_TRENDS",
        "source_report_count": len(history),
        "historical_comparison_available": any(
            len(values) >= 2 for values in metric_series.values()
        ),
        "trend_status": (
            "HISTORICAL_TREND_AVAILABLE"
            if any(len(values) >= 2 for values in metric_series.values())
            else "INSUFFICIENT_HISTORICAL_REPORTS"
        ),
        "metric_series": metric_series,
        "metric_changes": metric_changes,
        "positive_issue_counts_by_metric": dict(issue_counts),
        "issue_evidence": issue_evidence,
        "reports": history,
        "database_tables_discovered": database_tables,
        "limitations": [
            "Trends use discovered JSON artifacts, not a presumed historical quality table.",
            "File modification time orders reports but is not necessarily the business run timestamp.",
            "Repeated snapshots of the same report may not represent independent pipeline runs.",
            "Missing metrics are omitted rather than treated as zero.",
            "The report is read-only and does not modify quality reports or the database.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    try:
        report = build_quality_trends(args.report_dir, args.db)
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(2, f"ERROR: {exc}\n")

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("DATA QUALITY TREND REPORT")
        print("=" * 60)
        print(f"Source reports: {report['source_report_count']}")
        print(f"Trend status: {report['trend_status']}")
        for metric, change in report["metric_changes"].items():
            print(
                f"- {metric}: {change['first_value']} -> "
                f"{change['latest_value']} ({change['direction']})"
            )
        if not report["metric_changes"]:
            print("Insufficient distinct report history to calculate metric changes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
