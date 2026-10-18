"""Read-only pipeline performance and reliability analytics."""
import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

DEFAULT_DB = (
    Path(__file__).resolve().parent.parent
    / "data" / "processed" / "ingestion_manifest.db"
)


def _rows(conn, table):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not exists:
        return []
    return [
        dict(row)
        for row in conn.execute(f'SELECT * FROM "{table}"').fetchall()
    ]


def _number(value):
    try:
        result = float(value)
        return result if result >= 0 else None
    except (TypeError, ValueError):
        return None


def _percent(numerator, denominator):
    return round(100.0 * numerator / denominator, 2) if denominator else 0.0


def build_observability_report(db_path=DEFAULT_DB, slow_stage_seconds=60):
    """Aggregate recorded run, stage, and retry data without changing it."""
    if isinstance(slow_stage_seconds, bool) or slow_stage_seconds <= 0:
        raise ValueError("slow_stage_seconds must be greater than zero")

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        runs = _rows(conn, "pipeline_runs")
        stages = _rows(conn, "pipeline_stage_runs")
        attempts = _rows(conn, "pipeline_stage_attempts")

    if not runs:
        raise ValueError(
            f"No pipeline run history found in {db_path}; "
            "verify that the database exists and contains pipeline_runs."
        )

    run_statuses = Counter(
        str(row.get("status") or "UNKNOWN").upper() for row in runs
    )
    completed = [
        row for row in runs
        if str(row.get("status") or "").upper() in {"PASS", "SUCCESS", "SUCCEEDED"}
    ]
    failed_runs = [
        row for row in runs
        if str(row.get("status") or "").upper() in {"FAIL", "FAILED", "ERROR"}
    ]

    durations = [
        value for row in runs
        if (value := _number(row.get("duration_seconds"))) is not None
    ]

    stage_stats = defaultdict(lambda: {
        "executions": 0, "failures": 0, "durations": [], "statuses": Counter()
    })
    for row in stages:
        name = str(row.get("stage_name") or "UNKNOWN")
        stats = stage_stats[name]
        stats["executions"] += 1
        status = str(row.get("status") or "UNKNOWN").upper()
        stats["statuses"][status] += 1
        if status in {"FAIL", "FAILED", "ERROR"}:
            stats["failures"] += 1
        duration = _number(row.get("duration_seconds"))
        if duration is not None:
            stats["durations"].append(duration)

    stage_metrics = []
    for name, stats in stage_stats.items():
        values = stats["durations"]
        average = round(sum(values) / len(values), 3) if values else None
        stage_metrics.append({
            "stage_name": name,
            "executions": stats["executions"],
            "failures": stats["failures"],
            "failure_rate_percent": _percent(
                stats["failures"], stats["executions"]
            ),
            "average_duration_seconds": average,
            "max_duration_seconds": round(max(values), 3) if values else None,
            "slow_stage": bool(average is not None and average >= slow_stage_seconds),
            "statuses": dict(stats["statuses"]),
        })

    stage_metrics.sort(
        key=lambda item: (
            item["average_duration_seconds"] is not None,
            item["average_duration_seconds"] or -1,
        ),
        reverse=True,
    )

    attempt_statuses = Counter(
        str(row.get("status") or "UNKNOWN").upper() for row in attempts
    )
    retryable_attempts = sum(bool(row.get("retryable")) for row in attempts)
    stage_failure_hotspots = sorted(
        stage_metrics,
        key=lambda item: (item["failures"], item["failure_rate_percent"]),
        reverse=True,
    )

    return {
        "report_type": "PIPELINE_OBSERVABILITY",
        "database": str(db_path),
        "summary": {
            "total_runs": len(runs),
            "run_status_counts": dict(run_statuses),
            "successful_runs": len(completed),
            "failed_runs": len(failed_runs),
            "success_rate_percent": _percent(len(completed), len(runs)),
            "average_run_duration_seconds": (
                round(sum(durations) / len(durations), 3) if durations else None
            ),
            "stage_executions": len(stages),
            "distinct_stages": len(stage_metrics),
            "recorded_attempts": len(attempts),
            "retryable_attempts": retryable_attempts,
            "attempt_status_counts": dict(attempt_statuses),
        },
        "slow_stages": [
            stage for stage in stage_metrics if stage["slow_stage"]
        ],
        "stage_performance": stage_metrics,
        "failure_hotspots": [
            stage for stage in stage_failure_hotspots if stage["failures"]
        ],
        "limitations": [
            "Metrics reflect recorded database history only.",
            "Missing duration values are excluded from duration averages.",
            "A recorded retryable flag does not prove that retrying is safe.",
            "This report is read-only and does not execute pipeline actions.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Analyze pipeline reliability, stage performance, and retries."
    )
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path.")
    parser.add_argument(
        "--slow-stage-seconds", type=float, default=60,
        help="Average stage duration threshold for slow-stage reporting.",
    )
    parser.add_argument("--json", action="store_true", help="Print complete JSON.")
    args = parser.parse_args(argv)

    try:
        report = build_observability_report(args.db, args.slow_stage_seconds)
    except (ValueError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"ERROR: {exc}\n")

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        summary = report["summary"]
        print("PIPELINE OBSERVABILITY REPORT")
        print("=" * 60)
        print(f"Runs: {summary['total_runs']}")
        print(f"Success rate: {summary['success_rate_percent']}%")
        print(f"Average run duration: {summary['average_run_duration_seconds']} seconds")
        print(f"Stage executions: {summary['stage_executions']}")
        print(f"Recorded attempts: {summary['recorded_attempts']}")
        print(f"Slow stages: {len(report['slow_stages'])}")
        print(f"Failure hotspots: {len(report['failure_hotspots'])}")
        for stage in report["slow_stages"]:
            print(
                f"- SLOW: {stage['stage_name']} "
                f"(average {stage['average_duration_seconds']} seconds)"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
