"""Integrated, read-only executive demo for the banking data platform."""

# Support sibling imports in script and package execution.
import sys as _src_import_sys
from pathlib import Path as _src_import_path
_src_import_dir = str(_src_import_path(__file__).resolve().parent)
if _src_import_dir not in _src_import_sys.path:
    _src_import_sys.path.insert(0, _src_import_dir)
import argparse
import json
import sqlite3

from pipeline_observability import build_observability_report
from data_quality_trends import build_quality_trends
from data_quality_regression import build_regression_report
from data_quality_scorecard import build_scorecard
from recovery_assessment import build_assessment
from lineage_tracker import get_recovery_report


def build_platform_health():
    """Combine existing platform reports into a concise executive summary."""
    observability = build_observability_report()
    trends = build_quality_trends()
    regression = build_regression_report()
    scorecard = build_scorecard()
    recovery = build_assessment()
    platform_recovery = get_recovery_report()
    unfinished_runs = platform_recovery["interrupted_runs"]

    actions = []
    if regression["status"] == "REGRESSION_DETECTED":
        actions.append({
            "priority": "HIGH",
            "area": "DATA_QUALITY",
            "action": "Investigate detected quality regressions before downstream consumption.",
            "evidence_count": regression["regression_count"],
        })
    if recovery["status"] == "ACTION_REQUIRED" or unfinished_runs:
        actions.append({
            "priority": "HIGH",
            "area": "PIPELINE_RECOVERY",
            "action": "Review failed runs and lineage discrepancies before deciding on recovery.",
            "evidence_count": (
                len(recovery["recovery"]["failed_stages"])
                + len(recovery["runtime_lineage"]["discrepancies"])
                + len(unfinished_runs)
            ),
        })
    if observability["failure_hotspots"]:
        actions.append({
            "priority": "MEDIUM",
            "area": "RELIABILITY",
            "action": "Review stages with recorded failures and their error history.",
            "evidence_count": len(observability["failure_hotspots"]),
        })
    if scorecard["coverage_status"] != "COMPLETE":
        actions.append({
            "priority": "MEDIUM",
            "area": "QUALITY_COVERAGE",
            "action": "Improve quality metric coverage; unavailable dimensions must not be treated as passing.",
            "evidence_count": scorecard["total_dimensions"] - scorecard["scored_dimensions"],
        })
    if trends["trend_status"] == "INSUFFICIENT_HISTORICAL_REPORTS":
        actions.append({
            "priority": "LOW",
            "area": "HISTORICAL_COVERAGE",
            "action": "Collect comparable quality reports over multiple runs before interpreting trends.",
            "evidence_count": trends["source_report_count"],
        })

    priority_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    actions.sort(key=lambda item: priority_rank[item["priority"]])

    if any(action["priority"] == "HIGH" for action in actions):
        status = "ACTION_REQUIRED"
    elif actions:
        status = "REVIEW"
    else:
        status = "NO_ISSUES_DETECTED_IN_AVAILABLE_EVIDENCE"

    return {
        "report_type": "BANKING_PLATFORM_EXECUTIVE_HEALTH",
        "status": status,
        "pipeline": {
            "total_runs": observability["summary"]["total_runs"],
            "success_rate_percent": observability["summary"]["success_rate_percent"],
            "average_run_duration_seconds": observability["summary"]["average_run_duration_seconds"],
            "failure_hotspot_count": len(observability["failure_hotspots"]),
        },
        "data_quality": {
            "scorecard_status": scorecard["coverage_status"],
            "overall_score_percent": scorecard["overall_score_percent"],
            "scored_dimensions": scorecard["scored_dimensions"],
            "total_dimensions": scorecard["total_dimensions"],
            "regression_status": regression["status"],
            "regression_count": regression["regression_count"],
            "trend_status": trends["trend_status"],
        },
        "recovery": {
            "assessment_status": recovery["status"],
            "failed_stages": len(recovery["recovery"]["failed_stages"]),
            "unfinished_runs": len(unfinished_runs),
            "potentially_stale_runs": platform_recovery["potentially_stale_count"],
            "recently_started_runs": platform_recovery["recently_started_count"],
            "unknown_age_runs": platform_recovery["unknown_age_count"],
            "lineage_discrepancies": len(recovery["runtime_lineage"]["discrepancies"]),
        },
        "prioritized_actions": actions,
        "safety": {
            "read_only": True,
            "automatic_retries": False,
            "automatic_remediation": False,
        },
        "limitations": [
            "Status reflects available recorded evidence, not proof that every component is healthy.",
            "Historical trends depend on the discovered report artifacts and their timestamps.",
            "A recorded retryable flag does not prove that retrying is safe.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    args = parser.parse_args(argv)

    try:
        report = build_platform_health()
    except (ValueError, OSError, sqlite3.Error, json.JSONDecodeError) as exc:
        parser.exit(2, f"ERROR: {exc}\n")

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print("BANKING DATA PLATFORM — EXECUTIVE HEALTH")
        print("=" * 60)
        print(f"Overall status: {report['status']}")
        print(f"Pipeline success rate: {report['pipeline']['success_rate_percent']}%")
        print(f"Average run duration: {report['pipeline']['average_run_duration_seconds']} seconds")
        print(
            "Quality score: "
            f"{report['data_quality']['overall_score_percent']} "
            f"({report['data_quality']['scorecard_status']})"
        )
        print(f"Quality regressions: {report['data_quality']['regression_count']}")
        print(f"Lineage discrepancies: {report['recovery']['lineage_discrepancies']}")
        print("\nPRIORITIZED ACTIONS")
        for action in report["prioritized_actions"]:
            print(f"- [{action['priority']}] {action['area']}: {action['action']}")
        print("\nRead-only demo: no automatic retries or remediation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
