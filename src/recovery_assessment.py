"""Unified, read-only recovery assessment across runtime lineage and impact."""
import argparse
import json
import sqlite3
from pathlib import Path

from lineage_impact import analyze_impact
from lineage_tracker import get_recovery_report
from runtime_lineage_report import build_report

ROOT = Path(__file__).resolve().parent.parent

ISSUE_FIELDS = (
    "missing_recorded_inputs",
    "unexpected_recorded_inputs",
    "inputs_recorded_missing_on_disk",
    "missing_recorded_outputs",
    "unexpected_recorded_outputs",
    "unexpected_changed_outputs",
)


def build_assessment(run_id=None, stale_after_minutes=60):
    """Combine recovery state, runtime lineage mismatches, and downstream impact."""
    lineage = build_report(run_id=run_id)
    selected_run_id = lineage["run"]["run_id"]
    recovery = get_recovery_report(stale_after_minutes=stale_after_minutes)

    interrupted = [
        item for item in recovery["interrupted_runs"]
        if item["run_id"] == selected_run_id
    ]
    failed = [
        item for item in recovery["failed_stages"]
        if item["run_id"] == selected_run_id
    ]
    attempts = [
        item for item in recovery["attempt_history"]
        if item["run_id"] == selected_run_id
    ]

    discrepancies = []
    impact_targets = set()

    for stage in lineage["stages"]:
        for field in ISSUE_FIELDS:
            for artifact_path in stage[field]:
                discrepancies.append({
                    "stage_name": stage["stage_name"],
                    "issue_type": field,
                    "artifact_path": artifact_path,
                })
                impact_targets.add(artifact_path)

    for stage in failed:
        impact_targets.add(stage["stage_name"])

    impacts = []
    for target in sorted(impact_targets):
        try:
            impact = analyze_impact(target)
            impacts.append({
                "target": target,
                "analysis_mode": impact["analysis_mode"],
                "impacted_stages": impact["impacted_stages"],
                "impacted_artifacts": impact["impacted_artifacts"],
                "impact_count": impact["impact_count"],
            })
        except ValueError:
            # An unexpected artifact may not exist in the declared graph.
            impacts.append({
                "target": target,
                "analysis_mode": "NOT_IN_DECLARED_GRAPH",
                "impacted_stages": [],
                "impacted_artifacts": [],
                "impact_count": 0,
            })

    recommendations = []
    if interrupted:
        recommendations.append({
            "priority": "HIGH",
            "action": "Verify whether the pipeline process is still running before considering recovery.",
            "reason": "The selected run is recorded as unfinished.",
        })
    if failed:
        recommendations.append({
            "priority": "HIGH",
            "action": "Review failed-stage errors and attempt history before deciding whether to retry.",
            "reason": f"{len(failed)} failed stage record(s) belong to this run.",
        })
    if discrepancies:
        recommendations.append({
            "priority": "HIGH",
            "action": "Investigate the listed artifact discrepancies and assess downstream consumers before rerunning affected stages.",
            "reason": f"{len(discrepancies)} runtime lineage discrepancy record(s) were found.",
        })
    if lineage["uninstrumented_stages"]:
        recommendations.append({
            "priority": "MEDIUM",
            "action": "Add runtime artifact observations to stages that are not yet instrumented.",
            "reason": "Some declared stages have no observations for this run.",
        })
    if not recommendations:
        recommendations.append({
            "priority": "LOW",
            "action": "No immediate recovery action was identified; review the report limitations and uninstrumented stages.",
            "reason": "No selected-run failures, unfinished-run records, or lineage mismatches were found.",
        })

    if interrupted or failed or lineage["status"] == "WARN":
        status = "ACTION_REQUIRED"
    elif lineage["uninstrumented_stages"]:
        status = "REVIEW"
    else:
        status = "HEALTHY"

    return {
        "assessment_type": "UNIFIED_RECOVERY_ASSESSMENT",
        "status": status,
        "run": lineage["run"],
        "recovery": {
            "interrupted": interrupted,
            "failed_stages": failed,
            "attempt_history": attempts,
            "stale_after_minutes": recovery["stale_after_minutes"],
        },
        "runtime_lineage": {
            "status": lineage["status"],
            "summary": lineage["summary"],
            "discrepancies": discrepancies,
            "uninstrumented_stages": lineage["uninstrumented_stages"],
        },
        "downstream_impact": impacts,
        "recommendations": recommendations,
        "limitations": [
            "Recovery classifications identify candidates and do not prove a process is still running.",
            "Downstream impact is inferred from declared lineage, not runtime proof.",
            "Declared input/output observations do not prove physical reads or writes.",
            "This assessment is read-only and never restarts jobs or modifies pipeline data.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Combine recovery diagnostics, runtime lineage, and downstream impact."
    )
    parser.add_argument("--run-id", help="Run to assess; defaults to the latest recorded run.")
    parser.add_argument(
        "--stale-after-minutes", type=float, default=60,
        help="Age threshold for potentially stale unfinished runs (default: 60).",
    )
    parser.add_argument("--json", action="store_true", help="Print the full JSON assessment.")
    args = parser.parse_args(argv)

    if args.stale_after_minutes <= 0:
        parser.error("--stale-after-minutes must be greater than zero")

    try:
        report = build_assessment(args.run_id, args.stale_after_minutes)
    except (ValueError, OSError, sqlite3.Error, json.JSONDecodeError) as exc:
        parser.exit(2, f"ERROR: {exc}\n")

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print("UNIFIED RECOVERY ASSESSMENT")
        print("=" * 60)
        print(f"Run: {report['run']['run_id']}")
        print(f"Pipeline status: {report['run']['status']}")
        print(f"Assessment: {report['status']}")
        print(f"Lineage reconciliation: {report['runtime_lineage']['status']}")
        print(f"Discrepancies: {len(report['runtime_lineage']['discrepancies'])}")
        print(f"Failed stages: {len(report['recovery']['failed_stages'])}")
        print(f"Unfinished-run records: {len(report['recovery']['interrupted'])}")
        print(f"Downstream impact targets: {len(report['downstream_impact'])}")
        print("\nRECOMMENDED ACTIONS")
        for item in report["recommendations"]:
            print(f"- [{item['priority']}] {item['action']}")
            print(f"  Reason: {item['reason']}")
        print("\nRead-only assessment; recommendations do not trigger recovery.")

    return 0 if report["status"] == "HEALTHY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
