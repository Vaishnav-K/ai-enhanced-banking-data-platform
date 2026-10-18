"""Read-only recovery decision engine built on existing pipeline diagnostics."""

# Support sibling imports in script and package execution.
import sys as _src_import_sys
from pathlib import Path as _src_import_path
_src_import_dir = str(_src_import_path(__file__).resolve().parent)
if _src_import_dir not in _src_import_sys.path:
    _src_import_sys.path.insert(0, _src_import_dir)
import argparse
import json
import sqlite3

from recovery_assessment import build_assessment


def build_recovery_decisions(run_id=None, stale_after_minutes=60):
    """Classify recovery scenarios without executing retries or changing data."""
    report = build_assessment(
        run_id=run_id,
        stale_after_minutes=stale_after_minutes,
    )
    decisions = []
    run_status = str(report["run"].get("status", "")).upper()
    recovery = report["recovery"]
    failed_stages = recovery["failed_stages"]
    attempts = recovery["attempt_history"]
    interrupted = recovery["interrupted"]
    discrepancies = report["runtime_lineage"]["discrepancies"]

    if interrupted:
        for item in interrupted:
            classification = item.get("recovery_classification", "UNKNOWN")
            decisions.append({
                "scenario": "UNFINISHED_RUN",
                "target": item["run_id"],
                "decision": "VERIFY_PROCESS_BEFORE_RECOVERY",
                "priority": "HIGH" if classification == "POTENTIALLY_STALE" else "MEDIUM",
                "evidence": {
                    "classification": classification,
                    "age_minutes": item.get("age_minutes"),
                    "timestamp_parse_error": item.get("timestamp_parse_error"),
                },
                "reason": "A RUNNING record without a finish timestamp does not prove the process is still active or safe to retry.",
            })

    for stage in failed_stages:
        stage_attempts = [
            attempt for attempt in attempts
            if attempt.get("stage_order") == stage.get("stage_order")
            and attempt.get("run_id") == stage.get("run_id")
        ]
        retryable_recorded = any(
            bool(attempt.get("retryable")) for attempt in stage_attempts
        )
        decisions.append({
            "scenario": "FAILED_STAGE",
            "target": stage["stage_name"],
            "decision": (
                "RETRY_CANDIDATE_REQUIRES_SAFETY_CHECKS"
                if retryable_recorded
                else "INVESTIGATE_BEFORE_RETRY"
            ),
            "priority": "HIGH",
            "evidence": {
                "error": stage.get("error"),
                "exit_code": stage.get("exit_code"),
                "attempts_recorded": len(stage_attempts),
                "retryable_flag_recorded": retryable_recorded,
            },
            "reason": (
                "At least one recorded attempt is marked retryable; verify idempotency, inputs, and side effects before any retry."
                if retryable_recorded
                else "No recorded attempt is marked retryable; inspect the error and correct the cause before considering a retry."
            ),
        })

    for item in discrepancies:
        decisions.append({
            "scenario": "LINEAGE_DISCREPANCY",
            "target": item["artifact_path"],
            "decision": "INVESTIGATE_ARTIFACT_AND_DOWNSTREAM_IMPACT",
            "priority": "HIGH",
            "evidence": {
                "stage_name": item["stage_name"],
                "issue_type": item["issue_type"],
            },
            "reason": "Resolve the artifact discrepancy and assess downstream consumers before rerunning affected stages.",
        })

    if not decisions:
        decisions.append({
            "scenario": "NO_IMMEDIATE_RECOVERY_TRIGGER",
            "target": report["run"]["run_id"],
            "decision": (
                "REVIEW_COVERAGE"
                if report["status"] == "REVIEW"
                else "NO_ACTION_IDENTIFIED"
            ),
            "priority": "LOW",
            "evidence": {
                "pipeline_status": run_status,
                "assessment_status": report["status"],
                "uninstrumented_stages": report["runtime_lineage"]["uninstrumented_stages"],
            },
            "reason": "No selected-run unfinished record, failed stage, or lineage discrepancy was identified; this is not proof of complete pipeline health.",
        })

    return {
        "report_type": "RECOVERY_DECISION_ENGINE",
        "run": report["run"],
        "assessment_status": report["status"],
        "decision_count": len(decisions),
        "decisions": decisions,
        "automatic_actions_taken": [],
        "limitations": [
            "This engine is read-only and never retries, restarts, or modifies pipeline data.",
            "A retryable flag is evidence for review, not proof that retrying is safe.",
            "Decisions depend on recorded diagnostics and declared lineage; missing instrumentation can hide problems.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate evidence-based, read-only pipeline recovery decisions."
    )
    parser.add_argument("--run-id", help="Run to assess; defaults to the latest recorded run.")
    parser.add_argument(
        "--stale-after-minutes",
        type=float,
        default=60,
        help="Threshold for classifying unfinished runs as potentially stale.",
    )
    parser.add_argument("--json", action="store_true", help="Print complete JSON output.")
    args = parser.parse_args(argv)

    if args.stale_after_minutes <= 0:
        parser.error("--stale-after-minutes must be greater than zero")

    try:
        report = build_recovery_decisions(args.run_id, args.stale_after_minutes)
    except (ValueError, OSError, sqlite3.Error, json.JSONDecodeError) as exc:
        parser.exit(2, f"ERROR: {exc}\n")

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print("RECOVERY DECISION ENGINE")
        print("=" * 60)
        print(f"Run: {report['run']['run_id']}")
        print(f"Assessment: {report['assessment_status']}")
        print(f"Decisions: {report['decision_count']}")
        for item in report["decisions"]:
            print(f"\n[{item['priority']}] {item['scenario']}: {item['target']}")
            print(f"Decision: {item['decision']}")
            print(f"Reason: {item['reason']}")
        print("\nAutomatic actions taken: 0")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
