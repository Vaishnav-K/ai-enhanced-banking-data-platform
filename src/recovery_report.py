"""Command-line interface for pipeline recovery diagnostics."""

import argparse
import json

from lineage_tracker import get_recovery_report


def format_report(report, details=False):
    """Format recovery diagnostics; show history only in details mode."""
    interrupted = report["interrupted_runs"]
    failed = report["failed_stages"]
    attempts = report["attempt_history"]
    stale_count = report.get("potentially_stale_count", 0)
    recent_count = report.get("recently_started_count", 0)
    unknown_count = report.get("unknown_age_count", 0)

    lines = [
        "PIPELINE RECOVERY REPORT",
        "=" * 60,
        f"Interrupted runs: {len(interrupted)}",
        f"Failed stages:    {len(failed)}",
        f"Recorded attempts: {len(attempts)}",
        f"Potentially stale: {stale_count}",
        f"Recently started:  {recent_count}",
        f"Unknown age:       {unknown_count}",
        "",
    ]

    if interrupted:
        lines.append("INTERRUPTED RUNS")
        lines.append("-" * 60)
        for run in interrupted:
            lines.append(
                f"Run: {run['run_id']} | Pipeline: {run['pipeline_name']}"
            )
            lines.append(f"Started: {run['started_at_utc']}")
            lines.append("Status: RUNNING without a finish timestamp")
            lines.append(
                f"Age: {run.get('age_minutes', 'unknown')} minutes | "
                f"Classification: {run.get('recovery_classification', 'UNKNOWN')}"
            )
            if run.get("timestamp_parse_error"):
                lines.append(f"Timestamp issue: {run['timestamp_parse_error']}")
            lines.append("")

    if failed and details:
        lines.append("FAILED STAGES")
        lines.append("-" * 60)
        for stage in failed:
            lines.append(
                f"Run: {stage['run_id']} | "
                f"Stage: {stage['stage_name']} | "
                f"Exit code: {stage['exit_code']}"
            )
            lines.append(f"Error: {stage['error']}")
            lines.append("")

    if attempts and details:
        lines.append("RETRY ATTEMPT HISTORY")
        lines.append("-" * 60)
        for attempt in attempts:
            lines.append(
                f"Run: {attempt['run_id']} | "
                f"Stage: {attempt['stage_name']} | "
                f"Attempt: {attempt['attempt_number']} | "
                f"Status: {attempt['status']} | "
                f"Retryable: {bool(attempt['retryable'])}"
            )
            if attempt["error"]:
                lines.append(f"Error: {attempt['error']}")
        lines.append("")

    if not interrupted:
        lines.append("No unfinished runs detected.")
    if not details and (failed or attempts):
        lines.append("Historical failure and attempt details omitted; use --details to display them.")

    lines.extend([
        "",
        "Note: An interrupted run is a recovery candidate, not proof that "
        "its process is still running.",
    ])
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Inspect interrupted pipeline runs and retry history."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the complete report as JSON.",
    )
    parser.add_argument(
        "--details", action="store_true",
        help="Include historical failed stages and retry attempts in human-readable output.",
    )
    parser.add_argument(
        "--stale-after-minutes", type=float, default=60,
        help="Minutes before an unfinished run is classified as potentially stale (default: 60).",
    )
    args = parser.parse_args(argv)
    if args.stale_after_minutes <= 0:
        parser.error("--stale-after-minutes must be greater than zero")

    report = get_recovery_report(stale_after_minutes=args.stale_after_minutes)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(format_report(report, details=args.details))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
