"""Command-line interface for pipeline recovery diagnostics."""

import argparse
import json

from lineage_tracker import get_recovery_report


def format_report(report):
    """Format recovery diagnostics for terminal output."""
    interrupted = report["interrupted_runs"]
    failed = report["failed_stages"]
    attempts = report["attempt_history"]

    lines = [
        "PIPELINE RECOVERY REPORT",
        "=" * 60,
        f"Interrupted runs: {len(interrupted)}",
        f"Failed stages:    {len(failed)}",
        f"Recorded attempts: {len(attempts)}",
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
            lines.append("")

    if failed:
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

    if attempts:
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

    if not interrupted and not failed and not attempts:
        lines.append("No interrupted runs, failed stages, or retry history found.")

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
    args = parser.parse_args(argv)

    report = get_recovery_report()
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(format_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
