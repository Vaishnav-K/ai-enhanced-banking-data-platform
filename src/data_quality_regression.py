"""Detect material data-quality regressions from historical quality metrics."""

# Support sibling imports in script and package execution.
import sys as _src_import_sys
from pathlib import Path as _src_import_path
_src_import_dir = str(_src_import_path(__file__).resolve().parent)
if _src_import_dir not in _src_import_sys.path:
    _src_import_sys.path.insert(0, _src_import_dir)
import argparse
import json
from pathlib import Path

from data_quality_trends import build_quality_trends

INCREASING_IS_BAD = {"records_failed", "violations", "anomalies"}
DECREASING_IS_BAD = {"quality_score"}


def build_regression_report(report_dir=None, db_path=None, threshold_percent=10.0):
    """Compare the latest two available observations for each quality metric."""
    if isinstance(threshold_percent, bool) or threshold_percent < 0:
        raise ValueError("threshold_percent must be non-negative")

    kwargs = {}
    if report_dir is not None:
        kwargs["report_dir"] = report_dir
    if db_path is not None:
        kwargs["db_path"] = db_path

    trends = build_quality_trends(**kwargs)
    regressions = []
    comparisons = []
    insufficient = []

    for metric, observations in trends["metric_series"].items():
        if metric not in INCREASING_IS_BAD | DECREASING_IS_BAD:
            continue

        if len(observations) < 2:
            insufficient.append({
                "metric": metric,
                "observations_available": len(observations),
                "reason": "At least two observations are required.",
            })
            continue

        previous = observations[-2]
        latest = observations[-1]
        old_value = previous["value"]
        new_value = latest["value"]

        if old_value < 0 or new_value < 0:
            insufficient.append({
                "metric": metric,
                "observations_available": len(observations),
                "reason": "Negative values are not valid for this comparison.",
            })
            continue

        delta = new_value - old_value
        percent_change = (
            round((delta / abs(old_value)) * 100.0, 4)
            if old_value != 0 else None
        )

        if metric in INCREASING_IS_BAD:
            regressed = (
                new_value > old_value
                and (
                    old_value == 0
                    or percent_change >= threshold_percent
                )
            )
            direction = "INCREASE"
        else:
            regressed = (
                new_value < old_value
                and (
                    old_value == 0
                    or abs(percent_change) >= threshold_percent
                )
            )
            direction = "DECREASE"

        comparison = {
            "metric": metric,
            "direction_checked": direction,
            "previous_value": old_value,
            "latest_value": new_value,
            "absolute_change": round(delta, 4),
            "percent_change": percent_change,
            "previous_artifact": previous["artifact"],
            "latest_artifact": latest["artifact"],
            "regression_detected": regressed,
        }
        comparisons.append(comparison)

        if regressed:
            regressions.append({
                **comparison,
                "severity": "HIGH" if old_value == 0 else "MEDIUM",
                "recommended_action": (
                    "Inspect the latest quality report and affected records; "
                    "confirm the change is not caused by a different data volume "
                    "or reporting scope before remediation."
                ),
            })

    if regressions:
        status = "REGRESSION_DETECTED"
    elif not comparisons:
        status = "INSUFFICIENT_BASELINE"
    else:
        status = "NO_REGRESSION_DETECTED"

    return {
        "report_type": "DATA_QUALITY_REGRESSION",
        "status": status,
        "threshold_percent": threshold_percent,
        "regression_count": len(regressions),
        "comparisons_performed": len(comparisons),
        "regressions": regressions,
        "comparisons": comparisons,
        "insufficient_baseline": insufficient,
        "source_report_count": trends["source_report_count"],
        "limitations": [
            "Only metrics with an explicitly defined regression direction are evaluated.",
            "Comparisons use the latest two discovered observations for each metric.",
            "File modification order may not equal business execution order.",
            "Different report scopes or data volumes can affect metric comparisons.",
            "This detector is read-only and does not change source data or trigger remediation.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", help="Directory containing quality report JSON files.")
    parser.add_argument("--db", help="SQLite database path used for source context.")
    parser.add_argument(
        "--threshold-percent", type=float, default=10.0,
        help="Minimum relative change for non-zero baselines (default: 10).",
    )
    parser.add_argument("--json", action="store_true", help="Print full JSON output.")
    args = parser.parse_args(argv)

    try:
        report = build_regression_report(
            report_dir=args.report_dir,
            db_path=args.db,
            threshold_percent=args.threshold_percent,
        )
    except (ValueError, OSError) as exc:
        parser.exit(2, f"ERROR: {exc}\n")

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("DATA QUALITY REGRESSION REPORT")
        print("=" * 60)
        print(f"Status: {report['status']}")
        print(f"Source reports: {report['source_report_count']}")
        print(f"Comparisons: {report['comparisons_performed']}")
        print(f"Regressions: {report['regression_count']}")
        for item in report["regressions"]:
            print(
                f"- [{item['severity']}] {item['metric']}: "
                f"{item['previous_value']} -> {item['latest_value']}"
            )
        if report["status"] == "INSUFFICIENT_BASELINE":
            print("No eligible metric has enough historical observations.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
