"""Evidence-based banking data quality scorecard."""
import argparse
import json
from pathlib import Path

from data_quality_trends import build_quality_trends

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT_DIR = ROOT / "data" / "processed"

DIMENSIONS = {
    "completeness": ("completeness_score", "completeness_percent"),
    "validity": ("validity_score", "validity_percent"),
    "consistency": ("consistency_score", "consistency_percent"),
    "overall_quality": ("quality_score", "pass_rate_percent"),
}


def _find_latest_artifact(report_dir, terms):
    candidates = []
    for path in Path(report_dir).rglob("*.json"):
        name = path.name.lower()
        if any(term in name for term in terms):
            try:
                with path.open("r", encoding="utf-8-sig") as stream:
                    payload = json.load(stream)
                if isinstance(payload, dict):
                    candidates.append((path, payload))
            except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                continue
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0].stat().st_mtime)


def _find_metric(payload, keys):
    """Find an explicitly recorded metric, including within summary objects."""
    if not isinstance(payload, dict):
        return None
    for key in keys:
        value = payload.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if 0 <= value <= 100:
                return value
    for child in payload.values():
        if isinstance(child, dict):
            value = _find_metric(child, keys)
            if value is not None:
                return value
    return None


def build_scorecard(report_dir=DEFAULT_REPORT_DIR):
    report_dir = Path(report_dir)
    quality_history = build_quality_trends(report_dir=report_dir)
    quality_artifact = _find_latest_artifact(
        report_dir, ("quality_report", "quality_summary")
    )
    anomaly_artifact = _find_latest_artifact(
        report_dir, ("anomal", "transaction_anomal")
    )

    dimensions = {}
    for dimension, keys in DIMENSIONS.items():
        value = None
        source = None
        if quality_artifact:
            value = _find_metric(quality_artifact[1], keys)
            if value is not None:
                source = str(quality_artifact[0].relative_to(ROOT))
        if value is None and dimension == "overall_quality":
            changes = quality_history["metric_series"].get("quality_score", [])
            if changes:
                value = changes[-1]["value"]
                if 0 <= value <= 100:
                    source = changes[-1]["artifact"]
                else:
                    value = None
        dimensions[dimension] = {
            "score_percent": value,
            "evidence_available": value is not None,
            "source": source,
        }

    anomaly_count = None
    anomaly_source = None
    if anomaly_artifact:
        payload = anomaly_artifact[1]
        for key in ("anomalies", "anomaly_count", "total_anomalies"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                anomaly_count = len(candidate)
                anomaly_source = str(anomaly_artifact[0].relative_to(ROOT))
                break
            if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
                anomaly_count = candidate
                anomaly_source = str(anomaly_artifact[0].relative_to(ROOT))
                break

    available_scores = [
        item["score_percent"]
        for item in dimensions.values()
        if item["score_percent"] is not None
    ]
    overall = (
        round(sum(available_scores) / len(available_scores), 2)
        if available_scores else None
    )

    if len(available_scores) == len(DIMENSIONS):
        coverage = "COMPLETE"
    elif available_scores:
        coverage = "PARTIAL"
    else:
        coverage = "INSUFFICIENT_EVIDENCE"

    return {
        "report_type": "BANKING_DATA_QUALITY_SCORECARD",
        "coverage_status": coverage,
        "overall_score_percent": overall,
        "scored_dimensions": len(available_scores),
        "total_dimensions": len(DIMENSIONS),
        "dimensions": dimensions,
        "anomaly_count": anomaly_count,
        "anomaly_evidence_source": anomaly_source,
        "quality_history_reports": quality_history["source_report_count"],
        "limitations": [
            "Only explicitly recorded scores are used; missing dimensions remain unavailable.",
            "The overall score is an unweighted average of available dimensions, not a certified business KPI.",
            "Anomaly counts are not a quality score and are reported separately.",
            "The newest file by modification time may not represent the newest business run.",
            "This scorecard reads existing artifacts and does not change pipeline data.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    try:
        report = build_scorecard(args.report_dir)
    except (OSError, ValueError) as exc:
        parser.exit(2, f"ERROR: {exc}\n")

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("BANKING DATA QUALITY SCORECARD")
        print("=" * 60)
        print(f"Coverage: {report['coverage_status']}")
        print(f"Overall score: {report['overall_score_percent']}")
        for name, dimension in report["dimensions"].items():
            score = dimension["score_percent"]
            print(f"- {name}: {score if score is not None else 'UNAVAILABLE'}")
        print(f"Anomaly count: {report['anomaly_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
