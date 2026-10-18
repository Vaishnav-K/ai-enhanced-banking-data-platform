"""Evidence-based banking data quality scorecard."""

# Support sibling imports in script and package execution.
import sys as _src_import_sys
from pathlib import Path as _src_import_path
_src_import_dir = str(_src_import_path(__file__).resolve().parent)
if _src_import_dir not in _src_import_sys.path:
    _src_import_sys.path.insert(0, _src_import_dir)
import argparse
import json
from pathlib import Path

try:
    from .data_quality_trends import build_quality_trends
except ImportError:
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


# EVIDENCE_BACKED_SCORECARD_INTEGRATION_V1
_build_scorecard_before_evidence_integration = build_scorecard

def build_scorecard(report_dir=DEFAULT_REPORT_DIR):
    """Augment the existing scorecard with explicitly sourced pipeline metrics."""
    import json
    from pathlib import Path

    report = _build_scorecard_before_evidence_integration(report_dir)
    root = Path(__file__).resolve().parent.parent
    quality_path = root / "data" / "processed" / "quality_report.json"
    schema_path = root / "data" / "processed" / "schema_validation_report.json"

    if not quality_path.exists() or not schema_path.exists():
        return report

    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    def pct(numerator, denominator):
        if not isinstance(numerator, (int, float)) or isinstance(numerator, bool):
            return None
        if not isinstance(denominator, (int, float)) or isinstance(denominator, bool) or denominator <= 0:
            return None
        return round(100.0 * numerator / denominator, 2)

    raw = quality.get("raw_transaction_rows")
    deduped = quality.get("deduplicated_transaction_rows")
    valid = quality.get("valid_transactions")

    missing_required = 0
    for item in schema.get("files", {}).values():
        validation = item.get("value_validation", {})
        reasons = validation.get("issue_counts_by_reason", {})
        count = reasons.get("MISSING_REQUIRED_VALUE", 0)
        if isinstance(count, (int, float)) and not isinstance(count, bool):
            missing_required += count

    checks = quality.get("checks", {})
    required_checks = (
        "reconciliation",
        "no_duplicate_valid_transaction_ids",
        "valid_transactions_have_ids",
        "valid_transactions_have_positive_amounts",
        "valid_transactions_reference_known_customers",
    )
    observed = [
        checks[name]["passed"]
        for name in required_checks
        if isinstance(checks.get(name), dict)
        and isinstance(checks[name].get("passed"), bool)
    ]
    consistency = (
        100.0 if len(observed) == len(required_checks) and all(observed)
        else 0.0 if observed
        else None
    )

    calculated = {
        "completeness": (
            pct(max(0, raw - missing_required), raw)
            if isinstance(raw, (int, float)) and not isinstance(raw, bool)
            else None
        ),
        "validity": pct(valid, deduped),
        "consistency": consistency,
        "overall_quality": pct(valid, raw),
    }

    # Retain the established structure and annotate only metrics with valid evidence.
    metrics = report.setdefault("metrics", {})
    for name, score in calculated.items():
        if score is None:
            continue
        existing = metrics.setdefault(name, {})
        if not isinstance(existing, dict):
            existing = {"previous_value": existing}
            metrics[name] = existing
        existing["score"] = score
        existing["evidence_source"] = (
            "quality_report.json + schema_validation_report.json"
            if name == "completeness"
            else "quality_report.json"
        )
        existing["formula"] = {
            "completeness": "100 * (raw rows - recorded missing required values) / raw rows",
            "validity": "100 * valid transactions / deduplicated transaction rows",
            "consistency": "100 only when all five recorded reconciliation/integrity checks pass",
            "overall_quality": "100 * valid transactions / raw transaction rows",
        }[name]

    report["dimensions"] = {
        name: {
            "score_percent": score,
            "evidence_available": score is not None,
            "source": (
                "quality_report.json + schema_validation_report.json"
                if name == "completeness" else "quality_report.json"
            ),
        }
        for name, score in calculated.items()
    }
    available_scores = [score for score in calculated.values() if score is not None]
    report["scored_dimensions"] = len(available_scores)
    report["total_dimensions"] = len(calculated)
    report["overall_score_percent"] = (
        round(sum(available_scores) / len(available_scores), 2)
        if available_scores else None
    )
    report["coverage_status"] = (
        "COMPLETE" if len(available_scores) == len(calculated)
        else "PARTIAL" if available_scores else "INSUFFICIENT_EVIDENCE"
    )
    report["status"] = report["coverage_status"]
    report["limitations"] = list(report.get("limitations", [])) + [
        "Calculated completeness counts explicitly recorded missing-required-value issues only.",
        "Validity and overall quality use row-count ratios, not a domain-specific risk score.",
        "Consistency reflects the five recorded checks only.",
    ]
    return report

