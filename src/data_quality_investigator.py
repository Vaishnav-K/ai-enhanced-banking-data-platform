"""Build an evidence-based investigation report from existing pipeline outputs."""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUALITY_REPORT = ROOT / "data" / "processed" / "quality_report.json"
ANOMALIES_REPORT = ROOT / "data" / "processed" / "transaction_anomalies.json"
OUTPUT = ROOT / "data" / "processed" / "quality_investigation_report.json"


def load_json(path):
    """Load a JSON artifact and provide a useful error on invalid input."""
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            return json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read {path.name}: {exc}") from exc


def investigate(quality, anomalies):
    """Translate recorded quality evidence into actionable findings."""
    findings = []

    def add_finding(finding_id, severity, category, title, evidence, actions):
        findings.append({
            "finding_id": finding_id,
            "severity": severity,
            "category": category,
            "title": title,
            "evidence": evidence,
            "recommended_actions": actions,
        })

    checks = quality.get("checks", {})
    if not isinstance(checks, dict):
        checks = {}

    for name, check in checks.items():
        if not isinstance(check, dict) or check.get("passed") is not False:
            continue
        add_finding(
            f"QUALITY_{name.upper()}",
            "HIGH",
            "QUALITY_GATE",
            f"Quality check failed: {name}",
            check.get("details", {}),
            [
                "Inspect the failed check's evidence and affected records.",
                "Correct the upstream data or transformation responsible.",
                "Rerun the pipeline and verify that the check passes.",
            ],
        )

    source = quality.get("source_validation", {})
    if not isinstance(source, dict):
        source = {}

    source_status = source.get("status", "UNKNOWN")
    if source_status in {"FAIL", "UNAVAILABLE", "UNKNOWN"}:
        add_finding(
            "SOURCE_VALIDATION_BLOCKED",
            "HIGH",
            "SOURCE_VALIDATION",
            "Source validation is not healthy",
            {
                "status": source_status,
                "error": source.get("error"),
                "configuration_version": source.get("configuration_version"),
            },
            [
                "Inspect the source validation report and its file-level results.",
                "Resolve schema, configuration, or report-generation problems.",
                "Rerun source validation before trusting downstream results.",
            ],
        )
    elif source_status == "WARN":
        add_finding(
            "SOURCE_VALIDATION_WARNINGS",
            "MEDIUM",
            "SOURCE_VALIDATION",
            "Source validation reported warnings",
            {"status": source_status, "files": source.get("files", {})},
            [
                "Review the source report's file-level warnings.",
                "Determine whether the warnings require upstream remediation.",
            ],
        )

    reasons = quality.get("quarantine_reasons", {})
    if isinstance(reasons, dict):
        for reason, count in sorted(reasons.items()):
            if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
                continue
            add_finding(
                f"QUARANTINE_{str(reason).upper()}",
                "MEDIUM",
                "QUARANTINE",
                f"{count} record(s) quarantined: {reason}",
                {"rejection_reason": reason, "record_count": count},
                [
                    "Inspect quarantined records associated with this reason.",
                    "Correct the source data or mapping if the rejection is unexpected.",
                    "Replay corrected records through the validated ingestion path.",
                ],
            )

    if not isinstance(anomalies, list):
        raise ValueError("Transaction anomaly report must contain a JSON list.")

    anomaly_counts = Counter()
    for anomaly in anomalies:
        if not isinstance(anomaly, dict):
            continue
        reason = anomaly.get("anomaly_reason", "UNCLASSIFIED")
        anomaly_counts[str(reason)] += 1

    for reason, count in sorted(anomaly_counts.items()):
        examples = [
            {
                "transaction_id": item.get("transaction_id"),
                "customer_id": item.get("customer_id"),
                "amount": item.get("amount"),
                "timestamp": item.get("timestamp"),
            }
            for item in anomalies
            if isinstance(item, dict)
            and str(item.get("anomaly_reason", "UNCLASSIFIED")) == reason
        ][:5]

        actions = [
            "Review the sample transactions and compare them with the applicable business rules.",
            "Check source records and transaction context before taking action.",
        ]
        if reason == "HIGH_VALUE_TRANSACTION":
            actions.append(
                "Confirm whether the amount is expected for this transaction type and customer."
            )
        elif reason == "NON_POSITIVE_AMOUNT":
            actions.append(
                "Inspect amount parsing, source values, and transaction correction rules."
            )

        add_finding(
            f"ANOMALY_{reason.upper()}",
            "MEDIUM",
            "TRANSACTION_ANOMALY",
            f"{count} transaction anomaly record(s): {reason}",
            {"anomaly_reason": reason, "record_count": count, "examples": examples},
            actions,
        )

    quality_status = quality.get("status", "UNKNOWN")
    status = (
        "REVIEW_REQUIRED"
        if findings or quality_status != "PASS"
        else "PASS"
    )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "investigation_method": "DETERMINISTIC_RULES",
        "source_reports": {
            "quality_report": str(QUALITY_REPORT.relative_to(ROOT)),
            "anomaly_report": str(ANOMALIES_REPORT.relative_to(ROOT)),
        },
        "summary": {
            "finding_count": len(findings),
            "high_findings": sum(f["severity"] == "HIGH" for f in findings),
            "medium_findings": sum(f["severity"] == "MEDIUM" for f in findings),
            "quality_status": quality_status,
            "anomaly_records_reviewed": len(anomalies),
        },
        "findings": findings,
        "limitations": [
            "Findings are rule-based investigation guidance, not proof of root cause.",
            "Transaction anomalies are not automatically evidence of fraud.",
            "Only evidence present in the source reports is used.",
        ],
    }


def main():
    quality = load_json(QUALITY_REPORT)
    anomalies = load_json(ANOMALIES_REPORT)
    if not isinstance(quality, dict):
        raise ValueError("Quality report must be a JSON object.")

    report = investigate(quality, anomalies)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))
    print(f"\nInvestigation report saved to: {OUTPUT}")
    print(f"INVESTIGATION STATUS: {report['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
