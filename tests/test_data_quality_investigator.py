import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from data_quality_investigator import investigate


class TestDataQualityInvestigator(unittest.TestCase):
    def test_clean_inputs_return_pass(self):
        report = investigate(
            {
                "status": "PASS",
                "checks": {"reconciliation": {"passed": True, "details": {}}},
                "quarantine_reasons": {},
                "source_validation": {"status": "PASS"},
            },
            [],
        )
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["summary"]["finding_count"], 0)

    def test_failed_check_produces_evidence_and_actions(self):
        report = investigate(
            {
                "status": "FAIL",
                "checks": {
                    "reconciliation": {
                        "passed": False,
                        "details": {"deduplicated": 10, "valid": 8, "quarantined": 1},
                    }
                },
                "quarantine_reasons": {},
                "source_validation": {"status": "PASS"},
            },
            [],
        )
        self.assertEqual(report["status"], "REVIEW_REQUIRED")
        finding = report["findings"][0]
        self.assertEqual(finding["category"], "QUALITY_GATE")
        self.assertEqual(finding["evidence"]["deduplicated"], 10)
        self.assertTrue(finding["recommended_actions"])

    def test_quarantine_reasons_are_summarized(self):
        report = investigate(
            {
                "status": "PASS",
                "checks": {},
                "quarantine_reasons": {"INVALID_AMOUNT": 2},
                "source_validation": {"status": "PASS"},
            },
            [],
        )
        finding = next(f for f in report["findings"] if f["category"] == "QUARANTINE")
        self.assertEqual(finding["evidence"]["record_count"], 2)

    def test_anomaly_is_not_labelled_as_fraud(self):
        report = investigate(
            {
                "status": "PASS",
                "checks": {},
                "quarantine_reasons": {},
                "source_validation": {"status": "PASS"},
            },
            [{
                "transaction_id": "T-1",
                "customer_id": "C-1",
                "amount": 150000.0,
                "anomaly_reason": "HIGH_VALUE_TRANSACTION",
            }],
        )
        finding = next(
            f for f in report["findings"]
            if f["category"] == "TRANSACTION_ANOMALY"
        )
        self.assertIn("HIGH_VALUE_TRANSACTION", finding["title"])
        self.assertNotIn("fraud", finding["title"].lower())

    def test_unhealthy_source_validation_is_reported(self):
        report = investigate(
            {
                "status": "FAIL",
                "checks": {},
                "quarantine_reasons": {},
                "source_validation": {
                    "status": "UNAVAILABLE",
                    "error": "report missing",
                },
            },
            [],
        )
        finding = next(
            f for f in report["findings"]
            if f["category"] == "SOURCE_VALIDATION"
        )
        self.assertEqual(finding["severity"], "HIGH")
        self.assertEqual(finding["evidence"]["error"], "report missing")

    def test_invalid_anomaly_payload_is_rejected(self):
        with self.assertRaises(ValueError):
            investigate(
                {"status": "PASS", "checks": {}, "quarantine_reasons": {}},
                {"unexpected": "object"},
            )


if __name__ == "__main__":
    unittest.main()
