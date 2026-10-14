import csv
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "spark_processed"


def read_csv(relative_path):
    path = OUTPUT / relative_path / "data.csv"
    if not path.is_file():
        raise AssertionError(f"Missing Spark output: {path}")

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return reader.fieldnames, rows


class TestSparkOutputContract(unittest.TestCase):
    def test_valid_transactions(self):
        columns, rows = read_csv("valid_transactions")
        self.assertEqual(len(rows), 10000)
        self.assertTrue({
            "transaction_id", "customer_id", "amount", "status"
        }.issubset(columns))

    def test_quarantined_transactions(self):
        columns, rows = read_csv("quarantined_transactions")
        self.assertEqual(len(rows), 3)
        self.assertIn("quarantine_reason", columns)

        reasons = {}
        for row in rows:
            reason = row["quarantine_reason"]
            reasons[reason] = reasons.get(reason, 0) + 1

        self.assertEqual(
            reasons,
            {
                "INVALID_AMOUNT": 1,
                "MISSING_CUSTOMER_ID": 1,
                "UNKNOWN_CUSTOMER_ID": 1,
            },
        )

    def test_analytics_output(self):
        columns, rows = read_csv("transaction_analytics")
        self.assertTrue({
            "transaction_type",
            "status",
            "transaction_count",
            "total_amount",
            "average_amount",
        }.issubset(columns))
        self.assertEqual(len(rows), 12)
        self.assertEqual(
            sum(int(row["transaction_count"]) for row in rows),
            10000,
        )


if __name__ == "__main__":
    unittest.main()
