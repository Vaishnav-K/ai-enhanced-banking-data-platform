import csv
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "spark_processed"


@unittest.skipUnless(
    (OUTPUT / "valid_transactions" / "data.csv").is_file(),
    "Spark outputs are not available; run the Spark pipeline first.",
)
class TestSparkOutputContract(unittest.TestCase):
    def read_csv(self, relative_path):
        path = OUTPUT / relative_path / "data.csv"
        if not path.is_file():
            self.fail(f"Missing Spark output: {path}")

        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            return reader.fieldnames, list(reader)

    def test_valid_transactions(self):
        columns, rows = self.read_csv("valid_transactions")
        self.assertEqual(len(rows), 10000)
        self.assertTrue({
            "transaction_id", "customer_id", "amount", "status"
        }.issubset(columns))

    def test_quarantined_transactions(self):
        columns, rows = self.read_csv("quarantined_transactions")
        self.assertEqual(len(rows), 3)
        self.assertIn("quarantine_reason", columns)

        reasons = {}
        for row in rows:
            reason = row["quarantine_reason"]
            reasons[reason] = reasons.get(reason, 0) + 1

        self.assertEqual(reasons, {
            "INVALID_AMOUNT": 1,
            "MISSING_CUSTOMER_ID": 1,
            "UNKNOWN_CUSTOMER_ID": 1,
        })

    def test_analytics_output(self):
        columns, rows = self.read_csv("transaction_analytics")
        self.assertTrue({
            "transaction_type", "status", "transaction_count",
            "total_amount", "average_amount",
        }.issubset(columns))
        self.assertEqual(len(rows), 12)
        self.assertEqual(
            sum(int(row["transaction_count"]) for row in rows),
            10000,
        )


if __name__ == "__main__":
    unittest.main()
