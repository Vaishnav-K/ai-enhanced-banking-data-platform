"""Adversarial fixtures for DuckDB/Spark rule parity and row-level comparison."""
import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from etl_pipeline import run_etl
from compare_engines import reconcile

HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None


class CrossEngineFixture:
    @staticmethod
    def create(root):
        input_dir = root / "input"
        input_dir.mkdir(parents=True)
        with (input_dir / "customers.csv").open(
            "w", newline="", encoding="utf-8"
        ) as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "customer_id", "customer_name", "account_type", "registration_date"
            ])
            writer.writerows([
                ["C001", "Customer One", "SAVINGS", "2025-01-01"],
                ["C002", "Customer Two", "CURRENT", "2025-02-01"],
            ])

        with (input_dir / "transactions.csv").open(
            "w", newline="", encoding="utf-8"
        ) as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "transaction_id", "customer_id", "transaction_type",
                "amount", "transaction_timestamp", "status",
            ])
            writer.writerows([
                ["T001", "C001", " credit ", "10.00", "2026-10-01 10:00:00", " success "],
                ["T002", "C001", "WIRE", "25.00", "2026-10-01 10:05:00", "SUCCESS"],
                ["", "C001", "CREDIT", "not-money", "not-a-timestamp", "BAD"],
                ["T004", "C999", "TRANSFER", "50.00", "2026-10-01 10:10:00", "SUCCESS"],
                ["T005", "C001", "CREDIT", "20.00", "2026-10-01 10:15:00", "SUCCESS"],
                ["T005", "C001", "CREDIT", "10.00", "2026-10-01 10:15:00", "SUCCESS"],
                ["T006", "", "TRANSFER", "-5.00", "2026-10-01 10:20:00", "SUCCESS"],
                ["T007", "C002", "PAYMENT", "15.00", "2026-10-01 10:25:00", "UNKNOWN"],
            ])
        return input_dir


class TestDuckDbRuleCoverage(unittest.TestCase):
    def test_fixture_covers_compound_errors_and_deterministic_deduplication(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            input_dir = CrossEngineFixture.create(root)
            processed_dir = root / "duckdb_processed"
            quarantine_dir = root / "duckdb_quarantine"

            summary = run_etl(input_dir, processed_dir, quarantine_dir)

            self.assertEqual(summary["raw_transactions"], 8)
            self.assertEqual(summary["deduplicated_transactions"], 7)
            self.assertEqual(summary["duplicate_rows_removed"], 1)
            self.assertEqual(summary["valid_transactions"], 2)
            self.assertEqual(summary["quarantined_transactions"], 5)
            self.assertTrue(summary["reconciliation_passed"])

            import duckdb
            with duckdb.connect() as con:
                chosen_amount = con.execute(
                    "SELECT amount FROM read_parquet(?) WHERE transaction_id='T005'",
                    [str(processed_dir / "transactions.parquet")],
                ).fetchone()[0]
                missing_id_reason = con.execute(
                    "SELECT rejection_reason FROM read_parquet(?) "
                    "WHERE transaction_id IS NULL",
                    [str(quarantine_dir / "transactions.parquet")],
                ).fetchone()[0]

            self.assertEqual(str(chosen_amount), "10.00")
            self.assertEqual(
                missing_id_reason,
                "MISSING_TRANSACTION_ID; INVALID_AMOUNT; INVALID_TIMESTAMP; INVALID_STATUS",
            )


@unittest.skipUnless(HAS_PYSPARK, "PySpark is not installed in this environment.")
class TestSparkDuckDbParity(unittest.TestCase):
    def test_adversarial_fixture_matches_at_full_row_level(self):
        from spark_pipeline import run_spark

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            input_dir = CrossEngineFixture.create(root)
            processed_dir = root / "duckdb_processed"
            quarantine_dir = root / "duckdb_quarantine"
            spark_dir = root / "spark_output"
            report_path = root / "reconciliation.json"

            duck_summary = run_etl(input_dir, processed_dir, quarantine_dir)
            spark_summary = run_spark(
                input_dir, spark_dir, master="local[2]", shuffle_partitions=2
            )
            report = reconcile(
                spark_dir / "valid_transactions" / "data.csv",
                spark_dir / "quarantined_transactions" / "data.csv",
                spark_dir / "transaction_analytics" / "data.csv",
                processed_dir / "transactions.parquet",
                quarantine_dir / "transactions.parquet",
                report_path,
            )

            self.assertTrue(duck_summary["reconciliation_passed"])
            self.assertTrue(spark_summary["reconciliation_passed"])
            self.assertEqual(report["status"], "PASS", report["details"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
