"""Tests for safe, deterministic generation of the isolated 10M scale mock."""
import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import generate_scale_data


class TestScaleDataGenerator(unittest.TestCase):
    def test_default_mock_size_is_ten_million_transactions(self):
        with patch.object(sys, "argv", ["generate_scale_data.py"]):
            args = generate_scale_data.parse_args()

        self.assertEqual(args.customers, 100_000)
        self.assertEqual(args.transactions, 10_000_000)
        self.assertEqual(args.output_dir, ROOT / "data" / "scale" / "raw")

    def test_generator_streams_requested_small_fixture(self):
        with tempfile.TemporaryDirectory() as temp:
            output_dir = Path(temp) / "mock"
            with patch.object(
                sys,
                "argv",
                [
                    "generate_scale_data.py",
                    "--customers", "3",
                    "--transactions", "7",
                    "--output-dir", str(output_dir),
                    "--seed", "7",
                ],
            ):
                generate_scale_data.main()

            with (output_dir / "customers.csv").open(
                "r", newline="", encoding="utf-8"
            ) as handle:
                customers = list(csv.DictReader(handle))
            with (output_dir / "transactions.csv").open(
                "r", newline="", encoding="utf-8"
            ) as handle:
                transactions = list(csv.DictReader(handle))

            self.assertEqual(len(customers), 3)
            self.assertEqual(len(transactions), 7)
            self.assertEqual(len({row["transaction_id"] for row in transactions}), 7)

    def test_existing_scale_files_are_not_overwritten_by_default(self):
        with tempfile.TemporaryDirectory() as temp:
            output_dir = Path(temp)
            customers_path = output_dir / "customers.csv"
            transactions_path = output_dir / "transactions.csv"
            customers_path.write_text("preserve customer file", encoding="utf-8")
            transactions_path.write_text("preserve transaction file", encoding="utf-8")

            with patch.object(
                sys,
                "argv",
                ["generate_scale_data.py", "--output-dir", str(output_dir)],
            ):
                with self.assertRaises(SystemExit):
                    generate_scale_data.main()

            self.assertEqual(
                customers_path.read_text(encoding="utf-8"),
                "preserve customer file",
            )
            self.assertEqual(
                transactions_path.read_text(encoding="utf-8"),
                "preserve transaction file",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
