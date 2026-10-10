import unittest
import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
QUARANTINE = ROOT / "data" / "quarantine"


class TestBankingPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.con = duckdb.connect()

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def count_rows(self, path):
        return self.con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(path)]
        ).fetchone()[0]

    def test_customer_output_count(self):
        self.assertEqual(
            self.count_rows(PROCESSED / "customers.parquet"), 500
        )

    def test_valid_transaction_count(self):
        self.assertEqual(
            self.count_rows(PROCESSED / "transactions.parquet"), 10000
        )

    def test_quarantine_count(self):
        self.assertEqual(
            self.count_rows(QUARANTINE / "transactions.parquet"), 3
        )

    def test_quarantine_reasons_are_populated(self):
        empty_reasons = self.con.execute("""
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE rejection_reason IS NULL OR TRIM(rejection_reason) = ''
        """, [str(QUARANTINE / "transactions.parquet")]).fetchone()[0]

        self.assertEqual(empty_reasons, 0)

    def test_unknown_customer_is_quarantined(self):
        count = self.con.execute("""
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE customer_id = 'C99999'
              AND rejection_reason = 'UNKNOWN_CUSTOMER_ID'
        """, [str(QUARANTINE / "transactions.parquet")]).fetchone()[0]

        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)