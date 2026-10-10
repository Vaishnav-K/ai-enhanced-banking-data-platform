import unittest
import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed" / "transactions_incremental.parquet"
QUARANTINE = ROOT / "data" / "quarantine" / "transactions_incremental.parquet"


class TestIncrementalPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.con = duckdb.connect()

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def test_valid_incremental_count(self):
        count = self.con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(PROCESSED)]
        ).fetchone()[0]
        self.assertEqual(count, 2)

    def test_quarantine_count(self):
        count = self.con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(QUARANTINE)]
        ).fetchone()[0]
        self.assertEqual(count, 2)

    def test_invalid_amount_is_quarantined(self):
        count = self.con.execute("""
            SELECT COUNT(*) FROM read_parquet(?)
            WHERE rejection_reason = 'INVALID_AMOUNT'
        """, [str(QUARANTINE)]).fetchone()[0]
        self.assertEqual(count, 1)

    def test_unknown_customer_is_quarantined(self):
        count = self.con.execute("""
            SELECT COUNT(*) FROM read_parquet(?)
            WHERE rejection_reason = 'UNKNOWN_CUSTOMER_ID'
        """, [str(QUARANTINE)]).fetchone()[0]
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)