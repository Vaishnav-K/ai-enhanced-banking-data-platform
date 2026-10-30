"""DuckDB batch ETL for the synthetic banking data platform.

Can be run against the baseline mock or an isolated fixture directory:
    python src/etl_pipeline.py
    python src/etl_pipeline.py --input-dir data/scale/raw
"""
import argparse
from pathlib import Path

import duckdb

from transaction_rules import TRANSACTION_TYPES, TRANSACTION_STATUSES, DEDUPLICATION_COLUMNS

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine"


def sql_literal(value):
    """Return a safely quoted SQL string literal for a filesystem path."""
    return "'" + str(value).replace("'", "''") + "'"


def run_etl(input_dir=RAW_DIR, processed_dir=PROCESSED_DIR,
            quarantine_dir=QUARANTINE_DIR):
    input_dir = Path(input_dir).resolve()
    processed_dir = Path(processed_dir).resolve()
    quarantine_dir = Path(quarantine_dir).resolve()
    customers_path = input_dir / "customers.csv"
    transactions_path = input_dir / "transactions.csv"
    for path in (customers_path, transactions_path):
        if not path.is_file():
            raise FileNotFoundError(f"Required input file not found: {path}")

    processed_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE customers AS
            SELECT
                NULLIF(TRIM(customer_id), '') AS customer_id,
                TRIM(customer_name) AS customer_name,
                UPPER(TRIM(account_type)) AS account_type,
                TRY_CAST(registration_date AS DATE) AS registration_date
            FROM read_csv({sql_literal(customers_path.as_posix())},
                          header=true, all_varchar=true)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE raw_transactions AS
            SELECT
                NULLIF(TRIM(transaction_id), '') AS transaction_id,
                NULLIF(TRIM(customer_id), '') AS customer_id,
                UPPER(TRIM(transaction_type)) AS transaction_type,
                TRY_CAST(amount AS DECIMAL(18, 2)) AS amount,
                TRY_CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
                UPPER(TRIM(status)) AS status,
                ROW_NUMBER() OVER () AS _source_row_id
            FROM read_csv({sql_literal(transactions_path.as_posix())},
                          header=true, all_varchar=true)
        """)

        raw_count = con.execute(
            "SELECT COUNT(*) FROM raw_transactions"
        ).fetchone()[0]

        # Stable winner policy: lexicographically smallest normalized record
        # wins when the same transaction_id appears with conflicting values.
        order_columns = ", ".join(
            f"{column} ASC NULLS FIRST"
            for column in DEDUPLICATION_COLUMNS
        )
        con.execute(f"""
            CREATE OR REPLACE TABLE transactions AS
            SELECT * EXCLUDE (_dedupe_rank, _source_row_id)
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY transaction_id,
                               CASE WHEN transaction_id IS NULL
                                    THEN _source_row_id ELSE 0 END
                           ORDER BY {order_columns}
                       ) AS _dedupe_rank
                FROM raw_transactions
            )
            WHERE _dedupe_rank = 1
        """)
        deduplicated_count = con.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]
        duplicate_count = con.execute("""
            SELECT COALESCE(SUM(group_rows - 1), 0)
            FROM (
                SELECT COUNT(*) AS group_rows
                FROM raw_transactions
                WHERE transaction_id IS NOT NULL
                GROUP BY transaction_id
            )
        """).fetchone()[0]

        allowed_types = ", ".join(sql_literal(v) for v in TRANSACTION_TYPES)
        allowed_statuses = ", ".join(sql_literal(v) for v in TRANSACTION_STATUSES)
        con.execute(f"""
            CREATE OR REPLACE TABLE classified AS
            SELECT *,
                CONCAT_WS('; ',
                    CASE WHEN transaction_id IS NULL
                         THEN 'MISSING_TRANSACTION_ID' END,
                    CASE WHEN customer_id IS NULL
                         THEN 'MISSING_CUSTOMER_ID' END,
                    CASE WHEN transaction_type IS NULL OR transaction_type NOT IN
                         ({allowed_types})
                         THEN 'INVALID_TRANSACTION_TYPE' END,
                    CASE WHEN amount IS NULL OR amount <= 0
                         THEN 'INVALID_AMOUNT' END,
                    CASE WHEN transaction_timestamp IS NULL
                         THEN 'INVALID_TIMESTAMP' END,
                    CASE WHEN status IS NULL OR status NOT IN
                         ({allowed_statuses})
                         THEN 'INVALID_STATUS' END
                ) AS rejection_reason
            FROM transactions
        """)

        con.execute("""
            CREATE OR REPLACE TABLE invalid_transactions AS
            SELECT * FROM classified
            WHERE rejection_reason <> ''
        """)
        con.execute("""
            CREATE OR REPLACE TABLE candidate_transactions AS
            SELECT * FROM classified
            WHERE rejection_reason = ''
        """)
        con.execute("""
            CREATE OR REPLACE TABLE unknown_customer_transactions AS
            SELECT t.*,
                   'UNKNOWN_CUSTOMER_ID' AS full_rejection_reason
            FROM candidate_transactions t
            WHERE NOT EXISTS (
                SELECT 1 FROM customers c
                WHERE c.customer_id = t.customer_id
            )
        """)
        con.execute("""
            CREATE OR REPLACE TABLE valid_transactions AS
            SELECT t.* EXCLUDE (rejection_reason)
            FROM candidate_transactions t
            WHERE EXISTS (
                SELECT 1 FROM customers c
                WHERE c.customer_id = t.customer_id
            )
        """)
        con.execute("""
            CREATE OR REPLACE TABLE quarantine AS
            SELECT
                transaction_id, customer_id, transaction_type, amount,
                transaction_timestamp, status, rejection_reason
            FROM invalid_transactions
            UNION ALL
            SELECT
                transaction_id, customer_id, transaction_type, amount,
                transaction_timestamp, status,
                full_rejection_reason AS rejection_reason
            FROM unknown_customer_transactions
        """)

        customer_count = con.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        valid_count = con.execute("SELECT COUNT(*) FROM valid_transactions").fetchone()[0]
        invalid_count = con.execute("SELECT COUNT(*) FROM invalid_transactions").fetchone()[0]
        unknown_count = con.execute(
            "SELECT COUNT(*) FROM unknown_customer_transactions"
        ).fetchone()[0]
        quarantine_count = con.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]

        reconciliation = (
            raw_count == deduplicated_count + duplicate_count
            and deduplicated_count == valid_count + quarantine_count
        )
        if not reconciliation:
            raise RuntimeError(
                "DuckDB ETL reconciliation failed: raw rows must equal "
                "deduplicated rows plus removed duplicates, and deduplicated "
                "rows must equal valid plus quarantined rows."
            )

        con.execute(
            f"COPY customers TO {sql_literal((processed_dir / 'customers.parquet').as_posix())} "
            "(FORMAT PARQUET)"
        )
        con.execute(
            f"COPY valid_transactions TO "
            f"{sql_literal((processed_dir / 'transactions.parquet').as_posix())} "
            "(FORMAT PARQUET)"
        )
        con.execute(
            f"COPY quarantine TO "
            f"{sql_literal((quarantine_dir / 'transactions.parquet').as_posix())} "
            "(FORMAT PARQUET)"
        )

        summary = {
            "customers": customer_count,
            "raw_transactions": raw_count,
            "deduplicated_transactions": deduplicated_count,
            "duplicate_rows_removed": duplicate_count,
            "valid_transactions": valid_count,
            "invalid_transactions": invalid_count,
            "unknown_customer_transactions": unknown_count,
            "quarantined_transactions": quarantine_count,
            "reconciliation_passed": reconciliation,
        }
    finally:
        con.close()

    print("\n===== BANKING DATA PLATFORM: DUCKDB ETL SUMMARY =====")
    for key, value in summary.items():
        print(f"{key.replace('_', ' ').title():35} {value:,}" if type(value) is int else
              f"{key.replace('_', ' ').title():35} {value}")
    print(f"Processed output: {processed_dir}")
    print(f"Quarantine output: {quarantine_dir}")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--processed-dir", type=Path, default=PROCESSED_DIR)
    parser.add_argument("--quarantine-dir", type=Path, default=QUARANTINE_DIR)
    args = parser.parse_args()
    run_etl(args.input_dir, args.processed_dir, args.quarantine_dir)


if __name__ == "__main__":
    main()
