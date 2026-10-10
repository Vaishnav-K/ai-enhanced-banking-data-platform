
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine"


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    customers_path = (RAW_DIR / "customers.csv").as_posix()
    transactions_path = (RAW_DIR / "transactions.csv").as_posix()

    con.execute(f"""
        CREATE OR REPLACE TABLE customers AS
        SELECT
            customer_id::VARCHAR AS customer_id,
            customer_name::VARCHAR AS customer_name,
            account_type::VARCHAR AS account_type,
            TRY_CAST(registration_date AS DATE) AS registration_date
        FROM read_csv('{customers_path}', header = true, all_varchar = true)
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE raw_transactions AS
        SELECT
            transaction_id::VARCHAR AS transaction_id,
            customer_id::VARCHAR AS customer_id,
            transaction_type::VARCHAR AS transaction_type,
            TRY_CAST(amount AS DECIMAL(18, 2)) AS amount,
            TRY_CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
            status::VARCHAR AS status
        FROM read_csv('{transactions_path}', header = true, all_varchar = true)
    """)

    raw_count = con.execute(
        "SELECT COUNT(*) FROM raw_transactions"
    ).fetchone()[0]

    # Keep the first occurrence of each transaction ID.
    con.execute("""
        CREATE OR REPLACE TABLE transactions AS
        SELECT * EXCLUDE (row_num)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY transaction_id
                       ORDER BY transaction_id
                   ) AS row_num
            FROM raw_transactions
        )
        WHERE row_num = 1
    """)

    deduplicated_count = con.execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0]

    # Apply transaction-level quality rules.
    con.execute("""
        CREATE OR REPLACE TABLE classified AS
        SELECT *,
            CONCAT_WS('; ',
                CASE WHEN transaction_id IS NULL OR TRIM(transaction_id) = ''
                     THEN 'MISSING_TRANSACTION_ID' END,
                CASE WHEN customer_id IS NULL OR TRIM(customer_id) = ''
                     THEN 'MISSING_CUSTOMER_ID' END,
                CASE WHEN transaction_type IS NULL OR transaction_type NOT IN
                     ('CREDIT', 'DEBIT', 'TRANSFER', 'PAYMENT')
                     THEN 'INVALID_TRANSACTION_TYPE' END,
                CASE WHEN amount IS NULL OR amount <= 0
                     THEN 'INVALID_AMOUNT' END,
                CASE WHEN transaction_timestamp IS NULL
                     THEN 'INVALID_TIMESTAMP' END,
                CASE WHEN status IS NULL OR status NOT IN
                     ('SUCCESS', 'FAILED', 'PENDING')
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
        SELECT t.* EXCLUDE (rejection_reason),
       'UNKNOWN_CUSTOMER_ID' AS rejection_reason
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
            transaction_id,
            customer_id,
            transaction_type,
            amount,
            transaction_timestamp,
            status,
            rejection_reason
        FROM invalid_transactions

        UNION ALL

        SELECT
            transaction_id,
            customer_id,
            transaction_type,
            amount,
            transaction_timestamp,
            status,
            rejection_reason
        FROM unknown_customer_transactions
    """)

    # Write Parquet datasets.
    con.execute(f"""
        COPY customers TO '{(PROCESSED_DIR / "customers.parquet").as_posix()}'
        (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY valid_transactions TO
        '{(PROCESSED_DIR / "transactions.parquet").as_posix()}'
        (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY quarantine TO
        '{(QUARANTINE_DIR / "transactions.parquet").as_posix()}'
        (FORMAT PARQUET)
    """)

    customer_count = con.execute(
        "SELECT COUNT(*) FROM customers"
    ).fetchone()[0]
    valid_count = con.execute(
        "SELECT COUNT(*) FROM valid_transactions"
    ).fetchone()[0]
    invalid_count = con.execute(
        "SELECT COUNT(*) FROM invalid_transactions"
    ).fetchone()[0]
    unknown_count = con.execute(
        "SELECT COUNT(*) FROM unknown_customer_transactions"
    ).fetchone()[0]
    quarantine_count = con.execute(
        "SELECT COUNT(*) FROM quarantine"
    ).fetchone()[0]

    print("\n===== BANKING DATA PLATFORM: ETL SUMMARY =====")
    print(f"Customers loaded:             {customer_count:,}")
    print(f"Raw transaction rows:         {raw_count:,}")
    print(f"After deduplication:          {deduplicated_count:,}")
    print(f"Valid transactions:           {valid_count:,}")
    print(f"Invalid transactions:         {invalid_count:,}")
    print(f"Unknown-customer records:     {unknown_count:,}")
    print(f"Total quarantined records:    {quarantine_count:,}")
    print(
        "Reconciliation:              "
        f"{deduplicated_count == valid_count + quarantine_count}"
    )
    print(f"Processed output:             {PROCESSED_DIR}")
    print(f"Quarantine output:             {QUARANTINE_DIR}")

    con.close()


if __name__ == "__main__":
    main()
