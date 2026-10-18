from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw" / "transactions_incremental.csv"
CUSTOMERS = ROOT / "data" / "processed" / "customers.parquet"
EXISTING = ROOT / "data" / "processed" / "transactions.parquet"
PROCESSED = ROOT / "data" / "processed" / "transactions_incremental.parquet"
QUARANTINE = ROOT / "data" / "quarantine" / "transactions_incremental.parquet"


def sql_path(path):
    return "'" + str(path).replace("\\", "/").replace("'", "''") + "'"


con = duckdb.connect()

try:
    for path in [RAW, CUSTOMERS, EXISTING]:
        if not path.exists():
            raise FileNotFoundError(f"Required input not found: {path}")

    con.execute(f"""
        CREATE VIEW batch AS
        SELECT
            TRIM(transaction_id) AS transaction_id,
            TRIM(customer_id) AS customer_id,
            UPPER(TRIM(transaction_type)) AS transaction_type,
            TRY_CAST(amount AS DECIMAL(18,2)) AS amount,
            TRY_CAST(timestamp AS TIMESTAMP) AS timestamp,
            UPPER(TRIM(status)) AS status
        FROM read_csv(
            {sql_path(RAW)},
            delim = ',',
            header = true,
            all_varchar = true,
            strict_mode = false
        )
    """)

    prior_incremental_sql = (
        f"read_parquet({sql_path(PROCESSED)})"
        if PROCESSED.exists()
        else "(SELECT CAST(NULL AS VARCHAR) AS transaction_id WHERE FALSE)"
    )

    con.execute(f"""
        CREATE VIEW classified AS
        SELECT source.*,
            CASE
                WHEN transaction_id IS NULL OR transaction_id = ''
                    THEN 'MISSING_TRANSACTION_ID'
                WHEN customer_id IS NULL OR customer_id = ''
                    THEN 'MISSING_CUSTOMER_ID'
                WHEN transaction_type NOT IN ('DEBIT', 'CREDIT')
                    THEN 'INVALID_TRANSACTION_TYPE'
                WHEN amount IS NULL
                    THEN 'INVALID_AMOUNT'
                WHEN timestamp IS NULL
                    THEN 'INVALID_TIMESTAMP'
                WHEN status NOT IN ('SUCCESS', 'FAILED', 'PENDING')
                    THEN 'INVALID_STATUS'
                WHEN batch_duplicate > 1
                    THEN 'DUPLICATE_IN_BATCH'
                WHEN existing_id IS NOT NULL
                    THEN 'DUPLICATE_EXISTING_TRANSACTION'
                WHEN prior_incremental_id IS NOT NULL
                    THEN 'DUPLICATE_PRIOR_INCREMENTAL_TRANSACTION'
                WHEN known_customer IS NULL
                    THEN 'UNKNOWN_CUSTOMER_ID'
                ELSE NULL
            END AS rejection_reason
        FROM (
            SELECT
                b.transaction_id,
                b.customer_id,
                b.transaction_type,
                b.amount,
                b.timestamp,
                b.status,
                ROW_NUMBER() OVER (
                    PARTITION BY b.transaction_id
                    ORDER BY b.transaction_id
                ) AS batch_duplicate,
                e.transaction_id AS existing_id,
                p.transaction_id AS prior_incremental_id,
                c.customer_id AS known_customer
            FROM batch b
            LEFT JOIN read_parquet({sql_path(EXISTING)}) e
                ON b.transaction_id = e.transaction_id
            LEFT JOIN {prior_incremental_sql} p
                ON b.transaction_id = p.transaction_id
            LEFT JOIN read_parquet({sql_path(CUSTOMERS)}) c
                ON b.customer_id = c.customer_id
        ) source
    """)

    temp_processed = Path(str(PROCESSED) + ".tmp.parquet")
    try:
        if PROCESSED.exists():
            con.execute(f"""
                COPY (
                    SELECT transaction_id, customer_id, transaction_type,
                           amount, timestamp, status
                    FROM read_parquet({sql_path(PROCESSED)})
                    UNION ALL
                    SELECT transaction_id, customer_id, transaction_type,
                           amount, timestamp, status
                    FROM classified
                    WHERE rejection_reason IS NULL
                ) TO {sql_path(temp_processed)} (FORMAT PARQUET)
            """)
        else:
            con.execute(f"""
                COPY (
                    SELECT transaction_id, customer_id, transaction_type,
                           amount, timestamp, status
                    FROM classified
                    WHERE rejection_reason IS NULL
                ) TO {sql_path(temp_processed)} (FORMAT PARQUET)
            """)
        temp_processed.replace(PROCESSED)
    finally:
        if temp_processed.exists():
            temp_processed.unlink()

    con.execute(f"""
        COPY (
            SELECT * FROM classified
            WHERE rejection_reason IS NOT NULL
        ) TO {sql_path(QUARANTINE)} (FORMAT PARQUET)
    """)

    total = con.execute(
        "SELECT COUNT(*) FROM batch"
    ).fetchone()[0]

    valid = con.execute(
        f"SELECT COUNT(*) FROM read_parquet({sql_path(PROCESSED)})"
    ).fetchone()[0]

    rejected = con.execute(f"""
        SELECT rejection_reason, COUNT(*)
        FROM read_parquet({sql_path(QUARANTINE)})
        GROUP BY rejection_reason
        ORDER BY rejection_reason
    """).fetchall()

    print(f"Incremental rows received: {total}")
    print(f"Valid new transactions: {valid}")
    print(f"Quarantined records: {sum(row[1] for row in rejected)}")
    print("Quarantine breakdown:", rejected)
    print("Incremental processing completed.")

finally:
    con.close()