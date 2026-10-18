from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent
TRANSACTIONS = ROOT / "data" / "processed" / "transactions.parquet"
OUTPUT = ROOT / "data" / "processed"

con = duckdb.connect()

try:
    queries = {
        "daily_transaction_summary": """
            SELECT
                CAST(transaction_timestamp AS DATE) AS transaction_date,
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_count,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
                SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
            FROM read_parquet(?)
            GROUP BY 1
            ORDER BY 1
        """,
        "transaction_status_summary": """
            SELECT
                status,
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
            FROM read_parquet(?)
            GROUP BY status
            ORDER BY status
        """,
        "customer_transaction_summary": """
            SELECT
                customer_id,
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount,
                AVG(amount) AS average_amount
            FROM read_parquet(?)
            GROUP BY customer_id
            ORDER BY total_amount DESC
        """
    }

    for name, query in queries.items():
        destination = OUTPUT / f"{name}.csv"
        result = con.execute(query, [str(TRANSACTIONS)])
        columns = [description[0] for description in result.description]
        rows = result.fetchall()

        with destination.open("w", encoding="utf-8-sig", newline="") as file:
            import csv
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(rows)

        print(f"{name}: exported {len(rows)} rows -> {destination}")

finally:
    con.close()
