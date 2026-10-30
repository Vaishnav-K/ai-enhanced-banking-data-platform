"""Run an isolated, measurable Spark benchmark on a scale dataset.

Default input:  data/scale/raw
Default output: data/scale/spark_processed
Default report: reports/scale-benchmark-results.json

Example:
  python src/scale_spark_benchmark.py --input-dir data/scale/raw --output-dir data/scale/spark_processed --report reports/scale-benchmark-results.json
"""
import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
)

from transaction_rules import (
    TRANSACTION_TYPES, TRANSACTION_STATUSES, DEDUPLICATION_COLUMNS,
)

ROOT = Path(__file__).resolve().parent.parent


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=ROOT / "data" / "scale" / "raw")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data" / "scale" / "spark_processed")
    parser.add_argument("--report", type=Path, default=ROOT / "reports" / "scale-benchmark-results.json")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--overwrite", action="store_true", help="Allow replacing this benchmark output directory.")
    return parser.parse_args()


def write_dataframe_csv(dataframe, output_file):
    """Stream Spark rows to one CSV file without Hadoop's Windows output committer."""
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=dataframe.columns, extrasaction="ignore")
        writer.writeheader()
        for row in dataframe.toLocalIterator():
            writer.writerow(row.asDict(recursive=True))
    return output_file


def file_bytes(path):
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main():
    args = parse_args()
    input_dir = args.input_dir if args.input_dir.is_absolute() else (ROOT / args.input_dir).resolve()
    output_dir = args.output_dir if args.output_dir.is_absolute() else (ROOT / args.output_dir).resolve()
    report_path = args.report if args.report.is_absolute() else (ROOT / args.report).resolve()

    customers_path = input_dir / "customers.csv"
    transactions_path = input_dir / "transactions.csv"
    for path in (customers_path, transactions_path):
        if not path.is_file():
            raise FileNotFoundError(f"Required input file not found: {path}")

    valid_output = output_dir / "valid_transactions.csv"
    quarantine_output = output_dir / "quarantined_transactions.csv"
    analytics_output = output_dir / "transaction_analytics.csv"
    if output_dir.exists() and any(output_dir.iterdir()) and not args.overwrite:
        raise FileExistsError(
            f"Output directory is not empty: {output_dir}. Choose a new output path "
            "or rerun with --overwrite to replace only this scale output."
        )
    if args.shuffle_partitions < 1:
        raise ValueError("--shuffle-partitions must be at least 1")

    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("registration_date", StringType(), True),
    ])
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("status", StringType(), True),
    ])

    run_started = datetime.now(timezone.utc).isoformat()
    timer = time.perf_counter()
    spark = (
        SparkSession.builder.appName("BankingPlatform10MScaleBenchmark")
        .master(args.master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Spark version: {spark.version}")
        print(f"Master: {spark.sparkContext.master}")
        print(f"Input directory: {input_dir}")
        print(f"Output directory: {output_dir}")
        print("Reading scale dataset...")

        customers = (
            spark.read.option("header", True).schema(customer_schema).csv(str(customers_path))
            .select(
                F.when(F.trim("customer_id") == "", F.lit(None))
                 .otherwise(F.trim("customer_id")).alias("customer_id"),
                F.trim("customer_name").alias("customer_name"),
                F.upper(F.trim("account_type")).alias("account_type"),
                F.to_date("registration_date", "yyyy-MM-dd").alias("registration_date"),
            )
        )
        transactions_raw = (
            spark.read.option("header", True).schema(transaction_schema).csv(str(transactions_path))
            .select(
                F.when(F.trim("transaction_id") == "", F.lit(None))
                 .otherwise(F.trim("transaction_id")).alias("transaction_id"),
                F.trim("customer_id").alias("customer_id"),
                F.upper(F.trim("transaction_type")).alias("transaction_type"),
                F.expr("try_cast(amount AS DECIMAL(18,2))").alias("amount"),
                F.to_timestamp("transaction_timestamp", "yyyy-MM-dd HH:mm:ss").alias("transaction_timestamp"),
                F.upper(F.trim("status")).alias("status"),
            )
        )

        customer_count = customers.count()
        raw_count = transactions_raw.count()
        dedupe_window = (
            Window.partitionBy("transaction_id")
            .orderBy(*[
                F.col(column).asc_nulls_first()
                for column in DEDUPLICATION_COLUMNS
            ])
        )
        ranked_transactions = transactions_raw.withColumn(
            "_dedupe_rank", F.row_number().over(dedupe_window)
        )
        transactions = (
            ranked_transactions.filter(F.col("_dedupe_rank") == 1)
            .drop("_dedupe_rank")
        )
        deduplicated_count = transactions.count()
        duplicate_count = ranked_transactions.filter(
            F.col("_dedupe_rank") > 1
        ).count()

        customer_keys = customers.select("customer_id").distinct().withColumn("customer_exists", F.lit(True))
        field_reasons = F.concat_ws(
            "; ",
            F.when(F.col("transaction_id").isNull(), "MISSING_TRANSACTION_ID"),
            F.when(F.col("customer_id").isNull(), "MISSING_CUSTOMER_ID"),
            F.when(
                F.col("transaction_type").isNull()
                | (~F.col("transaction_type").isin(*TRANSACTION_TYPES)),
                "INVALID_TRANSACTION_TYPE",
            ),
            F.when(
                F.col("amount").isNull() | (F.col("amount") <= 0),
                "INVALID_AMOUNT",
            ),
            F.when(F.col("transaction_timestamp").isNull(), "INVALID_TIMESTAMP"),
            F.when(
                F.col("status").isNull()
                | (~F.col("status").isin(*TRANSACTION_STATUSES)),
                "INVALID_STATUS",
            ),
        )
        checked = (
            transactions.join(customer_keys, on="customer_id", how="left")
            .withColumn("_field_rejection_reason", field_reasons)
            .withColumn(
                "quarantine_reason",
                F.when(
                    F.col("_field_rejection_reason") != "",
                    F.col("_field_rejection_reason"),
                ).when(F.col("customer_exists").isNull(), "UNKNOWN_CUSTOMER_ID"),
            )
            .drop("_field_rejection_reason")
            .persist(StorageLevel.DISK_ONLY)
        )

        valid = checked.filter(F.col("quarantine_reason").isNull()).drop("customer_exists", "quarantine_reason")
        quarantined = checked.filter(F.col("quarantine_reason").isNotNull()).drop("customer_exists")

        valid_count = valid.count()
        quarantine_count = quarantined.count()
        quarantine_reasons = {
            row["quarantine_reason"]: row["count"]
            for row in quarantined.groupBy("quarantine_reason").count().collect()
        }

        print("Streaming Spark outputs to CSV (Windows-safe output path)...")
        write_dataframe_csv(valid, valid_output)
        write_dataframe_csv(quarantined, quarantine_output)

        analytics = (
            valid.groupBy("transaction_type", "status")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.round(F.avg("amount"), 2).alias("average_amount"),
            )
            .orderBy("transaction_type", "status")
        )
        write_dataframe_csv(analytics, analytics_output)
        analytics_rows = [
            {
                "transaction_type": row["transaction_type"],
                "status": row["status"],
                "transaction_count": row["transaction_count"],
                "total_amount": str(row["total_amount"]) if row["total_amount"] is not None else None,
                "average_amount": str(row["average_amount"]) if row["average_amount"] is not None else None,
            }
            for row in analytics.collect()
        ]

        elapsed = round(time.perf_counter() - timer, 3)
        reconciliation_passed = (
            raw_count == deduplicated_count + duplicate_count
            and deduplicated_count == valid_count + quarantine_count
        )
        report = {
            "benchmark_name": "banking-platform-spark-scale",
            "run_started_at_utc": run_started,
            "run_finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "PASS" if reconciliation_passed else "FAIL",
            "spark_version": spark.version,
            "master": spark.sparkContext.master,
            "shuffle_partitions": args.shuffle_partitions,
            "input_directory": str(input_dir),
            "output_directory": str(output_dir),
            "input_sizes_bytes": {
                "customers_csv": customers_path.stat().st_size,
                "transactions_csv": transactions_path.stat().st_size,
            },
            "counts": {
                "customers": customer_count,
                "raw_transactions": raw_count,
                "deduplicated_transactions": deduplicated_count,
                "duplicate_transaction_rows_removed": duplicate_count,
                "valid_transactions": valid_count,
                "quarantined_transactions": quarantine_count,
            },
            "quarantine_reasons": quarantine_reasons,
            "reconciliation": {
                "passed": reconciliation_passed,
                "raw_equals_deduplicated_plus_duplicates": raw_count == deduplicated_count + duplicate_count,
                "deduplicated_equals_valid_plus_quarantined": deduplicated_count == valid_count + quarantine_count,
            },
            "analytics": analytics_rows,
            "output_sizes_bytes": {
                "valid_transactions_csv": file_bytes(valid_output),
                "quarantined_transactions_csv": file_bytes(quarantine_output),
                "transaction_analytics_csv": file_bytes(analytics_output),
            },
            "elapsed_seconds": elapsed,
            "rows_per_second_raw_transactions": round(raw_count / elapsed, 2) if elapsed else None,
            "notes": [
                "Synthetic benchmark dataset; not production workload evidence.",
                "Elapsed time includes Spark session startup, reads, transformations, writes, and analytics.",
                "Input and output paths are recorded for reproducibility.",
            ],
        }
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print("\n=== SCALE BENCHMARK RESULTS ===")
        print(json.dumps({
            "status": report["status"],
            "spark_version": report["spark_version"],
            "counts": report["counts"],
            "reconciliation": report["reconciliation"],
            "elapsed_seconds": report["elapsed_seconds"],
            "rows_per_second_raw_transactions": report["rows_per_second_raw_transactions"],
            "output_sizes_bytes": report["output_sizes_bytes"],
            "report": str(report_path),
        }, indent=2))

        if not reconciliation_passed:
            raise RuntimeError("Reconciliation failed; inspect the report.")
    finally:
        try:
            checked.unpersist()
        except (NameError, Exception):
            pass
        spark.stop()


if __name__ == "__main__":
    main()
