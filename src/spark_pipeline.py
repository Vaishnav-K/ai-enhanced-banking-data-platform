"""Spark ETL for the banking platform's cross-engine baseline checks."""
import argparse
import csv
import json
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StructType, StructField, StringType

from transaction_rules import TRANSACTION_TYPES, TRANSACTION_STATUSES, DEDUPLICATION_COLUMNS

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
OUTPUT_ROOT = ROOT / "data" / "spark_processed"

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("registration_date", StringType(), True),
])
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("status", StringType(), True),
])


def write_dataframe_csv(dataframe, output_dir):
    """Stream rows into a single CSV without using Hadoop's Windows committer.

    This is the local cross-engine comparison format. The dedicated scale
    benchmark remains the route for measuring the 10-million-row workload.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "data.csv"
    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=dataframe.columns, extrasaction="ignore"
        )
        writer.writeheader()
        for row in dataframe.toLocalIterator():
            writer.writerow(row.asDict(recursive=True))
    return output_file


def run_spark(input_dir=RAW_DIR, output_root=OUTPUT_ROOT, master="local[2]",
              shuffle_partitions=4):
    input_dir = Path(input_dir).resolve()
    output_root = Path(output_root).resolve()
    customers_path = input_dir / "customers.csv"
    transactions_path = input_dir / "transactions.csv"
    for path in (customers_path, transactions_path):
        if not path.is_file():
            raise FileNotFoundError(f"Required input file not found: {path}")

    if shuffle_partitions < 1:
        raise ValueError("shuffle_partitions must be at least 1")

    spark = (
        SparkSession.builder
        .appName("BankingDataPlatformSpark")
        .master(master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    cached = []
    try:
        print(f"Spark version: {spark.version}")
        print(f"Execution master: {spark.sparkContext.master}")

        customers_raw = (
            spark.read.option("header", True)
            .schema(CUSTOMER_SCHEMA).csv(str(customers_path))
        )
        transactions_raw = (
            spark.read.option("header", True)
            .schema(TRANSACTION_SCHEMA).csv(str(transactions_path))
        )

        customers = customers_raw.select(
            F.when(F.trim("customer_id") == "", F.lit(None))
             .otherwise(F.trim("customer_id")).alias("customer_id"),
            F.trim("customer_name").alias("customer_name"),
            F.upper(F.trim("account_type")).alias("account_type"),
            F.to_date("registration_date", "yyyy-MM-dd").alias("registration_date"),
        ).cache()
        cached.append(customers)

        # Normalize before deduplication so Spark and DuckDB use identical
        # canonical values and the same deterministic survivor ordering.
        transactions_typed = transactions_raw.select(
            F.when(F.trim("transaction_id") == "", F.lit(None))
             .otherwise(F.trim("transaction_id")).alias("transaction_id"),
            F.when(F.trim("customer_id") == "", F.lit(None))
             .otherwise(F.trim("customer_id")).alias("customer_id"),
            F.upper(F.trim("transaction_type")).alias("transaction_type"),
            F.expr("try_cast(amount AS DECIMAL(18,2))").alias("amount"),
            F.to_timestamp(
                "transaction_timestamp", "yyyy-MM-dd HH:mm:ss"
            ).alias("transaction_timestamp"),
            F.upper(F.trim("status")).alias("status"),
        )

        raw_count = transactions_typed.count()
        customer_count = customers.count()

        dedupe_window = (
            Window.partitionBy("transaction_id")
            .orderBy(*[
                F.col(column).asc_nulls_first()
                for column in DEDUPLICATION_COLUMNS
            ])
        )
        transactions = (
            transactions_typed
            .withColumn("_dedupe_rank", F.row_number().over(dedupe_window))
            .filter(F.col("_dedupe_rank") == 1)
            .drop("_dedupe_rank")
            .cache()
        )
        cached.append(transactions)
        deduplicated_count = transactions.count()
        duplicate_count = raw_count - deduplicated_count

        customer_keys = (
            customers.select("customer_id").distinct()
            .withColumn("customer_exists", F.lit(True))
        )
        checked = transactions.join(
            customer_keys, on="customer_id", how="left"
        )

        # CONCAT_WS ignores null reason expressions and keeps all applicable
        # field-level errors in the same order as DuckDB.
        field_reasons = F.concat_ws(
            "; ",
            F.when(
                F.col("transaction_id").isNull(),
                F.lit("MISSING_TRANSACTION_ID"),
            ),
            F.when(
                F.col("customer_id").isNull(),
                F.lit("MISSING_CUSTOMER_ID"),
            ),
            F.when(
                F.col("transaction_type").isNull()
                | (~F.col("transaction_type").isin(*TRANSACTION_TYPES)),
                F.lit("INVALID_TRANSACTION_TYPE"),
            ),
            F.when(
                F.col("amount").isNull() | (F.col("amount") <= F.lit(0)),
                F.lit("INVALID_AMOUNT"),
            ),
            F.when(
                F.col("transaction_timestamp").isNull(),
                F.lit("INVALID_TIMESTAMP"),
            ),
            F.when(
                F.col("status").isNull()
                | (~F.col("status").isin(*TRANSACTION_STATUSES)),
                F.lit("INVALID_STATUS"),
            ),
        )
        checked = (
            checked
            .withColumn("_field_rejection_reason", field_reasons)
            .withColumn(
                "quarantine_reason",
                F.when(
                    F.col("_field_rejection_reason") != "",
                    F.col("_field_rejection_reason"),
                ).when(
                    F.col("customer_exists").isNull(),
                    F.lit("UNKNOWN_CUSTOMER_ID"),
                ),
            )
            .drop("_field_rejection_reason", "customer_exists")
            .cache()
        )
        cached.append(checked)

        valid = checked.filter(F.col("quarantine_reason").isNull()).cache()
        quarantined = checked.filter(
            F.col("quarantine_reason").isNotNull()
        ).cache()
        cached.extend([valid, quarantined])

        valid_count = valid.count()
        quarantine_count = quarantined.count()
        quarantine_breakdown = {
            row["quarantine_reason"]: row["count"]
            for row in quarantined.groupBy("quarantine_reason").count().collect()
        }

        analytics = (
            valid.groupBy("transaction_type", "status")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.round(F.avg("amount"), 2).alias("average_amount"),
            )
            .orderBy("transaction_type", "status")
        )

        raw_reconciliation = raw_count == deduplicated_count + duplicate_count
        quality_reconciliation = deduplicated_count == valid_count + quarantine_count
        reconciliation_passed = raw_reconciliation and quality_reconciliation

        print("\n=== SPARK PIPELINE RECONCILIATION ===")
        print(f"Customers:             {customer_count:,}")
        print(f"Raw transactions:      {raw_count:,}")
        print(f"Deduplicated:          {deduplicated_count:,}")
        print(f"Duplicate rows removed:{duplicate_count:>10,}")
        print(f"Valid transactions:    {valid_count:,}")
        print(f"Quarantined:           {quarantine_count:,}")
        print(f"Quarantine breakdown:  {json.dumps(quarantine_breakdown, sort_keys=True)}")
        print(f"Raw reconciliation:    {raw_reconciliation}")
        print(f"Quality reconciliation:{quality_reconciliation}")

        if not reconciliation_passed:
            raise RuntimeError("Spark pipeline reconciliation failed.")

        write_dataframe_csv(
            valid.select(*DEDUPLICATION_COLUMNS),
            output_root / "valid_transactions",
        )
        write_dataframe_csv(
            quarantined.select(
                *DEDUPLICATION_COLUMNS, "quarantine_reason"
            ),
            output_root / "quarantined_transactions",
        )
        write_dataframe_csv(
            analytics,
            output_root / "transaction_analytics",
        )

        print("\n=== SPARK OUTPUTS WRITTEN ===")
        print(f"Output root: {output_root}")
        print("SPARK PIPELINE PASSED")
        return {
            "customers": customer_count,
            "raw_transactions": raw_count,
            "deduplicated_transactions": deduplicated_count,
            "duplicate_rows_removed": duplicate_count,
            "valid_transactions": valid_count,
            "quarantined_transactions": quarantine_count,
            "quarantine_reasons": quarantine_breakdown,
            "reconciliation_passed": reconciliation_passed,
            "spark_version": spark.version,
        }
    finally:
        for dataframe in cached:
            dataframe.unpersist()
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--shuffle-partitions", type=int, default=4)
    args = parser.parse_args()
    run_spark(args.input_dir, args.output_dir, args.master, args.shuffle_partitions)


if __name__ == "__main__":
    main()
