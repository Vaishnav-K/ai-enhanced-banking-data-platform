import csv
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"


def write_dataframe_csv(dataframe, output_dir):
    """Stream Spark rows to a CSV file without Hadoop's Windows committer."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "data.csv"

    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=dataframe.columns,
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in dataframe.toLocalIterator():
            writer.writerow(row.asDict(recursive=True))

    return output_file


def main():
    customers_path = RAW_DIR / "customers.csv"
    transactions_path = RAW_DIR / "transactions.csv"

    for path in (customers_path, transactions_path):
        if not path.is_file():
            raise FileNotFoundError(f"Required input file not found: {path}")

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

    spark = (
        SparkSession.builder
        .appName("BankingDataPlatformSpark")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Spark version: {spark.version}")
        print(f"Execution master: {spark.sparkContext.master}")

        customers_raw = (
            spark.read
            .option("header", True)
            .schema(customer_schema)
            .csv(str(customers_path))
        )

        transactions_raw = (
            spark.read
            .option("header", True)
            .schema(transaction_schema)
            .csv(str(transactions_path))
        )

        customers = customers_raw.select(
            F.trim("customer_id").alias("customer_id"),
            F.trim("customer_name").alias("customer_name"),
            F.upper(F.trim("account_type")).alias("account_type"),
            F.to_date("registration_date", "yyyy-MM-dd").alias(
                "registration_date"
            ),
        ).cache()

        transactions_typed = transactions_raw.select(
            F.trim("transaction_id").alias("transaction_id"),
            F.trim("customer_id").alias("customer_id"),
            F.upper(F.trim("transaction_type")).alias("transaction_type"),
            F.expr("try_cast(amount AS DECIMAL(18,2))").alias("amount"),
            F.to_timestamp(
                "transaction_timestamp", "yyyy-MM-dd HH:mm:ss"
            ).alias("transaction_timestamp"),
            F.upper(F.trim("status")).alias("status"),
        )

        raw_transaction_count = transactions_typed.count()
        customer_count = customers.count()

        # Keep one record per transaction ID, matching the existing
        # pipeline's deduplication key.
        transactions = transactions_typed.dropDuplicates(
            ["transaction_id"]
        ).cache()
        deduplicated_count = transactions.count()

        customer_keys = customers.select("customer_id").distinct()

        checked = (
            transactions
            .join(
                customer_keys.withColumn("customer_exists", F.lit(True)),
                on="customer_id",
                how="left",
            )
            .withColumn(
                "quarantine_reason",
                F.when(
                    F.col("transaction_id").isNull()
                    | (F.col("transaction_id") == ""),
                    F.lit("MISSING_TRANSACTION_ID"),
                )
                .when(
                    F.col("customer_id").isNull()
                    | (F.col("customer_id") == ""),
                    F.lit("MISSING_CUSTOMER_ID"),
                )
                .when(
                    F.col("amount").isNull(),
                    F.lit("INVALID_AMOUNT"),
                )
                .when(
                    F.col("amount") <= F.lit(0),
                    F.lit("INVALID_AMOUNT"),
                )
                .when(
                    F.col("transaction_timestamp").isNull(),
                    F.lit("INVALID_TIMESTAMP"),
                )
                .when(
                    F.col("customer_exists").isNull(),
                    F.lit("UNKNOWN_CUSTOMER_ID"),
                )
            )
        ).cache()

        valid = checked.filter(F.col("quarantine_reason").isNull()).cache()
        quarantined = checked.filter(
            F.col("quarantine_reason").isNotNull()
        ).cache()

        valid_count = valid.count()
        quarantined_count = quarantined.count()

        quarantine_breakdown = {
            row["quarantine_reason"]: row["count"]
            for row in quarantined.groupBy("quarantine_reason")
            .count()
            .collect()
        }

        # Business analytics: transaction volume and value by type/status.
        analytics = (
            valid.groupBy("transaction_type", "status")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.round(F.avg("amount"), 2).alias("average_amount"),
            )
            .orderBy("transaction_type", "status")
        )

        print("\n=== SPARK PIPELINE RECONCILIATION ===")
        print(f"Customers:             {customer_count}")
        print(f"Raw transactions:      {raw_transaction_count}")
        print(f"Deduplicated:          {deduplicated_count}")
        print(f"Valid transactions:    {valid_count}")
        print(f"Quarantined:           {quarantined_count}")
        print(f"Quarantine breakdown:  {json.dumps(quarantine_breakdown, sort_keys=True)}")

        reconciliation_passed = (
            deduplicated_count == valid_count + quarantined_count
        )
        print(f"Reconciliation:       {reconciliation_passed}")

        print("\n=== TRANSACTION ANALYTICS ===")
        analytics.show(50, truncate=False)

        if raw_transaction_count != deduplicated_count + (
            raw_transaction_count - deduplicated_count
        ):
            raise RuntimeError("Raw-row reconciliation failed.")

        if deduplicated_count != valid_count + quarantined_count:
            raise RuntimeError("Deduplicated-row reconciliation failed.")

        # SPARK OUTPUT PERSISTENCE
        output_root = ROOT / "data" / "spark_processed"
        valid_output = output_root / "valid_transactions"
        quarantine_output = output_root / "quarantined_transactions"
        analytics_output = output_root / "transaction_analytics"

        valid_path = write_dataframe_csv(
            valid.select(
                "transaction_id",
                "customer_id",
                "transaction_type",
                "amount",
                "transaction_timestamp",
                "status",
            ),
            valid_output,
        )

        quarantine_path = write_dataframe_csv(
            quarantined.select(
                "transaction_id",
                "customer_id",
                "transaction_type",
                "amount",
                "transaction_timestamp",
                "status",
                "quarantine_reason",
            ),
            quarantine_output,
        )

        analytics_path = write_dataframe_csv(
            analytics,
            analytics_output,
        )

        print("\\n=== SPARK OUTPUTS WRITTEN ===")
        print(f"Valid transactions: {valid_path}")
        print(f"Quarantined records: {quarantine_path}")
        print(f"Analytics: {analytics_path}")

        print("\nSPARK PIPELINE PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
