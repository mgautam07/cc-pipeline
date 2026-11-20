"""Ingest one month of credit card statement CSVs into the bronze Delta table.

Files are expected in a Unity Catalog volume, one folder per statement month:

    /Volumes/<catalog>/<schema>/statements/statement_month=YYYY-MM/<bank>_<last4>.csv

Re-running a month replaces only that month's rows, so the job is safe to retry.
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    statement_month STRING        COMMENT 'YYYY-MM of the statement the line came from',
    bank            STRING,
    txn_date        DATE,
    merchant        STRING,
    raw_amount      STRING,
    amount          DECIMAL(14,2) COMMENT 'Positive = charge, negative = payment/refund',
    is_credit       BOOLEAN       COMMENT 'True = credit, False = debit',
    type            STRING        COMMENT 'upi or card',
    card_last4      STRING,
    ingested_at     TIMESTAMP,
    source_file     STRING,
    _line           BIGINT
) USING DELTA
COMMENT 'Bronze: one row per credit card statement line'
PARTITIONED BY (statement_month, bank)
"""


def previous_month() -> str:
    return (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--statement-month", default="", help="YYYY-MM; defaults to last month")
    parser.add_argument("--catalog", default="credit_card")
    parser.add_argument("--schema", default="data")
    parser.add_argument("--bank", choices=["hdfc", "axis", "icici"], required=True)
    return parser.parse_args()


def handle_bank_specifics(raw, bank):
    """Add the standard columns (txn_date, merchant, is_credit, txn_type, raw_amount, abs_amount) to raw."""
    if bank == "axis":
        txn_details = F.split(F.col("`Transaction Details`"), "/")
        txn_details_length = F.size(txn_details)
        raw_amount = F.trim(F.col("`Amount (INR)`"))

        merchant = F.when(txn_details_length > 1, F.trim(F.lower(F.get(txn_details, 1)))).otherwise(F.lower(F.get(txn_details, 0)))
        merchant = F.regexp_replace(merchant, r"^cashback.*$", "cashback")
        merchant = F.regexp_replace(merchant, r"^.*\bbus\b.*$", "bus")

        return (
            raw.withColumn("txn_date", F.to_date(F.trim(F.col("Date")), "dd/MM/yyyy"))
            .withColumn("merchant", merchant)
            .withColumn("is_credit", F.lower(F.trim(F.col("`Debit/Credit`"))) == "credit")
            .withColumn("txn_type", F.when(F.lower(F.get(txn_details, 0)) == "upi", "upi").otherwise("card"))
            .withColumn("raw_amount", raw_amount)
            .withColumn("abs_amount", F.abs(F.regexp_replace(raw_amount, r"[^0-9.]", "").cast("decimal(14,2)")))
            .withColumn("bank", F.lit("axis"))
        )
    raise ValueError(f"No column mapping for bank '{bank}'")


def read_month(spark, path, statement_month, bank):
    raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(path)
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("_line", F.monotonically_increasing_id())
    )

    raw = handle_bank_specifics(raw, bank)
    
    # TODO: Add a txn_id
    
    df = raw.select(
        F.lit(statement_month).alias("statement_month"),
        F.lit(bank).alias("bank"),
        F.col("txn_date").alias("txn_date"),
        F.col("merchant").alias("merchant"),
        F.col("raw_amount").alias("raw_amount"),
        F.when(F.col("is_credit") == True, -F.col("abs_amount")).otherwise(F.col("abs_amount")).alias("amount"),
        F.col("is_credit"),
        F.col("txn_type").alias("type"),
        F.col("card_number").alias("card_last4"),
        F.current_timestamp().alias("ingested_at"),
        "source_file",
        "_line",
    )

    return df

def main():
    args = parse_args()
    statement_month = args.statement_month or previous_month()
    bank = args.bank.lower()
    spark = SparkSession.builder.getOrCreate()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {args.catalog}.{args.schema}.statements")
    table = f"{args.catalog}.{args.schema}.transactions"
    spark.sql(BRONZE_DDL.format(table=table))

    path = f"/Volumes/{args.catalog}/{args.schema}/statements/{args.bank}/"
    df = read_month(spark, path, statement_month, bank)

    unparsed = df.where(F.col("txn_date").isNull()).count()
    if unparsed:
        raise ValueError(f"{unparsed} rows in {path} could not be parsed into a valid transaction date")

    columns = spark.table(table).columns
    (
        df.select(*columns)
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"statement_month = '{statement_month}'")
        .saveAsTable(table)
    )
    print(f"Loaded {df.count()} rows for {statement_month} into {table}")


if __name__ == "__main__":
    main()
