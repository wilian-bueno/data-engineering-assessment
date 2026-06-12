"""
Sales order transformation task callables.
"""

import logging
import os

from include.utils.db_utils import run_sql_file, upsert_to_store
from include.utils.quality_utils import deduplicate_by_completeness
from include.utils.spark_session import get_spark_session

SQL_STORE   = "/opt/airflow/sql/store"
SQL_PUBLISH = "/opt/airflow/sql/publish"
JDBC_URL    = os.getenv("ASSESSMENT_DB_JDBC_URL")
DB_USER     = os.getenv("ASSESSMENT_DB_USER")
DB_PASS     = os.getenv("ASSESSMENT_DB_PASSWORD")
JDBC_DRIVER = "org.postgresql.Driver"

LAKE_STORE   = "/opt/airflow/data/lake/store"
LAKE_PUBLISH = "/opt/airflow/data/lake/publish"


def _jdbc_write(df, table: str, mode: str = "overwrite") -> None:
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", JDBC_DRIVER)
        .mode(mode)
        .save()
    )


def _jdbc_read(spark, table: str):
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", JDBC_DRIVER)
        .load()
    )


# ─────────────────────────────────────────────────────────────────────────────
# store_sales_order_header
# ─────────────────────────────────────────────────────────────────────────────

def create_store_sales_order_header_schema() -> None:
    run_sql_file(f"{SQL_STORE}/create_store_sales_order_header.sql")
    logging.info("store_sales_order_header ready (CREATE IF NOT EXISTS).")


def load_store_sales_order_header() -> None:
    """
    Reads latest batch from raw_sales_order_header, casts types, and UPSERTs
    into store_sales_order_header (insert new, update existing by sales_order_id).

    OrderDate logic:
      · Full date "YYYY-MM-DD" → cast directly to DATE
      · Partial date "YYYY-MM" → ship_date - 7 days (confirmed 7-day pattern)
    """
    from pyspark.sql.functions import (
        col, trim, when, length, to_date, date_sub, current_timestamp
    )
    from pyspark.sql.types import IntegerType, BooleanType, DecimalType

    spark = get_spark_session("store_sales_order_header")

    latest_batch = """
        (
            SELECT *
            FROM raw_sales_order_header
            WHERE etl_created_at = (SELECT MAX(etl_created_at) FROM raw_sales_order_header)
        ) latest_raw
    """
    df_raw = _jdbc_read(spark, latest_batch)
    logging.info(f"Rows from raw_sales_order_header (latest batch): {df_raw.count()}")

    ship_date_col  = to_date(trim(col("ShipDate")), "yyyy-MM-dd")
    order_date_col = when(
        length(trim(col("OrderDate"))) > 7,
        to_date(trim(col("OrderDate")), "yyyy-MM-dd")
    ).otherwise(
        date_sub(ship_date_col, 7)
    )

    def nullable_int(c):
        return when(trim(col(c)) == "", None).otherwise(col(c).cast(IntegerType()))

    df_store = df_raw.select(
        col("SalesOrderID").cast(IntegerType())      .alias("sales_order_id"),
        order_date_col                               .alias("order_date"),
        ship_date_col                                .alias("ship_date"),
        col("OnlineOrderFlag").cast(BooleanType())   .alias("online_order_flag"),
        trim(col("AccountNumber"))                   .alias("account_number"),
        col("CustomerID").cast(IntegerType())        .alias("customer_id"),
        nullable_int("SalesPersonID")                .alias("sales_person_id"),
        col("Freight").cast(DecimalType(18, 4))      .alias("freight"),
        current_timestamp()                          .alias("etl_created_at"),
        col("source_file_name"),
    )

    df_store = deduplicate_by_completeness(df_store, pk_col="sales_order_id")

    logging.info("Upserting store_sales_order_header → PostgreSQL...")
    upsert_to_store(df_store, table="store_sales_order_header", pk_col="sales_order_id")

    parquet_path = f"{LAKE_STORE}/sales_order_header"
    logging.info(f"Writing Parquet: {parquet_path}")
    df_store.write.mode("overwrite").parquet(parquet_path)

    spark.stop()
    logging.info("store_sales_order_header load complete.")


# ─────────────────────────────────────────────────────────────────────────────
# store_sales_order_detail
# ─────────────────────────────────────────────────────────────────────────────

def create_store_sales_order_detail_schema() -> None:
    run_sql_file(f"{SQL_STORE}/create_store_sales_order_detail.sql")
    logging.info("store_sales_order_detail ready (CREATE IF NOT EXISTS).")


def load_store_sales_order_detail() -> None:
    """
    Reads latest batch from raw_sales_order_detail, casts types, and UPSERTs
    into store_sales_order_detail (insert new, update existing by sales_order_detail_id).
    """
    from pyspark.sql.functions import col, current_timestamp
    from pyspark.sql.types import IntegerType, DecimalType

    spark = get_spark_session("store_sales_order_detail")

    latest_batch = """
        (
            SELECT *
            FROM raw_sales_order_detail
            WHERE etl_created_at = (SELECT MAX(etl_created_at) FROM raw_sales_order_detail)
        ) latest_raw
    """
    df_raw = _jdbc_read(spark, latest_batch)
    logging.info(f"Rows from raw_sales_order_detail (latest batch): {df_raw.count()}")

    df_store = df_raw.select(
        col("SalesOrderDetailID").cast(IntegerType())    .alias("sales_order_detail_id"),
        col("SalesOrderID").cast(IntegerType())          .alias("sales_order_id"),
        col("OrderQty").cast(IntegerType())              .alias("order_qty"),
        col("ProductID").cast(IntegerType())             .alias("product_id"),
        col("UnitPrice").cast(DecimalType(18, 6))        .alias("unit_price"),
        col("UnitPriceDiscount").cast(DecimalType(18, 6)).alias("unit_price_discount"),
        current_timestamp()                              .alias("etl_created_at"),
        col("source_file_name"),
    )

    df_store = deduplicate_by_completeness(df_store, pk_col="sales_order_detail_id")

    logging.info("Upserting store_sales_order_detail → PostgreSQL...")
    upsert_to_store(df_store, table="store_sales_order_detail", pk_col="sales_order_detail_id")

    parquet_path = f"{LAKE_STORE}/sales_order_detail"
    logging.info(f"Writing Parquet: {parquet_path}")
    df_store.write.mode("overwrite").parquet(parquet_path)

    spark.stop()
    logging.info("store_sales_order_detail load complete.")


# ─────────────────────────────────────────────────────────────────────────────
# publish_orders
# ─────────────────────────────────────────────────────────────────────────────

def create_publish_orders_schema() -> None:
    run_sql_file(f"{SQL_PUBLISH}/create_publish_orders.sql")
    logging.info("publish_orders ready (CREATE IF NOT EXISTS).")


def load_publish_orders() -> None:
    """
    Reads the latest batch from store_sales_order_detail, JOINs with
    store_sales_order_header, applies business transformations, and UPSERTs
    into publish_orders.

    Transformations:
      · total_line_extended_price  = order_qty * (unit_price - unit_price_discount)
      · lead_time_in_business_days = weekdays (Mon–Fri) between order_date and ship_date
      · freight renamed to total_order_freight
    """
    from pyspark.sql.functions import col, current_timestamp, udf
    from pyspark.sql.types import IntegerType
    from datetime import timedelta

    spark = get_spark_session("publish_orders")

    @udf(returnType=IntegerType())
    def business_days_between(start_date, end_date):
        """
        Counts business days (Mon–Fri) between order_date and ship_date.

        Boundary decision:
          - start_date (order_date) is EXCLUSIVE — the order day itself is not a lead day.
          - end_date   (ship_date)  is INCLUSIVE — the shipping day is the last lead day.
          Example: ordered Mon, shipped Wed → 2 business days (Tue + Wed).

        Weekends: Saturday (weekday=5) and Sunday (weekday=6) are skipped.
        """
        if start_date is None or end_date is None:
            return None
        count = 0
        current = start_date
        while current < end_date:
            current += timedelta(days=1)
            if current.weekday() < 5:  # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri
                count += 1
        return count

    latest_detail = """
        (
            SELECT *
            FROM store_sales_order_detail
            WHERE etl_created_at = (SELECT MAX(etl_created_at) FROM store_sales_order_detail)
        ) latest_detail
    """
    df_detail = _jdbc_read(spark, latest_detail).select(
        "sales_order_detail_id", "sales_order_id", "order_qty",
        "product_id", "unit_price", "unit_price_discount",
    )

    df_header = _jdbc_read(spark, "store_sales_order_header").select(
        "sales_order_id", "order_date", "ship_date", "online_order_flag",
        "account_number", "customer_id", "sales_person_id",
        col("freight").alias("total_order_freight"),
    )

    logging.info(f"Latest detail rows: {df_detail.count()} | Header rows: {df_header.count()}")

    df = df_detail.join(df_header, on="sales_order_id", how="inner") \
        .withColumn(
            "total_line_extended_price",
            col("order_qty") * (col("unit_price") - col("unit_price_discount"))
        ) \
        .withColumn(
            "lead_time_in_business_days",
            business_days_between(col("order_date"), col("ship_date"))
        ) \
        .withColumn("etl_created_at", current_timestamp()) \
        .select(
            "sales_order_detail_id", "sales_order_id", "order_qty", "product_id",
            "unit_price", "unit_price_discount", "total_line_extended_price",
            "order_date", "ship_date", "online_order_flag", "account_number",
            "customer_id", "sales_person_id", "total_order_freight",
            "lead_time_in_business_days", "etl_created_at",
        )

    row_count = df.count()
    logging.info(f"Upserting publish_orders → PostgreSQL ({row_count} rows)...")
    upsert_to_store(df, table="publish_orders", pk_col="sales_order_detail_id")

    parquet_path = f"{LAKE_PUBLISH}/orders"
    df.write.mode("overwrite").parquet(parquet_path)

    spark.stop()
    logging.info(f"publish_orders complete. {row_count} rows upserted.")
