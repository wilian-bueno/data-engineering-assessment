"""
Product transformation task callables.
Used by the assessment_pipeline DAG — transform_products task group.
Covers: store_product_master → DQ check → publish_product.
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

# Category enrichment lookup tables
CLOTHING    = ["Gloves", "Shorts", "Socks", "Tights", "Vests"]
ACCESSORIES = ["Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"]
COMPONENTS  = ["Wheels", "Saddles"]


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


def create_store_product_schema() -> None:
    run_sql_file(f"{SQL_STORE}/create_store_product_master.sql")
    logging.info("store_product_master schema created.")


def load_store_product_master() -> None:
    """
    Reads the latest batch from raw_product_master, casts all columns
    to proper types, converts empty strings to NULL, and writes to
    store_product_master (PostgreSQL + Parquet).
    """
    from pyspark.sql.functions import col, trim, when, current_timestamp
    from pyspark.sql.types import IntegerType, BooleanType, DecimalType, FloatType

    spark = get_spark_session("store_product_master")

    latest_batch = """
        (
            SELECT *
            FROM raw_product_master
            WHERE etl_created_at = (SELECT MAX(etl_created_at) FROM raw_product_master)
        ) latest_raw
    """
    df_raw = _jdbc_read(spark, latest_batch)
    logging.info(f"Rows from raw_product_master (latest batch): {df_raw.count()}")

    def nullable(c):
        return when(trim(col(c)) == "", None).otherwise(trim(col(c)))

    df_store = df_raw.select(
        col("ProductID").cast(IntegerType())                        .alias("product_id"),
        col("ProductDesc")                                          .alias("product_desc"),
        col("ProductNumber")                                        .alias("product_number"),
        col("MakeFlag").cast(BooleanType())                         .alias("make_flag"),
        nullable("Color")                                           .alias("color"),
        col("SafetyStockLevel").cast(IntegerType())                 .alias("safety_stock_level"),
        col("ReorderPoint").cast(IntegerType())                     .alias("reorder_point"),
        col("StandardCost").cast(DecimalType(18, 6))                .alias("standard_cost"),
        col("ListPrice").cast(DecimalType(18, 6))                   .alias("list_price"),
        nullable("Size")                                            .alias("size"),
        nullable("SizeUnitMeasureCode")                             .alias("size_unit_measure_code"),
        when(trim(col("Weight")) == "", None)
            .otherwise(col("Weight").cast(FloatType()))             .alias("weight"),
        nullable("WeightUnitMeasureCode")                           .alias("weight_unit_measure_code"),
        nullable("ProductCategoryName")                             .alias("product_category_name"),
        nullable("ProductSubCategoryName")                          .alias("product_sub_category_name"),
        current_timestamp()                                         .alias("etl_created_at"),
        col("source_file_name"),
    )

    # 8 ProductIDs appear twice in the source CSV — keep the most complete row per product_id
    df_store = deduplicate_by_completeness(df_store, pk_col="product_id") 

    logging.info("Upserting store_product_master → PostgreSQL...")
    upsert_to_store(df_store, table="store_product_master", pk_col="product_id")

    parquet_path = f"{LAKE_STORE}/product_master"
    logging.info(f"Writing store_product_master → Parquet: {parquet_path}")
    df_store.write.mode("overwrite").parquet(parquet_path)

    spark.stop()
    logging.info("store_product_master load complete.")




def create_publish_product_schema() -> None:
    run_sql_file(f"{SQL_PUBLISH}/create_publish_product.sql")
    logging.info("publish_product schema created.")


def load_publish_product() -> None:
    """
    Reads store_product_master and applies two business transformations:
      Step 1 — color: NULL → 'N/A'
      Step 2 — product_category_name enrichment when NULL
    Writes to publish_product (PostgreSQL + Parquet).
    """
    from pyspark.sql.functions import col, when, lit, current_timestamp

    spark = get_spark_session("publish_product")

    latest_store = """
        (
            SELECT *
            FROM store_product_master
            WHERE etl_created_at = (SELECT MAX(etl_created_at) FROM store_product_master)
        ) latest_store
    """
    df = _jdbc_read(spark, latest_store)
    logging.info(f"Rows from store_product_master (latest batch): {df.count()}")

    # Step 1: color NULL → 'N/A'
    df = df.withColumn(
        "color",
        when(col("color").isNull(), lit("N/A")).otherwise(col("color"))
    )

    # Step 2: product_category_name enrichment
    df = df.withColumn(
        "product_category_name",
        when(col("product_category_name").isNotNull(), col("product_category_name"))
        .when(col("product_sub_category_name").isin(CLOTHING), lit("Clothing"))
        .when(col("product_sub_category_name").isin(ACCESSORIES), lit("Accessories"))
        .when(
            col("product_sub_category_name").contains("Frames") |
            col("product_sub_category_name").isin(COMPONENTS),
            lit("Components")
        )
        .otherwise(col("product_category_name"))
    )

    df = df.withColumn("etl_created_at", current_timestamp())
    df = df.drop("source_file_name")

    row_count = df.count()
    logging.info(f"Upserting publish_product → PostgreSQL ({row_count} rows)...")
    upsert_to_store(df, table="publish_product", pk_col="product_id")

    parquet_path = f"{LAKE_PUBLISH}/product"
    logging.info(f"Writing publish_product → Parquet: {parquet_path}")
    df.write.mode("overwrite").parquet(parquet_path)

    spark.stop()
    logging.info(f"publish_product complete. {row_count} rows written.")
