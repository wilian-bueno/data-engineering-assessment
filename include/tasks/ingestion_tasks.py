"""
Raw ingestion task callables.
"""

import logging
import os
from datetime import datetime, timezone
from include.utils.db_utils import get_assessment_connection, run_sql_file
from include.utils.file_utils import compute_file_hash, get_file_mtime, get_file_size
from include.utils.spark_session import get_spark_session

BASE_INPUT  = "/opt/airflow/data/input"
BASE_LAKE   = "/opt/airflow/data/lake/raw"
SQL_RAW     = "/opt/airflow/sql/raw"
JDBC_URL    = os.getenv("ASSESSMENT_DB_JDBC_URL")
DB_USER     = os.getenv("ASSESSMENT_DB_USER")
DB_PASS     = os.getenv("ASSESSMENT_DB_PASSWORD")
JDBC_DRIVER = "org.postgresql.Driver"

FILE_CONFIGS = {
    "products": {
        "folder":     "products",
        "file_name":  "products.csv",
        "table":      "raw_product_master",
        "lake_path":  f"{BASE_LAKE}/product_master",
    },
    "sales_order_header": {
        "folder":     "sales_order_header",
        "file_name":  "sales_order_header.csv",
        "table":      "raw_sales_order_header",
        "lake_path":  f"{BASE_LAKE}/sales_order_header",
    },
    "sales_order_detail": {
        "folder":     "sales_order_detail",
        "file_name":  "sales_order_detail.csv",
        "table":      "raw_sales_order_detail",
        "lake_path":  f"{BASE_LAKE}/sales_order_detail",
    },
}


def init_metadata_table() -> None:
    """Creates raw_file_metadata if it does not already exist."""
    run_sql_file(f"{SQL_RAW}/create_raw_file_metadata.sql")
    logging.info("raw_file_metadata table is ready.")


def file_needs_load(file_key: str) -> bool:
    """
    ShortCircuitOperator callable.
    Returns True  → file is new or changed, proceed with load.
    Returns False → file unchanged, skip load task.
    """
    config = FILE_CONFIGS[file_key]
    file_path = f"{BASE_INPUT}/{config['folder']}/{config['file_name']}"

    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Source file not found: {file_path}\n"
            f"Place {config['file_name']} inside data/input/{config['folder']}/"
        )

    current_hash = compute_file_hash(file_path)

    conn = get_assessment_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_hash FROM raw_file_metadata WHERE file_key = %s",
                (file_key,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if row is None:
        logging.info(f"[{file_key}] First load — no previous record found.")
        return True

    if row[0] != current_hash:
        logging.info(f"[{file_key}] File changed (hash mismatch) — will reload.")
        return True

    logging.info(f"[{file_key}] File unchanged — skipping load.")
    return False


def load_raw_file(file_key: str) -> None:
    """
    Loads a CSV file into the raw layer:
      - All columns read as STRING (no type casting at raw layer)
      - Adds etl_created_at and source_file_name audit columns
      - Appends to PostgreSQL raw_ table (historical record — never deleted)
      - Appends to Parquet in data/lake/raw/
      - Updates raw_file_metadata
    """
    config = FILE_CONFIGS[file_key]
    file_path = f"{BASE_INPUT}/{config['folder']}/{config['file_name']}"

    spark = get_spark_session(app_name=f"raw_ingestion_{file_key}")

    from pyspark.sql.functions import current_timestamp, lit

    logging.info(f"[{file_key}] Reading CSV: {file_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(file_path)
    )

    df = (
        df
        .withColumn("etl_created_at", current_timestamp())
        .withColumn("source_file_name", lit(config["file_name"]))
    )

    row_count = df.count()
    logging.info(f"[{file_key}] Rows read: {row_count}")

    logging.info(f"[{file_key}] Writing to PostgreSQL: {config['table']}")
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", config["table"])
        .option("driver", JDBC_DRIVER)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .mode("append")
        .save()
    )

    logging.info(f"[{file_key}] Writing Parquet to: {config['lake_path']}")
    df.write.mode("append").parquet(config["lake_path"])

    spark.stop()

    _update_file_metadata(file_key, file_path)
    logging.info(f"[{file_key}] Load complete — {row_count} rows.")


def _update_file_metadata(file_key: str, file_path: str) -> None:
    config = FILE_CONFIGS[file_key]
    now = datetime.now(tz=timezone.utc)

    conn = get_assessment_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_file_metadata
                    (file_key, file_name, file_path, file_hash,
                     file_size_bytes, last_modified_at, last_loaded_at, load_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                ON CONFLICT (file_key) DO UPDATE SET
                    file_hash        = EXCLUDED.file_hash,
                    file_size_bytes  = EXCLUDED.file_size_bytes,
                    last_modified_at = EXCLUDED.last_modified_at,
                    last_loaded_at   = EXCLUDED.last_loaded_at,
                    load_count       = raw_file_metadata.load_count + 1
                """,
                (
                    file_key,
                    config["file_name"],
                    file_path,
                    compute_file_hash(file_path),
                    get_file_size(file_path),
                    get_file_mtime(file_path),
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()
