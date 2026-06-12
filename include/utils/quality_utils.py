"""
Data quality utilities — deduplication and store layer DQ checks.
"""

import logging
from include.utils.db_utils import get_assessment_connection


# ─────────────────────────────────────────────────────────────────────────────
# Generic DQ framework
# ─────────────────────────────────────────────────────────────────────────────

def run_dq_checks(table: str, pk_col: str) -> None:
    """
    Standard DQ checks for any store table:
      1. No duplicate values in pk_col
      2. No NULL values in pk_col
      3. Table is not empty
    Raises ValueError if any check fails.
    """
    conn = get_assessment_connection()
    errors = []

    try:
        with conn.cursor() as cur:

            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk_col} FROM {table}
                    GROUP BY {pk_col} HAVING COUNT(*) > 1
                ) dups
            """)
            dups = cur.fetchone()[0]
            if dups > 0:
                errors.append(f"[FAIL] {dups} duplicate {pk_col}")
            else:
                logging.info(f"  [PASS] No duplicate {pk_col}")

            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {pk_col} IS NULL")
            nulls = cur.fetchone()[0]
            if nulls > 0:
                errors.append(f"[FAIL] {nulls} NULL {pk_col}")
            else:
                logging.info(f"  [PASS] No NULL {pk_col}")

            cur.execute(f"SELECT COUNT(*) FROM {table}")
            rows = cur.fetchone()[0]
            if rows == 0:
                errors.append(f"[FAIL] {table} is empty")
            else:
                logging.info(f"  [PASS] Row count: {rows}")

    finally:
        conn.close()

    if errors:
        raise ValueError(f"DQ failed for {table}:\n" + "\n".join(errors))
    logging.info(f"DQ passed for {table}.")


# ─────────────────────────────────────────────────────────────────────────────
# Table-specific DQ callables (called by the DAG)
# ─────────────────────────────────────────────────────────────────────────────

def dq_check_store_product_master() -> None:
    run_dq_checks("store_product_master", "product_id")


def dq_check_store_sales_order_header() -> None:
    run_dq_checks("store_sales_order_header", "sales_order_id")


def dq_check_store_sales_order_detail() -> None:
    run_dq_checks("store_sales_order_detail", "sales_order_detail_id")


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate_by_completeness(df, pk_col: str):
    """
    Keeps one row per PK value — the row with the fewest NULL columns.
    """
    from pyspark.sql.functions import col, when, row_number
    from pyspark.sql.window import Window

    non_pk_cols     = [c for c in df.columns if c != pk_col]
    null_count_expr = sum(when(col(c).isNull(), 1).otherwise(0) for c in non_pk_cols)

    df = df.withColumn("_null_count", null_count_expr)

    window  = Window.partitionBy(pk_col).orderBy(col("_null_count").asc())
    df      = df.withColumn("_rank", row_number().over(window))

    total_before = df.count()
    df_clean     = df.filter(col("_rank") == 1).drop("_null_count", "_rank")
    dropped      = total_before - df_clean.count()

    if dropped > 0:
        logging.warning(
            f"[dedup] pk='{pk_col}': dropped {dropped} duplicate row(s) "
            f"(kept the most complete record per key)"
        )
    else:
        logging.info(f"[dedup] pk='{pk_col}': no duplicates found")

    return df_clean
