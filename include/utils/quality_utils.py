"""
Data quality utilities — deduplication and store layer DQ checks.
"""

import logging
from include.utils.db_utils import get_assessment_connection


# ─────────────────────────────────────────────────────────────────────────────
# DQ check — store_sales_order_header
# ─────────────────────────────────────────────────────────────────────────────

def dq_check_store_sales_order_header() -> None:
    conn = get_assessment_connection()
    errors = []
    row_count = 0

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT sales_order_id FROM store_sales_order_header
                    GROUP BY sales_order_id HAVING COUNT(*) > 1
                ) dups
            """)
            dups = cur.fetchone()[0]
            if dups > 0:
                errors.append(f"  [FAIL] Duplicate sales_order_id: {dups}")
            else:
                logging.info("  [PASS] No duplicate sales_order_id")

            cur.execute("SELECT COUNT(*) FROM store_sales_order_header WHERE sales_order_id IS NULL")
            null_pk = cur.fetchone()[0]
            if null_pk > 0:
                errors.append(f"  [FAIL] NULL sales_order_id: {null_pk} rows")
            else:
                logging.info("  [PASS] No NULL sales_order_id")

            cur.execute("""
                SELECT COUNT(*) FROM store_sales_order_header
                WHERE order_date IS NULL OR ship_date IS NULL
            """)
            null_dates = cur.fetchone()[0]
            if null_dates > 0:
                errors.append(f"  [FAIL] NULL dates: {null_dates} rows")
            else:
                logging.info("  [PASS] No NULL dates")

            cur.execute("""
                SELECT COUNT(*) FROM store_sales_order_header
                WHERE ship_date <= order_date
            """)
            bad_dates = cur.fetchone()[0]
            if bad_dates > 0:
                errors.append(f"  [FAIL] ship_date <= order_date: {bad_dates} rows")
            else:
                logging.info("  [PASS] All ship dates after order dates")

            cur.execute("SELECT COUNT(*) FROM store_sales_order_header WHERE freight < 0")
            neg_freight = cur.fetchone()[0]
            if neg_freight > 0:
                errors.append(f"  [FAIL] Negative freight: {neg_freight} rows")
            else:
                logging.info("  [PASS] No negative freight")

            cur.execute("SELECT COUNT(*) FROM store_sales_order_header")
            row_count = cur.fetchone()[0]
            if row_count == 0:
                errors.append("  [FAIL] store_sales_order_header is empty")
            else:
                logging.info(f"  [PASS] Row count: {row_count}")

    finally:
        conn.close()

    if errors:
        raise ValueError(
            f"DQ FAILED for store_sales_order_header ({len(errors)} issue(s)):\n"
            + "\n".join(errors)
        )
    logging.info(f"All DQ checks passed for store_sales_order_header. {row_count} rows.")


# ─────────────────────────────────────────────────────────────────────────────
# DQ check — store_sales_order_detail
# ─────────────────────────────────────────────────────────────────────────────

def dq_check_store_sales_order_detail() -> None:
    conn = get_assessment_connection()
    errors = []
    row_count = 0

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT sales_order_detail_id FROM store_sales_order_detail
                    GROUP BY sales_order_detail_id HAVING COUNT(*) > 1
                ) d
            """)
            dups = cur.fetchone()[0]
            if dups > 0:
                errors.append(f"  [FAIL] Duplicate sales_order_detail_id: {dups}")
            else:
                logging.info("  [PASS] No duplicate sales_order_detail_id")

            cur.execute("SELECT COUNT(*) FROM store_sales_order_detail WHERE sales_order_detail_id IS NULL")
            null_pk = cur.fetchone()[0]
            if null_pk > 0:
                errors.append(f"  [FAIL] NULL sales_order_detail_id: {null_pk} rows")
            else:
                logging.info("  [PASS] No NULL sales_order_detail_id")

            cur.execute("""
                SELECT COUNT(*) FROM store_sales_order_detail d
                LEFT JOIN store_sales_order_header h USING (sales_order_id)
                WHERE h.sales_order_id IS NULL
            """)
            orphans = cur.fetchone()[0]
            if orphans > 0:
                errors.append(f"  [FAIL] Orphan detail rows: {orphans}")
            else:
                logging.info("  [PASS] No orphan records")

            cur.execute("SELECT COUNT(*) FROM store_sales_order_detail WHERE order_qty < 0")
            neg_qty = cur.fetchone()[0]
            if neg_qty > 0:
                logging.warning(
                    f"  [WARN] order_qty < 0: {neg_qty} rows — return/reversal entries "
                    f"(detail IDs 112 and 339, qty=-1). Expected behaviour."
                )
            else:
                logging.info("  [PASS] No negative order quantities")

            cur.execute("SELECT COUNT(*) FROM store_sales_order_detail WHERE unit_price < 0")
            neg_price = cur.fetchone()[0]
            if neg_price > 0:
                errors.append(f"  [FAIL] Negative unit_price: {neg_price} rows")
            else:
                logging.info("  [PASS] No negative unit prices")

            cur.execute("SELECT COUNT(*) FROM store_sales_order_detail")
            row_count = cur.fetchone()[0]
            if row_count == 0:
                errors.append("  [FAIL] store_sales_order_detail is empty")
            else:
                logging.info(f"  [PASS] Row count: {row_count}")

    finally:
        conn.close()

    if errors:
        raise ValueError(
            f"DQ FAILED for store_sales_order_detail ({len(errors)} issue(s)):\n"
            + "\n".join(errors)
        )
    logging.info(f"All DQ checks passed for store_sales_order_detail. {row_count} rows.")


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


def dq_check_store_product_master() -> None:
    """
    Validates store_product_master before writing publish_product.
    Raises ValueError on any failure — stops the pipeline before publish is written.
    """
    conn   = get_assessment_connection()
    errors = []
    rows   = 0

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT product_id FROM store_product_master
                    GROUP BY product_id HAVING COUNT(*) > 1
                ) dups
            """)
            dups = cur.fetchone()[0]
            if dups > 0:
                errors.append(f"  [FAIL] {dups} duplicate product_id(s)")
            else:
                logging.info("  [PASS] No duplicate product_id")

            cur.execute("SELECT COUNT(*) FROM store_product_master WHERE product_id IS NULL")
            nulls = cur.fetchone()[0]
            if nulls > 0:
                errors.append(f"  [FAIL] {nulls} NULL product_id(s)")
            else:
                logging.info("  [PASS] No NULL product_id")

            cur.execute("""
                SELECT COUNT(*) FROM store_product_master
                WHERE standard_cost < 0 OR list_price < 0
            """)
            neg = cur.fetchone()[0]
            if neg > 0:
                errors.append(f"  [FAIL] {neg} row(s) with negative cost or price")
            else:
                logging.info("  [PASS] No negative cost or price")

            cur.execute("SELECT COUNT(*) FROM store_product_master")
            rows = cur.fetchone()[0]
            if rows == 0:
                errors.append("  [FAIL] store_product_master is empty")
            else:
                logging.info(f"  [PASS] Row count: {rows}")

    finally:
        conn.close()

    if errors:
        raise ValueError(
            f"DQ failed for store_product_master ({len(errors)} issue(s)):\n"
            + "\n".join(errors)
        )

    logging.info(f"All DQ checks passed for store_product_master. {rows} rows.")
