"""
Final pipeline validation.

Runs ALL checks across every layer after the pipeline completes.
Logs a full health report. Raises ValueError if any critical check fails.
"""

import logging
from include.utils.db_utils import get_assessment_connection


def run_full_pipeline_validation() -> None:
    """Runs every validation check and logs a complete report. Fails on any critical issue."""

    conn = get_assessment_connection()
    errors = []
    warnings = []

    try:
        with conn.cursor() as cur:

            logging.info("=" * 60)
            logging.info("VALIDATION REPORT — full pipeline check")
            logging.info("=" * 60)
            logging.info("\n[ 1 ] ROW COUNTS")

            tables = [
                "raw_product_master",
                "raw_sales_order_header",
                "raw_sales_order_detail",
                "store_product_master",
                "store_sales_order_header",
                "store_sales_order_detail",
                "publish_product",
                "publish_orders",
                "analysis_revenue_by_color_year",
                "analysis_avg_lead_time_by_category",
            ]
            for tbl in tables:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                count = cur.fetchone()[0]
                status = "✓" if count > 0 else "✗ EMPTY"
                logging.info(f"  {tbl:<35} {count:>8} rows  {status}")
                if count == 0:
                    errors.append(f"[CRITICAL] {tbl} is empty")

            logging.info("\n[ 2 ] DUPLICATE PRIMARY KEYS")

            pk_checks = [
                ("store_product_master",      "product_id"),
                ("store_sales_order_header",  "sales_order_id"),
                ("store_sales_order_detail",  "sales_order_detail_id"),
                ("publish_product",           "product_id"),
                ("publish_orders",            "sales_order_detail_id"),
            ]
            for tbl, pk in pk_checks:
                cur.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT {pk} FROM {tbl}
                        GROUP BY {pk} HAVING COUNT(*) > 1
                    ) dups
                """)
                dups = cur.fetchone()[0]
                if dups > 0:
                    errors.append(f"[CRITICAL] {tbl}: {dups} duplicate {pk} value(s)")
                    logging.error(f"  ✗ {tbl} — {dups} duplicate {pk}")
                else:
                    logging.info(f"  ✓ {tbl} — no duplicate {pk}")

            logging.info("\n[ 3 ] NULL PRIMARY KEYS")

            for tbl, pk in pk_checks:
                cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {pk} IS NULL")
                nulls = cur.fetchone()[0]
                if nulls > 0:
                    errors.append(f"[CRITICAL] {tbl}: {nulls} NULL {pk} value(s)")
                    logging.error(f"  ✗ {tbl} — {nulls} NULL {pk}")
                else:
                    logging.info(f"  ✓ {tbl} — no NULL {pk}")

            logging.info("\n[ 4 ] ORPHAN RECORDS")

            cur.execute("""
                SELECT COUNT(*) FROM store_sales_order_detail d
                LEFT JOIN store_sales_order_header h USING (sales_order_id)
                WHERE h.sales_order_id IS NULL
            """)
            orphans = cur.fetchone()[0]
            if orphans > 0:
                errors.append(f"[CRITICAL] store_sales_order_detail: {orphans} rows with no matching header")
                logging.error(f"  ✗ store_sales_order_detail — {orphans} orphan rows")
            else:
                logging.info("  ✓ store_sales_order_detail — no orphan rows")

            cur.execute("""
                SELECT COUNT(*) FROM publish_orders o
                LEFT JOIN store_product_master p USING (product_id)
                WHERE p.product_id IS NULL
            """)
            orphan_orders = cur.fetchone()[0]
            if orphan_orders > 0:
                warnings.append(f"[WARN] publish_orders: {orphan_orders} rows with no matching product")
                logging.warning(f"  ! publish_orders — {orphan_orders} rows with no matching product")
            else:
                logging.info("  ✓ publish_orders — all rows have a matching product")

            logging.info("\n[ 5 ] BUSINESS RULES")

            cur.execute("SELECT COUNT(*) FROM publish_product WHERE color IS NULL")
            null_color = cur.fetchone()[0]
            if null_color > 0:
                errors.append(f"[CRITICAL] publish_product: {null_color} rows with NULL color")
                logging.error(f"  ✗ publish_product — {null_color} rows with NULL color")
            else:
                cur.execute("SELECT COUNT(*) FROM publish_product WHERE color = 'N/A'")
                na_color = cur.fetchone()[0]
                logging.info(f"  ✓ publish_product — no NULL color ({na_color} rows set to N/A)")

            cur.execute("""
                SELECT COUNT(*) FROM store_sales_order_header
                WHERE ship_date <= order_date
            """)
            bad_dates = cur.fetchone()[0]
            if bad_dates > 0:
                errors.append(f"[CRITICAL] store_sales_order_header: {bad_dates} rows where ship_date <= order_date")
                logging.error(f"  ✗ store_sales_order_header — {bad_dates} invalid date pairs")
            else:
                logging.info("  ✓ store_sales_order_header — all ship dates after order dates")

            cur.execute("SELECT COUNT(*) FROM publish_orders WHERE lead_time_in_business_days < 0")
            neg_lead = cur.fetchone()[0]
            if neg_lead > 0:
                errors.append(f"[CRITICAL] publish_orders: {neg_lead} rows with negative lead time")
                logging.error(f"  ✗ publish_orders — {neg_lead} rows with negative lead time")
            else:
                logging.info("  ✓ publish_orders — all lead times >= 0")

            cur.execute("SELECT COUNT(*) FROM publish_orders WHERE order_qty < 0")
            returns = cur.fetchone()[0]
            logging.info(f"  ✓ publish_orders — {returns} return/reversal rows (qty < 0) [expected: 2]")
            if returns != 2:
                warnings.append(f"[WARN] publish_orders: expected 2 return rows, found {returns}")

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT sales_order_detail_id
                    FROM publish_orders
                    WHERE ABS(total_line_extended_price -
                              ROUND(order_qty * (unit_price - unit_price_discount), 6)) > 0.01
                    LIMIT 1000
                ) x
            """)
            formula_errors = cur.fetchone()[0]
            if formula_errors > 0:
                errors.append(f"[CRITICAL] publish_orders: {formula_errors} rows where TotalLineExtendedPrice formula is wrong")
            else:
                logging.info("  ✓ publish_orders — TotalLineExtendedPrice formula correct")

            logging.info("\n[ 6 ] ANALYSIS RESULTS")

            logging.info("  Q1 — Highest revenue color by year:")
            cur.execute("""
                WITH rev AS (
                    SELECT EXTRACT(YEAR FROM o.order_date)::INT AS year,
                           p.color,
                           SUM(o.total_line_extended_price) AS total_revenue
                    FROM publish_orders o
                    JOIN publish_product p USING (product_id)
                    GROUP BY 1, 2
                ),
                ranked AS (
                    SELECT *, RANK() OVER (PARTITION BY year ORDER BY total_revenue DESC) AS rnk
                    FROM rev
                )
                SELECT year, color, ROUND(total_revenue, 2) AS total_revenue
                FROM ranked WHERE rnk = 1 ORDER BY year
            """)
            rows = cur.fetchall()
            if not rows:
                errors.append("[CRITICAL] Q1 returned no results — publish_orders may be empty or unjoined")
            else:
                for year, color, revenue in rows:
                    logging.info(f"    {year}  →  {color:<15}  ${revenue:>15,.2f}")

            logging.info("  Q2 — Average LeadTimeInBusinessDays by category:")
            cur.execute("""
                SELECT
                    p.product_category_name,
                    ROUND(AVG(o.lead_time_in_business_days), 2) AS avg_lead_time,
                    COUNT(*) AS order_lines
                FROM publish_orders o
                JOIN publish_product p USING (product_id)
                WHERE p.product_category_name IS NOT NULL
                GROUP BY 1 ORDER BY 2
            """)
            rows = cur.fetchall()
            if not rows:
                errors.append("[CRITICAL] Q2 returned no results")
            else:
                for category, avg_lead, lines in rows:
                    logging.info(f"    {category:<20}  avg={avg_lead} days  ({lines} lines)")

    finally:
        conn.close()

    logging.info("\n" + "=" * 60)
    if warnings:
        for w in warnings:
            logging.warning(f"  {w}")

    if errors:
        logging.error(f"\n  PIPELINE VALIDATION FAILED — {len(errors)} critical issue(s):")
        for e in errors:
            logging.error(f"  {e}")
        raise ValueError(
            f"Pipeline validation failed ({len(errors)} issue(s)):\n"
            + "\n".join(errors)
        )

    logging.info(f"  ALL VALIDATIONS PASSED")
    logging.info("=" * 60)
