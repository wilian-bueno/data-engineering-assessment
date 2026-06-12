"""
Analysis task callables — assessment Q1 and Q2.

Reads from publish_product and publish_orders and writes results
into two dedicated analysis tables:

  analysis_revenue_by_color_year      → Q1: highest revenue color per year
  analysis_avg_lead_time_by_category  → Q2: avg lead time by product category

Both tables are rebuilt from scratch on every run (DROP + CREATE in the SQL files).
"""

import logging
from include.utils.db_utils import get_assessment_connection, run_sql_file

SQL_ANALYSIS = "/opt/airflow/sql/analysis"


def run_revenue_by_color_analysis() -> None:
    """
    Q1: Which color generated the highest revenue each year?
    Writes results to analysis_revenue_by_color_year and logs them.
    """
    run_sql_file(f"{SQL_ANALYSIS}/create_analysis_revenue_by_color_year.sql")

    conn = get_assessment_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT year, color, total_revenue FROM analysis_revenue_by_color_year ORDER BY year")
            rows = cur.fetchall()
            logging.info("Q1 — Highest revenue color by year:")
            logging.info(f"  {'Year':<6} {'Color':<20} {'Total Revenue':>15}")
            logging.info(f"  {'-'*6} {'-'*20} {'-'*15}")
            for year, color, revenue in rows:
                logging.info(f"  {year:<6} {color:<20} ${revenue:>14,.2f}")
    finally:
        conn.close()


def run_avg_lead_time_analysis() -> None:
    """
    Q2: What is the average LeadTimeInBusinessDays by ProductCategoryName?
    Writes results to analysis_avg_lead_time_by_category and logs them.
    """
    run_sql_file(f"{SQL_ANALYSIS}/create_analysis_avg_lead_time_by_category.sql")

    conn = get_assessment_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT product_category_name, avg_lead_time_business_days
                FROM analysis_avg_lead_time_by_category
                ORDER BY avg_lead_time_business_days
            """)
            rows = cur.fetchall()
            logging.info("Q2 — Average LeadTimeInBusinessDays by ProductCategoryName:")
            logging.info(f"  {'Category':<20} {'Avg Lead Time':>14} {'Order Lines':>12}")
            logging.info(f"  {'-'*20} {'-'*14} {'-'*12}")
            for category, avg_lead, lines in rows:
                logging.info(f"  {category:<20} {avg_lead:>13} days {lines:>11,}")
    finally:
        conn.close()
