"""
PostgreSQL helpers.

  get_assessment_connection() → opens a psycopg2 connection to sales_assessment DB
  run_sql_file(path)          → reads a .sql file and executes it
  get_jdbc_properties()       → returns JDBC props dict for PySpark writes
  upsert_to_store(df, table, pk_col) → merges a PySpark DataFrame into a PostgreSQL table
"""

import logging
import math
import os

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import execute_values


def get_assessment_connection() -> connection:
    """Opens and returns a psycopg2 connection to the assessment database."""
    return psycopg2.connect(
        host=os.getenv("ASSESSMENT_DB_HOST"),
        port=int(os.getenv("ASSESSMENT_DB_PORT", 5432)),
        dbname=os.getenv("ASSESSMENT_DB_NAME"),
        user=os.getenv("ASSESSMENT_DB_USER"),
        password=os.getenv("ASSESSMENT_DB_PASSWORD"),
    )


def run_sql_file(sql_file_path: str) -> None:
    """Reads a .sql file and executes it against the assessment database."""
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql = f.read()
    conn = get_assessment_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def get_jdbc_properties() -> dict:
    """Returns the JDBC connection properties used by PySpark DataFrame.write.jdbc()."""
    return {
        "user": os.getenv("ASSESSMENT_DB_USER"),
        "password": os.getenv("ASSESSMENT_DB_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }


def upsert_to_store(df, table: str, pk_col: str, page_size: int = 1000) -> None:
    """
    Merges a PySpark DataFrame into a PostgreSQL table using INSERT ... ON CONFLICT.

    - If a row with the same pk_col already exists → UPDATE all other columns.
    - If no row with that pk_col exists            → INSERT the row.
    """
    pdf = df.toPandas()
    columns = list(pdf.columns)

    # Build the SQL: INSERT ... ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col, ...
    col_list   = ", ".join(f'"{c}"' for c in columns)
    update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in columns if c != pk_col)

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES %s
        ON CONFLICT ("{pk_col}") DO UPDATE SET {update_set}
    """

    def _safe(v):
        if hasattr(v, "item"):
            v = v.item()
        if isinstance(v, float) and math.isnan(v):
            return None
        return v

    rows = [
        tuple(_safe(v) for v in row)
        for row in pdf.itertuples(index=False, name=None)
    ]

    conn = get_assessment_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=page_size)
        conn.commit()
        logging.info(f"[upsert] {table}: {len(rows)} rows merged on pk='{pk_col}'")
    finally:
        conn.close()
