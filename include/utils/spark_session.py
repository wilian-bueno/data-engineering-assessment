"""
Shared SparkSession builder.

PySpark runs in local mode — no separate cluster needed.
The JDBC driver for PostgreSQL is required for df.write.format("jdbc").
"""

import os
from pyspark.sql import SparkSession

# Path to the PostgreSQL JDBC driver JAR (downloaded during Docker image build)
JDBC_JAR = os.getenv("POSTGRES_JDBC_JAR", "/opt/postgresql-jdbc.jar")


def get_spark_session(app_name: str = "sales_assessment") -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")          # use all available CPU cores
        .appName(app_name)
        .config("spark.jars", JDBC_JAR)
        .config("spark.sql.shuffle.partitions", "4")  # small data — keep partitions low
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
