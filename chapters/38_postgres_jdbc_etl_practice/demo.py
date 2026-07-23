"""Đọc bảng Postgres, clean đơn giản và ghi bảng target qua JDBC."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


JDBC_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:5432/etl_demo")
JDBC_USER = os.getenv("POSTGRES_USER", "etl_user")
JDBC_PASSWORD = os.getenv("POSTGRES_PASSWORD", "demo_password")  # Chỉ là mật khẩu demo.
JDBC_DRIVER = "org.postgresql.Driver"


def jdbc_options(table_name: str) -> dict[str, str]:
    return {"url": JDBC_URL, "dbtable": table_name, "user": JDBC_USER, "password": JDBC_PASSWORD, "driver": JDBC_DRIVER}


def main() -> None:
    spark = SparkSession.builder.appName("chapter-38-postgres-jdbc").getOrCreate()
    try:
        source_df = spark.read.format("jdbc").options(**jdbc_options("public.customer_source")).load()
        clean_df = source_df.withColumn("customer_id", trim(col("customer_id"))).withColumn("amount", col("amount").cast("double"))
        clean_df.write.format("jdbc").options(**jdbc_options("public.customer_target")).mode("overwrite").save()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
