import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-11-spark-sql-demo")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
        )

        province_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "province.csv"))
        )

        # Temp views only exist inside the current SparkSession.
        customer_df.createOrReplaceTempView("customers")
        province_df.createOrReplaceTempView("provinces")

        print("=== SQL SELECT ===")
        spark.sql("""
            SELECT
                customer_id,
                TRIM(name) AS customer_name,
                province_code,
                CAST(amount AS DOUBLE) AS amount
            FROM customers
        """).show(truncate=False)

        print("=== SQL WHERE amount > 0 ===")
        spark.sql("""
            SELECT
                customer_id,
                TRIM(name) AS customer_name,
                province_code,
                CAST(amount AS DOUBLE) AS amount
            FROM customers
            WHERE CAST(amount AS DOUBLE) > 0
        """).show(truncate=False)

        print("=== SQL JOIN WITH CASE WHEN ===")
        spark.sql("""
            SELECT
                c.customer_id,
                TRIM(c.name) AS customer_name,
                c.province_code,
                p.province_name,
                CAST(c.amount AS DOUBLE) AS amount,
                CASE
                    WHEN CAST(c.amount AS DOUBLE) >= 1000000 THEN 'VIP'
                    WHEN CAST(c.amount AS DOUBLE) >= 500000 THEN 'STANDARD'
                    ELSE 'BASIC'
                END AS customer_segment
            FROM customers c
            LEFT JOIN provinces p
                ON TRIM(c.province_code) = TRIM(p.province_code)
            WHERE CAST(c.amount AS DOUBLE) > 0
        """).show(truncate=False)

        print("=== SQL GROUP BY PROVINCE ===")
        spark.sql("""
            SELECT
                p.province_name,
                COUNT(*) AS customer_count,
                SUM(CAST(c.amount AS DOUBLE)) AS total_amount
            FROM customers c
            LEFT JOIN provinces p
                ON TRIM(c.province_code) = TRIM(p.province_code)
            WHERE CAST(c.amount AS DOUBLE) > 0
            GROUP BY p.province_name
            ORDER BY total_amount DESC
        """).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
