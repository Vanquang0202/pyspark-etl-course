import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-11-spark-sql-exercise")

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

        customer_df.createOrReplaceTempView("customers")
        province_df.createOrReplaceTempView("provinces")

        report_df = spark.sql("""
            WITH enriched_customers AS (
                SELECT
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
            )
            SELECT
                province_name,
                customer_segment,
                COUNT(*) AS customer_count,
                SUM(amount) AS total_amount,
                AVG(amount) AS avg_amount
            FROM enriched_customers
            GROUP BY province_name, customer_segment
            ORDER BY province_name ASC NULLS LAST, total_amount DESC
        """)

        print(f"report_rows: {report_df.count()}")

        print("=== SQL REPORT BY PROVINCE AND SEGMENT ===")
        report_df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
