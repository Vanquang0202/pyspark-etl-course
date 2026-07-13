import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, current_timestamp, initcap, trim, when

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-20-database-etl-practical-flow-demo")

    try:
        source_df = spark.createDataFrame(
            [
                ("001", " nguyen van an ", "HN", "1200000"),
                ("002", "tran thi binh", "HCM", "800000"),
                ("003", "", "DN", "-100"),
                (None, "le van cuong", "HN", "500000"),
            ],
            ["customer_id", "customer_name", "province_code", "amount"],
        )

        target_df = (
            source_df
            .withColumn("customer_name", initcap(trim(col("customer_name"))))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("amount", col("amount").cast("double"))
            .withColumn(
                "quality_status",
                when(col("customer_id").isNull(), "MISSING_CUSTOMER_ID")
                .when(col("customer_name") == "", "MISSING_NAME")
                .when(col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT")
                .otherwise("OK"),
            )
            .withColumn("etl_loaded_at", current_timestamp())
            .select(
                "customer_id",
                "customer_name",
                "province_code",
                "amount",
                "quality_status",
                "etl_loaded_at",
            )
        )

        print("=== SOURCE TABLE FROM VSS_ODS (MO PHONG) ===")
        source_df.show(truncate=False)

        print("=== TARGET TABLE FOR VSS_360 (MO PHONG) ===")
        target_df.show(truncate=False)

        print("=== JDBC WRITE CONFIG SAMPLE ===")
        print("url=jdbc:sqlserver://host:1433;databaseName=VSS_360")
        print("dbtable=dbo.customer_360")
        print("driver=com.microsoft.sqlserver.jdbc.SQLServerDriver")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
