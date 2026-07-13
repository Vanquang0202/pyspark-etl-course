import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, current_date, expr, initcap, trim, when

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-20-database-etl-practical-flow-exercise")

    try:
        source_customer_df = spark.createDataFrame(
            [
                ("C001", " pham minh ", "HN", "ACTIVE", "1000000"),
                ("C002", "vo lan", "HCM", "ACTIVE", "bad_amount"),
                ("C003", "hoang nam", None, "INACTIVE", "200000"),
                ("C004", "mai hoa", "DN", "ACTIVE", "300000"),
            ],
            ["src_customer_id", "src_name", "src_province_code", "src_status", "src_amount"],
        )

        cleaned_df = (
            source_customer_df
            .withColumn("customer_id", trim(col("src_customer_id")))
            .withColumn("customer_name", initcap(trim(col("src_name"))))
            .withColumn("province_code", trim(col("src_province_code")))
            .withColumn("status", trim(col("src_status")))
            .withColumn("amount", expr("try_cast(src_amount as double)"))
        )

        validated_df = cleaned_df.withColumn(
            "quality_status",
            when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
            .when(col("customer_name").isNull() | (col("customer_name") == ""), "MISSING_NAME")
            .when(col("province_code").isNull() | (col("province_code") == ""), "MISSING_PROVINCE")
            .when(col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT")
            .otherwise("OK"),
        )

        target_customer_df = (
            validated_df
            .filter(col("quality_status") == "OK")
            .select(
                col("customer_id").alias("customer_code"),
                "customer_name",
                "province_code",
                "status",
                "amount",
                current_date().alias("processing_date"),
            )
        )

        invalid_df = validated_df.filter(col("quality_status") != "OK")

        print("=== VALID TARGET ROWS ===")
        target_customer_df.show(truncate=False)

        print("=== INVALID SOURCE ROWS ===")
        invalid_df.select(
            "src_customer_id",
            "src_name",
            "src_province_code",
            "src_amount",
            "quality_status",
        ).show(truncate=False)

        # Khi ket noi DB that, co the ghi bang target_customer_df.write.format("jdbc").
        jdbc_options = {
            "url": "jdbc:sqlserver://host:1433;databaseName=VSS_360",
            "dbtable": "dbo.customer_360",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user": "<user>",
            "password": "<password>",
        }
        print("=== JDBC OPTIONS SAMPLE ===")
        print(jdbc_options)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
