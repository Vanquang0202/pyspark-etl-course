import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, when

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-29-upsert-scd-concept-demo")

    try:
        target_df = spark.createDataFrame(
            [
                ("C001", "Nguyen Van An", "01", "ACTIVE"),
                ("C002", "Tran Thi Binh", "79", "ACTIVE"),
                ("C003", "Le Van Cuong", "48", "INACTIVE"),
            ],
            ["customer_id", "customer_name", "province_code", "status"],
        )

        source_df = spark.createDataFrame(
            [
                ("C002", "Tran Thi Binh", "01", "ACTIVE"),
                ("C004", "Pham Van Dung", "79", "ACTIVE"),
            ],
            ["customer_id", "customer_name", "province_code", "status"],
        )

        joined_df = (
            source_df.alias("src")
            .join(target_df.alias("tgt"), on="customer_id", how="left")
            .select(
                col("customer_id"),
                col("src.customer_name").alias("src_customer_name"),
                col("tgt.customer_name").alias("tgt_customer_name"),
                col("src.province_code").alias("src_province_code"),
                col("tgt.province_code").alias("tgt_province_code"),
                col("src.status").alias("src_status"),
                col("tgt.status").alias("tgt_status"),
            )
            .withColumn(
                "change_type",
                when(col("tgt_customer_name").isNull(), "INSERT")
                .when(
                    (col("src_customer_name") != col("tgt_customer_name"))
                    | (col("src_province_code") != col("tgt_province_code"))
                    | (col("src_status") != col("tgt_status")),
                    "UPDATE",
                )
                .otherwise("NO_CHANGE"),
            )
        )

        insert_update_df = joined_df.filter(col("change_type").isin("INSERT", "UPDATE"))

        unchanged_df = (
            target_df.alias("tgt")
            .join(insert_update_df.select("customer_id"), on="customer_id", how="left_anti")
        )

        changed_target_df = insert_update_df.select(
            "customer_id",
            col("src_customer_name").alias("customer_name"),
            col("src_province_code").alias("province_code"),
            col("src_status").alias("status"),
        )

        final_target_df = unchanged_df.unionByName(changed_target_df)

        print("=== DETECT INSERT/UPDATE ===")
        joined_df.show(truncate=False)

        print("=== FINAL TARGET AFTER SCD TYPE 1 UPSERT ===")
        final_target_df.orderBy("customer_id").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
