import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, when

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-29-upsert-scd-concept-exercise")

    try:
        target_product_df = spark.createDataFrame(
            [
                ("P001", "Phone", "ACTIVE", 100.0),
                ("P002", "Laptop", "ACTIVE", 500.0),
            ],
            ["product_id", "product_name", "status", "price"],
        )

        source_product_df = spark.createDataFrame(
            [
                ("P002", "Laptop Pro", "ACTIVE", 650.0),
                ("P003", "Mouse", "ACTIVE", 20.0),
            ],
            ["product_id", "product_name", "status", "price"],
        )

        changes_df = (
            source_product_df.alias("src")
            .join(target_product_df.alias("tgt"), on="product_id", how="left")
            .withColumn(
                "change_type",
                when(col("tgt.product_name").isNull(), "INSERT")
                .when(
                    (col("src.product_name") != col("tgt.product_name"))
                    | (col("src.status") != col("tgt.status"))
                    | (col("src.price") != col("tgt.price")),
                    "UPDATE",
                )
                .otherwise("NO_CHANGE"),
            )
            .select(
                "product_id",
                col("src.product_name").alias("product_name"),
                col("src.status").alias("status"),
                col("src.price").alias("price"),
                "change_type",
            )
        )

        changed_keys_df = changes_df.filter(col("change_type").isin("INSERT", "UPDATE")).select("product_id")

        unchanged_target_df = target_product_df.join(changed_keys_df, on="product_id", how="left_anti")

        final_target_df = unchanged_target_df.unionByName(
            changes_df
            .filter(col("change_type").isin("INSERT", "UPDATE"))
            .drop("change_type")
        )

        print("=== PRODUCT CHANGES ===")
        changes_df.show(truncate=False)

        print("=== FINAL PRODUCT TARGET ===")
        final_target_df.orderBy("product_id").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
