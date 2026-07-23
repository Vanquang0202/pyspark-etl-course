from pyspark.sql import DataFrame
from pyspark.sql.functions import col, initcap, trim, when


def clean_customers(customer_df: DataFrame) -> DataFrame:
    return (
        customer_df
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("name", initcap(trim(col("name"))))
        .withColumn("province_code", trim(col("province_code")))
        .withColumn("amount_number", col("amount").cast("double"))
    )


def add_quality_status(customer_df: DataFrame) -> DataFrame:
    return customer_df.withColumn(
        "quality_status",
        when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
        .when(col("name").isNull() | (col("name") == ""), "MISSING_NAME")
        .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID_AMOUNT")
        .otherwise("OK"),
    )
