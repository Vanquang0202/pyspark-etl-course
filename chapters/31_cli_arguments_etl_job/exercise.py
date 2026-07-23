import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, lit, trim, when

from shared.path_utils import PROJECT_ROOT
from shared.spark_utils import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exercise ETL job with CLI arguments.")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--write-mode", default="overwrite", choices=["append", "overwrite", "ignore", "error"])
    return parser.parse_args()


def resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def main() -> None:
    args = parse_args()
    spark = create_spark_session("chapter-31-cli-arguments-etl-job-exercise")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(resolve_project_path(args.input_path)))
        )

        validated_df = (
            customer_df
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("amount_number", col("amount").cast("double"))
            .withColumn("run_date", lit(args.run_date))
            .withColumn(
                "quality_status",
                when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
                .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID_AMOUNT")
                .otherwise("OK"),
            )
        )

        print("=== EXERCISE RESULT ===")
        validated_df.show(truncate=False)
        print("=== OUTPUT PLAN ===")
        print(f"write_mode={args.write_mode}")
        print(f"output_path={resolve_project_path(args.output_path)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
