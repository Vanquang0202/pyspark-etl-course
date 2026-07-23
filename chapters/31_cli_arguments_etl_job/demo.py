import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, initcap, lit, trim

from shared.path_utils import PROJECT_ROOT
from shared.spark_utils import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo ETL job with CLI arguments.")
    parser.add_argument("--input-path", required=True, help="Input CSV path.")
    parser.add_argument("--output-path", required=True, help="Output folder path.")
    parser.add_argument("--run-date", required=True, help="Run date, example: 2026-07-20.")
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        choices=["append", "overwrite", "ignore", "error"],
        help="Spark write mode.",
    )
    return parser.parse_args()


def resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def main() -> None:
    args = parse_args()
    spark = create_spark_session("chapter-31-cli-arguments-etl-job-demo")

    try:
        input_path = resolve_project_path(args.input_path)
        output_path = resolve_project_path(args.output_path)

        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(input_path))
        )

        result_df = (
            customer_df
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("name", initcap(trim(col("name"))))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("run_date", lit(args.run_date))
        )

        print("=== CLI ARGUMENTS ===")
        print(f"input_path={input_path}")
        print(f"output_path={output_path}")
        print(f"run_date={args.run_date}")
        print(f"write_mode={args.write_mode}")

        print("=== RESULT PREVIEW ===")
        result_df.show(truncate=False)

        print("=== WRITE PLAN ===")
        print(f"result_df.write.mode('{args.write_mode}').parquet('{output_path}')")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
