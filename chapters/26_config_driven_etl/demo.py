import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, initcap, lit, trim

from shared.path_utils import PROJECT_ROOT
from shared.spark_utils import create_spark_session


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main() -> None:
    spark = create_spark_session("chapter-26-config-driven-etl-demo")

    try:
        config_path = Path(__file__).resolve().parent / "config.dev.json"
        config = load_config(config_path)

        input_path = PROJECT_ROOT / config["input_path"]
        output_path = PROJECT_ROOT / config["output_path"]
        run_date = config["run_date"]
        write_mode = config["write_mode"]

        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(input_path))
        )

        result_df = (
            customer_df
            .withColumn("name", initcap(trim(col("name"))))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("run_date", lit(run_date))
        )

        print("=== CONFIG ===")
        print(config)

        print("=== RESULT PREVIEW ===")
        result_df.show(truncate=False)

        print("=== WRITE PLAN ===")
        print(f"write_mode={write_mode}")
        print(f"output_path={output_path}")
        print("Demo chi in write plan. Khi can ghi that, dung result_df.write.mode(write_mode).parquet(...).")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
