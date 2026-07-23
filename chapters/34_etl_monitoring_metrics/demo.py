import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Window
from pyspark.sql.functions import col, count, when

from shared.path_utils import DATA_INPUT, DATA_OUTPUT
from shared.spark_utils import create_spark_session


def write_metrics_json(metrics: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(metrics, file, indent=2, ensure_ascii=False)


def main() -> None:
    start_time = datetime.now()
    metrics = {
        "start_time": start_time.isoformat(timespec="seconds"),
        "job_status": "RUNNING",
    }
    spark = create_spark_session("chapter-34-etl-monitoring-metrics-demo")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
        )

        duplicate_window = Window.partitionBy("customer_id")
        quality_df = (
            customer_df
            .withColumn("amount_number", col("amount").cast("double"))
            .withColumn("customer_id_count", count("*").over(duplicate_window))
            .withColumn(
                "quality_status",
                when(col("customer_id").isNull() | (col("customer_id") == ""), "INVALID")
                .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID")
                .otherwise("VALID"),
            )
        )

        metrics["total_records"] = quality_df.count()
        metrics["valid_records"] = quality_df.filter(col("quality_status") == "VALID").count()
        metrics["invalid_records"] = quality_df.filter(col("quality_status") == "INVALID").count()
        metrics["duplicate_records"] = quality_df.filter(col("customer_id_count") > 1).count()
        metrics["job_status"] = "SUCCESS"

        print("=== METRICS ===")
        print(metrics)
    except Exception:
        metrics["job_status"] = "FAILED"
        raise
    finally:
        end_time = datetime.now()
        metrics["end_time"] = end_time.isoformat(timespec="seconds")
        metrics["duration_seconds"] = round((end_time - start_time).total_seconds(), 2)
        write_metrics_json(metrics, DATA_OUTPUT / "chapter34" / "etl_metrics.json")
        spark.stop()


if __name__ == "__main__":
    main()
