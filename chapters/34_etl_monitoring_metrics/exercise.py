import csv
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, when

from shared.path_utils import DATA_INPUT, DATA_OUTPUT
from shared.spark_utils import create_spark_session


def write_metrics_csv(metrics: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(metrics.keys()))
        writer.writeheader()
        writer.writerow(metrics)


def main() -> None:
    start_time = datetime.now()
    metrics = {"start_time": start_time.isoformat(timespec="seconds"), "job_status": "RUNNING"}
    spark = create_spark_session("chapter-34-etl-monitoring-metrics-exercise")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
        )

        quality_df = customer_df.withColumn(
            "quality_status",
            when(col("name").isNull() | (col("name") == ""), "INVALID").otherwise("VALID"),
        )

        metrics["total_records"] = quality_df.count()
        metrics["valid_records"] = quality_df.filter(col("quality_status") == "VALID").count()
        metrics["invalid_records"] = quality_df.filter(col("quality_status") == "INVALID").count()
        metrics["duplicate_records"] = quality_df.groupBy("customer_id").count().filter(col("count") > 1).count()
        metrics["job_status"] = "SUCCESS"
    except Exception:
        metrics["job_status"] = "FAILED"
        raise
    finally:
        end_time = datetime.now()
        metrics["end_time"] = end_time.isoformat(timespec="seconds")
        metrics["duration_seconds"] = round((end_time - start_time).total_seconds(), 2)
        write_metrics_csv(metrics, DATA_OUTPUT / "chapter34" / "etl_metrics.csv")
        print("=== METRICS WRITTEN ===")
        print(metrics)
        spark.stop()


if __name__ == "__main__":
    main()
