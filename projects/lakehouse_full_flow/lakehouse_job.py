import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    countDistinct,
    current_timestamp,
    input_file_name,
    lit,
    expr,
    sum as spark_sum,
    avg,
    trim,
    when,
)

from config import LakehouseConfig, build_config
from shared.spark_utils import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local Data Lakehouse full flow ETL demo.")
    parser.add_argument("--batch-id", default="batch_20260720_001")
    parser.add_argument("--run-date", default="2026-07-20")
    parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append", "ignore", "error"])
    return parser.parse_args()


def write_json(payload: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False)


def read_sources(spark, config: LakehouseConfig) -> DataFrame:
    csv_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(str(config.customer_csv_path))
        .withColumn("source_file", input_file_name())
    )

    json_df = (
        spark.read
        .option("inferSchema", False)
        .json(str(config.customer_json_path))
        .withColumn("source_file", input_file_name())
    )

    return csv_df.unionByName(json_df)


def add_bronze_metadata(raw_df: DataFrame, config: LakehouseConfig) -> DataFrame:
    return (
        raw_df
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("batch_id", lit(config.batch_id))
        .withColumn("run_date", lit(config.run_date))
    )


def clean_and_validate(bronze_df: DataFrame) -> DataFrame:
    clean_df = (
        bronze_df
        .withColumn("customer_id", trim(col("customer_id")).cast("string"))
        .withColumn("name", trim(col("name")).cast("string"))
        .withColumn("province_code", trim(col("province_code")).cast("string"))
        .withColumn("amount", expr("try_cast(amount as double)"))
        .withColumn("event_time", expr("try_to_timestamp(event_time)"))
    )

    return clean_df.withColumn(
        "invalid_reason",
        when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
        .when(col("province_code").isNull() | (col("province_code") == ""), "MISSING_PROVINCE_CODE")
        .when(col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT")
        .otherwise("OK"),
    )


def build_gold_summary(valid_df: DataFrame) -> DataFrame:
    return (
        valid_df
        .groupBy("province_code")
        .agg(
            countDistinct("customer_id").alias("total_customers"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
        )
        .orderBy("province_code")
    )


def build_catalog(config: LakehouseConfig) -> dict:
    return {
        "datasets": [
            {
                "name": "customer_events_raw",
                "layer": "bronze",
                "path": str(config.bronze_path),
                "description": "Raw customer data from local CSV/JSON sources with ingestion metadata.",
            },
            {
                "name": "customer_clean",
                "layer": "silver",
                "path": str(config.silver_clean_path),
                "description": "Clean and valid customer records.",
            },
            {
                "name": "customer_invalid",
                "layer": "silver",
                "path": str(config.silver_invalid_path),
                "description": "Invalid customer records with invalid_reason.",
            },
            {
                "name": "customer_summary_by_province",
                "layer": "gold",
                "path": str(config.gold_summary_path),
                "description": "Aggregated customer metrics by province_code.",
            },
            {
                "name": "customer_summary_csv",
                "layer": "serving",
                "path": str(config.serving_summary_csv_path),
                "description": "CSV report for BI/report/API simulation.",
            },
        ]
    }


def build_lineage(config: LakehouseConfig) -> dict:
    return {
        "job_name": config.job_name,
        "batch_id": config.batch_id,
        "flow": [
            {"from": "data/input/lakehouse_full_flow/customer_raw.csv", "to": str(config.bronze_path)},
            {"from": "data/input/lakehouse_full_flow/customer_events.json", "to": str(config.bronze_path)},
            {"from": str(config.bronze_path), "to": str(config.silver_clean_path)},
            {"from": str(config.bronze_path), "to": str(config.silver_invalid_path)},
            {"from": str(config.silver_clean_path), "to": str(config.gold_summary_path)},
            {"from": str(config.gold_summary_path), "to": str(config.serving_summary_csv_path)},
        ],
    }


def main() -> None:
    args = parse_args()
    config = build_config(batch_id=args.batch_id, run_date=args.run_date, write_mode=args.write_mode)
    start_time = datetime.now()
    status = "RUNNING"
    spark = create_spark_session("lakehouse-full-flow-local")

    try:
        raw_df = read_sources(spark, config)

        bronze_df = add_bronze_metadata(raw_df, config)
        bronze_df.write.mode(config.write_mode).parquet(str(config.bronze_path))

        validated_df = clean_and_validate(bronze_df)
        valid_df = validated_df.filter(col("invalid_reason") == "OK")
        invalid_df = validated_df.filter(col("invalid_reason") != "OK")

        valid_df.write.mode(config.write_mode).parquet(str(config.silver_clean_path))
        invalid_df.write.mode(config.write_mode).parquet(str(config.silver_invalid_path))

        gold_df = build_gold_summary(valid_df)
        gold_df.write.mode(config.write_mode).parquet(str(config.gold_summary_path))
        gold_df.coalesce(1).write.mode(config.write_mode).option("header", True).csv(str(config.serving_summary_csv_path))

        total_records = validated_df.count()
        valid_records = valid_df.count()
        invalid_records = invalid_df.count()
        invalid_summary = {
            row["invalid_reason"]: row["count"]
            for row in invalid_df.groupBy("invalid_reason").count().collect()
        }

        data_quality_report = {
            "batch_id": config.batch_id,
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "invalid_reason_summary": invalid_summary,
        }

        write_json(build_catalog(config), config.governance_path / "catalog.json")
        write_json(build_lineage(config), config.governance_path / "lineage.json")
        write_json(data_quality_report, config.governance_path / "data_quality_report.json")

        status = "SUCCESS"
        print("Lakehouse full flow completed.")
        print(f"Bronze: {config.bronze_path}")
        print(f"Silver valid: {config.silver_clean_path}")
        print(f"Silver invalid: {config.silver_invalid_path}")
        print(f"Gold: {config.gold_summary_path}")
        print(f"Serving: {config.serving_summary_csv_path}")
        print(f"Governance: {config.governance_path}")
    except Exception:
        status = "FAILED"
        raise
    finally:
        end_time = datetime.now()
        metrics = {
            "job_name": config.job_name,
            "batch_id": config.batch_id,
            "start_time": start_time.isoformat(timespec="seconds"),
            "end_time": end_time.isoformat(timespec="seconds"),
            "duration_seconds": round((end_time - start_time).total_seconds(), 2),
            "status": status,
        }
        write_json(metrics, config.governance_path / "etl_metrics.json")
        spark.stop()


if __name__ == "__main__":
    main()
