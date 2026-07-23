from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_ROOT = PROJECT_ROOT / "data" / "input" / "lakehouse_full_flow"
LAKEHOUSE_ROOT = PROJECT_ROOT / "data" / "lakehouse"


@dataclass(frozen=True)
class LakehouseConfig:
    job_name: str
    batch_id: str
    run_date: str
    source_system: str
    write_mode: str
    customer_csv_path: Path
    customer_json_path: Path
    bronze_path: Path
    silver_clean_path: Path
    silver_invalid_path: Path
    gold_summary_path: Path
    serving_summary_csv_path: Path
    governance_path: Path


def build_config(
    batch_id: str = "batch_20260720_001",
    run_date: str = "2026-07-20",
    write_mode: str = "overwrite",
) -> LakehouseConfig:
    return LakehouseConfig(
        job_name="lakehouse_full_flow_customer_etl",
        batch_id=batch_id,
        run_date=run_date,
        source_system="local_file_simulation",
        write_mode=write_mode,
        customer_csv_path=INPUT_ROOT / "customer_raw.csv",
        customer_json_path=INPUT_ROOT / "customer_events.json",
        bronze_path=LAKEHOUSE_ROOT / "bronze" / "customer_events_raw",
        silver_clean_path=LAKEHOUSE_ROOT / "silver" / "customer_clean",
        silver_invalid_path=LAKEHOUSE_ROOT / "silver" / "customer_invalid",
        gold_summary_path=LAKEHOUSE_ROOT / "gold" / "customer_summary_by_province",
        serving_summary_csv_path=LAKEHOUSE_ROOT / "serving" / "customer_summary_csv",
        governance_path=LAKEHOUSE_ROOT / "governance",
    )
