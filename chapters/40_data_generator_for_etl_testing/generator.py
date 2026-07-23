"""Tạo customer event CSV có record hợp lệ và không hợp lệ để test ETL."""

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


FIELDS = ["customer_id", "name", "province_code", "amount", "event_time", "source_system"]


def build_event(index: int, invalid_rate: float) -> dict[str, object]:
    event = {"customer_id": f"C{index:05d}", "name": f"Customer {index}", "province_code": random.choice(["01", "48", "79"]), "amount": round(random.uniform(10, 1000), 2), "event_time": (datetime.now(timezone.utc) - timedelta(minutes=index)).isoformat(), "source_system": "generator"}
    if random.random() < invalid_rate:
        event["amount"] = -1  # Invalid record để test quality rule.
    return event


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sample customer events.")
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--invalid-rate", type=float, default=0.1)
    parser.add_argument("--output-path", default="data/input/generated/customer_events.csv")
    args = parser.parse_args()
    if args.rows < 1 or not 0 <= args.invalid_rate <= 1:
        raise ValueError("--rows must be positive and --invalid-rate must be between 0 and 1.")
    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(build_event(index, args.invalid_rate) for index in range(1, args.rows + 1))
    print(f"Generated {args.rows} records: {output_path}")


if __name__ == "__main__":
    main()
