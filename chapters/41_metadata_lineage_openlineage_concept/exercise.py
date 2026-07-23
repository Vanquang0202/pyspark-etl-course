import json
from datetime import datetime, timezone


def create_lineage_event(job_name: str, run_id: str, inputs: list[str], outputs: list[str]) -> dict[str, object]:
    now = datetime.now(timezone.utc).isoformat()
    return {"eventType": "COMPLETE", "job": {"name": job_name}, "run": {"runId": run_id}, "inputs": [{"name": name} for name in inputs], "outputs": [{"name": name} for name in outputs], "startTime": now, "endTime": now, "status": "SUCCESS"}


if __name__ == "__main__":
    print(json.dumps(create_lineage_event("exercise_etl", "run-001", ["raw.customer"], ["silver.customer"]), ensure_ascii=False, indent=2))
