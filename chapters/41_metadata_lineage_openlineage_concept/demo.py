import json
from pathlib import Path


if __name__ == "__main__":
    event_file = Path(__file__).with_name("sample_lineage_event.json")
    print(json.dumps(json.loads(event_file.read_text(encoding="utf-8")), ensure_ascii=False, indent=2))
