import json
import random
import time
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
except ImportError as exc:
    raise SystemExit("Missing kafka-python. Install with: python -m pip install kafka-python") from exc


TOPIC = "customer-events"
BOOTSTRAP_SERVERS = "localhost:9092"


def build_event(index: int) -> dict:
    province_codes = ["HN", "HCM", "DN", "CT"]
    names = ["An", "Binh", "Cuong", "Dung", "Ha"]

    return {
        "customer_id": f"C{index:03d}",
        "name": random.choice(names),
        "province_code": random.choice(province_codes),
        "amount": random.choice([250000, 500000, 1200000, 3000000]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    try:
        for index in range(1, 16):
            event = build_event(index)
            producer.send(TOPIC, key=event["customer_id"], value=event)
            producer.flush()
            print(f"sent: {event}")
            time.sleep(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
