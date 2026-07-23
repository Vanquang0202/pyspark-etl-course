"""Gửi vài customer event JSON vào Kafka/Redpanda local."""

import json
from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC = "customer-events"


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
    )
    event = {"customer_id": "C001", "amount": 120000.0, "province_code": "79", "event_time": datetime.now(timezone.utc).isoformat()}
    producer.send(TOPIC, event)
    producer.flush()
    producer.close()
    print(f"Sent event to topic: {TOPIC}")


if __name__ == "__main__":
    main()
