from consumer_spark import EVENT_SCHEMA


if __name__ == "__main__":
    print("Kafka topic: customer-events")
    print("Expected JSON schema:", EVENT_SCHEMA.simpleString())
