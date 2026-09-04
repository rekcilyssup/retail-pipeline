"""
Simulates a real-time stream of order events (e.g. from a checkout service
or a CDC connector like Debezium watching an orders table). Each message
carries an `op` field (I/U/D) the way Debezium CDC events do, so the Spark
consumer can distinguish inserts/updates/deletes.
"""
import json
import random
import time
import sys
import os
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

sys.path.append(os.path.dirname(__file__))
from utils.es_logger import get_logger

logger = get_logger("producer")
fake = Faker()

TOPIC = "orders_stream"
BOOTSTRAP_SERVERS = "localhost:9092"


def build_producer(retries: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except Exception as e:
            logger.warning(f"Kafka not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after retries")


def make_event(customer_id: int) -> dict:
    # Occasionally emit a malformed record on purpose, to prove the
    # downstream pipeline's exception handling / dead-letter path works.
    if random.random() < 0.05:
        return {"op": "I", "order_id": fake.uuid4(), "customer_id": "NOT_A_NUMBER",
                 "amount": "bad_data", "event_time": datetime.now(timezone.utc).isoformat()}

    return {
        "op": random.choice(["I", "I", "I", "U"]),  # mostly inserts, some updates
        "order_id": fake.uuid4(),
        "customer_id": customer_id,
        "amount": round(random.uniform(10, 500), 2),
        "product_category": random.choice(["Electronics", "Grocery", "Apparel", "Home"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def run(n_events: int = 200, delay_sec: float = 0.2):
    producer = build_producer()
    logger.info(f"Starting to stream {n_events} order events to topic '{TOPIC}'")
    for i in range(n_events):
        event = make_event(customer_id=random.randint(1, 10))
        producer.send(TOPIC, value=event)
        if i % 25 == 0:
            logger.info(f"Sent {i}/{n_events} events")
        time.sleep(delay_sec)
    producer.flush()
    logger.info("Finished streaming events")


if __name__ == "__main__":
    run()
