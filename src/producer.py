"""
Simulates a real-time stream of order events the way a checkout service or a
Debezium CDC connector would emit them.

Each message carries:
  op         I (insert) / U (update) / D (delete)
  order_id   stable primary key -- updates and deletes reference an existing order
  lsn        monotonically increasing version, so the consumer can reject
             replays and out-of-order arrivals

Chaos injection (so the consumer's failure paths are actually exercised):
  ~5%  schema-invalid record  -> rejected by validation
  ~2%  unparseable payload    -> rejected by the JSON parse stage
  ~3%  late/out-of-order event-> older LSN for a key already advanced
  ~2%  negative amount        -> legitimate refund, must NOT be rejected

All events for one order_id are published with order_id as the Kafka message
key so they land in the same partition and therefore keep per-key ordering.
"""
import json
import random
import sys
import os
import time
from datetime import datetime, timedelta, timezone
from faker import Faker
from kafka import KafkaProducer

sys.path.append(os.path.dirname(__file__))
from utils.es_logger import get_logger

logger = get_logger("producer")
fake = Faker()

TOPIC = os.getenv("KAFKA_TOPIC", "orders_stream")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CATEGORIES = ["Electronics", "Grocery", "Apparel", "Home"]
STATUSES = ["PENDING", "PAID", "SHIPPED", "DELIVERED"]


def _serialize(value):
    if isinstance(value, (bytes, bytearray)):
        return value
    return json.dumps(value).encode("utf-8")


def build_producer(retries: int = 10) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=_serialize,
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=20,
            )
            producer.bootstrap_connected()
            return producer
        except Exception as e:
            logger.warning(f"Kafka not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after retries")


class OrderEventSimulator:
    """Holds the state needed to emit meaningful CDC: updates and deletes act on
    orders that were actually inserted, and every event gets a higher LSN."""

    def __init__(self):
        self.live_orders = {}
        self.history = {}
        self.lsn = 0

    def _next_lsn(self) -> int:
        self.lsn += 1
        return self.lsn

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def insert(self) -> dict:
        order_id = str(fake.uuid4())
        event = {
            "op": "I",
            "order_id": order_id,
            "customer_id": random.randint(1, 10),
            "amount": round(random.uniform(10, 500), 2),
            "product_category": random.choice(CATEGORIES),
            "event_time": self._now(),
            "lsn": self._next_lsn(),
        }
        self.live_orders[order_id] = event
        return event

    def update(self) -> dict:
        if not self.live_orders:
            return self.insert()
        order_id = random.choice(list(self.live_orders))
        previous = self.live_orders[order_id]
        event = {
            "op": "U",
            "order_id": order_id,
            "customer_id": previous["customer_id"],
            "amount": previous["amount"],
            "product_category": previous["product_category"],
            "event_time": self._now(),
            "lsn": self._next_lsn(),
        }
        self.history.setdefault(order_id, []).append(dict(previous))
        self.live_orders[order_id] = event
        return event

    def delete(self) -> dict:
        if not self.live_orders:
            return self.insert()
        order_id = random.choice(list(self.live_orders))
        previous = self.live_orders.pop(order_id)
        return {
            "op": "D",
            "order_id": order_id,
            "customer_id": previous["customer_id"],
            "amount": previous["amount"],
            "product_category": previous["product_category"],
            "event_time": self._now(),
            "lsn": self._next_lsn(),
        }

    def late_event(self) -> dict:
        """Re-emit an older version of an order that has already moved on."""
        candidates = [o for o, h in self.history.items() if h]
        if not candidates:
            return self.insert()
        order_id = random.choice(candidates)
        stale = random.choice(self.history[order_id])
        event = dict(stale)
        event["event_time"] = (
            datetime.now(timezone.utc) - timedelta(minutes=random.randint(90, 600))
        ).isoformat()
        return event

    def refund(self) -> dict:
        """A negative amount is a legitimate retail event, not dirty data."""
        if not self.live_orders:
            return self.insert()
        order_id = random.choice(list(self.live_orders))
        previous = self.live_orders[order_id]
        event = {
            "op": "U",
            "order_id": order_id,
            "customer_id": previous["customer_id"],
            "amount": -round(random.uniform(10, 200), 2),
            "product_category": previous["product_category"],
            "event_time": self._now(),
            "lsn": self._next_lsn(),
        }
        self.history.setdefault(order_id, []).append(dict(previous))
        self.live_orders[order_id] = event
        return event

    def invalid_schema(self) -> dict:
        return {
            "op": "I",
            "order_id": str(fake.uuid4()),
            "customer_id": "NOT_A_NUMBER",
            "amount": "bad_data",
            "product_category": random.choice(CATEGORIES),
            "event_time": self._now(),
            "lsn": self._next_lsn(),
        }

    def unparseable(self) -> bytes:
        return b'{"op":"I","order_id":"truncated-'


def next_event(sim: OrderEventSimulator) -> tuple:
    """Return (payload, kafka_key, chaos_label)."""
    roll = random.random()
    if roll < 0.02:
        return sim.unparseable(), None, "unparseable"
    if roll < 0.04:
        return sim.late_event(), random.choice(list(sim.live_orders)) or None, "late/out-of-order"
    if roll < 0.06:
        return sim.refund(), random.choice(list(sim.live_orders)) or None, "refund(negative)"
    if roll < 0.11:
        return sim.invalid_schema(), None, "invalid_schema"
    if roll < 0.75:
        return sim.insert(), None, "insert"
    if roll < 0.93:
        event = sim.update()
        return event, event["order_id"], "update"
    event = sim.delete()
    return event, event["order_id"], "delete"


def run(n_events: int = 200, delay_sec: float = 0.2):
    producer = build_producer()
    sim = OrderEventSimulator()
    counts = {}
    logger.info(f"Streaming {n_events} order events to topic '{TOPIC}'")
    for i in range(n_events):
        payload, key, label = next_event(sim)
        counts[label] = counts.get(label, 0) + 1
        producer.send(TOPIC, key=key, value=payload)
        if (i + 1) % 50 == 0:
            logger.info(f"sent {i + 1}/{n_events} events {counts}")
        time.sleep(delay_sec)
    producer.flush()
    producer.close()
    logger.info(f"finished streaming. chaos/op mix: {counts}")
    logger.info(f"final state: {len(sim.live_orders)} live orders, lsn high-water mark {sim.lsn}")


if __name__ == "__main__":
    run()
