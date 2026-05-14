"""
Real-Time Event Producer
Simulates a mixed stream of clickstream + IoT sensor events
published to Kafka topic: events.raw
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("producer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "10"))
TOPIC_RAW = "events.raw"

# ── Simulated reference data ───────────────────────────────────────────
PAGES = ["/home", "/products", "/cart", "/checkout", "/profile", "/search"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
COUNTRIES = ["US", "IN", "DE", "GB", "BR", "JP", "CA", "AU"]
USER_IDS = [f"user_{i:04d}" for i in range(1, 201)]
SENSOR_IDS = [f"sensor_{i:03d}" for i in range(1, 31)]
ACTIONS = ["page_view", "click", "scroll", "form_submit", "purchase", "add_to_cart"]


def make_clickstream_event() -> dict:
    """Generate a realistic clickstream event."""
    user_id = random.choice(USER_IDS)
    session_id = str(uuid.uuid4())[:8]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "clickstream",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "session_id": session_id,
        "action": random.choice(ACTIONS),
        "page": random.choice(PAGES),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "country": random.choice(COUNTRIES),
        "response_time_ms": random.randint(50, 2000),
        # Inject occasional bad data for pipeline validation
        "revenue": round(random.uniform(0, 500), 2) if random.random() > 0.7 else None,
    }


def make_sensor_event() -> dict:
    """Generate an IoT sensor reading."""
    # ~5% chance of anomaly to make dashboards interesting
    is_anomaly = random.random() < 0.05
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "sensor",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sensor_id": random.choice(SENSOR_IDS),
        "temperature": round(random.uniform(60, 95) if is_anomaly else random.uniform(20, 45), 2),
        "humidity": round(random.uniform(0, 100), 2),
        "pressure": round(random.uniform(900, 1100), 2),
        "vibration": round(random.uniform(0.5, 9.0) if is_anomaly else random.uniform(0.0, 0.5), 4),
        "is_anomaly": is_anomaly,
        "location": f"zone_{random.randint(1, 5)}",
    }


def make_log_event() -> dict:
    """Generate an application log event."""
    levels = ["INFO", "INFO", "INFO", "WARN", "ERROR"]
    services = ["auth-service", "payment-service", "inventory-service", "api-gateway"]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "app_log",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": random.choice(levels),
        "service": random.choice(services),
        "message": f"Request processed in {random.randint(10, 500)}ms",
        "trace_id": str(uuid.uuid4())[:12],
        "status_code": random.choice([200, 200, 200, 200, 201, 400, 404, 500]),
    }


EVENT_FACTORIES = [
    (0.6, make_clickstream_event),   # 60% clickstream
    (0.3, make_sensor_event),        # 30% sensor
    (0.1, make_log_event),           # 10% logs
]


def pick_event() -> dict:
    roll = random.random()
    cumulative = 0
    for weight, factory in EVENT_FACTORIES:
        cumulative += weight
        if roll < cumulative:
            return factory()
    return make_clickstream_event()


def delivery_report(err, msg):
    if err:
        log.error(f"Delivery failed for {msg.key()}: {err}")


def wait_for_kafka(retries: int = 30) -> Producer:
    """Wait until Kafka is reachable, then return a Producer."""
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "event-producer",
        "linger.ms": 10,
        "batch.size": 16384,
        "compression.type": "lz4",
        "retries": 5,
    }
    for attempt in range(1, retries + 1):
        try:
            admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
            meta = admin.list_topics(timeout=5)
            if meta:
                log.info(f"Kafka reachable after {attempt} attempt(s)")
                return Producer(conf)
        except Exception as exc:
            log.warning(f"Kafka not ready (attempt {attempt}/{retries}): {exc}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after multiple retries")


def main():
    log.info(f"Starting producer → {BOOTSTRAP_SERVERS} @ {EVENTS_PER_SECOND} events/sec")
    producer = wait_for_kafka()
    sleep_interval = 1.0 / EVENTS_PER_SECOND
    produced = 0

    try:
        while True:
            event = pick_event()
            key = event.get("user_id") or event.get("sensor_id") or event["event_id"]
            producer.produce(
                topic=TOPIC_RAW,
                key=key.encode(),
                value=json.dumps(event).encode(),
                callback=delivery_report,
            )
            produced += 1

            if produced % 500 == 0:
                log.info(f"Produced {produced} events")
                producer.flush()

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        log.info("Shutting down producer…")
    finally:
        producer.flush()
        log.info(f"Total events produced: {produced}")


if __name__ == "__main__":
    main()
