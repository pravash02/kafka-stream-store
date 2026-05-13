import json
import time
import uuid

from confluent_kafka import Producer
from faker import Faker

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

faker = Faker()

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode('utf-8')}")
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")

try:
    while True:
        order = {
            "order_id": str(uuid.uuid4()),
            "user": faker.first_name(),
            "item": faker.random_element(elements=["Eggs", "Milk", "Bread", "Apples", "Cheese", "Yogurt", "Coffee"]),
            "quantity": faker.random_int(min=1, max=15)
        }
        value = json.dumps(order).encode("utf-8")

        producer.produce(
            topic="orders",
            value=value,
            callback=delivery_report
        )
        producer.flush()

        print(f"⏱ Waiting 10 seconds before producing the next order...")
        time.sleep(10)
except KeyboardInterrupt:
    print("\n🔴 Producer stopped by user")
