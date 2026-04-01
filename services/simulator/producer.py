import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC = "vehicle_telemetry"

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connected to Kafka")
            return producer
        except Exception as e:
            print("❌ Kafka not ready, retrying in 10 seconds...")
            time.sleep(10)

producer = create_producer()

vehicle_ids = ["Swift", "Etios", "Ertiga", "Innova"]
locations = ["BLR", "HYD", "CHE", "CBE"]

def generate_event():
    data = {
        "vehicle_id": random.choice(vehicle_ids),
        "trip_id": f"T{random.randint(1000,9999)}",
        "speed": random.randint(0, 120),
        "engine_temp": random.randint(70, 130),
        "fuel_level": random.randint(10, 100),
        "odometer": random.randint(10000, 200000),
        "location": random.choice(locations),
        "event_type": "RUNNING",
        "timestamp": datetime.utcnow().isoformat()
    }
    # Inject issues (IMPORTANT)
    if random.random() < 0.05:
        data["engine_temp"] = None

    if random.random() < 0.10:
        data["engine_temp"] = 300

    return data

while True:
    event = generate_event()
    producer.send(TOPIC, event)
    print(f"Sent: {event}")
    time.sleep(1)