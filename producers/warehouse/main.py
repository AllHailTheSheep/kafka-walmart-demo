import json
import time
import os
import random
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# Configuration from Environment Variables
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
TRAFFIC_MODE = os.getenv("TRAFFIC_MODE", "NORMAL")
TOPIC = "warehouse-restock"

# Hardcoded SKU list
SKUS = [
  "SKU-APPLE-001", "SKU-BANANA-002", "SKU-MILK-003", "SKU-BREAD-004",
  "SKU-EGGS-005", "SKU-RICE-006", "SKU-PASTA-007", "SKU-CHICKEN-008",
  "SKU-BEEF-009", "SKU-FISH-010", "SKU-YOGURT-011", "SKU-CHEESE-012",
  "SKU-CEREAL-013", "SKU-JUICE-014", "SKU-SODA-015", "SKU-COFFEE-016",
  "SKU-TEA-017", "SKU-SUGAR-018", "SKU-SALT-019", "SKU-FROZENPIZZA-020"
]

WAREHOUSES = ["WH-NJ-01", "WH-CA-02", "WH-TX-03", "WH-FL-04", "WH-IL-05"]

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'client.id': 'warehouse-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    
    # This callback is called once for each message produced to indicate delivery result.

    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_warehouse_restock():
    
    # Generate a warehouse restock event
    
    return {
        "shipment_id": f"SHIP-{fake.random_int(min=10000, max=99999)}",
        "sku_code": random.choice(SKUS),
        "qty_received": random.randint(1000, 10000),
        "warehouse": random.choice(WAREHOUSES),
        "received_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

def run_producer():
    print(f"Starting warehouse producer in {TRAFFIC_MODE} mode...")
    
    try:
        while True:
            restock_event = generate_warehouse_restock()
            
            # Produce the message
            producer.produce(
                topic=TOPIC,
                key=restock_event["warehouse"], # Partitioning by warehouse keeps events for a specific location in order
                value=json.dumps(restock_event),
                callback=delivery_report
            )
            
            producer.poll(0)
            
            # Rate logic based on TRAFFIC_MODE
            # Warehouse shipments happen less frequently than online orders, so we space them out more
            if TRAFFIC_MODE == "SURGE":
                time.sleep(0.5) 
            else:
                time.sleep(random.uniform(5.0, 10.0)) 
                
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    run_producer()