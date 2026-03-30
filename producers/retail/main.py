import json
import time
import os
import random
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# Configuration from Environment Variables
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
TRAFFIC_MODE = os.getenv("TRAFFIC_MODE", "NORMAL")
TOPIC = "store-sales"

# Hardcoded SKU list
SKUS = [
  "SKU-APPLE-001", "SKU-BANANA-002", "SKU-MILK-003", "SKU-BREAD-004",
  "SKU-EGGS-005", "SKU-RICE-006", "SKU-PASTA-007", "SKU-CHICKEN-008",
  "SKU-BEEF-009", "SKU-FISH-010", "SKU-YOGURT-011", "SKU-CHEESE-012",
  "SKU-CEREAL-013", "SKU-JUICE-014", "SKU-SODA-015", "SKU-COFFEE-016",
  "SKU-TEA-017", "SKU-SUGAR-018", "SKU-SALT-019", "SKU-FROZENPIZZA-020"
]

STORES = ["STORE-101", "STORE-204", "STORE-309", "STORE-412", "STORE-555"]

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'client.id': 'retail-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):

    # This callback is called once for each message produced to indicate delivery result.
    
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_retail_sale():

    # JSON structure for a retail sale event
    
    return {
        "transaction_id": f"TX-{fake.random_int(min=10000, max=99999)}",
        "store_id": random.choice(STORES),
        "product": random.choice(SKUS),
        "units_sold": random.randint(1, 20), 
        "sold_at": datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    }

def run_producer():

    print(f"Starting retail POS producer in {TRAFFIC_MODE} mode...")
    
    try:
        while True:
            sale_event = generate_retail_sale()
            
            # Produce the message
            producer.produce(
                topic=TOPIC,
                key=sale_event["store_id"], # Partitioning by store ID keeps a store's register events in order
                value=json.dumps(sale_event),
                callback=delivery_report
            )
            
            producer.poll(0)
            
            # rating logic
            if TRAFFIC_MODE == "SURGE":
                time.sleep(0.05) # this is about 20 messages a second
            else:
                time.sleep(random.uniform(0.5, 2.0)) # Normal checkout pacing
                
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    run_producer()