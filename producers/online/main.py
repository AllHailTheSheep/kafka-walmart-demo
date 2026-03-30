import json
import time
import os
import random
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# Configuration from Environment Variables
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
TRAFFIC_MODE = os.getenv("TRAFFIC_MODE", "NORMAL")
TOPIC = "online-orders"

# Hardcoded SKU list for stream normalization testing
SKUS = [
  "SKU-APPLE-001", "SKU-BANANA-002", "SKU-MILK-003", "SKU-BREAD-004",
  "SKU-EGGS-005", "SKU-RICE-006", "SKU-PASTA-007", "SKU-CHICKEN-008",
  "SKU-BEEF-009", "SKU-FISH-010", "SKU-YOGURT-011", "SKU-CHEESE-012",
  "SKU-CEREAL-013", "SKU-JUICE-014", "SKU-SODA-015", "SKU-COFFEE-016",
  "SKU-TEA-017", "SKU-SUGAR-018", "SKU-SALT-019", "SKU-FROZENPIZZA-020"
]

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'client.id': 'online-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    # Called once for each message produced to indicate delivery result.
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_online_order():

    # Generates the exact JSON structure specified for online orders
    
    # Pick 1 to 5 random items for this order
    num_items = random.randint(1, 5)
    selected_skus = random.sample(SKUS, num_items)
    
    items_array = [
        {
            "sku": sku,
            "quantity": random.randint(1, 10) 
        } for sku in selected_skus
    ]

    # return the order in our desired format
    return {
        "orderId": f"ORD-{fake.random_int(min=10000, max=99999)}",
        "items": items_array,
        "customerId": f"CUST-{fake.random_int(min=1000, max=9999)}",
        "orderTimestamp": int(time.time())
    }


def run_producer():
    print(f"Starting online producer in {TRAFFIC_MODE} mode...")
    
    try:
        while True:
            order = generate_online_order()
            
            # Produce the message
            producer.produce(
                topic=TOPIC,
                key=order["customerId"], # make sure it goes to the correct partition.
                value=json.dumps(order),
                callback=delivery_report
            )
            
            # poll for any delivery reports, and to trigger callbacks.
            producer.poll(0)
            
            # Rate logic based on TRAFFIC_MODE to simulate different traffic patterns, can be normal like how it currently is, or can be surge
            # to simulate black friday or cyber monday traffic spikes.
            if TRAFFIC_MODE == "SURGE":
                time.sleep(0.05) # Blast messages for load testing
            else:
                time.sleep(random.uniform(1.0, 3.0)) # Normal daily traffic
                
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        # Wait for any outstanding messages to be delivered before shutting down
        producer.flush()


# run the program 
if __name__ == "__main__":
    run_producer()