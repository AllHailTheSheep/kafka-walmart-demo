import json
import os
import signal
import sys
import time
from confluent_kafka import Consumer, Producer, KafkaException

INPUT_TOPICS = ["warehouse-restock", "online-orders", "store-sales"]
OUTPUT_TOPIC = "inventory-ssot"
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
GROUP_ID = os.getenv("GROUP_ID", "inventory-processor-group")

shutdown_requested = False


def current_iso_timestamp():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Published normalized event to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")


def make_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    return Consumer(conf)


def make_producer():
    return Producer({"bootstrap.servers": KAFKA_BROKERS})


def parse_message_value(value):
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        print("Skipping invalid JSON payload")
        return None


def normalize_warehouse(event):
    return [
        {
            "productId": event.get("sku_code"),
            "delta": int(event.get("qty_received", 0)),
            "source": "warehouse",
            "location": event.get("warehouse") or "UNKNOWN",
            "timestamp": event.get("received_at") or current_iso_timestamp(),
            "source_id": event.get("shipment_id"),
            "raw_payload": event,
        }
    ]


def normalize_retail(event):
    return [
        {
            "productId": event.get("product"),
            "delta": -int(event.get("units_sold", 0)),
            "source": "retail",
            "location": event.get("store_id") or "UNKNOWN",
            "timestamp": event.get("sold_at") or current_iso_timestamp(),
            "source_id": event.get("transaction_id"),
            "raw_payload": event,
        }
    ]


def normalize_online(event):
    items = event.get("items")
    if not isinstance(items, list):
        print("Online order missing items array; skipping event")
        return []

    normalized = []
    for item in items:
        normalized.append(
            {
                "productId": item.get("sku"),
                "delta": -int(item.get("quantity", 0)),
                "source": "online",
                "location": event.get("customerId") or "UNKNOWN",
                "timestamp": event.get("orderTimestamp") or current_iso_timestamp(),
                "source_id": event.get("orderId"),
                "raw_payload": {
                    "order": event,
                    "item": item,
                },
            }
        )
    return normalized


def normalize_message(topic, event):
    if topic == "warehouse-restock":
        return normalize_warehouse(event)
    if topic == "store-sales":
        return normalize_retail(event)
    if topic == "online-orders":
        return normalize_online(event)

    print(f"Unrecognized topic: {topic}")
    return []


def publish_normalized(producer, normalized_events):
    for event in normalized_events:
        if not event.get("productId"):
            print("Skipping normalized event without productId")
            continue

        key = event.get("productId") or event.get("source_id")
        producer.produce(
            OUTPUT_TOPIC,
            key=str(key),
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)


def shutdown(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    print("Shutdown signal received, stopping processor...")


def run_processor():
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consumer = make_consumer()
    producer = make_producer()

    try:
        consumer.subscribe(INPUT_TOPICS)
        print(f"Subscribed to input topics: {', '.join(INPUT_TOPICS)}")
        print(f"Publishing normalized output to topic: {OUTPUT_TOPIC}")

        while not shutdown_requested:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                continue

            source_topic = msg.topic()
            event = parse_message_value(msg.value())
            if event is None:
                continue

            normalized_events = normalize_message(source_topic, event)
            publish_normalized(producer, normalized_events)

    except Exception as exc:
        print(f"Processor error: {exc}")
    finally:
        print("Flushing producer and closing consumer...")
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    run_processor()
