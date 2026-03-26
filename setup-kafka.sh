#!/bin/bash

BROKER="kafka-1:9092"

docker exec kafka-1 kafka-topics \
  --create --if-not-exists \
  --topic warehouse-restock \
  --bootstrap-server $BROKER \
  --partitions 3 --replication-factor 2

docker exec kafka-1 kafka-topics \
  --create --if-not-exists \
  --topic online-orders \
  --bootstrap-server $BROKER \
  --partitions 3 --replication-factor 2

docker exec kafka-1 kafka-topics \
  --create --if-not-exists \
  --topic store-sales \
  --bootstrap-server $BROKER \
  --partitions 3 --replication-factor 2

docker exec kafka-1 kafka-topics \
  --create --if-not-exists \
  --topic inventory-ssot \
  --bootstrap-server $BROKER \
  --partitions 4 --replication-factor 2 \
  --config cleanup.policy=compact