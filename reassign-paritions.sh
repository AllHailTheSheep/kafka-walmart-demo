#!/bin/bash

BROKER="kafka-1:9092"

docker exec kafka-1 kafka-topics \
  --alter \
  --topic warehouse-restock \
  --bootstrap-server $BROKER \
  --partitions 5 --replication-factor 3

docker exec kafka-1 kafka-topics \
  --alter \
  --topic online-orders \
  --bootstrap-server $BROKER \
  --partitions 5 --replication-factor 3

docker exec kafka-1 kafka-topics \
  --alter \
  --topic store-sales \
  --bootstrap-server $BROKER \
  --partitions 5 --replication-factor 3

docker exec kafka-1 kafka-topics \
  --alter \
  --topic inventory-ssot \
  --bootstrap-server $BROKER \
  --partitions 5 --replication-factor 3