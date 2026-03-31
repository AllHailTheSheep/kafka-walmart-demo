# Kafka Walmart Demo

This project aims to provide a hands-on demo of Walmart's use of Kafka. We aim to be able to show the durability and throughput by increasing message production and destroying brokers at random times and see how both the backend and the client reacts.

Each producer produces events in different formats. These get fed to the processor, which transforms them into a standard format with a delta field for adding/subtracting a product. This gets fed to the inventory-ssot channel which the client consumes from to display real time inventory.
# Setup
- Run `docker compose up -d kafka-1 kafka-2 kafka-3 kafka-ui` in the root of the project to bring the containers up (run as root if needed).
- Run `./setup-kafka.sh` to create the topics. You may need to run as root and `chmod +x`.

- Run `docker compose up -d --build producer-online producer-warehouse producer-retail processor` to start the data producers and the processor.

- Navigate to localhost:8080 to see the UI and view the live data stream. Once on localhost:8080, you can click on Topics in the side panel, and then view all of the data being sent in real-time.

# Project Structure
- `docker-compose.yml`: contains the Kafka deployment info. Also has deployments for future producers and consumers.
- `setup-kafka.sh`: contains a short bash script to create the topics.
- `producers/` contains the producer code.
- `producers/warehouse` contains the warehouse producer which emits restock events.
- `producers/online` contains the online orders producer which emits "consume" events (but is NOT a consumer in the Kafka sense!).
- `producers/retail` contains the retail sales producer which emits events similar to above.
- `processor/` contains the processing code that takes the above's events in and standardizes them into the inventory-ssot topic.
- `consumer/` contains the consumer that consumes from the inventory-ssot to display current inventory.
