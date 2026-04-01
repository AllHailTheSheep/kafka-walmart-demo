import asyncio
import json
import websockets
from confluent_kafka import Consumer, KafkaError

connected_clients = set()

kafka_conf = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    'group.id': 'stock-dashboard-consumer',
    'auto.offset.reset': 'latest'
}

inventory = {}

async def broadcast(message):
    if connected_clients:
        dead = set()
        for ws in connected_clients:
            try:
                await ws.send(message)
            except:
                dead.add(ws)
        connected_clients.difference_update(dead)

async def handle_client(websocket):
    connected_clients.add(websocket)
    try:
        snapshot = json.dumps({"type": "snapshot", "data": inventory})
        await websocket.send(snapshot)
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)

async def kafka_loop():
    consumer = Consumer(kafka_conf)
    consumer.subscribe(['inventory-ssot'])

    loop = asyncio.get_event_loop()

    while True:
        msg = await loop.run_in_executor(None, lambda: consumer.poll(0.1))

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print("kafka error", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            item = data.get('productId') or data.get('item_id') or data.get('item') or data.get('product_id') or 'unknown'
            delta = int(data.get('delta', 0))

            if item not in inventory:
                inventory[item] = 0
            inventory[item] += delta
            if inventory[item] < 0:
                inventory[item] = 0

            update = json.dumps({
                "type": "update",
                "item": item,
                "quantity": inventory[item],
                "delta": delta,
                "ts": data.get('timestamp', '')
            })
            await broadcast(update)

        except Exception as e:
            print("failed to process message", e)

async def main():
    print("starting ws bridge on port 6789")
    server = await websockets.serve(handle_client, "0.0.0.0", 6789)
    await asyncio.gather(server.wait_closed(), kafka_loop())

if __name__ == "__main__":
    asyncio.run(main())
