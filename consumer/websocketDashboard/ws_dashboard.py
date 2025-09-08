from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
import json
from confluent_kafka import Consumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_ws")

app = FastAPI(title="Kafka WebSocket Multi-Topic Isolated")

# ---------------- WebSocket manager ----------------
class WSManager:
    def __init__(self):
        self.clients = set()
        self.queue = asyncio.Queue(maxsize=1)  # luôn giữ message mới nhất

    async def register(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)
        logger.info(f"Client connected: {ws.client}")

    def unregister(self, ws: WebSocket):
        self.clients.discard(ws)
        logger.info(f"Client disconnected: {ws.client}")

    async def push(self, msg):
        if self.queue.full():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        await self.queue.put(msg)

    async def broadcast_loop(self):
        while True:
            msg = await self.queue.get()
            for ws in list(self.clients):
                try:
                    await ws.send_json(msg)
                except:
                    self.unregister(ws)

# ---------------- Quản lý WSManager cho từng topic ----------------
ws_managers = {
    "eboard_table": WSManager(),
    "eboard_foreign": WSManager(),
    "indices": WSManager()
}

# ---------------- Kafka consumer ----------------
async def kafka_consumer_task(topic: str, consumer_id: int):
    consumer = Consumer({
        'bootstrap.servers': 'broker:29092',
        'group.id': f'ws_group_{topic}',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([topic])
    loop = asyncio.get_running_loop()
    logger.info(f"Consumer-{consumer_id} started for topic {topic}")

    while True:
        msg = await loop.run_in_executor(None, consumer.poll, 0.01)
        if msg is None:
            await asyncio.sleep(0.001)
            continue
        if msg.error():
            logger.warning(f"Kafka message error: {msg.error()}")
            continue
        try:
            data = json.loads(msg.value())
            await ws_managers[topic].push(data)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")

# ---------------- Startup ----------------
@app.on_event("startup")
async def startup():
    # start broadcast loop cho từng manager
    for manager in ws_managers.values():
        asyncio.create_task(manager.broadcast_loop())

    # topic configuration: topic -> số consumer song song
    topic_config = {
        "eboard_table": 40,    # 2000 partition -> 40 consumer song song
        "eboard_foreign": 32,  # 1600 partition -> 32 consumer
        "indices": 2           # 30 partition -> 2 consumer
    }

    for topic, num_consumers in topic_config.items():
        for i in range(num_consumers):
            asyncio.create_task(kafka_consumer_task(topic, i))

    logger.info("Kafka consumers + broadcast loops started")

# ---------------- WebSocket endpoints ----------------
async def websocket_endpoint(ws: WebSocket, topic: str):
    manager = ws_managers[topic]
    await manager.register(ws)
    try:
        while True:
            await asyncio.sleep(1)  # keep alive
    except WebSocketDisconnect:
        manager.unregister(ws)

@app.websocket("/ws/eboard_table")
async def ws_eboard_table(ws: WebSocket):
    await websocket_endpoint(ws, "eboard_table")

@app.websocket("/ws/eboard_foreign")
async def ws_eboard_foreign(ws: WebSocket):
    await websocket_endpoint(ws, "eboard_foreign")

@app.websocket("/ws/indices")
async def ws_indices(ws: WebSocket):
    await websocket_endpoint(ws, "indices")
