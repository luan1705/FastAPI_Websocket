from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
import json
from kafka import KafkaConsumer
from datetime import datetime, date, time as dtime
import logging
import contextlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Kafka WebSocket Service Optimized")

holiday = [
    date(2025, 1, 1),
    date(2025, 4, 30),
    date(2025, 5, 1),
    date(2025, 9, 2),
]

def is_trading_time():
    now = datetime.now()
    if now.weekday() >= 5 or now.date() in holiday:
        return False
    morning_start = dtime(9, 0)
    morning_end = dtime(11, 30)
    afternoon_start = dtime(13, 0)
    afternoon_end = dtime(15, 0)
    return (morning_start <= now.time() <= morning_end) or (afternoon_start <= now.time() <= afternoon_end)

class ConnectionManager:
    def __init__(self):
        self.active_connections: set[WebSocket] = set()
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"Client connected: {websocket.client}")
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        logger.info(f"Client disconnected: {websocket.client}")

manager = ConnectionManager()

class LatestMessageHolder:
    def __init__(self):
        self._message = None
        self._event = asyncio.Event()
    
    def set_message(self, msg):
        self._message = msg
        self._event.set()
    
    async def get_message(self):
        await self._event.wait()
        msg = self._message
        self._event.clear()
        return msg

async def kafka_consumer_task(topic: str, latest_message_holder: LatestMessageHolder):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['broker:29092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id=f'websocket_consumer_group_{topic}',
        enable_auto_commit=True,
        max_poll_records=10,
        consumer_timeout_ms=1000,
    )
    logger.info(f"Kafka consumer started for topic {topic}")
    loop = asyncio.get_event_loop()

    try:
        while True:
            msg_pack = await loop.run_in_executor(None, lambda: consumer.poll(timeout_ms=500))
            messages = []
            for tp, records in msg_pack.items():
                for record in records:
                    messages.append(record.value)  # chỉ lấy JSON
            if messages:
                latest_message_holder.set_message(messages[-1])
            else:
                await asyncio.sleep(0.1)
    except Exception as e:
        logger.error(f"Kafka consumer error on {topic}: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer closed for topic {topic}")

@app.on_event("startup")
async def startup_event():
    app.state.latest_messages = {
        "eboard_table": LatestMessageHolder(),
        "eboard_foreign": LatestMessageHolder(),
        "indices": LatestMessageHolder(),
    }
    app.state.kafka_tasks = [
        asyncio.create_task(kafka_consumer_task(topic, holder))
        for topic, holder in app.state.latest_messages.items()
    ]
    logger.info("Startup: Kafka consumers created for all topics")

@app.on_event("shutdown")
async def shutdown_event():
    for task in app.state.kafka_tasks:
        task.cancel()
    logger.info("Shutdown: Kafka consumer tasks cancelled")

async def websocket_handler(websocket: WebSocket, topic: str):
    await manager.connect(websocket)

    send_task = None
    try:
        await websocket.send_json({
            "status": "connected",
            "message": f"Kết nối thành công tới {topic}. Vui lòng nhập keyword xác thực!",
            "timestamp": datetime.now().isoformat()
        })

        # Xác thực
        try:
            keyword = await asyncio.wait_for(websocket.receive_text(), timeout=30)
        except asyncio.TimeoutError:
            await websocket.send_json({"status": "timeout", "message": "Timeout không nhận được keyword"})
            return
        
        if keyword.strip() != "VNSFintech":
            await websocket.send_json({"status": "unauthorized", "message": "Mã xác thực không hợp lệ"})
            return
        
        await websocket.send_json({
            "status": "authenticated",
            "message": f"Xác thực thành công, bắt đầu nhận dữ liệu từ {topic}...",
            "timestamp": datetime.now().isoformat()
        })

        client_queue = asyncio.Queue(maxsize=1)

        async def push_latest_message():
            while True:
                msg = await app.state.latest_messages[topic].get_message()
                if client_queue.full():
                    try:
                        client_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                await client_queue.put(msg)

        send_task = asyncio.create_task(push_latest_message())

        while True:
            msg = await client_queue.get()
            await websocket.send_json(msg)

    except WebSocketDisconnect:
        logger.info(f"Client WebSocket disconnected from {topic}")
    except Exception as e:
        logger.error(f"Unexpected error in websocket {topic}: {e}")
        try:
            await websocket.send_json({"status": "error", "message": str(e)})
        except:
            pass
    finally:
        if send_task:
            send_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await send_task
        manager.disconnect(websocket)

@app.websocket("/ws/eboard_table")
async def websocket_eboard_table(ws: WebSocket):
    await websocket_handler(ws, "eboard_table")

@app.websocket("/ws/eboard_foreign")
async def websocket_eboard_foreign(ws: WebSocket):
    await websocket_handler(ws, "eboard_foreign")

@app.websocket("/ws/indices")
async def websocket_indices(ws: WebSocket):
    await websocket_handler(ws, "indices")
