from fastapi import WebSocket, FastAPI
import asyncio
import time
from kafka import KafkaConsumer
import json
from datetime import datetime, time as dtime, date
from typing import Optional

# Danh sách ngày nghỉ lễ
holiday = [date(2026, 1, 1)]

# Kiểm tra giờ giao dịch
def is_trading_time():
    now = datetime.now()
    today = now.date()
    if now.weekday() >= 5 or today in holiday:
        return False

    t9h = dtime(9, 0)
    t12h = dtime(12, 0)
    t13h = dtime(13, 0)
    t15h = dtime(15, 0)

    check = now.time()
    return (t9h <= check <= t12h) or (t13h <= check <= t15h)

# Tạo KafkaConsumer
def kafka_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=['172.20.0.3:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id=None
    )

# Lấy message từ Kafka (non-blocking)
def get_kafka_msg(consumer: KafkaConsumer) -> Optional[dict]:
    msg_pack = consumer.poll(timeout_ms=1000)
    for _, records in msg_pack.items():
        for record in records:
            return record.value  # đã deserialize ở KafkaConsumer
    return None

app = FastAPI()

@app.websocket("/ws/eboard_table")
async def websocket_dashboard(websocket: WebSocket):
    await websocket.accept()
    try:
        # Gửi thông báo kết nối thành công
        await websocket.send_json({
            "status": "connected",
            "message": "Kết nối thành công với WebSocket. Yêu cầu nhập keyword!!!"
        })

        msg = await websocket.receive_text()

        # Xác thực keyword
        if msg != 'VNSFintech':
            await websocket.send_json({
                "status": "unauthorized",
                "message": "Mã xác thực không hợp lệ"
            })
            await websocket.close()
            return

        await websocket.send_json({
            "status": "authenticated",
            "message": "Xác thực thành công. Đang gửi dữ liệu..."
        })

        consumer = kafka_consumer("eboard_table")

        while True:
            if not is_trading_time():
                await websocket.send_json({
                    "status": "no_data",
                    "message": "Ngoài giờ giao dịch - không có dữ liệu realtime",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                })
                await asyncio.sleep(10)
                continue

            # Lấy dữ liệu Kafka (chạy trong thread để không block loop)
            data = await asyncio.get_event_loop().run_in_executor(
                None, lambda: get_kafka_msg(consumer)
            )

            print("📩 Received from Kafka:", data)

            if data:
                await websocket.send_json(data)
            else:
                await asyncio.sleep(1)

    except Exception as e:
        print(f"❌ Lỗi WebSocket: {e}")
        try:
            await websocket.send_json({
                "status": "error",
                "message": f"Lỗi kết nối WebSocket: {str(e)}"
            })
        except:
            pass
  

# @app.websocket("/ws/eboard_foreign")
# async def websocket_foreign(websocket: WebSocket):
#     await websocket.accept()
#     try:
#         await websocket.send_json({"status": "connected", "message": "Kết nối thành công với WebSocket. Yêu cầu nhập keyword!!!"})
#         msg = await websocket.receive_text()
#         if msg != 'VNSFintech':
#             await websocket.send_json({"status": "unauthorized", "message": "Mã xác thực không hợp lệ"})
#             await websocket.close()
#             return

#         await websocket.send_json({"status": "authenticated", "message": "Xác thực thành công. Đang gửi dữ liệu..."})
#         consumer = kafka_consumer("eboard_foreign")

#         while True:
#             if not is_trading_time():
#                 await websocket.send_json({
#                     "status": "no_data",
#                     "message": "Ngoài giờ giao dịch - không có dữ liệu realtime",
#                     "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
#                 })
#                 await asyncio.sleep(10)
#                 continue

#             data = await asyncio.get_event_loop().run_in_executor(None, lambda: next(consumer).value)
#             await websocket.send_json(data)

#     except Exception as e:
#         print(f"❌ Lỗi WebSocket: {e}")
#         try:
#             await websocket.send_json({"status": "error", "message": f"Lỗi kết nối WebSocket: {str(e)}"})
#         except:
#             pass   


# @app.websocket("/ws/index")
# async def websocket_index(websocket: WebSocket):
#     await websocket.accept()
#     try:
#         await websocket.send_json({"status": "connected", "message": "Kết nối thành công với WebSocket. Yêu cầu nhập keyword!!!"})
#         msg = await websocket.receive_text()
#         if msg != 'VNSFintech':
#             await websocket.send_json({"status": "unauthorized", "message": "Mã xác thực không hợp lệ"})
#             await websocket.close()
#             return

#         await websocket.send_json({"status": "authenticated", "message": "Xác thực thành công. Đang gửi dữ liệu..."})
#         consumer = kafka_consumer("index")

#         while True:
#             if not is_trading_time():
#                 await websocket.send_json({
#                     "status": "no_data",
#                     "message": "Ngoài giờ giao dịch - không có dữ liệu realtime",
#                     "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
#                 })
#                 await asyncio.sleep(10)
#                 continue

#             data = await asyncio.get_event_loop().run_in_executor(None, lambda: next(consumer).value)
#             await websocket.send_json(data)

#     except Exception as e:
#         print(f"❌ Lỗi WebSocket: {e}")
#         try:
#             await websocket.send_json({"status": "error", "message": f"Lỗi kết nối WebSocket: {str(e)}"})
#         except:
#             pass   
