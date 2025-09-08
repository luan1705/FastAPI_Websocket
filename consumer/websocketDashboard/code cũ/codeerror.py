from fastapi import WebSocket, FastAPI, WebSocketDisconnect
import asyncio
import time
from kafka import KafkaConsumer
import json
from datetime import datetime, time as dtime, date
from typing import Optional
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Danh sách ngày nghỉ lễ (có thể mở rộng thêm)
holiday = [
    date(2025, 1, 1),   # Tết Dương lịch
    date(2025, 4, 30),  # 30/4
    date(2025, 5, 1),   # Quốc tế lao động
    date(2025, 9, 2),   # Quốc khánh
    # Thêm các ngày lễ khác...
]

def is_trading_time():
    """Kiểm tra có đang trong giờ giao dịch hay không"""
    now = datetime.now()
    today = now.date()
    
    # Kiểm tra cuối tuần và ngày lễ
    if now.weekday() >= 5 or today in holiday:
        return False
    
    # Định nghĩa các khung giờ giao dịch
    morning_start = dtime(9, 0)
    morning_end = dtime(11, 30)
    afternoon_start = dtime(13, 0)
    afternoon_end = dtime(15, 0)
    
    current_time = now.time()
    
    # Kiểm tra trong khung giờ giao dịch
    return (morning_start <= current_time <= morning_end) or \
           (afternoon_start <= current_time <= afternoon_end)

def create_kafka_consumer(topic: str) -> KafkaConsumer:
    """Tạo Kafka consumer với error handling"""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['172.20.0.3:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  # Tạm thời đổi thành earliest để test
            group_id='websocket_consumer_group',  # Thêm group_id để tránh duplicate messages
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            consumer_timeout_ms=1000,
            max_poll_records=10,  # Giới hạn số records mỗi lần poll
            # Thêm config để force sử dụng đúng broker
            api_version=(2, 0, 1),
            connections_max_idle_ms=540000,
            request_timeout_ms=40000,
            # WORKAROUND: Force client to ignore broker metadata
            security_protocol='PLAINTEXT',
            metadata_max_age_ms=30000,
        )
        
        # Kiểm tra connection trước khi return
        partitions = consumer.partitions_for_topic(topic)
        if partitions is None:
            raise Exception(f"Topic '{topic}' không tồn tại hoặc không accessible")
            
        logger.info(f"✅ Kafka consumer đã được tạo cho topic: {topic} với {len(partitions)} partitions")
        return consumer
    except Exception as e:
        logger.error(f"❌ Lỗi tạo Kafka consumer: {e}")
        raise

def get_kafka_messages(consumer: KafkaConsumer) -> list:
    """Lấy tất cả messages từ Kafka (non-blocking)"""
    try:
        msg_pack = consumer.poll(timeout_ms=100)  # Giảm timeout để responsive hơn
        messages = []
        
        for topic_partition, records in msg_pack.items():
            for record in records:
                messages.append({
                    'data': record.value,
                    'offset': record.offset,
                    'partition': record.partition,
                    'timestamp': record.timestamp
                })
        
        return messages
    except Exception as e:
        logger.error(f"❌ Lỗi đọc Kafka message: {e}")
        return []

app = FastAPI(title="Kafka WebSocket Service", version="1.0.0")

# Biến global để theo dõi connections
active_connections = set()

@app.get("/")
async def root():
    return {
        "message": "Kafka WebSocket Service đang hoạt động",
        "endpoints": {
            "websocket": "/ws/eboard_table",
            "health": "/health"
        }
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "trading_time": is_trading_time(),
        "active_connections": len(active_connections),
        "timestamp": datetime.now().isoformat()
    }

@app.websocket("/ws/eboard_table")
async def websocket_eboard_table(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    consumer = None
    
    try:
        logger.info("🔌 Client mới kết nối WebSocket")
        
        # Gửi thông báo kết nối thành công
        await websocket.send_json({
            "status": "connected",
            "message": "Kết nối thành công với WebSocket. Vui lòng nhập keyword xác thực!",
            "timestamp": datetime.now().isoformat()
        })
        
        # Nhận keyword xác thực
        try:
            msg = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
        except asyncio.TimeoutError:
            await websocket.send_json({
                "status": "timeout",
                "message": "Timeout - không nhận được keyword trong 30 giây"
            })
            return
        
        # Xác thực keyword
        if msg.strip() != 'VNSFintech':
            await websocket.send_json({
                "status": "unauthorized", 
                "message": "Mã xác thực không hợp lệ"
            })
            return
        
        logger.info("✅ Client đã xác thực thành công")
        await websocket.send_json({
            "status": "authenticated",
            "message": "Xác thực thành công. Đang khởi tạo kết nối Kafka...",
            "timestamp": datetime.now().isoformat()
        })
        
        # Tạo Kafka consumer
        try:
            logger.info("🔄 Đang khởi tạo Kafka consumer...")
            consumer = create_kafka_consumer("eboard_table")
            logger.info("✅ Kafka consumer đã sẵn sàng")
        except Exception as e:
            error_msg = f"Lỗi kết nối Kafka: {str(e)}"
            logger.error(f"❌ {error_msg}")
            await websocket.send_json({
                "status": "error",
                "message": error_msg,
                "debug_info": "Kiểm tra Kafka broker config: advertised.listeners phải là 172.20.0.3:9092"
            })
            return
        
        await websocket.send_json({
            "status": "ready",
            "message": "Đã sẵn sàng nhận dữ liệu từ Kafka",
            "timestamp": datetime.now().isoformat()
        })
        
        # Vòng lặp chính để đọc và gửi dữ liệu
        consecutive_no_data = 0
        while True:
            try:
                # TÍNH NĂNG GIỜ GIAO DỊCH TẠM THỜI TẮT ĐỂ TEST
                # if not is_trading_time():
                #     if consecutive_no_data == 0:  # Chỉ gửi thông báo lần đầu
                #         await websocket.send_json({
                #             "status": "no_trading",
                #             "message": "Ngoài giờ giao dịch - tạm dừng nhận dữ liệu",
                #             "timestamp": datetime.now().isoformat(),
                #             "next_trading_session": "09:00 - 11:30 và 13:00 - 15:00"
                #         })
                #     await asyncio.sleep(30)  # Kiểm tra lại sau 30 giây
                #     consecutive_no_data += 1
                #     continue
                
                # Reset counter khi vào giờ giao dịch
                consecutive_no_data = 0
                
                # Lấy dữ liệu từ Kafka (chạy trong thread pool)
                messages = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: get_kafka_messages(consumer)
                )
                
                if messages:
                    # Gửi từng message
                    for msg in messages:
                        await websocket.send_json({
                            "status": "data",
                            "payload": msg['data'],
                            "kafka_info": {
                                "offset": msg['offset'],
                                "partition": msg['partition'],
                                "kafka_timestamp": msg['timestamp']
                            },
                            "timestamp": datetime.now().isoformat()
                        })
                    
                    logger.info(f"📤 Đã gửi {len(messages)} messages đến client")
                else:
                    # Không có dữ liệu mới, sleep ngắn
                    await asyncio.sleep(0.1)
                    
            except WebSocketDisconnect:
                logger.info("🔌 Client đã ngắt kết nối")
                break
            except Exception as e:
                logger.error(f"❌ Lỗi trong vòng lặp chính: {e}")
                await websocket.send_json({
                    "status": "error",
                    "message": f"Lỗi xử lý dữ liệu: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
                await asyncio.sleep(1)
                
    except WebSocketDisconnect:
        logger.info("🔌 WebSocket đã bị ngắt kết nối")
    except Exception as e:
        logger.error(f"❌ Lỗi WebSocket không mong muốn: {e}")
        try:
            await websocket.send_json({
                "status": "fatal_error",
                "message": f"Lỗi nghiêm trọng: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })
        except:
            pass
    finally:
        # Cleanup
        active_connections.discard(websocket)
        if consumer:
            try:
                consumer.close()
                logger.info("🔒 Đã đóng Kafka consumer")
            except Exception as e:
                logger.error(f"❌ Lỗi đóng Kafka consumer: {e}")

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(
#         "main:app", 
#         host="0.0.0.0", 
#         port=8000, 
#         reload=True,
#         log_level="info"
#     )