from kafka import KafkaConsumer
import json
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor

# Thiết lập logging để debug dễ hơn
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connection string với error handling
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,  # Tắt SQL logging để giảm noise
    pool_pre_ping=True  # Kiểm tra connection trước khi sử dụng
)

def processandsavedb(data):
    try:
        content = data.get("content", {})
        df = json_normalize(content)
        df.to_sql("eboard_foreign", engine, schema="history_data", if_exists="append", index=False)
        x=df["symbol"]
        logger.info(f"Inserted {x} into history_data.eboard_foreign")
    except Exception as e:
        logger.error(f"Error in processandsavedb: {e}")
        logger.error(traceback.format_exc())

def eboard_foreign_consumer():
    """Tạo và chạy KafkaConsumer"""
    print("Function eboard_foreign_consumer() called", flush=True)
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                "eboard_foreign",
                bootstrap_servers=["172.20.0.3:9092"],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id=None,
                consumer_timeout_ms=30000,  # Timeout sau 30 giây nếu không có message
                fetch_min_bytes=1,
                fetch_max_wait_ms=500
            )
            
            logger.info("🟢 Bắt đầu lắng nghe Kafka và xử lý đa luồng...")
            
            # Test consumer connection
            topics = consumer.subscription()
            logger.info(f"✅ Connected to Kafka. Subscribed topics: {topics}")
            
            executor = ThreadPoolExecutor(max_workers=20)
            
            try:
                for msg in consumer:
                    logger.info(f"Received message from topic: {msg.topic}, partition: {msg.partition}")
                    data = msg.value
                    executor.submit(processandsavedb, data)
                    
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
                
            except Exception as consume_error:
                logger.error(f"Error consuming messages: {consume_error}")
                raise
                
        except Exception as e:
            retry_count += 1
            logger.error(f"Kafka connection failed (attempt {retry_count}): {e}")
            if retry_count >= max_retries:
                logger.error("Max retries reached. Exiting...")
                raise
            logger.info(f"Retrying in 5 seconds...")
            import time
            time.sleep(5)
        
        finally:
            try:
                consumer.close()
                executor.shutdown(wait=True)
                logger.info("Consumer and executor closed")
            except:
                pass

if __name__ == "__main__":
    try:
        eboard_foreign_consumer()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")