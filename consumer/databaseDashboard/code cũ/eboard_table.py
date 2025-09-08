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
        buy_price_expanded = df['buy.price'].apply(pd.Series)
        buy_price_expanded = buy_price_expanded.rename(columns={
            0: 'buyPrice1', 
            1: 'buyPrice2', 
            2: 'buyPrice3'
        })
    
        buy_vol_expanded = df['buy.vol'].apply(pd.Series)
        buy_vol_expanded = buy_vol_expanded.rename(columns={
            0: 'buyVol1', 
            1: 'buyVol2', 
            2: 'buyVol3'
        })
    
        sell_price_expanded = df['sell.price'].apply(pd.Series)
        sell_price_expanded = sell_price_expanded.rename(columns={
            0: 'sellPrice1', 
            1: 'sellPrice2', 
            2: 'sellPrice3'
        })
    
        sell_vol_expanded = df['sell.vol'].apply(pd.Series)
        sell_vol_expanded = sell_vol_expanded.rename(columns={
            0: 'sellVol1', 
            1: 'sellVol2', 
            2: 'sellVol3'
        })
    
        df = pd.concat([
            df.drop(['buy.price', 'buy.vol', 'sell.price', 'sell.vol'], axis=1), 
            buy_price_expanded, 
            buy_vol_expanded,
            sell_price_expanded,
            sell_vol_expanded
        ], axis=1)
            
        # Rename columns với kiểm tra tồn tại
        column_mapping = {
            'match.price': 'matchPrice',
            'match.vol': 'matchVol',
            'match.change': 'matchChange',
            'match.ratioChange': 'matchRatioChange'
        }
        df = df.rename(columns=column_mapping)
        
        # Filtered columns
        filtered_columns = ['symbol', 'ceiling', 'floor', 'refPrice',
                           'buyPrice3', 'buyVol3', 'buyPrice2', 'buyVol2',
                           'buyPrice1', 'buyVol1', 'matchPrice', 'matchVol',
                           'matchChange', 'matchRatioChange', 'sellPrice1',
                           'sellVol1', 'sellPrice2', 'sellVol2', 'sellPrice3',
                           'sellVol3', 'totalVol', 'high', 'low']
        
        df = df[filtered_columns]
        df.to_sql("eboard_table", engine, schema="history_data", if_exists="append", index=False)
        x=df["symbol"]
        logger.info(f"Inserted {x} into history.eboard_table")
    except Exception as e:
        logger.error(f"Error in processandsavedb: {e}")
        logger.error(traceback.format_exc())

def eboard_table_consumer():
    """Tạo và chạy KafkaConsumer"""
    print("Function eboard_table_consumer() called", flush=True)
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                "eboard_table",
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
        eboard_table_consumer()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")