from confluent_kafka import Consumer
import json
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from sqlalchemy.dialects.postgresql import insert as pg_insert
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor
import time

# Thiết lập logging để debug dễ hơn
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connection string với error handling
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True
)

metadata = MetaData()

# Định nghĩa bảng eboard_table với khóa chính là symbol
eboard_table_pg = Table(
    "eboard_table", metadata,
    Column('symbol', String, primary_key=True),
    Column('ceiling', Float),
    Column('floor', Float),
    Column('refPrice', Float),
    Column('buyPrice3', Float), Column('buyVol3', Float),
    Column('buyPrice2', Float), Column('buyVol2', Float),
    Column('buyPrice1', Float), Column('buyVol1', Float),
    Column('matchPrice', Float), Column('matchVol', Float),
    Column('matchChange', Float), Column('matchRatioChange', Float),
    Column('sellPrice1', Float), Column('sellVol1', Float),
    Column('sellPrice2', Float), Column('sellVol2', Float),
    Column('sellPrice3', Float), Column('sellVol3', Float),
    Column('totalVol', Float), Column('high', Float), Column('low', Float),
    schema="history_data"
)

metadata.create_all(engine)

def processandsavedb(data):
    try:
        content = data.get("content", {})
        df = json_normalize(content)

        buy_price_expanded = df['buy.price'].apply(pd.Series).rename(columns={0: 'buyPrice1', 1: 'buyPrice2', 2: 'buyPrice3'})
        buy_vol_expanded = df['buy.vol'].apply(pd.Series).rename(columns={0: 'buyVol1', 1: 'buyVol2', 2: 'buyVol3'})
        sell_price_expanded = df['sell.price'].apply(pd.Series).rename(columns={0: 'sellPrice1', 1: 'sellPrice2', 2: 'sellPrice3'})
        sell_vol_expanded = df['sell.vol'].apply(pd.Series).rename(columns={0: 'sellVol1', 1: 'sellVol2', 2: 'sellVol3'})

        df = pd.concat([
            df.drop(['buy.price', 'buy.vol', 'sell.price', 'sell.vol'], axis=1),
            buy_price_expanded,
            buy_vol_expanded,
            sell_price_expanded,
            sell_vol_expanded
        ], axis=1)

        column_mapping = {
            'match.price': 'matchPrice',
            'match.vol': 'matchVol',
            'match.change': 'matchChange',
            'match.ratioChange': 'matchRatioChange'
        }
        df = df.rename(columns=column_mapping)

        filtered_columns = ['symbol', 'ceiling', 'floor', 'refPrice',
                           'buyPrice3', 'buyVol3', 'buyPrice2', 'buyVol2',
                           'buyPrice1', 'buyVol1', 'matchPrice', 'matchVol',
                           'matchChange', 'matchRatioChange', 'sellPrice1',
                           'sellVol1', 'sellPrice2', 'sellVol2', 'sellPrice3',
                           'sellVol3', 'totalVol', 'high', 'low']

        df = df[filtered_columns]

        # Chuyển DataFrame thành list dict để insert
        data_list = df.to_dict(orient='records')

        with engine.begin() as conn:
            stmt = pg_insert(eboard_table_pg).values(data_list)
            update_dict = {c.name: c for c in stmt.excluded if c.name != "symbol"}
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol'],
                set_=update_dict
            )
            conn.execute(stmt)

        logger.info(f"Upserted {df['symbol'].tolist()} rows into history_data.eboard_table")

    except Exception as e:
        logger.error(f"Error in processandsavedb: {e}")
        logger.error(traceback.format_exc())

def eboard_table_consumer():
    print("Function eboard_table_consumer() called", flush=True)
    
    while True:  # Vòng lặp vô tận để tự động khởi động lại
        consumer = None
        executor = None
        
        try:
            # Tạo consumer với cấu hình cải tiến
            consumer = KafkaConsumer(
                "eboard_table",
                bootstrap_servers=["broker:29092"],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='eboard_consumer_group',  # Thêm group_id để quản lý offset
                enable_auto_commit=True,  # Tự động commit offset
                auto_commit_interval_ms=1000,  # Commit mỗi 1 giây
                # Bỏ consumer_timeout_ms để không bị timeout
                fetch_min_bytes=1,
                fetch_max_wait_ms=500,
                max_poll_records=100,  # Xử lý tối đa 100 records mỗi lần poll
                session_timeout_ms=30000,  # Session timeout 30s
                heartbeat_interval_ms=3000,  # Heartbeat mỗi 3s
                max_poll_interval_ms=300000,  # Max poll interval 5 phút
            )

            logger.info("🟢 Bắt đầu lắng nghe Kafka và xử lý đa luồng...")
            logger.info(f"✅ Connected to Kafka. Subscribed topics: {consumer.subscription()}")

            # Tạo thread pool
            executor = ThreadPoolExecutor(max_workers=20)
            
            # Biến đếm heartbeat
            last_heartbeat = time.time()

            try:
                # Vòng lặp chính để consume messages
                for msg in consumer:
                    current_time = time.time()
                    
                    # Log heartbeat mỗi 30 giây
                    if current_time - last_heartbeat > 30:
                        logger.info(f"💓 Consumer is alive - processed messages at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                        last_heartbeat = current_time
                    
                    logger.info(f"📨 Received message from topic: {msg.topic}, partition: {msg.partition}, offset: {msg.offset}")
                    
                    # Submit task to thread pool
                    future = executor.submit(processandsavedb, msg.value)
                    
                    # Optional: Có thể thêm callback để handle kết quả
                    # future.add_done_callback(lambda f: logger.info("Task completed") if f.exception() is None else logger.error(f"Task failed: {f.exception()}"))

            except KeyboardInterrupt:
                logger.info("🛑 Received interrupt signal, shutting down gracefully...")
                break  # Thoát khỏi vòng lặp while True
                
            except Exception as consume_error:
                logger.error(f"❌ Error consuming messages: {consume_error}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                # Không raise exception, để vòng lặp while True tự động retry

        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")

        finally:
            # Cleanup resources
            if executor:
                logger.info("🔄 Shutting down executor...")
                executor.shutdown(wait=True, timeout=30)
                
            if consumer:
                logger.info("🔄 Closing consumer...")
                try:
                    consumer.close()
                except:
                    pass
            
            logger.info("💤 Waiting 10 seconds before retry...")
            time.sleep(10)  # Chờ 10 giây trước khi retry

if __name__ == "__main__":
    try:
        logger.info("🚀 Starting Kafka consumer application...")
        eboard_table_consumer()
    except KeyboardInterrupt:
        logger.info("👋 Application stopped by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")