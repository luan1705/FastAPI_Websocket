from kafka import KafkaConsumer
import json
from pandas import json_normalize
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float
from sqlalchemy.dialects.postgresql import insert as pg_insert
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kết nối DB
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True
)

# Định nghĩa bảng
metadata = MetaData()
eboard_foreign_table = Table(
    "eboard_foreign", metadata,
    Column("symbol", String, primary_key=True),
    Column("buy", Float),
    Column("sell", Float),
    Column("room", Float),
    schema="history_data"
)

def processandsavedb(data):
    try:
        content = data.get("content", {})
        df = json_normalize(content)

        # Chỉ lấy đúng 4 cột cần thiết
        df = df[["symbol", "buy", "sell", "room"]]

        with engine.begin() as conn:
            for row in df.to_dict(orient="records"):
                stmt = pg_insert(eboard_foreign_table).values(row)
                update_dict = {c.name: getattr(stmt.excluded, c.name)
                               for c in eboard_foreign_table.columns
                               if c.name != "symbol"}
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol"],
                    set_=update_dict
                )
                conn.execute(stmt)

        logger.info(f"Upserted symbols: {df['symbol'].tolist()} vào history_data.eboard_foreign")

    except Exception as e:
        logger.error(f"Error in processandsavedb: {e}")
        logger.error(traceback.format_exc())

def eboard_foreign_consumer():
    print("Function eboard_foreign_consumer() called", flush=True)

    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                "eboard_foreign",
                bootstrap_servers=["broker:29092"],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id=None,
                consumer_timeout_ms=30000,
                fetch_min_bytes=1,
                fetch_max_wait_ms=500
            )

            logger.info("🟢 Bắt đầu lắng nghe Kafka và xử lý đa luồng...")
            logger.info(f"✅ Connected to Kafka. Subscribed topics: {consumer.subscription()}")

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
            logger.info("Retrying in 5 seconds...")
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
