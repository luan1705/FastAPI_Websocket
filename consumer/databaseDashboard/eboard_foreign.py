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
import signal
import sys

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# DB setup
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True
)
metadata = MetaData()
eboard_foreign_table = Table(
    "eboard_foreign", metadata,
    Column("symbol", String, primary_key=True),
    Column("buy", Float),
    Column("sell", Float),
    Column("room", Float),
    schema="history_data"
)
metadata.create_all(engine)
# Shutdown flag for Docker-safe stop
shutdown_flag = False
def signal_handler(sig, frame):
    global shutdown_flag
    logger.info("🛑 Received shutdown signal, stopping consumer...")
    shutdown_flag = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)  # optional for local testing


# Function xử lý và upsert vào DB
def processandsavedb(data, consumer, msg):
    try:
        content = data.get("content", {})
        df = json_normalize(content)
        df = df[["symbol", "buy", "sell", "room"]]

        with engine.begin() as conn:
            for row in df.to_dict(orient="records"):
                stmt = pg_insert(eboard_foreign_table).values(row)
                update_dict = {c.name: getattr(stmt.excluded, c.name)
                               for c in eboard_foreign_table.columns if c.name != "symbol"}
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol"],
                    set_=update_dict
                )
                conn.execute(stmt)

        logger.info(f"Upserted symbols: {df['symbol'].tolist()} vào history_data.eboard_foreign")
        consumer.commit(message=msg)
    except Exception as e:
        logger.error(f"Error in processandsavedb: {e}")
        logger.error(traceback.format_exc())

# Consumer chính
def eboard_foreign_consumer():
    global shutdown_flag
    logger.info("🚀 Starting Kafka consumer...")
    config = {
            'bootstrap.servers': 'broker:29092',
            'group.id': 'eboard_foreign_group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'session.timeout.ms': 30000
        }
    consumer = None
    executor = ThreadPoolExecutor(max_workers=20)
    try:
        consumer = Consumer(config)
        topics=["eboard_foreign"]
        consumer.subscribe(topics)
        logger.info(f"🟢 Connected to Kafka. Subscribed topics: {topics}")
        
        last_heartbeat = time.time()
        
        while not shutdown_flag:
            msg = consumer.poll(timeout=0.01)
            if msg is None:
                continue
            if msg.error():
                logger.warning(f"Kafka message error: {msg.error()}")
                continue
            # Heartbeat log
            current_time = time.time()
            if current_time - last_heartbeat > 30:
                logger.info(f"💓 Consumer is alive at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                last_heartbeat = current_time
            try:
                raw = msg.value()
                if isinstance(raw, bytes):
                    raw = raw.decode('utf-8')
                data = json.loads(raw)
                executor.submit(processandsavedb, data, consumer, msg)
            except Exception as e:
                logger.error(f"JSON decode error: {e}")

    except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            logger.error(traceback.format_exc())

    finally:
            if executor:
                logger.info("🔄 Shutting down executor...")
                executor.shutdown(wait=True)
            if consumer:
                logger.info("🔄 Closing consumer...")
                try:
                    consumer.close()
                except:
                    pass
            if not shutdown_flag:
                logger.info("💤 Waiting 10 seconds before retry...")
                time.sleep(10)

    logger.info("👋 Kafka consumer stopped gracefully.")

if __name__ == "__main__":
    eboard_foreign_consumer()

