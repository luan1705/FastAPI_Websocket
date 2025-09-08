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

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connection string
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True
)

# Define table schema for upsert
metadata = MetaData()
indices_table = Table(
    "indices", metadata,
    Column("symbol", String, primary_key=True),
    Column("point", Float),
    Column("change", Float),
    Column("ratioChange", Float),
    Column("totalVolume", Float),
    Column("totalValue", Float),
    Column("advancers", Integer),
    Column("noChange", Integer),
    Column("decliners", Integer),
    schema="history_data"
)
metadata.create_all(engine)
shutdown_flag = False
def signal_handler(sig, frame):
    global shutdown_flag
    logger.info("🛑 Received shutdown signal, stopping consumer...")
    shutdown_flag = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)  # optional for local testing



def processandsavedb(data, consumer, msg):
    try:
        content = data.get("content", {})
        df = json_normalize(content)

        # Tách advancersDecliners thành 3 cột
        advancersDecliners_expanded = df['advancersDecliners'].apply(pd.Series)
        advancersDecliners_expanded = advancersDecliners_expanded.rename(columns={
            0: 'advancers',
            1: 'noChange',
            2: 'decliners'
        })

        df = pd.concat([df.drop(['advancersDecliners'], axis=1),
                        advancersDecliners_expanded], axis=1)

        # Chỉ lấy các cột cần thiết
        filtered_columns = ['symbol', 'point', 'change', 'ratioChange',
                            'totalVolume', 'totalValue', 'advancers', 'noChange', 'decliners']
        df = df[filtered_columns]

        # Upsert theo symbol
        with engine.begin() as conn:
            for row in df.to_dict(orient="records"):
                stmt = pg_insert(indices_table).values(row)
                update_dict = {c.name: getattr(stmt.excluded, c.name)
                               for c in indices_table.columns if c.name != "symbol"}
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol"],
                    set_=update_dict
                )
                conn.execute(stmt)

        logger.info(f"Upserted symbols: {df['symbol'].tolist()} into history_data.indices")
        consumer.commit(message=msg)
    except Exception as e:
        logger.error(f"Error in processandsavedb: {e}")
        logger.error(traceback.format_exc())

def indices_consumer():
    global shutdown_flag
    logger.info("🚀 Starting Kafka consumer...")
    config = {
                'bootstrap.servers': 'broker:29092',
                'group.id': 'indices_consumer_group',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': False,
                'session.timeout.ms': 30000
            }
    consumer = None
    executor = ThreadPoolExecutor(max_workers=20)
    try:
        consumer = Consumer(config)
        topics=["indices"]
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
    indices_consumer()

