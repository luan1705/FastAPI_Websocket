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

# PostgreSQL connection
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True
)
metadata = MetaData()

# Define table
eboard_table = Table(
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

# Graceful shutdown flag
shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    logger.info("🛑 Received shutdown signal, stopping consumer...")
    shutdown_flag = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)  # optional for local testing

# Function to process and save data to DB
def processandsavedb(data, consumer, msg):
    try:
        content = data.get("content", {})
        df = json_normalize(content)

        # Expand buy/sell prices and volumes
        buy_price_expanded = df['buy.price'].apply(pd.Series).rename(columns={0: 'buyPrice1', 1: 'buyPrice2', 2: 'buyPrice3'})
        buy_vol_expanded = df['buy.vol'].apply(pd.Series).rename(columns={0: 'buyVol1', 1: 'buyVol2', 2: 'buyVol3'})
        sell_price_expanded = df['sell.price'].apply(pd.Series).rename(columns={0: 'sellPrice1', 1: 'sellPrice2', 2: 'sellPrice3'})
        sell_vol_expanded = df['sell.vol'].apply(pd.Series).rename(columns={0: 'sellVol1', 1: 'sellVol2', 2: 'sellVol3'})

        df = pd.concat([
            df.drop(['buy.price', 'buy.vol', 'sell.price', 'sell.vol'], axis=1),
            buy_price_expanded, buy_vol_expanded, sell_price_expanded, sell_vol_expanded
        ], axis=1)

        # Rename match columns
        df = df.rename(columns={
            'match.price': 'matchPrice',
            'match.vol': 'matchVol',
            'match.change': 'matchChange',
            'match.ratioChange': 'matchRatioChange'
        })

        # Keep only needed columns
        filtered_columns = [
            'symbol', 'ceiling', 'floor', 'refPrice',
            'buyPrice3', 'buyVol3', 'buyPrice2', 'buyVol2',
            'buyPrice1', 'buyVol1', 'matchPrice', 'matchVol',
            'matchChange', 'matchRatioChange', 'sellPrice1',
            'sellVol1', 'sellPrice2', 'sellVol2', 'sellPrice3',
            'sellVol3', 'totalVol', 'high', 'low'
        ]
        df = df[filtered_columns]

        # Upsert to PostgreSQL
        with engine.begin() as conn:
            for row in df.to_dict(orient="records"):
                stmt = pg_insert(eboard_table).values(row)
                update_dict = {c.name: getattr(stmt.excluded, c.name)
                               for c in eboard_table.columns if c.name != "symbol"}
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol"],
                    set_=update_dict
                )
                conn.execute(stmt)  # Gửi vào DB ngay

        logger.info(f"✅ Upserted {df['symbol'].tolist()} rows into history_data.eboard_table")
        consumer.commit(message=msg)
    except Exception as e:
        logger.error(f"❌ Error in processandsavedb: {e}")
        logger.error(traceback.format_exc())

# Kafka consumer function
def eboard_table_consumer():
    global shutdown_flag
    logger.info("🚀 Starting Kafka consumer...")
    config = {
                'bootstrap.servers': 'broker:29092',
                'group.id': 'eboard_consumer_group',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': False,
                'session.timeout.ms': 30000
            }
    consumer = None
    executor = ThreadPoolExecutor(max_workers=20)
    try:
        consumer = Consumer(config)
        topics=["eboard_table"]
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
            current_time = time.time()
            if current_time - last_heartbeat > 30:
                logger.info(f"💓 Consumer alive at {time.strftime('%Y-%m-%d %H:%M:%S')}")
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
    eboard_table_consumer()
