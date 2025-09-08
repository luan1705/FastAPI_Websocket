from kafka import KafkaConsumer
import json
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from sqlalchemy.dialects.postgresql import insert as pg_insert
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor

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
    Column("raitoChange", Float),
    Column("totalVolume", Float),
    Column("totalValue", Float),
    Column("advancers", Integer),
    Column("noChange", Integer),
    Column("decliners", Integer),
    schema="history_data"
)

def processandsavedb(data):
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
        filtered_columns = ['symbol', 'point', 'change', 'raitoChange',
                            'totalVolume', 'totalValue', 'advancers', 'noChange', 'decliners']
        df = df[filtered_columns]

        # Upsert theo symbol
        with engine.begin() as conn:
            for row in df.to_dict(orient="records"):
                stmt = pg_insert(indices_table).values(row)
                update_dict = {c.name: getattr(stmt.excluded, c.name)
                               for c in indices_table.columns
                               if c.name != "symbol"}
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol"],
                    set_=update_dict
                )
                conn.execute(stmt)

        logger.info(f"Upserted symbols: {df['symbol'].tolist()} into history_data.indices")

    except Exception as e:
        logger.error(f"Error in processandsavedb: {e}")
        logger.error(traceback.format_exc())

def indices_consumer():
    print("Function indices_consumer() called", flush=True)
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                "indices",
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
        indices_consumer()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
