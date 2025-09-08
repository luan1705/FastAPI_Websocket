import json
import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from confluent_kafka import Producer
import time
from datetime import datetime, time as dtime, date
import threading
from filtered_stock.active_stocks import active_stocks
import signal
import sys
import logging

# -------------------- Logging setup --------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stream.log"),
        logging.StreamHandler()
    ])

# -------------------- Globals --------------------
stop_event = threading.Event()
last_msg_time = None
holiday = [date(2026, 1, 1)]

# Kafka Producer tối ưu batch
producer = Producer({
    'bootstrap.servers':'broker:29092',
    'linger.ms':0,
    'batch.num.messages':1000,
    'queue.buffering.max.kbytes': 32768,
    'compression.type': 'lz4',
    'acks': '1'
})
topic_name = "eboard_foreign"

# -------------------- Shutdown handler --------------------
def shutdown_handler(sig, frame):
    logging.info("🔄 Shutdown signal received, flushing producer...")
    producer.flush()
    logging.info("✅ Producer flushed. Exiting.")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, shutdown_handler)  # Docker stop

# -------------------- Trading time check --------------------
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


# -------------------- Message handler --------------------
def on_message(message):
    global last_msg_time
    try:
        data = message.get('Content', '{}')
        data = json.loads(data)
        symbol=data['Symbol']
        #lọc cổ phiếu
        # if symbol not in active_stocks:
        #     return

        result = {
            'function': 'eboard_foreign',
            'content': {
                'symbol': data['Symbol'],
                'buy': data['BuyVol'],
                'sell': data['SellVol'],
                'room': data['CurrentRoom']
            }
        }
        producer.produce(
            topic=topic_name,
            key=symbol,
            value=json.dumps(result)
            )
        producer.poll(0)
        last_msg_time = time.time()

    except Exception as e:
        logging.exception("❗ Lỗi giải mã JSON: ")

# -------------------- Stream error handlers --------------------
def on_error(error):
    logging.error("❗ Lỗi: %s", error)
    stop_event.set()

def on_close():
    logging.info("🔒 Kết nối đã đóng")
    stop_event.set()

def on_open():
    logging.info("🔓 Kết nối đã mở")

# -------------------- Main streaming loop --------------------
def stream():
    global last_msg_time
    while True:
        try:
            stop_event.clear()
            mm = MarketDataStream(config, MarketDataClient(config))
            last_msg_time = time.time()

            mm.start(on_message, on_error, "R:ALL")

            while is_trading_time() and not stop_event.is_set():
                time.sleep(1)
                if time.time() - last_msg_time > 10:
                    logging.warning("⚠️ Không có data mới trong 10s, reconnect...")
                    stop_event.set()

        except Exception as e:
            logging.exception("⚠️ Lỗi trong stream():")
        import json
import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from confluent_kafka import Producer
import time
from datetime import datetime, time as dtime, date
import threading
from filtered_stock.active_stocks import active_stocks
import signal
import sys
import logging

# -------------------- Logging setup --------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stream.log"),
        logging.StreamHandler()
    ])

# -------------------- Globals --------------------
stop_event = threading.Event()
last_msg_time = None
holiday = [date(2026, 1, 1)]

# Kafka Producer tối ưu batch
producer = Producer({
    'bootstrap.servers':'broker:29092',
    'linger.ms':5,
    'batch.num.messages':1000,
    'queue.buffering.max.kbytes': 32768,
    'compression.type': 'lz4',
    'acks': '1' 
})
topic_name = "eboard_table"

# -------------------- Shutdown handler --------------------
def shutdown_handler(sig, frame):
    logging.info("🔄 Shutdown signal received, flushing producer...")
    producer.flush()
    logging.info("✅ Producer flushed. Exiting.")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, shutdown_handler)  # Docker stop

# -------------------- Trading time check --------------------
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


# -------------------- Message handler --------------------
def on_message(message):
    global last_msg_time
    try:
        data = message.get('Content', '{}')
        data = json.loads(data)
        symbol=data['Symbol']
        #lọc cp
        # if symbol not in active_stocks:
        #     return

        result={
            'function':'eboard_table',
            'content': {
                'symbol': data['Symbol'],
                'ceiling': data['Ceiling'] / 1000,
                'floor': data['Floor'] / 1000,
                'refPrice': data['RefPrice'] / 1000,
                'buy':{
                    'price': [data['BidPrice1'] / 1000,data['BidPrice2'] / 1000,data['BidPrice3'] / 1000],
                    'vol': [data['BidVol1'] ,data['BidVol2'] ,data['BidVol3'] ]
                },
                'match':{
                    'price': data['LastPrice'] / 1000,
                    'vol': data['LastVol'],
                    'change': data['Change'],
                    'ratioChange': data['RatioChange'],
                },
                'sell':{
                    'price': [data['AskPrice1'] / 1000,data['AskPrice2'] / 1000,data['AskPrice3'] / 1000],
                    'vol': [data['AskVol1'] ,data['AskVol2'] ,data['AskVol3'] ]
                },
                'totalVol': data['TotalVol'],
                'high': data['High'] / 1000,
                'low': data['Low'] / 1000
            }

        }
        
        producer.produce(
            topic=topic_name,
            key=symbol,
            value=json.dumps(result)
            )
        producer.poll(0)
        last_msg_time = time.time()

    except Exception as e:
        logging.exception("❗ Lỗi giải mã JSON: ")

# -------------------- Stream error handlers --------------------
def on_error(error):
    logging.error("❗ Lỗi: %s" ,error)
    stop_event.set()

def on_close():
    logging.info("🔒 Kết nối đã đóng")
    stop_event.set()

def on_open():
    logging.info("🔓 Kết nối đã mở")

# -------------------- Main streaming loop --------------------
def stream():
    global last_msg_time
    while True:
        try:
            stop_event.clear()
            mm = MarketDataStream(config, MarketDataClient(config))
            last_msg_time = time.time()

            mm.start(on_message, on_error, "X:ALL")

            while is_trading_time() and not stop_event.is_set():
                time.sleep(1)
                if time.time() - last_msg_time > 10:
                    logging.warning("⚠️ Không có data mới trong 10s, reconnect...")
                    stop_event.set()

        except Exception as e:
            logging.exception("⚠️ Lỗi trong stream():")
        finally:
            try:
                mm.stop()
                logging.info("🛑 Đã dừng stream cũ trước khi reconnect")
            except Exception:
                pass

            logging.info("🔄 Thử kết nối lại sau 5 giây...")
            time.sleep(5)

if __name__ == "__main__":
    stream()

