import json
import config
import threading
from datetime import datetime, time as dtime, date
import logging
import signal
from exchange_map import exchange_map
from indices_map import indices_map
from confluent_kafka import Producer
import sys
from fastapi import FastAPI
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient



# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("stream.log"), logging.StreamHandler()],
)

# -------------------- FastAPI --------------------
stop_event = threading.Event()

# -------------------- Trading time check --------------------
def is_trading_time():
    now = datetime.now()
    today = now.date()
    if now.weekday() >= 5: #or today in holiday:
        return False
    t9h = dtime(9, 0)
    t12h = dtime(12, 0)
    t13h = dtime(13, 0)
    t15h = dtime(15, 0)
    return (t9h <= now.time() <= t12h) or (t13h <= now.time() <= t15h)
    
# -------------------- Kafka Producer --------------------
producer = Producer({
    'bootstrap.servers': 'broker:29092',
    'linger.ms': 0,
    'batch.num.messages': 1000,
    'queue.buffering.max.kbytes': 32768,
    'compression.type': 'lz4',
    'acks': '1'
})
def flush_and_exit(code=0, note=""):
    try:
        logging.info("🔄 Flushing producer... %s", note)
        producer.flush()
    except Exception:
        logging.exception("⚠️ Flush error (ignored)")
    logging.info("✅ Exit code %s", code)
    sys.exit(code)

# -------------------- Streaming message handler --------------------
#get exchange_map
def find_exchange(symbol: str, exchange_map: dict) -> str:
    for exchange, symbols in exchange_map.items():
        if symbol in symbols:
            return exchange
    return None  # không tìm thấy

#get indices_map
def find_indice(symbol: str, indices_map: dict) -> str:
    indices_list=[]
    for indices, symbols in indices_map.items():
        if symbol in symbols:
            indices_list.append(indices)
    return indices_list if indices_list else None  # không tìm thấy

def on_message_X(message):
    try:
        data = json.loads(message.get("Content","{}"))
        symbol=data['Symbol']
        exchange=find_exchange(symbol,exchange_map)
        indices=find_indice(symbol,indices_map)
        #lọc cp
        # if symbol != 'ACB':
        #     return
        result={
            'function':'eboard_table',
            'content': {
                'symbol': symbol,
                'exchange': exchange,
                'indices':indices,
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
                    'change': data['Change']/1000,
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
        producer.produce(topic='eboard_table', key=symbol, value=json.dumps(result))
        producer.poll(0)
        
    except Exception:
        logging.exception("❗ X message error")
        
def on_message_R(message):
    try:
        data = json.loads(message.get("Content","{}"))
        symbol=data['Symbol']
        result = {
            'function': 'eboard_foreign',
            'content': {
                'symbol': symbol,
                'buy': data['BuyVol'],
                'sell': data['SellVol'],
                'room': data['CurrentRoom']
            }
        }
        producer.produce(topic='eboard_foreign', key=symbol, value=json.dumps(result))
        producer.poll(0)
    except Exception:
        logging.exception("❗ R message error")

def on_message_MI(message):
    try:
        data = json.loads(message.get("Content","{}"))
        symbol=data['IndexId']
        result = {
            'function': 'indices',
            'content': {
                'symbol': symbol,
                'point': data['IndexValue'],
                'change': data['Change'],
                'ratioChange': data['RatioChange'],
                'totalVolume': data['AllQty'],
                'totalValue': data['AllValue'],
                'advancersDecliners': [
                    data['Advances'],
                    data['NoChanges'],
                    data['Declines']
                ]
            }
        }
        producer.produce(topic='indices', key=symbol, value=json.dumps(result))
        producer.poll(0)
    except Exception:
        logging.exception("❗ MI message error")

# -------------------- Stream lifecycle --------------------
def on_error(error):
    logging.error(f"❗ Lỗi: {error}")

def stream(channel, on_message_func, stream_code):
    try:
        mm = MarketDataStream(config, MarketDataClient(config))
        logging.info(f"🚀 Starting stream {channel} {stream_code}")
        mm.start(on_message_func, on_error, stream_code)
        logging.info(f"✅ Stream {channel} started and running...")
    except Exception:
        logging.exception(f"❌ {channel} stream error (stopped)")


def start_stream(channel, on_message_func, stream_code):
    t = threading.Thread(target=stream, args=(channel,on_message_func,stream_code), daemon=True)
    t.start()
    return t
    

# -------------------- FastAPI startup --------------------
app = FastAPI(title="Streaming Keepalive")

@app.on_event("startup")
async def startup_event():
    # khởi động 3 stream ở background threads
    start_stream("X",  on_message_X,  "X:ALL")
    start_stream("R",  on_message_R,  "R:ALL")
    start_stream("MI", on_message_MI, "MI:ALL")
    logging.info("🚀 All streams started (FastAPI keeps process alive)")

@app.get("/")
def health():
    return {"status": "ok", "message": "FastAPI is keeping the process alive"}