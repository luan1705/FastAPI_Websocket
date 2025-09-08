import orjson
import os, time,queue
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

NUM_WORKERS = int(os.getenv("NUM_WORKERS", "4"))  # tăng/giảm theo CPU
Q_MAXSIZE   = int(os.getenv("Q_MAXSIZE", "200000"))
# -------------------- FastAPI --------------------


# # -------------------- Trading time check --------------------
# def is_trading_time():
#     now = datetime.now()
#     today = now.date()
#     if now.weekday() >= 5: #or today in holiday:
#         return False
#     t9h = dtime(9, 0)
#     t12h = dtime(12, 0)
#     t13h = dtime(13, 0)
#     t15h = dtime(15, 0)
#     return (t9h <= now.time() <= t12h) or (t13h <= now.time() <= t15h)
    
# ---------------- Producer config: ưu tiên latency ----------------
producer_conf = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "broker:29092"),
    "linger.ms": 0.01,                      # 0-2ms ưu tiên latency
    "batch.num.messages": 5000,          # vẫn giữ batch vừa phải
    "queue.buffering.max.messages": 300000,
    "queue.buffering.max.kbytes": 524288,  # 512MB
    "compression.type": "lz4",
    "acks": os.getenv("KAFKA_ACKS", "1"), # "0" nếu muốn latency thấp nhất
    "message.timeout.ms": 15000,          # tránh timeout dài
}

topic_x  = "eboard_table"
topic_r  = "eboard_foreign"
topic_mi = "indices"

# ---------------- Sharded queues ----------------
qs = [queue.Queue(maxsize=Q_MAXSIZE) for _ in range(NUM_WORKERS)]

def shard_for_symbol(symbol: str) -> int:
    # consistent sharding theo symbol
    # (nếu bạn đã set key=symbol để giữ order per partition, vẫn ổn)
    return (hash(symbol) & 0xfffffff) % NUM_WORKERS
    
    
# ---------------- Worker: mỗi worker có 1 Producer + poll thread riêng ----------------
class KafkaWorker(threading.Thread):
    def __init__(self, wid: int):
        super().__init__(daemon=True)
        self.wid = wid
        self.producer = Producer(producer_conf)
        self.running = True
        self.poll_thread = threading.Thread(target=self._poller, daemon=True)
        self.poll_thread.start()

    def _poller(self):
        while self.running:
            # poll rất ngắn để bắt DR/IO mà không block callback SSI
            self.producer.poll(0)
            time.sleep(0.001)

    def run(self):
        q = qs[self.wid]
        while self.running:
            try:
                topic, key, payload = q.get()
                # key = symbol để Kafka giữ order per-key (per partition)
                self.producer.produce(topic=topic, key=key, value=payload)
            except BufferError:
                # queue full ở client Kafka -> short sleep
                time.sleep(0.001)
            except Exception:
                logging.exception("produce error")
            finally:
                q.task_done()

    def stop(self):
        self.running = False
        try:
            self.producer.flush(5)
        except Exception:
            pass

workers = [KafkaWorker(i) for i in range(NUM_WORKERS)]
for w in workers:
    w.start()

# ---------------- Fast path: callback SSI siêu nhẹ ----------------
def _enqueue(topic: str, symbol: str, obj: dict):
    # orjson dumps nhanh + nhỏ
    payload = orjson.dumps(obj)
    shard = shard_for_symbol(symbol)
    try:
        qs[shard].put_nowait((topic, symbol, payload))
    except queue.Full:
        # Bảo vệ: nếu full, drop hoặc log nhẹ tùy chính sách
        # (để latency không bùng nổ)
        # logging.warning("local queue full -> dropping")
        pass

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
        data = orjson.loads(message.get("Content","{}"))
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
        _enqueue(topic_x, symbol, result)
    except Exception:
        logging.exception("❗ X message error")
        
def on_message_R(message):
    try:
        data = orjson.loads(message.get("Content","{}"))
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
        _enqueue(topic_r, symbol, result)
    except Exception:
        logging.exception("❗ R message error")

def on_message_MI(message):
    try:
        data = orjson.loads(message.get("Content","{}"))
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
        _enqueue(topic_mi, str(symbol), result)
    except Exception:
        logging.exception("❗ MI message error")

# -------------------- Stream lifecycle --------------------
def on_error(channel):
    def _inner(error):
        logging.error("SSI %s error: %s", channel, error)
    return _inner

def start_stream(code: str, on_msg):
    t = threading.Thread(
        target=lambda: MarketDataStream(config, MarketDataClient(config)).start(on_msg, on_error(code), code),
        daemon=True
    )
    t.start()
    return t
    
if __name__ == "__main__":
    # chạy 3 stream; process sống mãi (không FastAPI)
    threads = [
        start_stream("X:ALL",  on_message_X),
        start_stream("R:ALL",  on_message_R),
        start_stream("MI:ALL", on_message_MI),
    ]
    logging.info("🚀 Streams running; producer sharded = %d workers", NUM_WORKERS)

    # giữ tiến trình sống
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        for w in workers:
            w.stop()