import json
import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from kafka import KafkaProducer
import time
from datetime import datetime, time as dtime, date
import threading

stop_event = threading.Event()
last_msg_time = None
holiday = [date(2026, 1, 1)]

# Kafka Producer tối ưu batch
producer = KafkaProducer(
    bootstrap_servers=['172.20.0.3:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=50,
    batch_size=32768
)
topic_name = "eboard_table"

# Kiểm tra giờ giao dịch
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

# Xử lý message nhận được
def on_message(message):
    global last_msg_time
    try:
        data = message.get('Content', '{}')
        data = json.loads(data)

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
        producer.send(topic_name, result)
        last_msg_time = time.time()

    except Exception as e:
        print("❗ Lỗi giải mã JSON:", e)

# Xử lý lỗi stream
def on_error(error):
    print("❗ Lỗi:", error)
    stop_event.set()

def on_close():
    print("🔒 Kết nối đã đóng")
    stop_event.set()

def on_open():
    print("🔓 Kết nối đã mở")

# Luồng chính
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
                    print("⚠️ Không có data mới trong 10s, reconnect...")
                    stop_event.set()

        except Exception as e:
            print("⚠️ Lỗi trong stream():", e)

        print("🔄 Thử kết nối lại sau 5 giây...")
        time.sleep(5)

if __name__ == "__main__":
    stream()
