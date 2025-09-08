import json  
import config 
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from kafka import KafkaProducer
import time
import threading
from datetime import datetime, time as dtime, date

stop_event = threading.Event()
holiday = [date(2026, 1, 1)]

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['172.20.0.3:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
topic_name = "index"

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

# Xử lý message nhận về
def on_message(message):
    global last_msg_time
    try:
        data = message.get('Content', '{}')
        data = json.loads(data)

        result = {
            'function': 'index',
            'content': {
                'symbol': data['IndexId'],
                'point': data['IndexValue'],
                'change': data['Change'],
                'raitoChange': data['RatioChange'],
                'totalVolume': data['AllQty'],
                'totalValue': data['AllValue'],
                'advancersDecliners': [
                    data['Advances'],
                    data['NoChanges'],
                    data['Declines']
                ]
            }
        }

        # Gửi lên Kafka
        producer.send(topic_name, result)
        producer.flush()
        last_msg_time = time.time()

    except Exception as e:
        print("❗ Lỗi giải mã JSON:", e)

# Xử lý lỗi
def on_error(error):
    print("❗ Lỗi:", error)
    stop_event.set()

def on_close():
    print("🔒 Kết nối đã đóng")
    stop_event.set()

def on_open():
    print("🔓 Kết nối đã mở")

# Vòng lặp stream
def stream():
    global last_msg_time
    while True:
        try:
            stop_event.clear()
            mm = MarketDataStream(config, MarketDataClient(config))
            selected_channel = "MI:ALL"
            last_msg_time = time.time()

            t = threading.Thread(
                target=mm.start,
                args=(on_message, on_error, selected_channel),
                daemon=True
            )
            t.start()

            while is_trading_time() and not stop_event.is_set():
                time.sleep(1)
                if (time.time() - last_msg_time > 10):
                    print("Không có data mới trong 10s, reconnect...")
                    stop_event.set()

            t.join(timeout=1)
            print("🔁 Đang reconnect sau 5s...")
            time.sleep(5)

        except Exception as e:
            print("⚠️ Lỗi trong stream():", e)
            time.sleep(5)
