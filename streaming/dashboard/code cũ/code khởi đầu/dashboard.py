import json  
import config 
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from kafka import KafkaProducer  
import time
import threading
from datetime import datetime, time as dtime, date

stop_event=threading.Event()
# holiday=[date(2026,1, 1)]

#tạo producer
producer = KafkaProducer(
    bootstrap_servers=['172.20.0.3:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
topic_name="eboard_table"

# #kiểm tra giờ giao dịch
# def is_trading_time():
#     now=datetime.now()
#     today=now.date()
#     if now.weekday()>=5 or today in holiday:
#         return False
        
#     t9h=dtime(9,0)
#     t12h=dtime(12,0)
#     t13h=dtime(13,0)
#     t15h=dtime(15,0)
    
#     check=now.time()
#     return (t9h<=check<=t12h)or(t13h<=check<=t15h)

#định dạng message nhận về
def on_message( message):
    global last_msg_time
    try:
        data=message.get('Content','{}')
        data=json.loads(data)  #chuyển đổi json string thành dict

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
        #gửi vào kafka
        producer.send(topic_name,result)
        producer.flush()
        last_msg_time = time.time()
    except Exception as e:
        print("❗ Lỗi giải mã JSON:", e)

#định dạng message lỗi nhận về
def on_error( error):
    print("❗ Lỗi:", error)
    stop_event.set()

#định dạng message đóng kết nối
def on_close():
    print("🔒 Kết nối đã đóng")
    stop_event.set()

#định dạng message mở kết nối
def on_open():
    print("🔓 Kết nối đã mở")

#khởi tạo websocket
def stream():
    global last_msg_time
    while True:
        # if not is_trading_time():
        #     now = datetime.now()
        #     print(f"⏸ Ngoài giờ giao dịch ({now.strftime('%H:%M:%S')}), sẽ không trả dữ liệu. Chờ  10s...")
        #     time.sleep(10)
        #     continue
        try:
            stop_event.clear()
            
            # Khởi tạo stream
            mm=MarketDataStream(config,MarketDataClient(config))
            selected_channel = "X:ALL"
            last_msg_time=time.time()
            
            # Chạy stream trong luồng phụ
            t=threading.Thread(target=mm.start, args=(on_message, on_error, selected_channel), daemon=True)
            t.start()
            
            # # Đợi cho đến khi có lỗi hoặc bị đóng
            # while is_trading_time() and not stop_event.is_set():
            #     time.sleep(1)
            #     if (time.time()-last_msg_time >10):
            #         print("Không có data mới trong 10s,reconnect...")
            #         stop_event.set()
                
            t.join(timeout=1)

            # print("🔁 Đang reconnect sau 5s...")
            # time.sleep(5)

        except Exception as e:
            print("⚠️ Lỗi trong stream():", e)
            time.sleep(5)

if __name__ == "__main__":
    stream()
