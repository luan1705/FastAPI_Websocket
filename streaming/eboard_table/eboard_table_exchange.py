import os, json, time, logging, signal, sys, redis
# from unittest import result
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from datetime import datetime, time as dtime , timezone
from zoneinfo import ZoneInfo
import pandas as pd
from notify import notify

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List import configviet as config
#from List.upsert import upsert_eboard, update_price_now
from List.exchange import EBOARD_GROUPS1, EXCHANGE_LISTS
from List.indices_map import indices_map
import threading
# =========================================

# ---------- Cấu hình qua ENV ----------
GROUP_KEY = os.getenv("EBOARD_GROUP") # vd: hose1, hnx3, upcom2, cw...
# Lấy list mã theo group
SYMBOL_LIST = EBOARD_GROUPS1.get(GROUP_KEY)
if not SYMBOL_LIST:
    raise RuntimeError(f"Unknown EBOARD_GROUP={GROUP_KEY}. Check EBOARD_GROUPS in List.exchange")
REDIS_URL   = "redis://root:Dnl_123456@tanhungsoft.com:6379"
STREAM_CODE = "X:" + "-".join(SYMBOL_LIST)
CHANNEL = "asset"
ACTIVE_CHANNEL = "active"
ALERT_FUNCTION_CHANNEL = os.getenv("ALERT_FUNCTION_CHANNEL", "alert_function")
# --------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
POOL = None
r = None

def create_redis():
    global POOL

    # clear pool cũ
    if POOL:
        try:
            POOL.disconnect()
        except Exception:
            pass

    POOL = redis.BlockingConnectionPool.from_url(
        REDIS_URL,
        decode_responses=True,

        socket_timeout=5,
        socket_connect_timeout=5,

        retry_on_timeout=True,
        health_check_interval=30,

        max_connections=10,
        timeout=1.0,
    )

    return redis.Redis(connection_pool=POOL)

def reconnect_redis():
    global r

    logging.warning("Reconnecting Redis...")

    try:
        r = create_redis()

        # force test connect
        r.ping()

        logging.info("Redis reconnect OK")
        return True

    except Exception as e:
        logging.error("Redis reconnect failed: %s", e)

# init lần đầu
r = create_redis()

# ---- Helpers & cấu hình tối giản ----
def null0(v):
    """Chỉ đổi 0 -> None (giữ nguyên các giá trị khác)."""
    try:
        if v is None:
            return None
        if v == 0 or v == "0":
            return None
        if isinstance(v, float) and abs(v) < 1e-12:
            return None
        return v
    except Exception:
        return v

def normalize_buy_sell(content: dict):
    """
    CHỈ xử lý BUY/SELL level 1..3 (giá + vol) theo dạng field phẳng:
    buyPrice1..3, buyVol1..3, sellPrice1..3, sellVol1..3
    """
    for i in range(1, 4):
        bp = f"buyPrice{i}";  bv = f"buyVol{i}"
        sp = f"sellPrice{i}"; sv = f"sellVol{i}"

        if bp in content: content[bp] = null0(content.get(bp))
        if bv in content: content[bv] = null0(content.get(bv))
        if sp in content: content[sp] = null0(content.get(sp))
        if sv in content: content[sv] = null0(content.get(sv))

def publish(payload: dict, channel: str = CHANNEL):
    global r
    data = json.dumps(payload, ensure_ascii=False)

    for attempt in range(1, 4):  # thử tối đa 3 lần
        try:
            r.publish(channel, data)
            return  # thành công → thoát
        except Exception as e:
            logging.warning("Redis publish fail (%s) attempt %d/3: %s", channel, attempt, e)
            if attempt == 2:
                try:
                    notify(f"⚠️ [{GROUP_KEY}] Redis publish fail ({channel}) attempt 2/3", level="warning")
                except Exception:
                    pass
            reconnect_redis()
            time.sleep(1)

    # hết 3 lần vẫn lỗi
    logging.error("Redis publish give up (%s), exiting...", channel)
    try:
        notify(f"🔴 [{GROUP_KEY}] Redis publish give up ({channel}), restarting...", level="error")
    except Exception:
        pass
    os._exit(1)

def save_redis_alert(msg: dict, tz_name: str = "Asia/Ho_Chi_Minh") -> bool:
    symbol = (msg.get("symbol") or "").strip()
    if not symbol:
        logging.warning(f"save_redis_alert: missing symbol in msg={msg}")
        return False

    key = f"latest_ssi_stream_message:{symbol}"
    payload = json.dumps(msg, ensure_ascii=False)

    for attempt in range(1, 4):  # thử tối đa 3 lần
        try:
            r.set(key, payload)
            return True  # thành công → thoát
        except Exception as e:
            logging.warning("Redis SET fail attempt %d/3: %s", attempt, e)
            if attempt == 2:
                try:
                    notify(f"⚠️ [{GROUP_KEY}] Redis publish fail attempt 2/3", level="warning")
                except Exception:
                    pass
            reconnect_redis()
            time.sleep(1)

    # hết 3 lần vẫn lỗi
    logging.error("Redis SET give up, exiting...")
    try:
        notify(f"🔴 [{GROUP_KEY}] Redis SET give up, restarting...", level="error")
    except Exception:
        pass
    os._exit(1)
        
def find_indices(symbol: str) -> list[str] | None:
    res = [idx for idx, symbols in indices_map.items() if symbol in symbols]
    return res or None

_SYMBOL2EX = {s: ex for ex, lst in EXCHANGE_LISTS.items() for s in lst}

def get_exchange(symbol: str):
    if not symbol:
        return None
    return _SYMBOL2EX.get(symbol.strip().upper())


def on_message_X(message):
    sym=""
    try:
        data = json.loads(message.get("Content","{}"))
        if data.get("RatioChange") == -100:
            return 
        sym = data["Symbol"]
        ngaygio = datetime.strptime(data['TradingDate'] + ' ' + data['Time'], '%d/%m/%Y %H:%M:%S')
        ngaygio = ngaygio.strftime("%Y-%m-%d %H:%M:%S")

        result = {
                'time': ngaygio,
                "symbol":   sym,
                # "exchange": get_exchange(sym),
                # "indices":  find_indices(sym),
                "ceiling":  data["Ceiling"] / 1000,
                "floor":    data["Floor"] / 1000,
                "refPrice": data["RefPrice"] / 1000,
                "buyPrice3": data["BidPrice3"]/1000,
                "buyVol3":   data["BidVol3"],
                "buyPrice2": data["BidPrice2"]/1000,
                "buyVol2":   data["BidVol2"],
                "buyPrice1": data["BidPrice1"]/1000,
                "buyVol1":   data["BidVol1"],
                "matchPrice": data["LastPrice"]/1000,
                "matchVol":   data["LastVol"],
                "matchChange": data["Change"]/1000,
                "matchRatioChange": data["RatioChange"],
                "sellPrice1": data["AskPrice1"]/1000,
                "sellVol1":   data["AskVol1"],
                "sellPrice2": data["AskPrice2"]/1000,
                "sellVol2":   data["AskVol2"],
                "sellPrice3": data["AskPrice3"]/1000,
                "sellVol3":   data["AskVol3"],
                "totalVol": data["TotalVol"],
                "totalVal": data["TotalVal"],
                "high":  data["High"]/1000,
                "low":   data["Low"]/1000,
                "open":  data["Open"]/1000,
                "close": data["Close"]/1000,
            }
        active = {
                "symbol":   sym,
                "active": True 
        }
        normalize_buy_sell(result)
        # Publish result sang Redis để Hub gom về 1 WS port
        publish(result, CHANNEL)
        publish(result, ALERT_FUNCTION_CHANNEL)
        publish(active, ACTIVE_CHANNEL)
        save_redis_alert(result)


    except Exception:
        logging.exception("X message error - %s", sym)

RECONNECT = threading.Event()

def on_error(err):
    error_text = str(err)
    logging.error("X stream error: %s", error_text)

    if "server busy" in str(err).lower():
        os._exit(1)

    RECONNECT.set()

def on_close():
    logging.warning("Stream closed, will reconnect...")
    RECONNECT.set()

def main():
    logging.info("Producer X | stream=%s | publish=%s", STREAM_CODE, CHANNEL)
    # notify(f"✅ [{GROUP_KEY}] Container started", level="info")  # 👈 thêm dòng này

    # graceful shutdown
    signal.signal(signal.SIGINT,  lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    connectssi=MarketDataClient(config)
    while True:
        try:
            mm = MarketDataStream(config,connectssi)
            mm.start(on_message_X, on_error, STREAM_CODE, on_close)
            woke = RECONNECT.wait(timeout=86400)
            if woke:
                RECONNECT.clear()

        except Exception as e:
            logging.error("Stream crashed: %s", e)
            time.sleep(3)

if __name__ == "__main__":
    main()
