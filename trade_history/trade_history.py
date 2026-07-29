import os
import json
import time
import logging
import signal
import sys
import redis
import threading
import requests
import importlib
from datetime import datetime
from zoneinfo import ZoneInfo
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from sqlalchemy import create_engine, text
from List import configviet as config

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# ==================================================
# LOAD SYMBOL LIST
# ==================================================
SYMBOL_MODULE = os.getenv("SYMBOL_MODULE", "List.exchange")
SYMBOL_NAME   = os.getenv("SYMBOL_NAME",   "HNX1")

if not SYMBOL_NAME:
    raise RuntimeError("SYMBOL_NAME chưa được set")

try:
    module  = importlib.import_module(SYMBOL_MODULE)
    SYMBOLS = getattr(module, SYMBOL_NAME)
except Exception as e:
    raise RuntimeError(f"Không load được list {SYMBOL_MODULE}.{SYMBOL_NAME}: {e}")

if not isinstance(SYMBOLS, (list, tuple)) or not SYMBOLS:
    raise RuntimeError(f"SYMBOL_NAME {SYMBOL_NAME} không hợp lệ hoặc rỗng")

print(f"Loaded {len(SYMBOLS)} symbols from {SYMBOL_MODULE}.{SYMBOL_NAME}", flush=True)

# ==================================================
# DERIVE EXCHANGE
# ==================================================
if SYMBOL_NAME.startswith("HNX"):
    EXCHANGE = "HNX"
elif SYMBOL_NAME.startswith("HOSE"):
    EXCHANGE = "HOSE"
elif SYMBOL_NAME.startswith("UPCOM"):
    EXCHANGE = "UPCOM"
elif SYMBOL_NAME == "INDICE":
    EXCHANGE = "INDEX"
elif SYMBOL_NAME == "DERIVATIVES":
    EXCHANGE = "DERIVATIVES"
elif SYMBOL_NAME == "CW":
    EXCHANGE = "CW"
elif SYMBOL_NAME == "ETFHOSE":
    EXCHANGE = "ETFHOSE"
elif SYMBOL_NAME == "HNXBOND":
    EXCHANGE = "HNXBOND"
else:
    EXCHANGE = "UNKNOWN"

# ==================================================
# CONFIG
# ==================================================
DB_URL         = os.getenv("DB_URL",          "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl")
SCHEMA         = os.getenv("DB_SCHEMA",        "trade_history")
REDIS_URL      = os.getenv("REDIS_URL",        "redis://root:Dnl_123456@tanhungsoft.com:6379")
REDIS_CHANNEL  = os.getenv("REDIS_CHANNEL",    "trade_history")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ==================================================
# POSTGRES
# ==================================================
engine = create_engine(
    DB_URL,
    pool_size=5,
    max_overflow=5,
    pool_timeout=20,
    pool_recycle=1800,
    pool_pre_ping=True,
)

# ==================================================
# REDIS
# ==================================================
redis_pool = None
redis_client = None


def create_redis():
    global redis_pool

    # clear pool cũ
    if redis_pool:
        try:
            redis_pool.disconnect()
        except Exception:
            pass

    redis_pool = redis.BlockingConnectionPool.from_url(
        REDIS_URL,
        decode_responses=True,

        socket_timeout=5,
        socket_connect_timeout=5,

        retry_on_timeout=True,
        health_check_interval=30,

        max_connections=30,
        timeout=1.0,
    )

    return redis.Redis(connection_pool=redis_pool)


def reconnect_redis():
    global redis_client

    logging.warning("Reconnecting Redis...")

    try:
        redis_client = create_redis()

        redis_client.ping()

        logging.info("Redis reconnect OK")
        return True

    except Exception as e:
        logging.error("Redis reconnect failed: %s", e)
        return False

def publish_redis(payload: dict, channel: str):
    global redis_client

    data = json.dumps(payload, default=str)

    for attempt in range(1, 4):
        try:
            redis_client.publish(channel, data)
            return True

        except Exception as e:
            logging.warning(
                f"Redis publish fail ({channel}) attempt {attempt}/3: {e}"
            )

            if not reconnect_redis():
                time.sleep(1)
                continue

            time.sleep(1)

    logging.error(f"Redis publish give up ({channel})")
    os._exit(1)

# init lần đầu
redis_client = create_redis()
redis_client.ping()

print("Connected Redis", flush=True)

# ==================================================
# PARSE TIME — hỗ trợ mọi format SSI có thể trả về
# ==================================================
# Các format SSI thực tế đã gặp:
#   "12/02/2025 09:15:30"  → %d/%m/%Y %H:%M:%S
#   "2025-02-12 09:15:30"  → %Y-%m-%d %H:%M:%S
#   "2025-02-12T09:15:30"  → %Y-%m-%dT%H:%M:%S
#   "12/02/2025"           → chỉ có date, không có time

_TIME_FORMATS = [
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y",
    "%Y-%m-%d",
]

def parse_vn_time(trading_date: str, trading_time: str):
    """
    Thử parse tất cả format có thể.
    Trả về datetime có tzinfo=VN_TZ, hoặc None nếu không parse được.
    """
    # Trường hợp 1: ghép date + time
    if trading_date and trading_time:
        combined = f"{trading_date} {trading_time}".strip()
        for fmt in _TIME_FORMATS:
            try:
                dt = datetime.strptime(combined, fmt)
                return dt.replace(tzinfo=VN_TZ)
            except ValueError:
                continue

    # Trường hợp 2: chỉ có date
    if trading_date:
        for fmt in _TIME_FORMATS:
            try:
                dt = datetime.strptime(trading_date.strip(), fmt)
                return dt.replace(tzinfo=VN_TZ)
            except ValueError:
                continue

    return None

# ==================================================
# PRE-CREATE SCHEMA + TABLES
# ==================================================
_initialized_tables: set = set()

def ensure_table(symbol: str):
    """Tạo bảng nếu chưa có — chỉ chạy 1 lần mỗi symbol."""
    if symbol in _initialized_tables:
        return
    table = f'"{SCHEMA}"."{symbol.upper()}"'
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";'))
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol            TEXT,
                    time              TIMESTAMP WITH TIME ZONE,
                    ceiling           DOUBLE PRECISION,
                    floor             DOUBLE PRECISION,
                    "refPrice"        DOUBLE PRECISION,
                    high              DOUBLE PRECISION,
                    low               DOUBLE PRECISION,
                    "avgPrice"        DOUBLE PRECISION,
                    "matchPrice"      DOUBLE PRECISION,
                    change            DOUBLE PRECISION,
                    "ratioChange"     DOUBLE PRECISION,
                    "estMatchedPrice" DOUBLE PRECISION,
                    "lastVol"         DOUBLE PRECISION,
                    "totalVol"        DOUBLE PRECISION,
                    "totalVal"        DOUBLE PRECISION,
                    "priorVal"        DOUBLE PRECISION,
                    "tradingSession"  TEXT,
                    side              TEXT
                );
            """))
        _initialized_tables.add(symbol)
        # logging.info(f"✅ Table ready: {SCHEMA}.{symbol.upper()}")
    except Exception as e:
        logging.error(f"❌ ensure_table FAIL [{symbol}]: {e}")
        raise   # re-raise để caller biết

def init_all_tables():
    logging.info(f"📦 Initializing {len(SYMBOLS)} tables...")
    for sym in SYMBOLS:
        try:
            ensure_table(sym.upper())
        except Exception as e:
            logging.error(f"❌ Init table fail [{sym}]: {e}")
    logging.info("✅ All tables initialized")

# ==================================================
# HÀM HỖ TRỢ
# ==================================================
def safe_div_1000(value):
    try:
        if value is None or value == "":
            return None
        return float(value) / 1000
    except Exception:
        return None

def upsert_order_matching(symbol: str, data: dict):
    """Insert một row vào bảng symbol."""
    try:
        time_vn = data.get("_time_parsed")   # đã parse sẵn từ on_message_X
        if time_vn is None:
            # fallback: thử parse lại từ time string
            time_str = data.get("time")
            if time_str:
                try:
                    time_vn = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=VN_TZ)
                except Exception:
                    pass

        if time_vn is None:
            logging.error(
                f"❌ SKIP INSERT [{symbol}]: không parse được time. "
                f"TradingDate={data.get('_raw_date')!r} Time={data.get('_raw_time')!r}"
            )
            return

        ensure_table(symbol)
        table = f'"{SCHEMA}"."{symbol.upper()}"'

        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    INSERT INTO {table}
                        (symbol, time, ceiling, floor, "refPrice", high, low,
                         "avgPrice", "matchPrice", change, "ratioChange",
                         "estMatchedPrice", "lastVol", "totalVol", "totalVal",
                         "priorVal", "tradingSession", side)
                    VALUES
                        (:symbol, :time, :ceiling, :floor, :refPrice, :high, :low,
                         :avgPrice, :matchPrice, :change, :ratioChange,
                         :estMatchedPrice, :lastVol, :totalVol, :totalVal,
                         :priorVal, :tradingSession, :side);
                """),
                {
                    "symbol":           symbol.upper(),
                    "time":             time_vn,
                    "ceiling":          data.get("ceiling"),
                    "floor":            data.get("floor"),
                    "refPrice":         data.get("refPrice"),
                    "high":             data.get("high"),
                    "low":              data.get("low"),
                    "avgPrice":         data.get("avgPrice"),
                    "matchPrice":       data.get("matchPrice"),
                    "change":           data.get("change"),
                    "ratioChange":      data.get("ratioChange"),
                    "estMatchedPrice":  data.get("estMatchedPrice"),
                    "lastVol":          data.get("lastVol"),
                    "totalVol":         data.get("totalVol"),
                    "totalVal":         data.get("totalVal"),
                    "priorVal":         data.get("priorVal"),
                    "tradingSession":   data.get("tradingSession"),
                    "side":             data.get("side"),
                },
            )
        # logging.info(f"[DB ✅] {symbol} @ {time_vn.strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception:
        logging.exception(f"[DB ❌] {symbol}")
        os._exit(1)   # in full stack trace ra stderr để không bỏ sót

# ==================================================
# CALLBACK SSI
# ==================================================
def on_message_X(message):
    try:
        # ── BƯỚC 1: lấy raw string/dict từ message ──
        if isinstance(message, str):
            raw = message
        elif isinstance(message, dict):
            raw = (
                message.get("Content")
                or message.get("content")
                or message.get("data")
                or message.get("Data")
            )
            if raw is None:
                raw = json.dumps(message)   # bản thân message là payload
        else:
            logging.warning(f"⚠️ Unknown message type: {type(message)}: {repr(message)[:200]}")
            return

        # ── BƯỚC 2: parse JSON ──
        data = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(data, dict):
            logging.warning(f"⚠️ data không phải dict: {type(data)}: {repr(data)[:200]}")
            return

        # ── BƯỚC 3: lấy symbol ──
        symbol = data.get("Symbol") or data.get("symbol")
        if not symbol:
            logging.warning(f"⚠️ Không có Symbol trong: {repr(data)[:300]}")
            return

        # ── BƯỚC 4: parse time — LOG RÕ để debug ──
        trading_date = data.get("TradingDate")
        trading_time = data.get("Time")

        time_vn = parse_vn_time(trading_date, trading_time)

        if time_vn is None:
            # LOG ĐẦY ĐỦ để biết format thực tế SSI trả về
            logging.error(
                f"❌ [{symbol}] Không parse được time! "
                f"TradingDate={trading_date!r} | Time={trading_time!r} | "
                f"Raw data keys: {list(data.keys())}"
            )
            # Vẫn tiếp tục xử lý nhưng time sẽ là None
        else:
            time_str = time_vn.strftime("%Y-%m-%d %H:%M:%S")

        # ── BƯỚC 5: map data ──
        mapped_data = {
            "function":         "match",
            "time":             time_vn.strftime("%Y-%m-%d %H:%M:%S") if time_vn else None,
            "_time_parsed":     time_vn,       # object datetime, dùng nội bộ cho DB
            "_raw_date":        trading_date,  # giữ lại để debug
            "_raw_time":        trading_time,  # giữ lại để debug
            "symbol":           symbol,
            "ceiling":          safe_div_1000(data.get("Ceiling")),
            "floor":            safe_div_1000(data.get("Floor")),
            "refPrice":         safe_div_1000(data.get("RefPrice")),
            "high":             safe_div_1000(data.get("Highest")),
            "low":              safe_div_1000(data.get("Lowest")),
            "avgPrice":         safe_div_1000(data.get("AvgPrice")),
            "matchPrice":       safe_div_1000(data.get("LastPrice")),
            "change":           safe_div_1000(data.get("Change")),
            "ratioChange":      data.get("RatioChange"),
            "estMatchedPrice":  safe_div_1000(data.get("EstMatchedPrice")),
            "lastVol":          data.get("LastVol"),
            "totalVol":         data.get("TotalVol"),
            "totalVal":         data.get("TotalVal"),
            "priorVal":         data.get("PriorVal"),
            "exchange":         data.get("Exchange") or EXCHANGE,
            "tradingSession":   data.get("TradingSession"),
            "side":             data.get("Side"),
        }

        # ── BƯỚC 6: Publish Redis ──
        redis_payload = {
            k: v for k, v in mapped_data.items()
            if not k.startswith("_")
        }

        upsert_order_matching(symbol, mapped_data)

        publish_redis(redis_payload, REDIS_CHANNEL)




    except Exception:
        logging.exception("❌ on_message_X unhandled error")
        os._exit(1)

RECONNECT = threading.Event()

def on_error(err):
    logging.error(f"❌ Stream error: {err}")
    RECONNECT.set()

def on_close():
    logging.warning("⚠️ Stream closed → sẽ reconnect...")
    RECONNECT.set()

# ==================================================
# STREAM WORKER
# ==================================================
def stream_worker():
    connectssi = MarketDataClient(config)

    while True:
        try:
            logging.info("🔌 Connecting to SSI data stream...")

            mm = MarketDataStream(config, connectssi)

            mm.start(
                on_message_X,
                on_error,
                "X-Trade:" + "-".join(SYMBOLS),
                on_close,
            )

            woke = RECONNECT.wait(timeout=86400)

            if woke:
                RECONNECT.clear()

        except Exception as e:
            logging.error("Stream crashed: %s", e)
            time.sleep(1)

# ==================================================
# MAIN
# ==================================================
def main():
    print(f"🚀 SSI Order Matching → schema={SCHEMA} | Redis={REDIS_CHANNEL}", flush=True)
    print(f"📊 {len(SYMBOLS)} symbols | Exchange={EXCHANGE}", flush=True)

    def shutdown_handler(sig, frame):
        logging.warning("🛑 Stopping...")
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    init_all_tables()
    stream_worker()

if __name__ == "__main__":
    main()