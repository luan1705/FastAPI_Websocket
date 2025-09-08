import orjson
import config
import asyncio
import threading
import time
from datetime import datetime, time as dtime, date
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import logging
import signal
from exchange_map import exchange_map
from indices_map import indices_map

from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

# DB
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from sqlalchemy.dialects.postgresql import insert as pg_insert

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("stream.log"), logging.StreamHandler()],
)

# -------------------- FastAPI --------------------
app = FastAPI(title="Streaming WebSocket + Multi-DB Production")

clients = {"X": set(), "R": set(), "MI": set()}
stop_event = threading.Event()
last_msg_time = {"X": None, "R": None, "MI": None}
holiday = [date(2026, 1, 1)]

# -------------------- DB setup --------------------
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True,
)
metadata = MetaData()

# X:ALL table
x_table =  Table(
    "eboard_table", metadata,
    Column('symbol', String, primary_key=True),
    Column('exchange', String),
    Column('indices', String),
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
    Column('totalVol', Float), Column('totalVal', Float),
    Column('high', Float), Column('low', Float),
    schema="history_data"
)

# R:ALL table
r_table = Table(
    "eboard_foreign", metadata,
    Column("symbol", String, primary_key=True),
    Column("buyVol", Float),
    Column("sellVol", Float),
    Column("room", Float),
    Column("buyVal", Float),
    Column("sellVal", Float),
    schema="history_data"
)

# MI:ALL table
mi_table = Table(
    "indices", metadata,
    Column("symbol", String, primary_key=True),
    Column("point", Float),
    Column("change", Float),
    Column("ratioChange", Float),
    Column("totalVolume", Float),
    Column("totalValue", Float),
    Column("advancers", Integer),
    Column("noChange", Integer),
    Column("decliners", Integer),
    schema="history_data"
)


metadata.create_all(engine)

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
    return (t9h <= now.time() <= t12h) or (t13h <= now.time() <= t15h)

# -------------------- WebSocket broadcast --------------------
async def broadcast(channel, data: dict):
    dead_clients = []
    for ws in clients[channel]:
        try:
            await ws.send_text(orjson.dumps(data).decode())
        except Exception:
            dead_clients.append(ws)
    for ws in dead_clients:
        clients[channel].remove(ws)
        
async def websocket_endpoint(websocket: WebSocket, channel: str):
    await websocket.accept()
    clients[channel].add(websocket)
    logging.info(f"✅ Client connected: {websocket.client} to {channel}")
    try:
        while True:
            # Treo chờ client gửi gì đó (nếu không có thì vẫn idle)
            await websocket.receive_text()
    except WebSocketDisconnect:
        logging.info(f"❌ Client disconnected: {websocket.client} from {channel}")
    finally:
        clients[channel].discard(websocket)

@app.websocket("/ws/eboard_table")
async def websocket_x(ws: WebSocket):
    await websocket_endpoint(ws, "X")

@app.websocket("/ws/eboard_foreign")
async def websocket_r(ws: WebSocket):
    await websocket_endpoint(ws, "R")

@app.websocket("/ws/indices")
async def websocket_mi(ws: WebSocket):
    await websocket_endpoint(ws, "MI")

# -------------------- DB upsert functions --------------------
def save_x(result):
    try:
        c = result["content"]

        # indices: list -> "A|B|C"
        indices = c.get("indices")
        if isinstance(indices, list):
            indices = "|".join(indices)

        bp = c["buy"]["price"];  bv = c["buy"]["vol"]
        sp = c["sell"]["price"]; sv = c["sell"]["vol"]
        m  = c["match"]

        row = {
            "symbol":  c["symbol"],
            "exchange": c.get("exchange"),
            "indices":  indices,
            "ceiling":  c["ceiling"],
            "floor":    c["floor"],
            "refPrice": c["refPrice"],

            "buyPrice1": bp[0], "buyVol1": bv[0],
            "buyPrice2": bp[1], "buyVol2": bv[1],
            "buyPrice3": bp[2], "buyVol3": bv[2],

            "matchPrice":       m["price"],
            "matchVol":         m["vol"],
            "matchChange":      m["change"],
            "matchRatioChange": m["ratioChange"],

            "sellPrice1": sp[0], "sellVol1": sv[0],
            "sellPrice2": sp[1], "sellVol2": sv[1],
            "sellPrice3": sp[2], "sellVol3": sv[2],

            "totalVol": c["totalVol"],
            "totalVal": c["totalVal"],
            "high":     c["high"],
            "low":      c["low"],
        }

        with engine.begin() as conn:
            stmt = pg_insert(x_table).values([row])
            update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)

        logging.info("✅ X:ALL upserted %s", c.get("symbol",""))
    except Exception as e:
        logging.error("❌ X DB save error: %s", e)


def save_r(result):
    try:
        c = result["content"]
        row = {
            "symbol": c["symbol"],
            "buyVol":    c["buyVol"],
            "sellVol":   c["sellVol"],
            "room":   c["room"],
            "buyVal":    c["buyVal"],
            "sellVal":   c["sellVal"],
        }
        with engine.begin() as conn:
            stmt = pg_insert(r_table).values([row])
            update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)
        logging.info("✅ R:ALL upserted %s", c.get("symbol",""))
    except Exception as e:
        logging.error("❌ R DB save error: %s", e)

def save_mi(result):
    try:
        c = result["content"]

        # Chuẩn hoá advancersDecliners về đúng 3 phần tử
        raw = c.get("advancersDecliners")
        if isinstance(raw, dict):
            seq = [raw.get("Advances"), raw.get("NoChanges"), raw.get("Declines")]
        else:
            seq = list(raw) if raw is not None else []

        adv, nc, dec = (seq + [None, None, None])[:3]

        row = {
            "symbol":       c["symbol"],
            "point":        c["point"],
            "change":       c["change"],
            "ratioChange":  c["ratioChange"],
            "totalVolume":  c["totalVolume"],
            "totalValue":   c["totalValue"],
            "advancers":    adv,
            "noChange":     nc,
            "decliners":    dec,
        }

        with engine.begin() as conn:
            stmt = pg_insert(mi_table).values([row])
            update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)

        logging.info("✅ MI:ALL upserted %s", c.get("symbol",""))
    except Exception as e:
        logging.error("❌ MI DB save error: %s", e)


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
    global last_msg_time
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
                'totalVal': data['TotalVal'],
                'high': data['High'] / 1000,
                'low': data['Low'] / 1000
            }

        }
        asyncio.run_coroutine_threadsafe(broadcast("X", result), app.state.loop)
        save_x(result)
        last_msg_time["X"] = time.time()
    except Exception:
        logging.exception("❗ X message error")
        
def on_message_R(message):
    global last_msg_time
    try:
        data = orjson.loads(message.get("Content","{}"))
        symbol=data['Symbol']
        result = {
            'function': 'eboard_foreign',
            'content': {
                'symbol': symbol,
                'buyVol': data['BuyVol'],
                'sellVol': data['SellVol'],
                'room': data['CurrentRoom'],
                'buyVal': data['BuyVal'] ,
                'sellVal': data['SellVal']
            }
        }
        asyncio.run_coroutine_threadsafe(broadcast("R", result), app.state.loop)
        save_r(result)
        last_msg_time["R"] = time.time()
    except Exception:
        logging.exception("❗ R message error")

def on_message_MI(message):
    global last_msg_time
    try:
        data = orjson.loads(message.get("Content","{}"))
        symbol=data['IndexId']
        symbol = 'UpcomIndex' if symbol == 'HNXUpcomIndex' else symbol

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
        asyncio.run_coroutine_threadsafe(broadcast("MI", result), app.state.loop)
        save_mi(result)
        last_msg_time["MI"] = time.time()
    except Exception:
        logging.exception("❗ MI message error")

# -------------------- Stream lifecycle --------------------
def on_error(error):
    logging.error(f"❗ Lỗi: {error}")
    stop_event.set()

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
    

# -------------------- FastAPI startup --------------------
@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    app.state.loop = loop
    start_stream("X", on_message_X, "X:ALL")
    start_stream("R", on_message_R, "R:ALL")
    start_stream("MI", on_message_MI, "MI:ALL")
    logging.info("🚀 All streaming + WebSocket + DB services started")