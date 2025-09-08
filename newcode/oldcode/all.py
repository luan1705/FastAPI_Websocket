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
    Column('totalVol', Float), Column('high', Float), Column('low', Float),
    schema="history_data"
)

# R:ALL table
r_table = Table(
    "eboard_foreign", metadata,
    Column("symbol", String, primary_key=True),
    Column("buy", Float),
    Column("sell", Float),
    Column("room", Float),
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
        df = json_normalize(result["content"])
         # ⚡ Convert list indices -> custom string format ["A"|"B"|"C"]
        if "indices" in df.columns:
            df["indices"] = df["indices"].apply(
                lambda x: "|".join(x) if isinstance(x, list) else x)
        
        buy_price_expanded = df['buy.price'].apply(pd.Series).rename(columns={0: 'buyPrice1', 1: 'buyPrice2', 2: 'buyPrice3'})
        buy_vol_expanded = df['buy.vol'].apply(pd.Series).rename(columns={0: 'buyVol1', 1: 'buyVol2', 2: 'buyVol3'})
        sell_price_expanded = df['sell.price'].apply(pd.Series).rename(columns={0: 'sellPrice1', 1: 'sellPrice2', 2: 'sellPrice3'})
        sell_vol_expanded = df['sell.vol'].apply(pd.Series).rename(columns={0: 'sellVol1', 1: 'sellVol2', 2: 'sellVol3'})

        df = pd.concat([
            df.drop(['buy.price', 'buy.vol', 'sell.price', 'sell.vol'], axis=1),
            buy_price_expanded, buy_vol_expanded, sell_price_expanded, sell_vol_expanded
        ], axis=1)

        # Rename match columns
        df = df.rename(columns={
            'match.price': 'matchPrice',
            'match.vol': 'matchVol',
            'match.change': 'matchChange',
            'match.ratioChange': 'matchRatioChange'
        })

        # Keep only needed columns
        filtered_columns = [
            'symbol', 'exchange', 'indices', 'ceiling', 'floor', 'refPrice',
            'buyPrice3', 'buyVol3', 'buyPrice2', 'buyVol2',
            'buyPrice1', 'buyVol1', 'matchPrice', 'matchVol',
            'matchChange', 'matchRatioChange', 'sellPrice1',
            'sellVol1', 'sellPrice2', 'sellVol2', 'sellPrice3',
            'sellVol3', 'totalVol', 'high', 'low'
        ]
        df = df[filtered_columns]
        with engine.begin() as conn:
            stmt = pg_insert(x_table).values(df.to_dict(orient="records"))
            update_dict = {c.name: getattr(stmt.excluded, c.name) for c in x_table.columns if c.name != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)
        logging.info(f"✅ X:ALL upserted {result['content'].get('symbol','')}")
    except Exception as e:
        logging.error(f"❌ X DB save error: {e}")

def save_r(result):
    try:
        df = json_normalize(result["content"])
        df = df[["symbol", "buy", "sell", "room"]]
        with engine.begin() as conn:
            stmt = pg_insert(r_table).values(df.to_dict(orient="records"))
            update_dict = {c.name: getattr(stmt.excluded, c.name) for c in r_table.columns if c.name != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)
        logging.info(f"✅ R:ALL upserted {result['content'].get('symbol','')}")
    except Exception as e:
        logging.error(f"❌ R DB save error: {e}")

def save_mi(result):
    try:
        df = json_normalize(result["content"])
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
        filtered_columns = ['symbol', 'point', 'change', 'ratioChange',
                            'totalVolume', 'totalValue', 'advancers', 'noChange', 'decliners']
        df = df[filtered_columns]
        with engine.begin() as conn:
            stmt = pg_insert(mi_table).values(df.to_dict(orient="records"))
            update_dict = {c.name: getattr(stmt.excluded, c.name) for c in mi_table.columns if c.name != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)
        logging.info(f"✅ MI:ALL upserted {result['content'].get('symbol','')}")
    except Exception as e:
        logging.error(f"❌ MI DB save error: {e}")

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
                'buy': data['BuyVol'],
                'sell': data['SellVol'],
                'room': data['CurrentRoom']
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