import json
import config
import asyncio
import threading
import time
from datetime import datetime, time as dtime, date
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import logging
import signal
from concurrent.futures import ThreadPoolExecutor
import queue
import weakref

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

# -------------------- Configuration --------------------
from symbols_list import symbols_list   # import list SYMBOLS từ file symbols.py

MAX_WORKERS = 50  # Số thread pool workers
RECONNECT_DELAY = 3  # Giây delay khi reconnect

# -------------------- FastAPI --------------------
app = FastAPI(title="Real-time Individual Symbol Streaming")

# WebSocket clients - organized by symbol for targeted broadcasting
clients = {
    "X": set(),  # All X data clients
    "R": set(),  # All R data clients  
    "MI": set(), # All MI data clients
}

# Symbol-specific clients for targeted updates
symbol_clients = {}  # {"ACB": set(), "VCB": set(), ...}

stop_events = {}  # Per-symbol stop events
last_msg_time = {}  # Per-symbol last message time
holiday = [date(2026, 1, 1)]

# -------------------- DB setup --------------------
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True,
    pool_size=30,  # Increased for high concurrency
    max_overflow=50,
    pool_recycle=3600,  # Recycle connections every hour
    pool_timeout=30
)
metadata = MetaData()

# Tables definition (same as before)
x_table = Table(
    "eboard_table", metadata,
    Column('symbol', String, primary_key=True),
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

r_table = Table(
    "eboard_foreign", metadata,
    Column("symbol", String, primary_key=True),
    Column("buy", Float),
    Column("sell", Float),
    Column("room", Float),
    schema="history_data"
)

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

# -------------------- Async DB Queue --------------------
db_queue = asyncio.Queue(maxsize=1000)

async def db_worker():
    """Background worker to handle DB operations asynchronously"""
    while True:
        try:
            operation = await db_queue.get()
            if operation is None:  # Shutdown signal
                break
                
            channel, data = operation
            await asyncio.get_event_loop().run_in_executor(
                None, 
                save_to_db, 
                channel, 
                data
            )
            db_queue.task_done()
        except Exception as e:
            logging.error(f"DB worker error: {e}")

def save_to_db(channel, data):
    """Synchronous DB save function"""
    try:
        if channel == "X":
            save_x_sync(data)
        elif channel == "R":
            save_r_sync(data)
        elif channel == "MI":
            save_mi_sync(data)
    except Exception as e:
        logging.error(f"DB save error for {channel}: {e}")

def save_x_sync(result):
    try:
        content = result["content"]
        record = {
            'symbol': content['symbol'],
            'ceiling': content['ceiling'],
            'floor': content['floor'],
            'refPrice': content['refPrice'],
            'buyPrice1': content['buy']['price'][0],
            'buyPrice2': content['buy']['price'][1],
            'buyPrice3': content['buy']['price'][2],
            'buyVol1': content['buy']['vol'][0],
            'buyVol2': content['buy']['vol'][1],
            'buyVol3': content['buy']['vol'][2],
            'matchPrice': content['match']['price'],
            'matchVol': content['match']['vol'],
            'matchChange': content['match']['change'],
            'matchRatioChange': content['match']['ratioChange'],
            'sellPrice1': content['sell']['price'][0],
            'sellPrice2': content['sell']['price'][1],
            'sellPrice3': content['sell']['price'][2],
            'sellVol1': content['sell']['vol'][0],
            'sellVol2': content['sell']['vol'][1],
            'sellVol3': content['sell']['vol'][2],
            'totalVol': content['totalVol'],
            'high': content['high'],
            'low': content['low']
        }
        
        with engine.begin() as conn:
            stmt = pg_insert(x_table).values([record])
            update_dict = {c.name: getattr(stmt.excluded, c.name) for c in x_table.columns if c.name != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)
    except Exception as e:
        logging.error(f"X DB save error: {e}")

def save_r_sync(result):
    try:
        content = result["content"]
        record = {
            'symbol': content['symbol'],
            'buy': content['buy'],
            'sell': content['sell'],
            'room': content['room']
        }
        
        with engine.begin() as conn:
            stmt = pg_insert(r_table).values([record])
            update_dict = {c.name: getattr(stmt.excluded, c.name) for c in r_table.columns if c.name != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)
    except Exception as e:
        logging.error(f"R DB save error: {e}")

def save_mi_sync(result):
    try:
        content = result["content"]
        record = {
            'symbol': content['symbol'],
            'point': content['point'],
            'change': content['change'],
            'ratioChange': content['ratioChange'],
            'totalVolume': content['totalVolume'],
            'totalValue': content['totalValue'],
            'advancers': content['advancersDecliners'][0],
            'noChange': content['advancersDecliners'][1],
            'decliners': content['advancersDecliners'][2]
        }
        
        with engine.begin() as conn:
            stmt = pg_insert(mi_table).values([record])
            update_dict = {c.name: getattr(stmt.excluded, c.name) for c in mi_table.columns if c.name != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)
    except Exception as e:
        logging.error(f"MI DB save error: {e}")

# -------------------- Trading time check --------------------
def is_trading_time():
    now = datetime.now()
    today = now.date()
    if now.weekday() >= 5 or today in holiday:
        return False
    t9h = dtime(2, 0)
    t12h = dtime(5, 0)
    t13h = dtime(6, 0)
    t15h = dtime(8, 0)
    return (t9h <= now.time() <= t12h) or (t13h <= now.time() <= t15h)

# -------------------- WebSocket Management --------------------
async def broadcast_to_all(channel, data: dict):
    """Broadcast to all clients in a channel"""
    if not clients[channel]:
        return
        
    dead_clients = []
    for ws in clients[channel]:
        try:
            await ws.send_json(data)
        except Exception:
            dead_clients.append(ws)
    
    # Clean up dead connections
    for ws in dead_clients:
        clients[channel].discard(ws)

async def broadcast_to_symbol(symbol, data: dict):
    """Broadcast to clients listening to specific symbol"""
    if symbol not in symbol_clients:
        return
        
    dead_clients = []
    for ws in symbol_clients[symbol]:
        try:
            await ws.send_json(data)
        except Exception:
            dead_clients.append(ws)
    
    # Clean up dead connections
    for ws in dead_clients:
        symbol_clients[symbol].discard(ws)

async def websocket_endpoint(websocket: WebSocket, channel: str, symbol: str = None):
    await websocket.accept()
    
    # Add to appropriate client set
    clients[channel].add(websocket)
    if symbol:
        if symbol not in symbol_clients:
            symbol_clients[symbol] = set()
        symbol_clients[symbol].add(websocket)
        logging.info(f"✅ Client connected to {channel}:{symbol}")
    else:
        logging.info(f"✅ Client connected to {channel}:ALL")
    
    try:
        while True:
            # Send keepalive ping
            ping_data = {"type": "ping", "ts": datetime.now().isoformat()}
            if symbol:
                ping_data["symbol"] = symbol
            await websocket.send_json(ping_data)
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        pass
    finally:
        # Cleanup
        clients[channel].discard(websocket)
        if symbol and symbol in symbol_clients:
            symbol_clients[symbol].discard(websocket)
        logging.info(f"❌ Client disconnected from {channel}" + (f":{symbol}" if symbol else ""))

@app.websocket("/ws/eboard_table")
async def websocket_x_all(ws: WebSocket):
    await websocket_endpoint(ws, "X")

@app.websocket("/ws/eboard_table/{symbol}")
async def websocket_x_symbol(ws: WebSocket, symbol: str):
    await websocket_endpoint(ws, "X", symbol.upper())

@app.websocket("/ws/eboard_foreign")
async def websocket_r_all(ws: WebSocket):
    await websocket_endpoint(ws, "R")

@app.websocket("/ws/eboard_foreign/{symbol}")
async def websocket_r_symbol(ws: WebSocket, symbol: str):
    await websocket_endpoint(ws, "R", symbol.upper())

@app.websocket("/ws/indices")
async def websocket_mi_all(ws: WebSocket):
    await websocket_endpoint(ws, "MI")

@app.websocket("/ws/indices/{symbol}")
async def websocket_mi_symbol(ws: WebSocket, symbol: str):
    await websocket_endpoint(ws, "MI", symbol.upper())

# -------------------- Individual Symbol Streaming --------------------
def create_symbol_message_handler(symbol, channel):
    """Factory function to create message handler for each symbol"""
    
    def on_message_X_symbol(message):
        try:
            data = json.loads(message.get("Content", "{}"))
            if data.get('Symbol') != symbol:
                return  # Skip if not our symbol
                
            result = {
                'function': 'eboard_table',
                'symbol': symbol,  # Add symbol to result
                'content': {
                    'symbol': symbol,
                    'ceiling': data['Ceiling'] / 1000,
                    'floor': data['Floor'] / 1000,
                    'refPrice': data['RefPrice'] / 1000,
                    'buy': {
                        'price': [data['BidPrice1'] / 1000, data['BidPrice2'] / 1000, data['BidPrice3'] / 1000],
                        'vol': [data['BidVol1'], data['BidVol2'], data['BidVol3']]
                    },
                    'match': {
                        'price': data['LastPrice'] / 1000,
                        'vol': data['LastVol'],
                        'change': data['Change'],
                        'ratioChange': data['RatioChange'],
                    },
                    'sell': {
                        'price': [data['AskPrice1'] / 1000, data['AskPrice2'] / 1000, data['AskPrice3'] / 1000],
                        'vol': [data['AskVol1'], data['AskVol2'], data['AskVol3']]
                    },
                    'totalVol': data['TotalVol'],
                    'high': data['High'] / 1000,
                    'low': data['Low'] / 1000,
                    'timestamp': datetime.now().isoformat()  # Add timestamp for real-time tracking
                }
            }
            
            # Broadcast immediately - no delay
            asyncio.run_coroutine_threadsafe(
                broadcast_to_all("X", result), 
                app.state.loop
            )
            
            # Broadcast to symbol-specific clients
            asyncio.run_coroutine_threadsafe(
                broadcast_to_symbol(symbol, result), 
                app.state.loop
            )
            
            # Queue DB operation (non-blocking)
            try:
                app.state.loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(db_queue.put(("X", result)))
                )
            except:
                pass  # Don't let DB issues block real-time updates
                
            last_msg_time[f"X_{symbol}"] = time.time()
            
        except Exception as e:
            logging.error(f"❗ X message error for {symbol}: {e}")
    
    def on_message_R_symbol(message):
        try:
            data = json.loads(message.get("Content", "{}"))
            if data.get('Symbol') != symbol:
                return
                
            result = {
                'function': 'eboard_foreign',
                'symbol': symbol,
                'content': {
                    'symbol': symbol,
                    'buy': data['BuyVol'],
                    'sell': data['SellVol'],
                    'room': data['CurrentRoom'],
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            asyncio.run_coroutine_threadsafe(broadcast_to_all("R", result), app.state.loop)
            asyncio.run_coroutine_threadsafe(broadcast_to_symbol(symbol, result), app.state.loop)
            
            try:
                app.state.loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(db_queue.put(("R", result)))
                )
            except:
                pass
                
            last_msg_time[f"R_{symbol}"] = time.time()
            
        except Exception as e:
            logging.error(f"❗ R message error for {symbol}: {e}")
    
    # Return appropriate handler based on channel
    if channel == "X":
        return on_message_X_symbol
    elif channel == "R":
        return on_message_R_symbol
    else:
        return None

def on_error_factory(symbol, channel):
    """Factory for error handlers"""
    def on_error(error):
        logging.error(f"❗ {channel}:{symbol} error: {error}")
        if f"{channel}_{symbol}" in stop_events:
            stop_events[f"{channel}_{symbol}"].set()
    return on_error

def stream_individual_symbol(symbol, channel):
    """Stream data for individual symbol"""
    stream_key = f"{channel}_{symbol}"
    stop_events[stream_key] = threading.Event()
    
    message_handler = create_symbol_message_handler(symbol, channel)
    error_handler = on_error_factory(symbol, channel)
    
    if not message_handler:
        logging.error(f"No message handler for channel {channel}")
        return
    
    while True:
        try:
            stop_events[stream_key].clear()
            
            # Create new stream for this symbol
            mm = MarketDataStream(config, MarketDataClient(config))
            last_msg_time[stream_key] = time.time()
            
            stream_code = f"{channel}:{symbol}"
            mm.start(message_handler, error_handler, stream_code)
            
            # Keep alive during trading time
            while is_trading_time() and not stop_events[stream_key].is_set():
                time.sleep(1)
                
                # Check for timeout (no data for 60 seconds)
                if time.time() - last_msg_time.get(stream_key, time.time()) > 60:
                    logging.warning(f"⚠️ {stream_key}: no data for 60s, reconnecting...")
                    stop_events[stream_key].set()
                    
        except Exception as e:
            logging.exception(f"⚠️ {stream_key} stream error: {e}")
        finally:
            try:
                mm.stop()
            except:
                pass
            
            if is_trading_time():
                logging.info(f"🔄 {stream_key} reconnecting in {RECONNECT_DELAY}s...")
                time.sleep(RECONNECT_DELAY)
            else:
                logging.info(f"🛑 {stream_key} stopped (outside trading hours)")
                break

# -------------------- Stream Management --------------------
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

def start_all_symbol_streams(channel):
    """Start individual streams for all symbols"""
    for symbol in symbols_list:
        executor.submit(stream_individual_symbol, symbol, channel)
        # Small delay to avoid overwhelming the API
        time.sleep(0.1)
    
    logging.info(f"🚀 Started {len(SYMBOLS)} individual {channel} streams")

# -------------------- Monitoring & Management --------------------
@app.get("/symbols")
async def get_symbols():
    return {
        "symbols": SYMBOLS,
        "total": len(SYMBOLS),
        "active_streams": len([k for k in last_msg_time.keys() if time.time() - last_msg_time[k] < 60])
    }

@app.get("/status")
async def get_status():
    active_streams = {k: time.time() - v for k, v in last_msg_time.items() if time.time() - v < 300}
    
    return {
        "is_trading_time": is_trading_time(),
        "total_symbols": len(SYMBOLS),
        "active_streams": len(active_streams),
        "connected_clients": {k: len(v) for k, v in clients.items()},
        "symbol_clients": {k: len(v) for k, v in symbol_clients.items() if v},
        "db_queue_size": db_queue.qsize(),
        "recent_active_streams": list(active_streams.keys())[:10]  # Show 10 most recent
    }

@app.get("/symbol/{symbol}/status")
async def get_symbol_status(symbol: str):
    symbol = symbol.upper()
    stream_keys = [f"X_{symbol}", f"R_{symbol}"]
    
    status = {}
    for key in stream_keys:
        if key in last_msg_time:
            status[key] = {
                "last_update": datetime.fromtimestamp(last_msg_time[key]).isoformat(),
                "seconds_ago": time.time() - last_msg_time[key],
                "is_active": time.time() - last_msg_time[key] < 60
            }
        else:
            status[key] = {"status": "not_started"}
    
    return {
        "symbol": symbol,
        "streams": status,
        "connected_clients": len(symbol_clients.get(symbol, set()))
    }

# -------------------- FastAPI Lifecycle --------------------
@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    app.state.loop = loop
    
    # Start DB worker
    asyncio.create_task(db_worker())
    
    # Start individual symbol streams for X channel
    start_all_symbol_streams("X")
    
    # Uncomment for other channels
    # start_all_symbol_streams("R")
    
    logging.info(f"🚀 Started real-time streaming for {len(SYMBOLS)} symbols")
    logging.info(f"💾 DB queue worker started")
    logging.info(f"🌐 WebSocket endpoints available:")
    logging.info(f"   - /ws/eboard_table (all symbols)")
    logging.info(f"   - /ws/eboard_table/{{symbol}} (specific symbol)")

@app.on_event("shutdown")
async def shutdown_event():
    # Signal all streams to stop
    for stop_event in stop_events.values():
        stop_event.set()
    
    # Stop DB worker
    await db_queue.put(None)
    
    # Shutdown executor
    executor.shutdown(wait=True)
    
    logging.info("🛑 All streams stopped")