import ccxt.pro as ccxtpro
import asyncio
import os
import zmq
import zmq.asyncio

import json
import websockets
from datetime import datetime
import zstandard as zstd
import signal
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

log_pre_fix = f'./data/{os.path.splitext(os.path.basename(__file__))[0]}'

# Ensure the data directory exists
os.makedirs('./data', exist_ok=True)

# Global variables for signal handling
log_file_handler = None
cctx = None
running = True

def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

async def watch_tickers_ccxtpro(exchange, symbol, exchange_name, ticker_data, coin):
    while running:
        try:
            order_book = await exchange.watch_order_book(symbol)
            ticker_data[coin][exchange_name] = {
                'datetime': order_book['datetime'],
                'timestamp': order_book['timestamp'],
                'bid': float(order_book['bids'][0][0]) if order_book['bids'] else None,
                'ask': float(order_book['asks'][0][0]) if order_book['asks'] else None
            }
        except Exception as e:
            print(get_current_datetime(), f'Error with {exchange_name} for {coin}:', "upbit", e)
            await asyncio.sleep(60)  # Retry after 1 minute

async def watch_korbit(symbols, ticker_data, coins):
    uri = "wss://ws2.korbit.co.kr/v1/user/push"
    while running:
        try:
            async with websockets.connect(uri) as websocket:
                now = datetime.now()
                timestamp = int(now.timestamp() * 1000)
                subscribe_fmt = {
                    "accessToken": None,
                    "timestamp": timestamp,
                    "event": "korbit:subscribe",
                    "data": {
                        "channels": symbols
                    }
                }
                await websocket.send(json.dumps(subscribe_fmt))

                while running:
                    recv_data = await websocket.recv()
                    data = json.loads(recv_data)
                    if 'event' in data and (data['event'] == 'korbit:connected' or data['event'] == 'korbit:subscribe'):
                        continue
                    if 'data' in data and 'channel' in data['data']:
                        symbol = data['data']['currency_pair']
                        coin = symbol.split('_')[0].upper()
                        ticker_data[coin]['korbit'] = {
                            'bid': float(data['data']['bid']),
                            'ask': float(data['data']['ask']),
                            'timestamp': data['timestamp']
                        }
        except websockets.ConnectionClosed:
            if not running:
                break
            print(get_current_datetime(), "Connection closed. Retrying in 1 minute...", "korbit")
            await asyncio.sleep(60)  # Retry after 1 minute
        except websockets.InvalidURI as e:
            print(get_current_datetime(), f"Invalid URI: {e.uri}. Retrying in 1 minute...", "korbit")
            await asyncio.sleep(60)  # Retry after 1 minute
        except Exception as e:
            print(get_current_datetime(), f"Unexpected error: {e}. Retrying in 1 minute...", "korbit")
            await asyncio.sleep(60)  # Retry after 1 minute

async def watch_coinone(symbols, ticker_data, coins):
    uri = "wss://stream.coinone.co.kr"
    while running:
        try:
            async with websockets.connect(uri) as websocket:
                subscribe_messages = [
                    {
                        "request_type": "SUBSCRIBE",
                        "channel": "ORDERBOOK",
                        "topic": {
                            "quote_currency": "KRW",
                            "target_currency": symbol
                        }
                    } for symbol in symbols
                ]
                for subscribe_message in subscribe_messages:
                    await websocket.send(json.dumps(subscribe_message))

                while running:
                    recv_data = await websocket.recv()
                    data = json.loads(recv_data)
                    if data['response_type'] == 'DATA' and data['channel'] == 'ORDERBOOK':
                        order_book = data['data']
                        bids = order_book['bids']
                        asks = order_book['asks']
                        coin = order_book['target_currency']
                        ticker_data[coin]['coinone'] = {
                            'datetime': datetime.fromtimestamp(order_book['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                            'timestamp': order_book['timestamp'],
                            'bid': float(bids[0]['price']) if bids else None,
                            'ask': float(asks[len(asks)-1]['price']) if asks else None
                        }
        except websockets.ConnectionClosed:
            if not running:
                break
            print(get_current_datetime(), "Connection closed. Retrying in 1 minute...", "coinone")
            await asyncio.sleep(60)  # Retry after 1 minute
        except Exception as e:
            print(get_current_datetime(), f"Unexpected error: {e}. Retrying in 1 minute...", "coinone")
            await asyncio.sleep(60)  # Retry after 1 minute

async def watch_bithumb(symbols, ticker_data, coins):
    uri = "wss://pubwss.bithumb.com/pub/ws"
    while running:
        try:
            async with websockets.connect(uri) as websocket:
                subscribe_message = {
                    "type": "orderbooksnapshot",
                    "symbols": [f"{symbol}_KRW" for symbol in symbols]
                }
                await websocket.send(json.dumps(subscribe_message))

                while running:
                    recv_data = await websocket.recv()
                    data = json.loads(recv_data)
                    if 'type' in data and data['type'] == 'orderbooksnapshot':
                        order_book = data['content']
                        bids = order_book['bids']
                        asks = order_book['asks']
                        coin = order_book['symbol'].split('_')[0]

                        try:
                            timestamp = int(order_book['datetime']) // int(1e6)
                            readable_datetime = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        except (ValueError, OSError):
                            readable_datetime = 'Invalid timestamp'
                            timestamp = None

                        bid_price = float(bids[0][0]) if bids else None
                        ask_price = float(asks[0][0]) if asks else None

                        ticker_data[coin]['bithumb'] = {
                            'datetime': readable_datetime,
                            'timestamp': timestamp,
                            'bid': bid_price,
                            'ask': ask_price
                        }
        except websockets.ConnectionClosed:
            if not running:
                break
            print(get_current_datetime(), "Connection closed. Retrying in 1 minute...", "bithumb")
            await asyncio.sleep(60)  # Retry after 1 minute
        except Exception as e:
            print(get_current_datetime(), f"Unexpected error: {e}. Retrying in 1 minute...", "bithumb")
            await asyncio.sleep(60)  # Retry after 1 minute

async def publish_ticker_data(ticker_data, exchanges, coins, publisher):
    global log_file_handler, cctx, running
    current_date = datetime.now().strftime('%Y%m%d')
    log_file = f"{log_pre_fix}_sum_{current_date}.log.zst"
    dctx = zstd.ZstdCompressor()

    def open_log_file(date):
        log_file_name = f"{log_pre_fix}_sum_{date}.log.zst"
        f = open(log_file_name, 'ab')  # 'ab' 모드로 파일을 열어 추가 기록 가능하도록 설정
        return f, dctx.stream_writer(f)

    log_file_handler, cctx = open_log_file(current_date)

    while running:
        await asyncio.sleep(1)  # Adjust the interval as needed

        # Check if the date has changed
        new_date = datetime.now().strftime('%Y%m%d')
        if new_date != current_date:
            cctx.close()
            log_file_handler.close()
            current_date = new_date
            log_file_handler, cctx = open_log_file(current_date)

        for coin in coins:
            for base_exchange in exchanges:
                for compare_exchange in exchanges:
                    if base_exchange != compare_exchange:
                        base_bid = ticker_data[coin][base_exchange]['bid']
                        compare_ask = ticker_data[coin][compare_exchange]['ask']
                        if base_bid is not None and compare_ask is not None:
                            diff = ((base_bid - compare_ask) / compare_ask) * 100
                            message = {
                                'symbol': coin,
                                'base_exchange': base_exchange,
                                'compare_exchange': compare_exchange,
                                'base_bid': base_bid,
                                'compare_ask': compare_ask,
                                'base_time': ticker_data[coin][base_exchange]['timestamp'],
                                'compare_time': ticker_data[coin][compare_exchange]['timestamp'],
                            }
                            publisher.send_string(str(message))
                            cctx.write((str(message) + '\n').encode('utf-8'))

async def relay_messages(publisher):
    #context = zmq.Context()
    context = zmq.asyncio.Context()

    receiver = context.socket(zmq.PULL)
    receiver.bind("tcp://*:6021")

    while True:
        message = await receiver.recv_string()
        publisher.send_string(message)

def handle_signal(signal, frame):
    global running, log_file_handler, cctx
    print(get_current_datetime(), "Signal received, shutting down...")
    running = False
    if cctx:
        cctx.close()
    if log_file_handler:
        log_file_handler.close()
    asyncio.get_event_loop().stop()

def setup_signal_handlers():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

async def main_loop():
    setup_signal_handlers()
    #context = zmq.Context()
    context = zmq.asyncio.Context()
    publisher = context.socket(zmq.PUB)
    publisher.bind("tcp://*:6020")

    coins = os.getenv('DM_COINS', 'BTC,ETH,BCH,ETC,SOL,USDT').split(',')
    exchanges = os.getenv('DM_EXCHANGES', 'upbit,bithumb,coinone,korbit').split(',')
    exchange_instances_pro = {
        'upbit': ccxtpro.upbit({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future'
            }
        })
    }

    ticker_data = {coin: {exchange: {'bid': None, 'ask': None, 'timestamp': None} for exchange in exchanges} for coin in coins}

    tasks = []
    for coin in coins:
        for exchange_name, exchange in exchange_instances_pro.items():
            symbol = f"{coin}/KRW"
            tasks.append(watch_tickers_ccxtpro(exchange, symbol, exchange_name, ticker_data, coin))

    korbit_symbols = [f"ticker:{coin.lower()}_krw" for coin in coins]
    tasks.append(watch_korbit(korbit_symbols, ticker_data, coins))

    coinone_symbols = [coin for coin in coins]
    tasks.append(watch_coinone(coinone_symbols, ticker_data, coins))

    bithumb_symbols = [coin for coin in coins]
    tasks.append(watch_bithumb(bithumb_symbols, ticker_data, coins))

    tasks.append(publish_ticker_data(ticker_data, exchanges, coins, publisher))
    tasks.append(relay_messages(publisher))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main_loop())

