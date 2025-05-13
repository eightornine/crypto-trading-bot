import sys
import os
# Remove the current working directory from sys.path to avoid loading local talib
current_dir = os.getcwd()
if current_dir in sys.path:
    sys.path.remove(current_dir)

import asyncio
import json
import logging
import smtplib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from email.mime.text import MIMEText
import ccxt.async_support as ccxt
import requests
from keras.models import Sequential
from keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler
import talib  # Use talib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('/root/crypto-trading-bot/xrp_moonshot.log'), logging.StreamHandler()]
)
logger = logging.getLogger()

# Load environment variables
load_dotenv()
required_env_vars = {
    "INFLUXDB_TOKEN": os.getenv("INFLUXDB_TOKEN"),
    "KRAKEN_API_KEY": os.getenv("KRAKEN_API_KEY"),
    "KRAKEN_API_SECRET": os.getenv("KRAKEN_API_SECRET"),
    "COINBASE_API_KEY": os.getenv("COINBASE_API_KEY"),
    "COINBASE_API_SECRET": os.getenv("COINBASE_API_SECRET"),
    "LUNARCRUSH_API_KEY": os.getenv("LUNARCRUSH_API_KEY"),
    "EMAIL_SENDER": os.getenv("EMAIL_SENDER"),
    "EMAIL_PASSWORD": os.getenv("EMAIL_PASSWORD"),
    "X_API_KEY": os.getenv("X_API_KEY")
}
missing_vars = [key for key, value in required_env_vars.items() if value is None]
if missing_vars:
    logger.error(f"Missing environment variables: {', '.join(missing_vars)}. Set them in .env.")
    raise EnvironmentError(f"Missing environment variables: {', '.join(missing_vars)}")

# Load configuration from JSON
config_file = "/root/crypto-trading-bot/config.json"
try:
    with open(config_file, 'r') as f:
        config = json.load(f)
    SCORE_THRESHOLD = config.get('SCORE_THRESHOLD', 80)
    STOP_LOSS_PERCENT = config.get('STOP_LOSS_PERCENT', 0.05)
    TAKE_PROFIT_PERCENT = config.get('TAKE_PROFIT_PERCENT', 0.10)
    MAX_DRAWDOWN_PERCENT = config.get('MAX_DRAWDOWN_PERCENT', 0.20)
    MIN_AVG_VOLUME = config.get('MIN_AVG_VOLUME', 1000)  # XRP
    MIN_LIQUIDITY = config.get('MIN_LIQUIDITY', 5000)   # USD
    MAX_CONCURRENT_TRADES = config.get('MAX_CONCURRENT_TRADES', 3)
    FETCH_INTERVAL = config.get('FETCH_INTERVAL', 60)   # Seconds
    SIMULATION_MODE = config.get('SIMULATION_MODE', True)
except Exception as e:
    logger.error(f"Failed to load config: {e}. Using defaults.")
    SCORE_THRESHOLD = 80
    STOP_LOSS_PERCENT = 0.05
    TAKE_PROFIT_PERCENT = 0.10
    MAX_DRAWDOWN_PERCENT = 0.20
    MIN_AVG_VOLUME = 1000
    MIN_LIQUIDITY = 5000
    MAX_CONCURRENT_TRADES = 3
    FETCH_INTERVAL = 60
    SIMULATION_MODE = True

# Configurations
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = required_env_vars["INFLUXDB_TOKEN"]
INFLUXDB_ORG = "eightornine_trading"
INFLUXDB_BUCKET = "market_data"
EMAIL_SENDER = required_env_vars["EMAIL_SENDER"]
EMAIL_PASSWORD = required_env_vars["EMAIL_PASSWORD"]
KRAKEN_API_KEY = required_env_vars["KRAKEN_API_KEY"]
KRAKEN_API_SECRET = required_env_vars["KRAKEN_API_SECRET"]
COINBASE_API_KEY = required_env_vars["COINBASE_API_KEY"]
COINBASE_API_SECRET = required_env_vars["COINBASE_API_SECRET"]
LUNARCRUSH_API_KEY = required_env_vars["LUNARCRUSH_API_KEY"]
X_API_KEY = required_env_vars["X_API_KEY"]

# Initialize InfluxDB
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
query_api = influx_client.query_api()
logger.info("InfluxDB client initialized successfully.")

# Initialize exchanges
kraken = ccxt.kraken({"apiKey": KRAKEN_API_KEY, "secret": KRAKEN_API_SECRET})
coinbase = ccxt.coinbase({"apiKey": COINBASE_API_KEY, "secret": COINBASE_API_SECRET})

# Global state
STARTING_BALANCE = 120  # XRP
current_balance = STARTING_BALANCE
active_trades = {}
monitor_tasks = {}
xrp_data_file = "/root/crypto-trading-bot/xrp_data.csv"
potential_trades_file = "/root/crypto-trading-bot/xrp_potential_trades.csv"
paused_state_file = "/root/crypto-trading-bot/xrp_paused_state.txt"
portfolio_value_history = [STARTING_BALANCE]
max_portfolio_value = STARTING_BALANCE
trade_stats = {'total_trades': 0, 'wins': 0, 'total_profit_xrp': 0.0}
last_xrp_usd_price = 2.20  # Default fallback
fee_data = {'current_fee': 0.001, 'historical_fees': [0.001] * 24}  # Simulated fee data
sentiment_scores = {}
x_hype_score = 0
last_volatility = 0.05  # Default ATR/price ratio

# Ensure directory exists
os.makedirs(os.path.dirname(paused_state_file), exist_ok=True)

# Load/save paused state
def load_paused_state():
    global paused
    try:
        if os.path.exists(paused_state_file):
            with open(paused_state_file, 'r') as f:
                paused = f.read().strip().lower() == 'true'
                logger.info(f"Loaded paused state: {paused}")
        else:
            paused = False
            logger.info("No paused state file found, initializing paused to False")
    except Exception as e:
        logger.error(f"Failed to load paused state: {e}. Defaulting to True.")
        paused = True
    return paused

def save_paused_state():
    global paused
    try:
        with open(paused_state_file, 'w') as f:
            f.write(str(paused).lower())
        logger.info(f"Saved paused state: {paused}")
    except Exception as e:
        logger.error(f"Failed to save paused state: {e}. Setting paused to True.")
        paused = True

paused = load_paused_state()

# Email notification
async def send_email_notification(subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    try:
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        logger.info("Email notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")

# Manage file size
def manage_file_size(filename):
    if os.path.exists(filename) and os.path.getsize(filename) > 500 * 1024 * 1024:  # 500 MB
        df = pd.read_csv(filename)
        df = df.tail(10000)  # Keep last 10,000 entries
        df.to_csv(filename, index=False)
        logger.info(f"Trimmed {filename} to last 10,000 entries.")

# Fetch XRP data
async def fetch_xrp_data():
    global last_xrp_usd_price, last_volatility
    try:
        kraken_price = (await kraken.fetch_ticker('XRP/USD'))['last']
        coinbase_price = (await coinbase.fetch_ticker('XRP/USD'))['last']
        last_xrp_usd_price = kraken_price  # Prefer Kraken
        order_book = await kraken.fetch_order_book('XRP/USD', limit=10)
        ohlcv_1m = await kraken.fetch_ohlcv('XRP/USD', timeframe='1m', limit=50)
        ohlcv_15m = await kraken.fetch_ohlcv('XRP/USD', timeframe='15m', limit=50)
        ohlcv_1h = await kraken.fetch_ohlcv('XRP/USD', timeframe='1h', limit=50)
        df_1m = pd.DataFrame(ohlcv_1m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df_15m = pd.DataFrame(ohlcv_15m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Calculate EMAs, ATR, Bollinger Bands, RSI, MACD using talib
        df_1m['ema5_1m'] = talib.EMA(df_1m['close'], timeperiod=5)
        df_1m['ema20_1m'] = talib.EMA(df_1m['close'], timeperiod=20)
        df_15m['ema5_15m'] = talib.EMA(df_15m['close'], timeperiod=5)
        df_15m['ema20_15m'] = talib.EMA(df_15m['close'], timeperiod=20)
        df_1h['ema5_1h'] = talib.EMA(df_1h['close'], timeperiod=5)
        df_1h['ema20_1h'] = talib.EMA(df_1h['close'], timeperiod=20)
        atr_1m = talib.ATR(df_1m['high'], df_1m['low'], df_1m['close'], timeperiod=14)[-1]
        upper, middle, lower = talib.BBANDS(df_1m['close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
        df_1m['bb_upper'] = upper
        df_1m['bb_middle'] = middle
        df_1m['bb_lower'] = lower
        bb_width = (df_1m['bb_upper'].iloc[-1] - df_1m['bb_lower'].iloc[-1]) / df_1m['bb_middle'].iloc[-1]
        rsi = talib.RSI(df_1m['close'], timeperiod=14)[-1]
        macd, signal, _ = talib.MACD(df_1m['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        macd_signal = macd[-1] > signal[-1]
        
        # Calculate recent high/low for breakout
        recent_high = df_1m['high'].tail(20).max()
        recent_low = df_1m['low'].tail(20).min()
        breakout_upper = recent_high + (2 * atr_1m)
        breakout_lower = recent_low - (2 * atr_1m)
        
        last_volatility = atr_1m / kraken_price if kraken_price > 0 else last_volatility
        
        data = {
            'timestamp': datetime.utcnow().isoformat(),
            'price': kraken_price,
            'coinbase_price': coinbase_price,
            'volume': df_1m['volume'].iloc[-1],
            'ema5_1m': df_1m['ema5_1m'].iloc[-1],
            'ema20_1m': df_1m['ema20_1m'].iloc[-1],
            'ema5_15m': df_15m['ema5_15m'].iloc[-1],
            'ema20_15m': df_15m['ema20_15m'].iloc[-1],
            'ema5_1h': df_1h['ema5_1h'].iloc[-1],
            'ema20_1h': df_1h['ema20_1h'].iloc[-1],
            'atr': atr_1m,
            'bb_width': bb_width,
            'rsi': rsi,
            'macd_signal': macd_signal,
            'breakout_upper': breakout_upper,
            'breakout_lower': breakout_lower,
            'bids': order_book['bids'],
            'asks': order_book['asks']
        }
        
        point = Point("xrp_moonshot").field("price", float(kraken_price)).time(data['timestamp'], WritePrecision.NS)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        return data
    except Exception as e:
        logger.error(f"Failed to fetch XRP data: {e}. Using last known price: {last_xrp_usd_price}")
        return None

# Fetch sentiment and fee data
async def fetch_sentiment_score():
    try:
        url = "https://api.lunarcrush.com/v2?data=assets&symbol=XRP&key=" + LUNARCRUSH_API_KEY
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            sentiment = data['data'][0]['sentiment_relative'] / 100  # 0 to 1
            sentiment_scores['XRP'] = sentiment
            return sentiment
        logger.warning(f"Sentiment fetch failed: {response.status_code}")
        return sentiment_scores.get('XRP', 0.5)
    except Exception as e:
        logger.error(f"Sentiment fetch error: {e}")
        return sentiment_scores.get('XRP', 0.5)

async def fetch_x_hype_score():
    global x_hype_score
    try:
        url = "https://api.twitter.com/2/tweets/search/recent?query=XRP"
        headers = {"Authorization": f"Bearer {X_API_KEY}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            tweets = response.json()['data']
            texts = [tweet['text'] for tweet in tweets[:100]]
            scores = [1 if "bull" in t.lower() else -1 if "bear" in t.lower() else 0 for t in texts]
            x_hype_score = min(max((sum(scores) / len(scores) + 1) * 50, 0), 100)  # 0-100
        return x_hype_score
    except Exception as e:
        logger.error(f"X hype fetch error: {e}")
        return x_hype_score

async def fetch_fee_data():
    global fee_data
    try:
        # Simulate fee data (since RippleNet API is not accessible)
        current_fee = 0.001 + np.random.uniform(-0.0002, 0.0002)  # Simulate fee between 0.08% and 0.12%
        fee_data['historical_fees'].append(current_fee)
        fee_data['historical_fees'] = fee_data['historical_fees'][-24:]  # Keep last 24 hours
        fee_data['current_fee'] = current_fee
    except Exception as e:
        logger.error(f"Fee fetch error: {e}")

# Predict low-fee periods
async def predict_low_fee_period():
    try:
        fees = fee_data['historical_fees']
        avg_fee = sum(fees) / len(fees)
        current_fee = fee_data.get('current_fee', 0.001)
        return current_fee < avg_fee * 0.8  # True if current fee is 20% below average
    except Exception as e:
        logger.error(f"Fee prediction error: {e}")
        return False

# LSTM model for trend reversal
scaler = MinMaxScaler()
lstm_model = Sequential([LSTM(50, return_sequences=True, input_shape=(90, 2)), LSTM(50), Dense(1)])
lstm_model.compile(optimizer='adam', loss='mse')

async def train_lstm():
    try:
        query = f'from(bucket: "{INFLUXDB_BUCKET}") |> range(start: -90d) |> filter(fn: (r) => r["_measurement"] == "xrp_moonshot")'
        tables = query_api.query(query, org=INFLUXDB_ORG)
        prices = []
        volumes = []
        for table in tables:
            for record in table.records:
                prices.append(float(record["_value"]))
                volumes.append(float(record.get("volume", 0)))
        if len(prices) < 90:
            logger.warning("Insufficient data for LSTM training")
            return
        data = np.array(list(zip(prices[-90:], volumes[-90:]))).reshape(-1, 2)
        scaled_data = scaler.fit_transform(data)
        X = np.array([scaled_data[i-90:i] for i in range(90, len(scaled_data))])
        y = scaled_data[90:, 0]  # Predict price
        lstm_model.fit(X, y, epochs=10, batch_size=32, verbose=0)
        logger.info("LSTM model trained successfully")
    except Exception as e:
        logger.error(f"LSTM training failed: {e}")

async def predict_trend_reversal(current_price, current_volume):
    try:
        query = f'from(bucket: "{INFLUXDB_BUCKET}") |> range(start: -90d) |> filter(fn: (r) => r["_measurement"] == "xrp_moonshot") |> limit(n: 89)'
        tables = query_api.query(query, org=INFLUXDB_ORG)
        prices = [float(record["_value"]) for table in tables for record in table.records]
        volumes = [float(record.get("volume", 0)) for table in tables for record in table.records]
        if len(prices) < 89:
            logger.warning("Insufficient data for LSTM prediction")
            return None
        prices.append(current_price)
        volumes.append(current_volume)
        data = np.array(list(zip(prices, volumes))).reshape(-1, 2)
        scaled = scaler.transform(data)
        X = scaled.reshape(1, 90, 2)
        pred = lstm_model.predict(X, verbose=0)
        pred_price = scaler.inverse_transform(np.array([[pred[0][0], 0]]))[0][0]
        return pred_price > current_price  # True if upward trend predicted
    except Exception as e:
        logger.error(f"LSTM prediction failed: {e}")
        return None

# Calculate trade signal
async def calculate_trade_signal(data):
    sentiment = await fetch_sentiment_score()
    x_hype = await fetch_x_hype_score()
    fee_level = fee_data.get('current_fee', 0.001)  # Default 0.1%
    
    # Multi-timeframe EMA confirmation
    signal_1m = data['ema5_1m'] > data['ema20_1m'] and data['ema5_1m'][-2] <= data['ema20_1m'][-2]
    signal_15m = data['ema5_15m'] > data['ema20_15m']
    signal_1h = data['ema5_1h'] > data['ema20_1h']
    base_score = 100 if (signal_1m and signal_15m and signal_1h) else 0
    
    # Sentiment boost (LunarCrush >60%, X hype >80)
    sentiment_boost = 20 if sentiment > 0.6 else 0
    hype_boost = 20 if x_hype > 80 else 0
    
    # Volume and liquidity check
    volume_score = min((data['volume'] / MIN_AVG_VOLUME) * 30, 30)
    liquidity = data['price'] * STARTING_BALANCE
    liquidity_score = min((liquidity / MIN_LIQUIDITY) * 50, 50)
    
    # Breakout signal (placeholder for Volatility Breakout module)
    breakout_score = 50 if data['price'] > data['breakout_upper'] and data['volume'] > MIN_AVG_VOLUME else 0
    
    total_score = base_score + sentiment_boost + hype_boost + volume_score + liquidity_score + breakout_score
    return total_score, data['atr'], data['bb_width'], fee_level, data['rsi'], data['macd_signal']

# Arbitrage opportunity
async def check_arbitrage():
    try:
        kraken_price = (await kraken.fetch_ticker('XRP/USD'))['bid']
        coinbase_price = (await coinbase.fetch_ticker('XRP/USD'))['ask']
        spread = (coinbase_price - kraken_price) / kraken_price
        fees = 0.0026 + 0.006  # Kraken 0.26% + Coinbase 0.6%
        if spread > fees + 0.005:  # 0.5% profit threshold
            return kraken_price, coinbase_price, spread
        return None, None, 0
    except Exception as e:
        logger.error(f"Arbitrage check failed: {e}")
        return None, None, 0

# Scalping opportunity
async def check_scalping(data):
    bids = np.array([b[0] for b in data['bids']])
    asks = np.array([a[0] for a in data['asks']])
    bid_depth = sum(b[1] for b in data['bids'])
    ask_depth = sum(a[1] for a in data['asks'])
    if bid_depth > ask_depth * 1.5 and asks[0] - bids[0] < 0.003 * data['price']:  # Tight spread, buy pressure
        return bids[0] + 0.001, asks[0] - 0.001  # Scalp 0.1% profit
    return None, None

# Simulate trade
async def simulate_trade(data, trade_type='ema'):
    global current_balance, paused, last_volatility
    if paused:
        logger.info("Trading paused due to max drawdown.")
        return
    
    total_score, atr, bb_width, fee_level, rsi, macd_signal = await calculate_trade_signal(data)
    if total_score < SCORE_THRESHOLD:
        logger.info(f"Score {total_score} < {SCORE_THRESHOLD}, skipping trade.")
        return
    
    if len(active_trades) >= MAX_CONCURRENT_TRADES:
        logger.info(f"Max trades ({MAX_CONCURRENT_TRADES}) reached.")
        return
    
    # Fixed position size for EMA trades (10%), dynamic for others
    if trade_type == 'ema':
        amount_xrp = current_balance * 0.10  # Fixed 10% as per requirement
    else:
        # Dynamic position sizing (5-15% based on ATR and Bollinger Band width)
        volatility_factor = (atr / data['price'] + bb_width) / 2
        position_size_percent = max(min(0.15 / (volatility_factor + 0.01), 0.15), 0.05)
        amount_xrp = current_balance * position_size_percent
    
    if amount_xrp > current_balance or amount_xrp < 0.1:
        logger.warning(f"Insufficient balance: {current_balance} XRP for {amount_xrp} XRP trade.")
        return
    
    buy_price = data['price']
    if trade_type == 'arbitrage':
        buy_price, sell_price, _ = await check_arbitrage()
        if not buy_price:
            return
        amount_xrp = min(amount_xrp, current_balance * 0.10)  # 10% cap for arbitrage
    elif trade_type == 'scalping':
        buy_price, sell_price = await check_scalping(data)
        if not buy_price:
            return
        amount_xrp = min(amount_xrp, current_balance * 0.02)  # 2% cap for scalping
    
    # Fee optimization: Delay if fee > 0.05% or schedule for low-fee period
    if trade_type == 'ema':
        if fee_level > 0.0005:
            logger.info(f"High fee {fee_level}, delaying trade.")
            await asyncio.sleep(300)  # Wait 5 minutes
            return
        if not await predict_low_fee_period():
            logger.info("Not a low-fee period, delaying trade.")
            await asyncio.sleep(300)  # Wait 5 minutes
            return
    
    if SIMULATION_MODE:
        logger.info(f"Simulation mode: Simulating {trade_type} buy: {amount_xrp} XRP at {buy_price}, balance {current_balance} XRP")
    else:
        logger.info(f"Live mode: Executing {trade_type} buy: {amount_xrp} XRP at {buy_price}, balance {current_balance} XRP")
        # Placeholder for actual trade execution
        pass
    
    current_balance -= amount_xrp
    task = asyncio.create_task(monitor_position('XRP', buy_price, amount_xrp, data['price'], atr, trade_type))
    monitor_tasks['XRP_' + str(len(monitor_tasks))] = task

# Monitor position
async def monitor_position(token_address, buy_price, amount_xrp, xrp_usd_price, atr, trade_type):
    global last_volatility
    try:
        stop_loss_price = buy_price * (1 - STOP_LOSS_PERCENT)
        take_profit_price = buy_price * (1 + TAKE_PROFIT_PERCENT) if trade_type != 'arbitrage' else buy_price * 1.005
        highest_price = buy_price
        trailing_stop_price = buy_price * (1 - STOP_LOSS_PERCENT)
        position = {
            'buy_price': buy_price,
            'amount_xrp': amount_xrp,
            'start_time': datetime.utcnow(),
            'highest_price': highest_price,
            'trailing_stop_price': trailing_stop_price,
            'initial_volatility': last_volatility
        }
        active_trades[token_address] = position
        
        while token_address in active_trades:
            data = await fetch_xrp_data()
            if not data:
                await asyncio.sleep(60)
                continue
            current_price = data['price']
            current_volume = data['volume']
            
            # Futures hedging (placeholder)
            if trade_type == 'ema' and current_price < buy_price * 0.98:
                logger.info(f"Hedging {token_address} with futures at {current_price}")
                # Placeholder for Kraken Futures API call
            
            # Trend reversal prediction with enhanced features
            trend_up = await predict_trend_reversal(current_price, current_volume)
            if trend_up is False:
                logger.info(f"Trend reversal predicted for {token_address}, exiting at {current_price}")
                profit_xrp = (current_price - buy_price) * amount_xrp
                return profit_xrp, profit_xrp * xrp_usd_price, "trend_reversal"
            
            # Confirm momentum with RSI and MACD
            if trade_type == 'ema' and (data['rsi'] < 30 or not data['macd_signal']):
                logger.info(f"Momentum indicators (RSI {data['rsi']}, MACD {data['macd_signal']}) suggest exit at {current_price}")
                profit_xrp = (current_price - buy_price) * amount_xrp
                return profit_xrp, profit_xrp * xrp_usd_price, "momentum_exit"
            
            # Volatility trigger: Tighten stop-loss if volatility spikes
            current_volatility = data['atr'] / current_price
            if current_volatility > position['initial_volatility'] * 2:
                stop_loss_price = current_price * (1 - STOP_LOSS_PERCENT / 2)
                trailing_stop_price = position['highest_price'] * (1 - STOP_LOSS_PERCENT / 2)
                logger.info(f"Volatility spike detected ({current_volatility:.4f}), tightening stop-loss to {stop_loss_price:.2f}")
            
            position['highest_price'] = max(position['highest_price'], current_price)
            position['trailing_stop_price'] = position['highest_price'] * (1 - STOP_LOSS_PERCENT)
            
            if current_price >= take_profit_price:
                profit_xrp = (current_price - buy_price) * amount_xrp
                logger.info(f"Take-profit {token_address}: {profit_xrp} XRP")
                return profit_xrp, profit_xrp * xrp_usd_price, "take_profit"
            elif current_price <= stop_loss_price:
                profit_xrp = (current_price - buy_price) * amount_xrp
                logger.info(f"Stop-loss {token_address}: {profit_xrp} XRP")
                return profit_xrp, profit_xrp * xrp_usd_price, "stop_loss"
            elif current_price <= position['trailing_stop_price']:
                profit_xrp = (current_price - buy_price) * amount_xrp
                logger.info(f"Trailing stop {token_address}: {profit_xrp} XRP")
                return profit_xrp, profit_xrp * xrp_usd_price, "trailing_stop"
            
            await asyncio.sleep(60)
    except Exception as e:
        logger.error(f"Monitor error {token_address}: {e}")
        return 0, 0, "error"
    finally:
        if token_address in active_trades:
            del active_trades[token_address]
        if token_address in monitor_tasks:
            del monitor_tasks[token_address]

async def monitor_position_wrapper(token_address, buy_price, amount_xrp, xrp_usd_price, atr, trade_type):
    global current_balance, max_portfolio_value, trade_stats, paused
    profit_xrp, profit_usd, exit_reason = await monitor_position(token_address, buy_price, amount_xrp, xrp_usd_price, atr, trade_type)
    current_balance += profit_xrp
    portfolio_value_history.append(current_balance)
    max_portfolio_value = max(max_portfolio_value, current_balance)
    
    trade_stats['total_trades'] += 1
    if profit_xrp > 0:
        trade_stats['wins'] += 1
    trade_stats['total_profit_xrp'] += profit_xrp
    drawdown = (max_portfolio_value - current_balance) / max_portfolio_value if max_portfolio_value > 0 else 0
    
    if drawdown > MAX_DRAWDOWN_PERCENT:
        paused = True
        save_paused_state()
        logger.warning(f"Max drawdown exceeded: {drawdown*100:.2f}%. Pausing trading.")
        await send_email_notification(
            "XRP Moonshot Alert: Max Drawdown Exceeded",
            f"Drawdown reached {drawdown*100:.2f}%, paused trading."
        )
    
    trade_data = {
        'timestamp': datetime.utcnow().isoformat(),
        'amount_xrp': amount_xrp,
        'buy_price': buy_price,
        'profit_xrp': profit_xrp,
        'profit_usd': profit_usd,
        'exit_reason': exit_reason,
        'current_balance': current_balance
    }
    df = pd.DataFrame([trade_data])
    manage_file_size(potential_trades_file)
    mode = 'a' if os.path.exists(potential_trades_file) else 'w'
    df.to_csv(potential_trades_file, mode=mode, header=(mode == 'w'), index=False)
    logger.info(f"Trade recorded: {profit_xrp} XRP, reason {exit_reason}")

# Main trading loop
async def trading_loop():
    await train_lstm()
    while True:
        try:
            data = await fetch_xrp_data()
            if data:
                manage_file_size(xrp_data_file)
                df = pd.DataFrame([data])
                mode = 'a' if os.path.exists(xrp_data_file) else 'w'
                df.to_csv(xrp_data_file, mode=mode, header=(mode == 'w'), index=False)
                
                await fetch_fee_data()
                await simulate_trade(data, 'ema')
                await simulate_trade(data, 'arbitrage')
                await simulate_trade(data, 'scalping')
            await asyncio.sleep(FETCH_INTERVAL)
        except Exception as e:
            logger.error(f"Trading loop error: {e}")
            await asyncio.sleep(10)

async def monitor_tasks():
    while True:
        if monitor_tasks:
            for token_address, task in list(monitor_tasks.items()):
                if task.done():
                    try:
                        await task
                    except Exception as e:
                        logger.error(f"Task {token_address} failed: {e}")
                    finally:
                        if token_address in monitor_tasks:
                            del monitor_tasks[token_address]
            logger.info(f"Active tasks: {list(monitor_tasks.keys())}")
        await asyncio.sleep(60)

async def main():
    tasks = [
        asyncio.create_task(trading_loop()),
        asyncio.create_task(monitor_tasks())
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
