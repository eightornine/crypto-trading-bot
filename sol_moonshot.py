import asyncio
import logging
import os
import smtplib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sol_moonshot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Load environment variables
load_dotenv()
required_env_vars = {
    "INFLUXDB_TOKEN": os.getenv("INFLUXDB_TOKEN"),
    "EMAIL_SENDER": os.getenv("EMAIL_SENDER"),
    "EMAIL_PASSWORD": os.getenv("EMAIL_PASSWORD")
}
missing_vars = [key for key, value in required_env_vars.items() if value is None]
if missing_vars:
    error_msg = f"Missing environment variables: {', '.join(missing_vars)}. Set them in .env."
    logger.error(error_msg)
    raise EnvironmentError(error_msg)

# Configurations
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = required_env_vars["INFLUXDB_TOKEN"]
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = "market_data"
EMAIL_SENDER = required_env_vars["EMAIL_SENDER"]
EMAIL_PASSWORD = required_env_vars["EMAIL_PASSWORD"]
EMAIL_RECEIVER = "mango.butt3r@gmail.com"

# Trading parameters
BALANCE = 1.225645  # Starting SOL balance
MAX_CONCURRENT_TRADES = 3
MAX_DRAWDOWN = 0.20  # 20% portfolio loss
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB

# Directories and files
CSV_DIR = "data"
TRADE_LOG = "potential_trades.csv"
PAUSED_STATE_FILE = "/root/crypto-trading-bot/paused_state.txt"

# Target coins (must match core_bot.py)
target_coins = [
    {"name": "POPCAT", "symbol": "POPCAT"},
    {"name": "MUSKIT", "symbol": "MUSKIT"},
    {"name": "Bonk", "symbol": "BONK"},
    {"name": "Dogwifhat", "symbol": "WIF"},
    {"name": "Fartcoin", "symbol": "FARTCOIN"},
    {"name": "All Will Retire", "symbol": "AWR"},
    {"name": "Book of Meme", "symbol": "BOME"},
    {"name": "Myro", "symbol": "MYRO"},
    {"name": "Official Trump", "symbol": "TRUMP"},
    {"name": "Peanut the Squirrel", "symbol": "PNUT"},
]

# Global state
current_balance = BALANCE
active_trades = {}
monitor_tasks = {}
portfolio_value_history = [BALANCE]
max_portfolio_value = BALANCE
trade_stats = {'total_trades': 0, 'wins': 0, 'total_profit_sol': 0.0}
last_sol_usd_price = 88.71  # Default fallback
paused = False

# InfluxDB client setup
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()
write_api = client.write_api(write_options=SYNCHRONOUS)

# Ensure directory exists for paused state file
paused_state_dir = os.path.dirname(PAUSED_STATE_FILE)
if not os.path.exists(paused_state_dir):
    os.makedirs(paused_state_dir)
    logging.info(f"Created directory for paused state: {paused_state_dir}")

# Load paused state from file
def load_paused_state():
    global paused
    try:
        if os.path.exists(PAUSED_STATE_FILE):
            with open(PAUSED_STATE_FILE, 'r') as f:
                state = f.read().strip().lower()
                paused = state == 'true'
                logging.info(f"Loaded paused state: {paused}")
        else:
            paused = False
            logging.info("No paused state file found, initializing paused to False")
    except Exception as e:
        logging.error(f"Failed to load paused state: {str(e)}. Defaulting to True for safety.")
        paused = True
    return paused

# Save paused state to file
def save_paused_state():
    global paused
    try:
        with open(PAUSED_STATE_FILE, 'w') as f:
            f.write(str(paused).lower())
        logging.info(f"Saved paused state: {paused}")
    except Exception as e:
        logging.error(f"Failed to save paused state: {str(e)}. Setting paused to True for safety.")
        paused = True

# Initialize paused state
paused = load_paused_state()

# Send email notification
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
        logging.info("Email notification sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email notification: {str(e)}")

# Manage file size
def manage_file_size(filename):
    if os.path.exists(filename) and os.path.getsize(filename) > MAX_FILE_SIZE:
        df = pd.read_csv(filename)
        df = df.tail(10000)  # Keep last 10,000 entries
        df.to_csv(filename, index=False)
        logging.info(f"Trimmed {filename} to last 10,000 entries.")

# Train ML model
async def train_ml_model():
    try:
        # Load 30 days of data
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=30)
        query = f'''
        from(bucket:"{INFLUXDB_BUCKET}")
        |> range(start: {start_time.isoformat()}Z, stop: {end_time.isoformat()}Z)
        |> filter(fn: (r) => r._field == "price_change_24h" or r._field == "volume_24h" or r._field == "sentiment" or r._field == "market_cap")
        '''
        result = query_api.query(query=query, org=INFLUXDB_ORG)
        data = []
        for table in result:
            for record in table.records:
                coin = record.get_measurement()
                field = record.get_field()
                value = record.get_value()
                timestamp = record.get_time()
                data.append({"coin": coin, "field": field, "value": value, "timestamp": timestamp})
        df = pd.DataFrame(data)
        df_pivot = df.pivot(index=["timestamp", "coin"], columns="field", values="value").reset_index()
        df_pivot["target"] = (df_pivot["price_change_24h"].shift(-48) > 100).astype(int)  # Predict 100%+ gain in 48 hours
        features = ["price_change_24h", "volume_24h", "sentiment", "market_cap"]
        X = df_pivot[features].dropna()
        y = df_pivot["target"].loc[X.index]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        accuracy = accuracy_score(y_test, model.predict(X_test))
        logging.info(f"ML Model Accuracy: {accuracy}")
        return model
    except Exception as e:
        logging.error(f"Error training ML model: {str(e)}")
        await send_email_notification(
            "SOL Moonshot Error: ML Training Failed",
            f"Failed to train ML model: {str(e)}"
        )
        return None

# Fetch data from InfluxDB
async def fetch_data(coin_symbol):
    try:
        query = f'''
        from(bucket:"{INFLUXDB_BUCKET}")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "{coin_symbol.lower()}")
        |> last()
        '''
        result = query_api.query(query=query, org=INFLUXDB_ORG)
        data = {}
        for table in result:
            for record in table.records:
                data[record.get_field()] = record.get_value()
        if not data:
            logging.warning(f"No recent data for {coin_symbol}")
            return {}
        return data
    except Exception as e:
        logging.error(f"Error fetching data for {coin_symbol}: {str(e)}")
        await send_email_notification(
            "SOL Moonshot Error: Data Fetch Failed",
            f"Failed to fetch data for {coin_symbol} from InfluxDB: {str(e)}"
        )
        return {}

# Fetch SOL/USD price from InfluxDB
async def fetch_sol_usd_price():
    global last_sol_usd_price
    try:
        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "market_data")
          |> filter(fn: (r) => r["pair"] == "SOL/USD")
          |> filter(fn: (r) => r["exchange"] == "coingecko")
          |> last()
        '''
        tables = query_api.query(query, org=INFLUXDB_ORG)
        for table in tables:
            for record in table.records:
                last_sol_usd_price = float(record["_value"])
                return last_sol_usd_price
        logging.warning("No recent SOL/USD price. Using last known.")
        return last_sol_usd_price
    except Exception as e:
        logging.error(f"Failed to fetch SOL/USD price: {str(e)}. Using last known: {last_sol_usd_price}")
        await send_email_notification(
            "SOL Moonshot Error: SOL/USD Price Fetch Failed",
            f"Failed to fetch SOL/USD price: {str(e)}. Using last known: {last_sol_usd_price}"
        )
        return last_sol_usd_price

# Calculate score
def calculate_score(data, ml_model):
    momentum = 0
    if data.get("price_change_24h", 0) > 50:
        momentum += 40
    if data.get("price_change_7d", 0) > 100:
        momentum += 20
    sentiment = data.get("sentiment", 3)
    sentiment_score = 0 if sentiment < 1 else 5 * (sentiment - 1) if sentiment < 3 else 15 + 10 * (sentiment - 3) if sentiment < 5 else 30
    cfgi_index = data.get("cfgi_index", 50)
    if cfgi_index > 75:
        sentiment_score += 10
    volume = 0 if data.get("volume_24h", 0) < 500000 else 20 * (data["volume_24h"] - 500000) / 500000 if data["volume_24h"] < 1000000 else 20
    liquidity = 10 if data.get("liquidity", 0) > 50000 else 0
    ml_score = 0
    if ml_model:
        features = np.array([[data.get("price_change_24h", 0), data.get("volume_24h", 0), sentiment, data.get("market_cap", 0)]])
        try:
            ml_score = ml_model.predict_proba(features)[0][1] * 20  # Probability of 100%+ gain
        except Exception as e:
            logging.warning(f"ML prediction failed: {str(e)}")
    total_score = momentum + sentiment_score + volume + liquidity + ml_score
    return total_score

# Estimate fees
def estimate_fees(amount, platform="raydium"):
    fee_rates = {"raydium": 0.0025, "pumpfun": 0.005, "gmgn": 0.01}  # Fee per trade (buy/sell)
    fee_rate = fee_rates.get(platform, 0.005)
    return amount * fee_rate * 2  # Round trip (buy + sell)

# Monitor position (adapted from current code with updated risk controls)
async def monitor_position(coin_symbol, buy_price, amount_sol, sol_usd_price, data):
    global trade_stats
    logging.info(f"Starting monitor_position for {coin_symbol}")
    try:
        buy_usd = amount_sol * sol_usd_price
        score = calculate_score(data, None)  # Recalculate score for stop-loss/take-profit
        stop_loss = 0.10 if score <= 80 else 0.15
        take_profit = 0.50 if score <= 80 else 1.00
        trailing_stop = 0.10
        time_exit = 3600 if score <= 80 else 86400  # 60min or 24h
        stop_loss_price = buy_price * (1 - stop_loss)
        take_profit_price = buy_price * (1 + take_profit)
        highest_price = buy_price
        trailing_stop_price = buy_price * (1 - stop_loss)
        position = {
            'coin_symbol': coin_symbol,
            'buy_price': buy_price,
            'amount_sol': amount_sol,
            'buy_usd': buy_usd,
            'stop_loss_price': stop_loss_price,
            'take_profit_price': take_profit_price,
            'start_time': datetime.utcnow(),
            'highest_price': highest_price,
            'trailing_stop_price': trailing_stop_price,
            'whale_volume_usd': data.get("cumulative_volume", 0.0),
            'whale_activity': False,
            'whale_start_time': datetime.utcnow()
        }
        active_trades[coin_symbol] = position
        logging.info(f"Monitoring {coin_symbol}: buy {buy_price}, stop-loss {stop_loss_price}, take-profit {take_profit_price}")

        last_price = buy_price
        while coin_symbol in active_trades:
            logging.info(f"Checking price for {coin_symbol}")
            data = await fetch_data(coin_symbol.lower())
            if not data:
                current_price = last_price * (1 + np.random.uniform(-0.1, 0.1))
                logging.info(f"Using simulated price for {coin_symbol}: {current_price} (last known: {last_price})")
            else:
                current_price = data.get("price", last_price)
                last_price = current_price

            sol_usd_price = await fetch_sol_usd_price()
            position['highest_price'] = max(position['highest_price'], current_price)
            position['trailing_stop_price'] = position['highest_price'] * (1 - stop_loss)

            position['whale_volume_usd'] = data.get("cumulative_volume", 0.0)
            large_txs = data.get("large_txs", 0)
            if (large_txs > 0 or position['whale_volume_usd'] > 100000) and not position['whale_activity']:
                position['whale_activity'] = True
                position['whale_start_time'] = datetime.utcnow()
            if position['whale_activity'] and (datetime.utcnow() - position['whale_start_time']).total_seconds() <= 300:
                logging.info(f"Whale activity for {coin_symbol}: {position['whale_volume_usd']} USD")
                position['trailing_stop_price'] = position['highest_price'] * (1 - stop_loss / 2)

            if current_price >= take_profit_price:
                profit_sol = (current_price - buy_price) * amount_sol
                profit_usd = profit_sol * sol_usd_price
                logging.info(f"Take-profit for {coin_symbol}: sold at {current_price}, profit {profit_sol} SOL ({profit_usd} USD)")
                return profit_sol, profit_usd, "take_profit"
            elif current_price <= stop_loss_price:
                profit_sol = (current_price - buy_price) * amount_sol
                profit_usd = profit_sol * sol_usd_price
                logging.info(f"Stop-loss for {coin_symbol}: sold at {current_price}, profit {profit_sol} SOL ({profit_usd} USD)")
                return profit_sol, profit_usd, "stop_loss"
            elif current_price <= position['trailing_stop_price']:
                profit_sol = (current_price - buy_price) * amount_sol
                profit_usd = profit_sol * sol_usd_price
                logging.info(f"Trailing stop for {coin_symbol}: sold at {current_price}, profit {profit_sol} SOL ({profit_usd} USD)")
                return profit_sol, profit_usd, "trailing_stop"

            elapsed = (datetime.utcnow() - position['start_time']).total_seconds()
            if elapsed > time_exit:
                profit_sol = (current_price - buy_price) * amount_sol
                profit_usd = profit_sol * sol_usd_price
                logging.info(f"Time exit for {coin_symbol}: sold at {current_price}, profit {profit_sol} SOL ({profit_usd} USD)")
                return profit_sol, profit_usd, "time_exit"

            await asyncio.sleep(60)
    except Exception as e:
        logging.error(f"Error monitoring {coin_symbol}: {str(e)}")
        return 0.0, 0.0, "error"
    finally:
        if coin_symbol in active_trades:
            del active_trades[coin_symbol]
        if coin_symbol in monitor_tasks:
            del monitor_tasks[coin_symbol]

# Monitor position wrapper (adapted from current code)
async def monitor_position_wrapper(coin_symbol, amount_sol, data, ml_model):
    global current_balance, max_portfolio_value, trade_stats, paused
    try:
        sol_usd_price = await fetch_sol_usd_price()
        buy_price = data.get("price", 0)
        score = calculate_score(data, ml_model)
        profit_sol, profit_usd, exit_reason = await monitor_position(coin_symbol, buy_price, amount_sol, sol_usd_price, data)
        platform = "raydium" if data["volume_24h"] > 1000000 else "pumpfun"
        fees = estimate_fees(amount_sol, platform)
        profit_sol -= fees
        profit_usd = profit_sol * sol_usd_price
        current_balance += profit_sol
        portfolio_value_history.append(current_balance)
        max_portfolio_value = max(max_portfolio_value, current_balance)

        trade_stats['total_trades'] += 1
        if profit_sol > 0:
            trade_stats['wins'] += 1
        trade_stats['total_profit_sol'] += profit_sol
        win_rate = trade_stats['wins'] / trade_stats['total_trades'] if trade_stats['total_trades'] > 0 else 0
        avg_profit = trade_stats['total_profit_sol'] / trade_stats['total_trades'] if trade_stats['total_trades'] > 0 else 0
        drawdown = (max_portfolio_value - current_balance) / max_portfolio_value if max_portfolio_value > 0 else 0

        if drawdown > MAX_DRAWDOWN:
            paused = True
            save_paused_state()
            logging.warning(f"Max drawdown exceeded: {drawdown*100:.2f}%. Pausing trading.")
            await send_email_notification(
                "SOL Moonshot Alert: Max Drawdown Exceeded",
                f"Portfolio drawdown reached {drawdown*100:.2f}%, exceeding {MAX_DRAWDOWN*100}%. Trading paused."
            )

        trade_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'coin': coin_symbol,
            'amount_sol': amount_sol,
            'buy_price': buy_price,
            'score': score,
            'profit_sol': profit_sol,
            'profit_usd': profit_usd,
            'exit_reason': exit_reason,
            'fees_sol': fees,
            'current_balance': current_balance,
            'drawdown': drawdown,
            'win_rate': win_rate,
            'avg_profit_sol': avg_profit
        }
        df = pd.DataFrame([trade_data])
        manage_file_size(TRADE_LOG)
        mode = 'a' if os.path.exists(TRADE_LOG) else 'w'
        df.to_csv(TRADE_LOG, mode=mode, header=(mode == 'w'), index=False)
        logging.info(f"Trade recorded: {coin_symbol}, profit {profit_sol} SOL ({profit_usd} USD), reason {exit_reason}")
    except Exception as e:
        logging.error(f"Monitor position wrapper failed for {coin_symbol}: {str(e)}")

# Simulate trade from historical data
async def simulate_from_historical_data():
    if not os.path.exists(CSV_DIR):
        logging.error(f"Historical data directory {CSV_DIR} not found.")
        return
    ml_model = await train_ml_model()
    for coin in target_coins:
        csv_file = os.path.join(CSV_DIR, f"{coin['symbol'].lower()}_data.csv")
        if not os.path.exists(csv_file):
            logging.warning(f"No historical data file for {coin['symbol']}")
            continue
        df = pd.read_csv(csv_file)
        for _, row in df.iterrows():
            data = row.to_dict()
            await process_token(data, ml_model)
            await asyncio.sleep(1)

# Process token data
async def process_token(data, ml_model):
    global current_balance, paused
    if paused:
        logging.info("Trading paused due to max drawdown.")
        return

    try:
        coin_symbol = data.get("coin_symbol", "unknown")
        score = calculate_score(data, ml_model)
        if score < 75:
            logging.info(f"Skipping trade for {coin_symbol}: score {score} < 75")
            return

        sol_usd_price = await fetch_sol_usd_price()
        if sol_usd_price <= 0:
            logging.warning(f"Invalid SOL/USD price for {coin_symbol}: {sol_usd_price}")
            return

        volume_limit = data.get("volume_24h", 0) * 0.01 / sol_usd_price  # 1% of daily volume in SOL
        atr_ratio = data.get("atr_ratio", 0)
        allocation = 0.05 if score <= 80 else 0.10 if score <= 85 else 0.15
        amount = current_balance * allocation
        if atr_ratio > 0.15:
            amount = amount / 2  # Reduce position size for high volatility
        amount = min(amount, volume_limit)
        if amount < 0.01:
            logging.warning(f"Insufficient amount for {coin_symbol}: {amount} SOL")
            return

        expected_gain = 0.50 if score <= 80 else 1.00
        fees = estimate_fees(amount)
        net_profit = (expected_gain * amount - fees) * sol_usd_price
        if net_profit / (amount * sol_usd_price) < 0.10:  # Require 10% net profit
            logging.info(f"Skipping trade for {coin_symbol}: net profit {net_profit} USD < 10%")
            return

        if len(active_trades) >= MAX_CONCURRENT_TRADES and score <= 90:
            logging.info(f"Max trades ({MAX_CONCURRENT_TRADES}) reached, skipping {coin_symbol}")
            return

        current_balance -= amount
        logging.info(f"Simulating buy for {coin_symbol}: {amount} SOL, new balance {current_balance} SOL")

        task = asyncio.create_task(monitor_position_wrapper(coin_symbol, amount, data, ml_model))
        monitor_tasks[coin_symbol] = task
        logging.info(f"Monitor task scheduled for {coin_symbol}, total active tasks: {len(monitor_tasks)}")
    except Exception as e:
        logging.error(f"Error processing token {coin_symbol}: {str(e)}")

# Monitor active tasks
async def monitor_active_tasks():
    logging.info("Starting monitor_active_tasks coroutine")
    while True:
        try:
            if monitor_tasks:
                active_tasks = []
                for coin_symbol, task in list(monitor_tasks.items()):
                    if task.done():
                        try:
                            result = await task
                            logging.info(f"Task for {coin_symbol} completed with result: {result}")
                        except Exception as e:
                            logging.error(f"Task for {coin_symbol} failed: {str(e)}")
                        finally:
                            if coin_symbol in monitor_tasks:
                                del monitor_tasks[coin_symbol]
                    else:
                        active_tasks.append(coin_symbol)
                if active_tasks:
                    logging.info(f"Active monitor tasks: {active_tasks}")
                else:
                    logging.info("No active monitor tasks remaining.")
            else:
                logging.info("No active monitor tasks.")
            await asyncio.sleep(60)
        except Exception as e:
            logging.error(f"Error in monitor_active_tasks: {str(e)}")
            await asyncio.sleep(60)

# Main loop
async def main():
    try:
        ml_model = await train_ml_model()
        tasks = [
            asyncio.create_task(simulate_from_historical_data()),
            asyncio.create_task(monitor_active_tasks())
        ]
        logging.info("Starting main event loop with tasks: simulate_from_historical_data, monitor_active_tasks")
        await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        await send_email_notification("SOL Moonshot Error: Main Loop Failure", f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Script crashed: {str(e)}")
        raise
