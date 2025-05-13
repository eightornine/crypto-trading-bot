import asyncio
import logging
import os
import smtplib
import psutil
import aiohttp
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Custom logging filter to redact sensitive data
class SecretFilter(logging.Filter):
    def filter(self, record):
        sensitive_words = ['apiKey', 'secret', 'password', 'token']
        msg = record.msg
        for word in sensitive_words:
            if word.lower() in str(msg).lower():
                msg = msg.replace(getattr(record, 'msg', ''), '[REDACTED]')
                record.msg = msg
                break
        return True

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('core_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()
logger.addFilter(SecretFilter())

# Load environment variables
load_dotenv()
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = "market_data"
LUNARCRUSH_API_KEY = os.getenv("LUNARCRUSH_API_KEY")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = "mango.butt3r@gmail.com"

# InfluxDB client setup
try:
    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    buckets_api = influx_client.buckets_api()
    bucket = buckets_api.find_bucket_by_name(INFLUXDB_BUCKET)
    if bucket is None:
        buckets_api.create_bucket(bucket_name=INFLUXDB_BUCKET, org=INFLUXDB_ORG)
        logging.info(f"Created InfluxDB bucket {INFLUXDB_BUCKET}")
    logging.info("InfluxDB client initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize InfluxDB client: {str(e)}")
    raise

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

# Monitor system resources
async def monitor_resources():
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    logging.info(f"Performance: CPU {cpu_percent}% | Memory {memory.percent}% used ({memory.used / (1024 ** 3):.2f}/{memory.total / (1024 ** 3):.2f} GB)")

# Initialize exchanges
async def initialize_exchanges():
    exchanges = {}
    for ex_name in ['kraken', 'coinbase']:
        try:
            api_key = os.getenv(f'{ex_name.upper()}_API_KEY')
            api_secret = os.getenv(f'{ex_name.upper()}_API_SECRET')
            if not api_key or not api_secret:
                logging.warning(f"API key or secret for {ex_name} not found, skipping initialization")
                continue
            exchange_class = getattr(ccxt, ex_name)
            exchanges[ex_name] = exchange_class({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True
            })
            await exchanges[ex_name].load_markets()
            logging.info(f"{ex_name.capitalize()} exchange initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize {ex_name}: {str(e)}")
    return exchanges

# Fetch market data from exchanges (fallback for SOL/USD)
async def fetch_market_data(exchanges, symbol="SOL/USD"):
    market_data = {}
    market_data[symbol] = {}
    for ex_name, exchange in exchanges.items():
        for attempt in range(3):
            try:
                ticker = await exchange.fetch_ticker(symbol)
                market_data[symbol][ex_name] = {
                    'price': ticker.get('last', 0.0),
                    'volume': ticker.get('baseVolume', 0.0) or 0.0,
                    'timestamp': datetime.utcnow().isoformat(),
                    'score': 0.5
                }
                logging.info(f"Ticker fetched for {symbol} on {ex_name}")
                break
            except Exception as e:
                wait_time = 5 * (2 ** attempt)
                logging.warning(f"Failed to fetch {symbol} from {ex_name}: {str(e)}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                if attempt == 2:
                    logging.error(f"Failed to fetch {symbol} from {ex_name} after 3 attempts: {str(e)}")
                    await send_email_notification(
                        "Core Bot Error: Market Data Fetch Failed",
                        f"Failed to fetch {symbol} from {ex_name} after 3 attempts: {str(e)}"
                    )
                    market_data[symbol][ex_name] = {'price': 0.0, 'volume': 0.0, 'timestamp': datetime.utcnow().isoformat(), 'score': 0.5}
    return market_data

# Target coins (updated list of valid Solana meme coins)
target_coins = [
    {"name": "POPCAT", "symbol": "popcat", "lunarcrush_symbol": "POPCAT", "address": "7GCihgDB8fe6KNjn2MYtkzZcRjQy4t9uu4bagryL7Y7"},
    {"name": "Bonk", "symbol": "bonk", "lunarcrush_symbol": "BONK", "address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"},
    {"name": "Dogwifhat", "symbol": "wif", "lunarcrush_symbol": "WIF", "address": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"},
    {"name": "Book of Meme", "symbol": "bome", "lunarcrush_symbol": "BOME", "address": "ukHH6c7mMyiWCf1bW5VhpMXk7gU5S1Y4A1kA2kYvNZ"},
    {"name": "Myro", "symbol": "myro", "lunarcrush_symbol": "MYRO", "address": "HhJpBhRRJ5X8xH3F2qUkE5qExORtF3vM7ZhRJrjnuoC"},
    {"name": "Wen", "symbol": "wen", "lunarcrush_symbol": "WEN", "address": "WENWENvqqNya429ubCdR81ZmD69brwQaaBAY6pREh4o"},
    {"name": "Sillycat", "symbol": "silly", "lunarcrush_symbol": "SILLY", "address": "7EYnhQoR9tDoLtpyRJhFCSQ2hQ7o2hFXnJSeZPhuF6M"},
    {"name": "Cat in a Dogs World", "symbol": "mew", "lunarcrush_symbol": "MEW", "address": "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5"},
    {"name": "Ponke", "symbol": "ponke", "lunarcrush_symbol": "PONKE", "address": "5z3EqYQo9DqR5thv7XrHr8jWEN6xGQT8GL5bH8A6Qev"},
    {"name": "Slothana", "symbol": "sloth", "lunarcrush_symbol": "SLOTH", "address": "26rbyuWLA1fSwh9sj5x5icrAJk5c3t2w9Xq3PXRb2gK"},
]

# File paths for CSVs
CSV_DIR = "data"
if not os.path.exists(CSV_DIR):
    os.makedirs(CSV_DIR)

# Fetch CoinGecko data
async def fetch_coingecko_data(coin_symbol, session):
    for attempt in range(3):
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_symbol.lower()}?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false"
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                price = data["market_data"]["current_price"]["usd"]
                volume_24h = data["market_data"]["total_volume"]["usd"]
                market_cap = data["market_data"]["market_cap"]["usd"]
                price_change_24h = data["market_data"]["price_change_percentage_24h"]
                price_change_7d = data["market_data"]["price_change_percentage_7d"]
                return {
                    "price": price,
                    "volume_24h": volume_24h,
                    "market_cap": market_cap,
                    "price_change_24h": price_change_24h,
                    "price_change_7d": price_change_7d
                }
        except Exception as e:
            wait_time = 5 * (2 ** attempt)
            logging.warning(f"Error fetching CoinGecko data for {coin_symbol}: {str(e)}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
            if attempt == 2:
                logging.error(f"Failed to fetch CoinGecko data for {coin_symbol} after 3 attempts: {str(e)}")
                await send_email_notification(
                    "Core Bot Error: CoinGecko Fetch Failed",
                    f"Failed to fetch CoinGecko data for {coin_symbol} after 3 attempts: {str(e)}"
                )
                return {
                    "price": 0.0,
                    "volume_24h": 0.0,
                    "market_cap": 0.0,
                    "price_change_24h": 0.0,
                    "price_change_7d": 0.0
                }
    await asyncio.sleep(1)  # Rate limiting: CoinGecko allows ~50-60 requests/min

# Fetch Birdeye data
async def fetch_birdeye_data(coin_symbol, session):
    for attempt in range(3):
        try:
            url = f"https://public-api.birdeye.so/public/price?symbol={coin_symbol}"
            headers = {"X-API-KEY": os.getenv("BIRDEYE_API_KEY")}  # Add to .env
            async with session.get(url, headers=headers, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                liquidity = data.get("liquidity", 0)
                return {"liquidity": liquidity}
        except Exception as e:
            wait_time = 5 * (2 ** attempt)
            logging.warning(f"Error fetching Birdeye data for {coin_symbol}: {str(e)}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
            if attempt == 2:
                logging.error(f"Failed to fetch Birdeye data for {coin_symbol} after 3 attempts: {str(e)}")
                await send_email_notification(
                    "Core Bot Error: Birdeye Fetch Failed",
                    f"Failed to fetch Birdeye data for {coin_symbol} after 3 attempts: {str(e)}"
                )
                return {"liquidity": 0}
    await asyncio.sleep(1)

# Fetch LunarCrush sentiment
async def fetch_lunarcrush_sentiment(session):
    for attempt in range(3):
        try:
            url = f"https://api.lunarcrush.com/v2?request=coins/list&key={LUNARCRUSH_API_KEY}"
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                sentiment_scores = {}
                for coin in data["data"]:
                    symbol = coin["symbol"]
                    sentiment = coin.get("sentiment", 3)  # Default to 3 if missing
                    sentiment_scores[symbol] = sentiment
                logging.info("LunarCrush sentiment data fetched successfully")
                return sentiment_scores
        except Exception as e:
            wait_time = 5 * (2 ** attempt)
            logging.warning(f"Error fetching LunarCrush sentiment: {str(e)}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
            if attempt == 2:
                logging.error(f"Failed to fetch LunarCrush sentiment after 3 attempts: {str(e)}")
                await send_email_notification(
                    "Core Bot Error: LunarCrush Fetch Failed",
                    f"Failed to fetch LunarCrush sentiment after 3 attempts: {str(e)}"
                )
                return {}
    await asyncio.sleep(1)

# Fetch CFGI index
async def fetch_cfgi_index(session):
    for attempt in range(3):
        try:
            url = "https://api.cfgi.io/v1/fear-greed/solana"
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("value", 50)  # Default to 50 if missing
        except Exception as e:
            wait_time = 5 * (2 ** attempt)
            logging.warning(f"Error fetching CFGI index: {str(e)}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
            if attempt == 2:
                logging.error(f"Failed to fetch CFGI index after 3 attempts: {str(e)}")
                await send_email_notification(
                    "Core Bot Error: CFGI Fetch Failed",
                    f"Failed to fetch CFGI index after 3 attempts: {str(e)}"
                )
                return 50
    await asyncio.sleep(1)

# Fetch Helius whale activity
async def fetch_helius_whale_activity(coin_address, session):
    for attempt in range(3):
        try:
            url = f"https://api.helius.xyz/v0/addresses/{coin_address}/transactions?api-key={HELIUS_API_KEY}"
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                txs = await response.json()
                large_txs = 0
                cumulative_volume = 0
                five_minutes_ago = (datetime.utcnow() - timedelta(minutes=5)).timestamp()
                for tx in txs:
                    # Check transaction timestamp
                    timestamp = tx.get("timestamp", 0)
                    if timestamp < five_minutes_ago:
                        continue
                    # Check transaction type and amount
                    if "fee" in tx:
                        amount = tx.get("fee", 0) / 1e9  # Convert lamports to SOL
                        usd_value = amount * 88.71  # SOL/USD price
                        if usd_value > 50000:
                            large_txs += 1
                        cumulative_volume += usd_value
                return {"large_txs": large_txs, "cumulative_volume": cumulative_volume}
        except Exception as e:
            wait_time = 5 * (2 ** attempt)
            logging.warning(f"Error fetching Helius whale activity for {coin_address}: {str(e)}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
            if attempt == 2:
                logging.error(f"Failed to fetch Helius whale activity for {coin_address} after 3 attempts: {str(e)}")
                await send_email_notification(
                    "Core Bot Error: Helius Fetch Failed",
                    f"Failed to fetch Helius whale activity for {coin_address} after 3 attempts: {str(e)}"
                )
                return {"large_txs": 0, "cumulative_volume": 0}
    await asyncio.sleep(1)

# Calculate ATR
async def calculate_atr(coin_symbol, session, period=14):
    for attempt in range(3):
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_symbol.lower()}/market_chart?vs_currency=usd&days={period}"
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                prices = data["prices"]
                highs = [p[1] for p in prices]
                lows = [p[1] for p in prices]
                closes = [p[1] for p in prices]
                trs = []
                for i in range(1, len(highs)):
                    tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
                    trs.append(tr)
                atr = np.mean(trs[-period:]) if len(trs) >= period else 0
                return atr
        except Exception as e:
            wait_time = 5 * (2 ** attempt)
            logging.warning(f"Error calculating ATR for {coin_symbol}: {str(e)}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
            if attempt == 2:
                logging.error(f"Failed to calculate ATR for {coin_symbol} after 3 attempts: {str(e)}")
                await send_email_notification(
                    "Core Bot Error: ATR Calculation Failed",
                    f"Failed to calculate ATR for {coin_symbol} after 3 attempts: {str(e)}"
                )
                return 0
    await asyncio.sleep(1)

# Fetch historical data from CoinGecko
async def fetch_coingecko_historical_data(coins, start_date, end_date, session):
    coingecko_historical_data = {}
    start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    days = (end - start).days + 1
    for coin in coins:
        coingecko_historical_data[coin["symbol"]] = []
        current_date = start
        for _ in range(days):
            date_str = current_date.strftime('%d-%m-%Y')
            url = f"https://api.coingecko.com/api/v3/coins/{coin['symbol'].lower()}/history"
            params = {'date': date_str, 'localization': 'false'}
            for attempt in range(3):
                try:
                    async with session.get(url, params=params) as response:
                        response.raise_for_status()
                        data = await response.json()
                        price = data.get('market_data', {}).get('current_price', {}).get('usd', 0.0)
                        volume = data.get('market_data', {}).get('total_volume', {}).get('usd', 0.0) or 0.0
                        market_cap = data.get('market_data', {}).get('market_cap', {}).get('usd', 0.0) or 0.0
                        coingecko_historical_data[coin["symbol"]].append({
                            'timestamp': current_date.isoformat(),
                            'price': price,
                            'volume': volume,
                            'market_cap': market_cap,
                            'score': 0.5
                        })
                    logging.info(f"Historical data fetched for {coin['symbol']} on {date_str}")
                    break
                except Exception as e:
                    wait_time = 5 * (2 ** attempt)
                    logging.warning(f"Failed to fetch historical {coin['symbol']} on {date_str}: {str(e)}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    if attempt == 2:
                        logging.error(f"Failed to fetch historical {coin['symbol']} on {date_str} after 3 attempts: {str(e)}")
                        coingecko_historical_data[coin["symbol"]].append({
                            'timestamp': current_date.isoformat(),
                            'price': 0.0,
                            'volume': 0.0,
                            'market_cap': 0.0,
                            'score': 0.5
                        })
            current_date += timedelta(days=1)
            await asyncio.sleep(30)  # Increased delay to 30 seconds for rate limiting
    return coingecko_historical_data

# Write data to InfluxDB
async def write_to_influxdb(coin_name, data, measurement="market_data"):
    point = Point(measurement).tag("coin", coin_name).time(datetime.utcnow(), WritePrecision.NS)
    for key, value in data.items():
        point = point.field(key, float(value))
    for attempt in range(3):
        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            logging.info(f"Data written to InfluxDB for {coin_name}")
            return
        except Exception as e:
            wait_time = 5 * (2 ** attempt)
            logging.warning(f"Failed to write to InfluxDB for {coin_name}: {str(e)}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
            if attempt == 2:
                logging.error(f"Failed to write to InfluxDB for {coin_name} after 3 attempts: {str(e)}")
                await send_email_notification(
                    "Core Bot Error: InfluxDB Write Failed",
                    f"Failed to write to InfluxDB for {coin_name} after 3 attempts: {str(e)}"
                )

# Write data to CSV
async def write_to_csv(coin_name, data):
    csv_file = os.path.join(CSV_DIR, f"{coin_name.lower()}_data.csv")
    df = pd.DataFrame([data])
    df["timestamp"] = datetime.utcnow()
    try:
        if os.path.exists(csv_file):
            existing_df = pd.read_csv(csv_file)
            df = pd.concat([existing_df, df], ignore_index=True)
            # Enforce 1 GB limit
            if os.path.getsize(csv_file) > 1e9:
                df = df.iloc[-1000:]  # Keep last 1000 rows
        df.to_csv(csv_file, index=False)
        logging.info(f"Data written to {csv_file}")
    except Exception as e:
        logging.error(f"Failed to write to {csv_file}: {str(e)}")

# Main loop
async def main():
    exchanges = await initialize_exchanges()
    try:
        # Fetch historical data (March 26 - April 24, 2025)
        start_date = "2025-03-26T00:00:00Z"
        end_date = "2025-04-24T23:59:59Z"
        async with aiohttp.ClientSession() as session:
            historical_data = await fetch_coingecko_historical_data(target_coins, start_date, end_date, session)
            for coin_symbol, entries in historical_data.items():
                for entry in entries:
                    data = entry.copy()
                    data["volume_24h"] = data.pop("volume")
                    await write_to_influxdb(coin_symbol.lower(), data)
                    await write_to_csv(coin_symbol.lower(), data)
            logging.info("Historical data collection completed")

        # Real-time data collection
        iteration = 0
        while True:
            async with aiohttp.ClientSession() as session:
                try:
                    # Fetch fallback SOL/USD price if needed
                    market_data = await fetch_market_data(exchanges, "SOL/USD")
                    sol_usd_price = market_data["SOL/USD"].get("coinbase", {}).get("price", 88.71) or 88.71
                    # Fetch data for target coins
                    lunarcrush_scores = await fetch_lunarcrush_sentiment(session)
                    cfgi_index = await fetch_cfgi_index(session)
                    for coin in target_coins:
                        symbol = coin["symbol"]
                        lunar_symbol = coin["lunarcrush_symbol"]
                        coingecko_data = await fetch_coingecko_data(symbol, session)
                        if not coingecko_data:
                            continue
                        birdeye_data = await fetch_birdeye_data(symbol, session)
                        address = coin["address"]
                        if address != "placeholder_address":
                            whale_data = await fetch_helius_whale_activity(address, session)
                        else:
                            whale_data = {"large_txs": 0, "cumulative_volume": 0}
                        atr = await calculate_atr(symbol, session)
                        price = coingecko_data["price"]
                        atr_ratio = atr / price if price > 0 else 0
                        sentiment = lunarcrush_scores.get(lunar_symbol, 3)  # Default to 3
                        data = {
                            "price": price,
                            "volume_24h": coingecko_data["volume_24h"],
                            "market_cap": coingecko_data["market_cap"],
                            "price_change_24h": coingecko_data["price_change_24h"],
                            "price_change_7d": coingecko_data["price_change_7d"],
                            "liquidity": birdeye_data["liquidity"],
                            "atr": atr,
                            "atr_ratio": atr_ratio,
                            "sentiment": sentiment,
                            "cfgi_index": cfgi_index,
                            "large_txs": whale_data["large_txs"],
                            "cumulative_volume": whale_data["cumulative_volume"]
                        }
                        await write_to_influxdb(symbol.lower(), data)
                        await write_to_csv(symbol.lower(), data)
                        logging.info(f"Data fetched for {coin['name']}: {data}")
                    # Monitor resources every 5 iterations (~5 minutes)
                    iteration += 1
                    if iteration % 5 == 0:
                        await monitor_resources()
                    await asyncio.sleep(900)  # Fetch every 15 minutes
                except Exception as e:
                    logging.error(f"Main loop error: {str(e)}")
                    await send_email_notification("Core Bot Error: Main Loop Failure", f"Error: {str(e)}")
                    await asyncio.sleep(60)
    finally:
        for ex_name, exchange in exchanges.items():
            try:
                await exchange.close()
                logging.info(f"Closed {ex_name} exchange connection")
            except Exception as e:
                logging.error(f"Failed to close {ex_name} exchange: {str(e)}")
        try:
            influx_client.close()
            logging.info("Closed InfluxDB client connection")
        except Exception as e:
            logging.error(f"Failed to close InfluxDB client: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
