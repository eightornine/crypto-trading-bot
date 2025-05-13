import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List
from collections import defaultdict, deque

import ccxt.async_support as ccxt
import requests
import websockets
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import joblib
import pandas as pd
import numpy as np

# Load environment variables from .env file
load_dotenv()

# Configure logging to write to sol_moonshot.log with timestamps
logging.basicConfig(
    level=logging.INFO,  # Set to INFO for concise logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sol_moonshot.log'),
        logging.StreamHandler()
    ]
)

# Configure logging for token data collection (SOL data)
data_logger = logging.getLogger('data_logger')
data_handler = logging.FileHandler('token_data.csv')
data_handler.setFormatter(logging.Formatter('%(message)s'))
data_logger.addHandler(data_handler)
data_logger.setLevel(logging.INFO)

# Configure logging for potential trades (SOL hypothetical trades)
trade_logger = logging.getLogger('trade_logger')
trade_handler = logging.FileHandler('potential_trades.csv')
trade_handler.setFormatter(logging.Formatter('%(message)s'))
trade_logger.addHandler(trade_handler)
trade_logger.setLevel(logging.INFO)

# Configure logging for Bitcoin data collection
btc_data_logger = logging.getLogger('btc_data_logger')
btc_data_handler = logging.FileHandler('btc_data.csv')
btc_data_handler.setFormatter(logging.Formatter('%(message)s'))
btc_data_logger.addHandler(btc_data_handler)
btc_data_logger.setLevel(logging.INFO)

# Configure logging for Ethereum data collection
eth_data_logger = logging.getLogger('eth_data_logger')
eth_data_handler = logging.FileHandler('eth_data.csv')
eth_data_handler.setFormatter(logging.Formatter('%(message)s'))
eth_data_logger.addHandler(eth_data_handler)
eth_data_logger.setLevel(logging.INFO)

# Configure logging for XRP data collection
xrp_data_logger = logging.getLogger('xrp_data_logger')
xrp_data_handler = logging.FileHandler('xrp_data.csv')
xrp_data_handler.setFormatter(logging.Formatter('%(message)s'))
xrp_data_logger.addHandler(xrp_data_handler)
xrp_data_logger.setLevel(logging.INFO)

# Configure logging for Bitcoin potential trades
btc_trade_logger = logging.getLogger('btc_trade_logger')
btc_trade_handler = logging.FileHandler('btc_potential_trades.csv')
btc_trade_handler.setFormatter(logging.Formatter('%(message)s'))
btc_trade_logger.addHandler(btc_trade_handler)
btc_trade_logger.setLevel(logging.INFO)

# Configure logging for Ethereum potential trades
eth_trade_logger = logging.getLogger('eth_trade_logger')
eth_trade_handler = logging.FileHandler('eth_potential_trades.csv')
eth_trade_handler.setFormatter(logging.Formatter('%(message)s'))
eth_trade_logger.addHandler(eth_trade_handler)
eth_trade_logger.setLevel(logging.INFO)

# Configure logging for XRP potential trades
xrp_trade_logger = logging.getLogger('xrp_trade_logger')
xrp_trade_handler = logging.FileHandler('xrp_potential_trades.csv')
xrp_trade_handler.setFormatter(logging.Formatter('%(message)s'))
xrp_trade_logger.addHandler(xrp_trade_handler)
xrp_trade_logger.setLevel(logging.INFO)

# InfluxDB configuration
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "iFL-cXc-PPg3pA7mEyGKWRKD1tZsS7nEkpqByPOu6LJIHpeuLYTHGI6Q6oEaSfTt4Myjp0NARLjx7IrqPLaUAQ==")
INFLUXDB_ORG = "eightornine_trading"
INFLUXDB_BUCKET = "market_data"

# PumpPortal WebSocket URL and Trading API
PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
PUMPPORTAL_TRADE_URL = "https://pumpportal.fun/api/trade"
PUMPPORTAL_API_KEY = os.getenv("PUMPPORTAL_API_KEY")

# Verify API key
if not PUMPPORTAL_API_KEY:
    logging.error("PUMPPORTAL_API_KEY is not set in the .env file. Please add it and restart the script.")
    raise ValueError("PUMPPORTAL_API_KEY is required to execute trades.")

# Mode configuration
DATA_GATHERING_MODE = True  # Set to True to disable trading and focus on data collection

# Scoring thresholds for new tokens
INITIAL_BUY_THRESHOLD_SOL = 1.0  # 1 SOL for volume criterion
LIQUIDITY_THRESHOLD_USD = 4000   # $4,000 for liquidity criterion
MOMENTUM_THRESHOLD = 0.5         # 50% price increase for momentum
MOMENTUM_WINDOW_MINUTES = 5      # 5 minutes to measure momentum
SCORE_THRESHOLD = 70             # Minimum score to trade
BASE_TRADE_AMOUNT_USD = 5        # $5 per trade
HIGH_POTENTIAL_TRADE_AMOUNT_USD = 10  # $10 for high-potential tokens
HIGH_POTENTIAL_LIQUIDITY_USD = 4000   # $4,000
HIGH_POTENTIAL_VVolume_USD = 10000     # $10,000
SCORE_OVERRIDE_THRESHOLD = 15    # Score difference to override a position

# Scoring thresholds for existing tokens
EXISTING_MOMENTUM_THRESHOLD = 0.5  # 50% price increase in 5 minutes
EXISTING_VOLUME_SURGE_THRESHOLD = 2.0  # 100% volume increase over average
EXISTING_LIQUIDITY_THRESHOLD_USD = 4000  # $4,000 for liquidity
EXISTING_SCORE_THRESHOLD = 70  # Minimum score for existing tokens

# Position management thresholds
TIME_BASED_EXIT_MINUTES = 15     # Sell 50% if no 50% increase in 15 minutes
VOLUME_DROP_THRESHOLD = 0.5      # Sell 100% if volume drops >50%
TRAILING_STOP_PERCENTAGE = 0.1   # Sell if price drops 10% from peak after 100%+ run
PROFIT_THRESHOLD = 10.0          # Sell 100% if profit >1000% (10x)
MAX_CONCURRENT_TRADES = 1        # Limit to 1 concurrent trade (scalable to 5 with profits)

# Fee structures
SOL_FEE_PERCENTAGE = 0.005  # 0.5% fee for Pump.fun trades (buy and sell)
BTC_FEE_PERCENTAGE = 0.0043  # Average of Kraken (0.26%) and Coinbase (0.6%) taker fees
ETH_FEE_PERCENTAGE = 0.0043  # Average of Kraken (0.26%) and Coinbase (0.6%) taker fees
XRP_FEE_PERCENTAGE = 0.0043  # Average of Kraken (0.26%) and Coinbase (0.6%) taker fees

# Global variables
sol_usd_price: float = 100.0  # Placeholder, will be fetched
monitored_tokens: Dict[str, Dict] = {}  # Track tokens for trade monitoring
open_positions: Dict[str, Dict] = {}  # Track open positions
simulated_positions: Dict[str, Dict] = {}  # Track simulated positions for potential trades
message_queues: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)  # Message queues for each mint
existing_tokens: Dict[str, Dict] = {}  # Track existing tokens for monitoring
token_metrics: Dict[str, Dict] = defaultdict(lambda: {"prices": deque(maxlen=300), "volumes": deque(maxlen=3600), "last_updated": None})  # Track price and volume history

# Simulation state for BTC, ETH, XRP
btc_positions = {}  # {timestamp: {"amount_btc": float, "buy_price": float}}
eth_positions = {}  # {timestamp: {"amount_eth": float, "buy_price": float, "yield_amount": float}}
xrp_positions = {}  # {timestamp: {"amount_xrp": float, "buy_price": float}}
btc_ema_10 = deque(maxlen=10)  # For 10-period EMA (daily for BTC)
btc_ema_50 = deque(maxlen=50)  # For 50-period EMA (daily for BTC)
btc_atr = deque(maxlen=14)  # For 14-period ATR (daily for BTC)
eth_ema_7 = deque(maxlen=7)    # For 7-period EMA (hourly for ETH)
eth_ema_21 = deque(maxlen=21)  # For 21-period EMA (hourly for ETH)
xrp_ema_5 = deque(maxlen=5)    # For 5-period EMA (1-minute for XRP)
xrp_ema_20 = deque(maxlen=20)  # For 20-period EMA (1-minute for XRP)

# Load the ML model
try:
    ml_model = joblib.load("ml_model.pkl")
    logging.info("ML model loaded successfully.")
except Exception as e:
    logging.error(f"Failed to load ML model: {str(e)}")
    ml_model = None

async def initialize_exchanges() -> Dict[str, ccxt.Exchange]:
    """Initialize exchange connections."""
    exchanges = {}
    try:
        kraken = ccxt.kraken({
            'apiKey': os.getenv('KRAKEN_API_KEY'),
            'secret': os.getenv('KRAKEN_SECRET'),
            'enableRateLimit': True
        })
        await kraken.load_markets()
        exchanges['kraken'] = kraken
        logging.info("Kraken exchange initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize Kraken: {str(e)}")
    
    try:
        coinbase = ccxt.coinbase({
            'apiKey': os.getenv('COINBASE_API_KEY'),
            'secret': os.getenv('COINBASE_API_SECRET'),
            'enableRateLimit': True
        })
        await coinbase.load_markets()
        exchanges['coinbase'] = coinbase
        logging.info("Coinbase exchange initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize Coinbase: {str(e)}")
    
    return exchanges

async def initialize_influxdb() -> InfluxDBClient:
    """Initialize InfluxDB client."""
    try:
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        logging.info("InfluxDB client initialized successfully.")
        return client, write_api
    except Exception as e:
        logging.error(f"Failed to initialize InfluxDB: {str(e)}")
        raise

async def fetch_sol_usd_price(exchange: ccxt.Exchange) -> float:
    """Fetch the current SOL/USD price from Kraken."""
    try:
        ticker = await exchange.fetch_ticker("SOL/USD")
        price = ticker['last']
        logging.info(f"Fetched SOL/USD price: {price}")
        return price
    except Exception as e:
        logging.error(f"Error fetching SOL/USD price: {str(e)}")
        return 100.0  # Fallback price

async def fetch_market_data(exchanges: Dict[str, ccxt.Exchange], write_api, asset: str, symbol_kraken: str, symbol_coinbase: str, logger, trade_logger, ema_short: deque, ema_long: deque, atr: deque = None, position_size_usd: float = 10.0, starting_capital_usd: float = 100.0, asset_unit: str = "USD", is_btc: bool = False, is_eth: bool = False, is_xrp: bool = False):
    """Fetch market data for a given asset, simulate trades with fees, and store data."""
    positions = {}  # Local positions dictionary for this asset
    starting_amount = starting_capital_usd if asset_unit == "USD" else starting_capital_usd  # Starting capital in USD or asset units
    if asset == "BTC":
        starting_amount = starting_capital_usd / 83059.0  # Convert $100 to BTC at last known price
        fee_percentage = BTC_FEE_PERCENTAGE
    elif asset == "ETH":
        starting_amount = starting_capital_usd / 1918.24  # Convert $100 to ETH at last known price
        fee_percentage = ETH_FEE_PERCENTAGE
    elif asset == "XRP":
        starting_amount = starting_capital_usd  # Already in XRP units (120 XRP)
        fee_percentage = XRP_FEE_PERCENTAGE
        position_size_usd = 12.0  # 12 XRP per trade
    else:
        fee_percentage = 0.0  # Shouldn't happen, but default to 0

    while True:
        try:
            current_time = datetime.utcnow()
            timestamp = current_time.isoformat()
            price_usd = 0.0
            volume_usd = 0.0
            exchange_name = "kraken"  # Default to Kraken for price

            # Fetch data from each exchange
            for ex_name, exchange in exchanges.items():
                try:
                    # Fetch ticker for the asset
                    symbol = symbol_kraken if ex_name == "kraken" else symbol_coinbase
                    ticker = await exchange.fetch_ticker(symbol)
                    price = ticker['last']
                    volume = ticker['baseVolume'] * price if 'baseVolume' in ticker else 0.0

                    # Use Kraken price for trading decisions
                    if ex_name == "kraken":
                        price_usd = price
                        volume_usd = volume
                        exchange_name = ex_name

                    # Log to the asset's CSV file
                    logger.info(f"{timestamp},{price},{volume},{ex_name},0.5")

                    # Log to InfluxDB
                    point = Point("market_data") \
                        .tag("asset", asset) \
                        .tag("exchange", ex_name) \
                        .field("price_usd", float(price)) \
                        .field("volume_usd", float(volume)) \
                        .field("sentiment_score", 0.5) \
                        .time(current_time)
                    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
                    logging.info(f"Logged {asset} data for {ex_name}: price={price}, volume={volume}")
                except Exception as e:
                    logging.error(f"Error fetching {asset} data from {ex_name}: {str(e)}")
                    continue

            # Simulate trading based on the asset's strategy
            if price_usd > 0:  # Ensure we have a valid price
                # Calculate EMAs
                if is_btc:  # Bitcoin: 10/50 EMA on daily chart (approximated with 1-minute data)
                    ema_short.append(price_usd)
                    ema_long.append(price_usd)
                    if len(ema_short) == ema_short.maxlen and len(ema_long) == ema_long.maxlen:
                        ema_10 = np.mean(ema_short)  # Simplified EMA calculation
                        ema_50 = np.mean(ema_long)
                        # Calculate ATR (simplified as price range over 14 periods)
                        high = max(ema_short)
                        low = min(ema_short)
                        atr_value = (high - low) / price_usd
                        atr.append(atr_value)
                        atr_avg = np.mean(atr) if len(atr) == atr.maxlen else 0
                        # Trading logic: Buy if EMA 10 > EMA 50 and ATR > 1%
                        if ema_10 > ema_50 and atr_avg > 0.01:
                            if not positions:  # No open position
                                amount_usd = position_size_usd
                                fee_usd = amount_usd * fee_percentage  # Apply fee on buy
                                amount_btc = (amount_usd - fee_usd) / price_usd
                                positions[timestamp] = {"amount_btc": amount_btc, "buy_price": price_usd}
                                trade_logger.info(f"{timestamp},buy,{price_usd},{amount_usd},{amount_btc},{fee_usd}")
                        # Sell if EMA 10 < EMA 50
                        elif ema_10 < ema_50 and positions:
                            for buy_time, pos in list(positions.items()):
                                amount_btc = pos["amount_btc"]
                                buy_price = pos["buy_price"]
                                sell_amount_usd = amount_btc * price_usd
                                fee_usd = sell_amount_usd * fee_percentage  # Apply fee on sell
                                net_sell_amount_usd = sell_amount_usd - fee_usd
                                profit_usd = net_sell_amount_usd - (buy_price * amount_btc)
                                trade_logger.info(f"{timestamp},sell,{price_usd},{net_sell_amount_usd},{amount_btc},{profit_usd},{fee_usd}")
                                del positions[buy_time]

                elif is_eth:  # Ethereum: 7/21 EMA on hourly chart (approximated with 1-minute data)
                    ema_short.append(price_usd)
                    ema_long.append(price_usd)
                    if len(ema_short) == ema_short.maxlen and len(ema_long) == ema_long.maxlen:
                        ema_7 = np.mean(ema_short)  # Simplified EMA calculation
                        ema_21 = np.mean(ema_long)
                        # Trading logic: Buy if EMA 7 > EMA 21
                        if ema_7 > ema_21:
                            if not positions:  # No open position
                                amount_usd = position_size_usd
                                fee_usd = amount_usd * fee_percentage  # Apply fee on buy
                                amount_eth = (amount_usd - fee_usd) / price_usd
                                positions[timestamp] = {"amount_eth": amount_eth, "buy_price": price_usd, "yield_amount": 0.0}
                                trade_logger.info(f"{timestamp},buy,{price_usd},{amount_usd},{amount_eth},{fee_usd}")
                        # Sell if EMA 7 < EMA 21
                        elif ema_7 < ema_21 and positions:
                            for buy_time, pos in list(positions.items()):
                                amount_eth = pos["amount_eth"]
                                buy_price = pos["buy_price"]
                                # Simulate DeFi yield (3% APR over 48 hours)
                                yield_amount = (amount_eth * 0.2) * (0.03 / 365 * 2)  # 20% of position, 3% APR for 2 days
                                sell_amount_usd = amount_eth * price_usd
                                fee_usd = sell_amount_usd * fee_percentage  # Apply fee on sell
                                net_sell_amount_usd = sell_amount_usd - fee_usd
                                profit_usd = (net_sell_amount_usd - (buy_price * amount_eth)) + (yield_amount * price_usd)
                                trade_logger.info(f"{timestamp},sell,{price_usd},{net_sell_amount_usd},{amount_eth},{profit_usd},{fee_usd}")
                                del positions[buy_time]

                elif is_xrp:  # XRP: 5/20 EMA on 1-minute chart
                    ema_short.append(price_usd)
                    ema_long.append(price_usd)
                    if len(ema_short) == ema_short.maxlen and len(ema_long) == ema_long.maxlen:
                        ema_5 = np.mean(ema_short)  # Simplified EMA calculation
                        ema_20 = np.mean(ema_long)
                        # Trading logic: Buy if EMA 5 > EMA 20
                        if ema_5 > ema_20:
                            if not positions:  # No open position
                                amount_xrp = position_size_usd  # 12 XRP per trade
                                amount_usd = amount_xrp * price_usd
                                fee_usd = amount_usd * fee_percentage  # Apply fee on buy
                                net_amount_usd = amount_usd - fee_usd
                                amount_xrp = net_amount_usd / price_usd  # Adjust amount_xrp after fee
                                positions[timestamp] = {"amount_xrp": amount_xrp, "buy_price": price_usd}
                                trade_logger.info(f"{timestamp},buy,{price_usd},{net_amount_usd},{amount_xrp},{fee_usd}")
                        # Sell if EMA 5 < EMA 20
                        elif ema_5 < ema_20 and positions:
                            for buy_time, pos in list(positions.items()):
                                amount_xrp = pos["amount_xrp"]
                                buy_price = pos["buy_price"]
                                sell_amount_usd = amount_xrp * price_usd
                                fee_usd = sell_amount_usd * fee_percentage  # Apply fee on sell
                                net_sell_amount_usd = sell_amount_usd - fee_usd
                                profit_usd = net_sell_amount_usd - (buy_price * amount_xrp)
                                trade_logger.info(f"{timestamp},sell,{price_usd},{net_sell_amount_usd},{amount_xrp},{profit_usd},{fee_usd}")
                                del positions[buy_time]

            # Wait 60 seconds before the next fetch
            await asyncio.sleep(60)
        except Exception as e:
            logging.error(f"Error in fetch_{asset.lower()}_data loop: {str(e)}")
            await asyncio.sleep(60)  # Wait before retrying

def calculate_price(market_cap_sol: float) -> float:
    """Calculate the token price based on market cap (total supply = 1 billion)."""
    total_supply = 1_000_000_000  # 1 billion tokens for Pump.fun tokens
    return market_cap_sol / total_supply

def calculate_score(token_data: Dict, sol_usd_price: float, price_change: float = 0.0) -> float:
    """Calculate the score for a new token based on initial data, price_change, and ML prediction."""
    score = 0.0

    # Volume: Check initial buy volume (using solAmount)
    sol_amount = token_data.get("solAmount", 0.0)
    if sol_amount >= INITIAL_BUY_THRESHOLD_SOL:
        score += 50
        logging.info(f"Token {token_data['mint']} awarded 50 points for volume: {sol_amount} SOL")

    # Liquidity: Convert vSolInBondingCurve to USD
    v_sol_in_bonding_curve = token_data.get("vSolInBondingCurve", 0.0)
    liquidity_usd = v_sol_in_bonding_curve * sol_usd_price
    if liquidity_usd >= LIQUIDITY_THRESHOLD_USD:
        score += 20
        logging.info(f"Token {token_data['mint']} awarded 20 points for liquidity: ${liquidity_usd}")

    # Momentum: Check price change over the first 5 minutes
    if price_change >= MOMENTUM_THRESHOLD:
        score += 30
        logging.info(f"Token {token_data['mint']} awarded 30 points for momentum: {price_change*100}%")

    # ML Prediction: Add points based on ML model's success probability
    if ml_model:
        try:
            # Prepare features for prediction (using simulated sentiment for now)
            features = pd.DataFrame([{
                "initial_buy_sol": sol_amount,
                "liquidity_sol": v_sol_in_bonding_curve,
                "market_cap_sol": token_data.get("marketCapSol", 0.0),
                "price_change_5min": price_change,
                "sentiment_score": 0.5  # Placeholder until we add real sentiment analysis
            }])
            success_prob = ml_model.predict_proba(features)[0][1]  # Probability of success
            ml_score = success_prob * 50  # Scale probability (0-1) to 0-50 points
            score += ml_score
            logging.info(f"Token {token_data['mint']} awarded {ml_score:.2f} points for ML prediction (success probability: {success_prob:.2f})")
        except Exception as e:
            logging.error(f"Error in ML prediction for {token_data['mint']}: {str(e)}")

    logging.info(f"Token {token_data['mint']} total score: {score}")
    return score, liquidity_usd, sol_amount * sol_usd_price

def calculate_existing_token_score(mint: str, sol_usd_price: float, current_price: float, current_volume: float) -> float:
    """Calculate the score for an existing token based on momentum and volume surge."""
    score = 0.0
    metrics = token_metrics[mint]
    prices = metrics["prices"]
    volumes = metrics["volumes"]

    # Momentum: Calculate price change over the last 5 minutes
    if len(prices) >= 2:
        price_change = (current_price - prices[0]) / prices[0] if prices[0] > 0 else 0
        if price_change >= EXISTING_MOMENTUM_THRESHOLD:
            score += 50
            logging.info(f"Existing token {mint} awarded 50 points for momentum: {price_change*100}%")

    # Volume Surge: Calculate volume increase over the average of the last hour
    if len(volumes) >= 2:
        avg_volume = sum(volumes) / len(volumes)
        # Require a minimum average volume to avoid over-scoring low-activity tokens
        MIN_AVG_VOLUME = 0.1  # 0.1 SOL as a threshold
        if avg_volume >= MIN_AVG_VOLUME:
            volume_surge = current_volume / avg_volume if avg_volume > 0 else 0
            if volume_surge >= EXISTING_VOLUME_SURGE_THRESHOLD:
                score += 30
                logging.info(f"Existing token {mint} awarded 30 points for volume surge: {volume_surge*100}%")
        else:
            logging.debug(f"Existing token {mint} average volume {avg_volume} SOL is below threshold {MIN_AVG_VOLUME} SOL, skipping volume surge scoring.")

    # Liquidity: Use the latest vSolInBondingCurve from existing_tokens
    v_sol_in_bonding_curve = existing_tokens[mint].get("vSolInBondingCurve", 0.0)
    liquidity_usd = v_sol_in_bonding_curve * sol_usd_price
    if liquidity_usd >= EXISTING_LIQUIDITY_THRESHOLD_USD:
        score += 20
        logging.info(f"Existing token {mint} awarded 20 points for liquidity: ${liquidity_usd}")

    logging.info(f"Existing token {mint} total score: {score}")
    return score, liquidity_usd

async def execute_trade(action: str, mint: str, amount_sol: float) -> bool:
    """Execute a buy or sell order using PumpPortal's Trading API."""
    try:
        url = f"{PUMPPORTAL_TRADE_URL}?api-key={PUMPPORTAL_API_KEY}"
        payload = {
            "action": action,
            "mint": mint,
            "amount": amount_sol,
            "priorityFee": 0.0005,
            "denominatedInSol": "true" if action == "buy" else "false",
            "slippage": 10,  # 10% slippage
            "pool": "pump"
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        logging.info(f"{action.capitalize()} order executed for {mint}: {result}")
        return True
    except Exception as e:
        logging.error(f"Failed to execute {action} order for {mint}: {str(e)}")
        return False

async def log_trade_to_influxdb(write_api, mint: str, action: str, amount_usd: float, amount_sol: float, price: float, additional_fields: Dict = None):
    """Log the trade to InfluxDB."""
    try:
        point = Point("trade") \
            .tag("token", mint) \
            .field("action", action) \
            .field("amount_usd", amount_usd) \
            .field("amount_sol", amount_sol) \
            .field("price", price) \
            .time(datetime.utcnow())
        if additional_fields:
            for key, value in additional_fields.items():
                point = point.field(key, value)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logging.info(f"Trade logged to InfluxDB for {mint}: {action}")
    except Exception as e:
        logging.error(f"Failed to log trade to InfluxDB for {mint}: {str(e)}")

async def log_token_data(token_data: Dict, sol_usd_price: float, price_change: float = 0.0, tx_type: str = "create"):
    """Log token data for ML model retraining."""
    try:
        sol_amount = token_data.get("solAmount", 0.0)
        v_sol_in_bonding_curve = token_data.get("vSolInBondingCurve", 0.0)
        market_cap_sol = token_data.get("marketCapSol", 0.0)
        data = {
            "mint": token_data["mint"],
            "timestamp": token_data["timestamp"],
            "tx_type": tx_type,
            "sol_amount": sol_amount,
            "liquidity_sol": v_sol_in_bonding_curve,
            "market_cap_sol": market_cap_sol,
            "price_change_5min": price_change,
            "sentiment_score": 0.5  # Placeholder
        }
        # Log as CSV row (we'll add success_label later)
        data_logger.info(f"{data['mint']},{data['timestamp']},{data['tx_type']},{data['sol_amount']},{data['liquidity_sol']},{data['market_cap_sol']},{data['price_change_5min']},{data['sentiment_score']}")
    except Exception as e:
        logging.error(f"Failed to log token data for {token_data['mint']}: {str(e)}")

async def log_potential_trade(mint: str, timestamp: str, action: str, price: float, sol_amount: float, token_amount: float, profit_sol: float = 0.0):
    """Log a potential trade to potential_trades.csv with fees."""
    try:
        # Calculate fee (0.5% of the trade amount in SOL)
        fee_sol = sol_amount * SOL_FEE_PERCENTAGE
        net_sol_amount = sol_amount - fee_sol if action == "buy" else sol_amount + fee_sol  # Subtract fee on buy, add on sell for logging
        net_profit_sol = profit_sol - (2 * fee_sol) if action == "sell" else 0.0  # Total fees for buy and sell
        trade_logger.info(f"{mint},{timestamp},{action},{price},{net_sol_amount},{token_amount},{net_profit_sol},{fee_sol}")
    except Exception as e:
        logging.error(f"Failed to log potential trade for {mint}: {str(e)}")

async def monitor_position(new_token_queue: asyncio.Queue, trade_event_queue: asyncio.Queue, write_api, mint: str, initial_price: float, amount_bought: float, sol_usd_price: float, score: float):
    """Monitor an open position and apply exit strategies using a message queue."""
    position = open_positions[mint]
    position["score"] = score  # Store the score for comparison
    start_time = datetime.fromisoformat(position["timestamp"].replace("Z", "+00:00"))
    peak_price = initial_price
    total_volume = 0.0
    prices = [(start_time, initial_price)]

    # Process messages from the queue
    queue = message_queues[mint]
    while mint in open_positions:
        try:
            message = await asyncio.wait_for(queue.get(), timeout=60)
            data = json.loads(message)
            if data.get("txType") in ["buy", "sell"] and data.get("mint") == mint:
                current_price = calculate_price(data.get("marketCapSol", 0.0))
                current_time = datetime.utcnow()
                prices.append((current_time, current_price))
                total_volume += data.get("solAmount", 0.0)

                # Update peak price
                peak_price = max(peak_price, current_price)

                # Time-Based Exit: Sell 50% if price hasn't increased by 50% in 15 minutes
                if (current_time - start_time).total_seconds() >= TIME_BASED_EXIT_MINUTES * 60:
                    price_change = (current_price - initial_price) / initial_price if initial_price > 0 else 0
                    if price_change < 0.5:
                        amount_to_sell = amount_bought * 0.5
                        success = await execute_trade("sell", mint, amount_to_sell)
                        if success:
                            await log_trade_to_influxdb(write_api, mint, "sell", amount_to_sell * sol_usd_price, amount_to_sell, current_price, {"reason": "time_based_exit"})
                            position["amount"] -= amount_to_sell
                            if position["amount"] <= 0:
                                del open_positions[mint]
                                del message_queues[mint]  # Clean up the queue
                                logging.info(f"Closed position for {mint} due to time-based exit.")
                                return
                        break

                # Trailing Stop: Sell if price drops 10% from peak after 100%+ run
                price_increase = (current_price - initial_price) / initial_price if initial_price > 0 else 0
                if price_increase >= 1.0:  # 100%+ run
                    drop_from_peak = (peak_price - current_price) / peak_price if peak_price > 0 else 0
                    if drop_from_peak >= TRAILING_STOP_PERCENTAGE:
                        success = await execute_trade("sell", mint, amount_bought)
                        if success:
                            await log_trade_to_influxdb(write_api, mint, "sell", amount_bought * sol_usd_price, amount_bought, current_price, {"reason": "trailing_stop"})
                            del open_positions[mint]
                            del message_queues[mint]  # Clean up the queue
                            logging.info(f"Closed position for {mint} due to trailing stop.")
                            return

                # Post-Migration Sell: Sell 100% if profit >1000% (check bonding curve progress)
                if data.get("vTokensInBondingCurve", 1_000_000_000) < 500_000_000:  # Simplified migration check
                    profit = (current_price - initial_price) / initial_price if initial_price > 0 else 0
                    if profit >= PROFIT_THRESHOLD:
                        success = await execute_trade("sell", mint, amount_bought)
                        if success:
                            await log_trade_to_influxdb(write_api, mint, "sell", amount_bought * sol_usd_price, amount_bought, current_price, {"reason": "post_migration_profit"})
                            del open_positions[mint]
                            del message_queues[mint]  # Clean up the queue
                            logging.info(f"Closed position for {mint} due to post-migration profit.")
                            return
        except asyncio.TimeoutError:
            logging.warning(f"No trade updates for {mint} in the last 60 seconds.")
            continue
        except Exception as e:
            logging.error(f"Error monitoring position for {mint}: {str(e)}")
            break

async def simulate_monitor_position(new_token_queue: asyncio.Queue, trade_event_queue: asyncio.Queue, write_api, mint: str, initial_price: float, amount_bought_sol: float, token_amount: float, sol_usd_price: float, score: float, buy_timestamp: str):
    """Simulate monitoring a position to log potential sell opportunities with fees."""
    simulated_positions[mint] = {
        "amount_sol": amount_bought_sol,
        "token_amount": token_amount,
        "initial_price": initial_price,
        "timestamp": buy_timestamp,
        "score": score
    }
    start_time = datetime.fromisoformat(buy_timestamp.replace("Z", "+00:00"))
    peak_price = initial_price
    total_volume = 0.0
    prices = [(start_time, initial_price)]

    # Process messages from the queue
    queue = message_queues[mint]
    while mint in simulated_positions:
        try:
            message = await asyncio.wait_for(queue.get(), timeout=60)
            data = json.loads(message)
            if data.get("txType") in ["buy", "sell"] and data.get("mint") == mint:
                current_price = calculate_price(data.get("marketCapSol", 0.0))
                current_time = datetime.utcnow()
                current_timestamp = current_time.isoformat()
                prices.append((current_time, current_price))
                total_volume += data.get("solAmount", 0.0)

                # Update peak price
                peak_price = max(peak_price, current_price)

                # Time-Based Exit: Sell 50% if price hasn't increased by 50% in 15 minutes
                if (current_time - start_time).total_seconds() >= TIME_BASED_EXIT_MINUTES * 60:
                    price_change = (current_price - initial_price) / initial_price if initial_price > 0 else 0
                    if price_change < 0.5:
                        amount_to_sell_sol = amount_bought_sol * 0.5
                        amount_to_sell_tokens = token_amount * 0.5
                        sol_received = amount_to_sell_tokens * current_price
                        profit_sol = sol_received - amount_to_sell_sol
                        await log_potential_trade(mint, current_timestamp, "sell", current_price, sol_received, amount_to_sell_tokens, profit_sol)
                        simulated_positions[mint]["amount_sol"] -= amount_to_sell_sol
                        simulated_positions[mint]["token_amount"] -= amount_to_sell_tokens
                        if simulated_positions[mint]["token_amount"] <= 0:
                            del simulated_positions[mint]
                            del message_queues[mint]
                            logging.info(f"Simulated position for {mint} closed due to time-based exit.")
                            return
                        break

                # Trailing Stop: Sell if price drops 10% from peak after 100%+ run
                price_increase = (current_price - initial_price) / initial_price if initial_price > 0 else 0
                if price_increase >= 1.0:  # 100%+ run
                    drop_from_peak = (peak_price - current_price) / peak_price if peak_price > 0 else 0
                    if drop_from_peak >= TRAILING_STOP_PERCENTAGE:
                        sol_received = token_amount * current_price
                        profit_sol = sol_received - amount_bought_sol
                        await log_potential_trade(mint, current_timestamp, "sell", current_price, sol_received, token_amount, profit_sol)
                        del simulated_positions[mint]
                        del message_queues[mint]
                        logging.info(f"Simulated position for {mint} closed due to trailing stop.")
                        return

                # Post-Migration Sell: Sell 100% if profit >1000% (check bonding curve progress)
                if data.get("vTokensInBondingCurve", 1_000_000_000) < 500_000_000:  # Simplified migration check
                    profit = (current_price - initial_price) / initial_price if initial_price > 0 else 0
                    if profit >= PROFIT_THRESHOLD:
                        sol_received = token_amount * current_price
                        profit_sol = sol_received - amount_bought_sol
                        await log_potential_trade(mint, current_timestamp, "sell", current_price, sol_received, token_amount, profit_sol)
                        del simulated_positions[mint]
                        del message_queues[mint]
                        logging.info(f"Simulated position for {mint} closed due to post-migration profit.")
                        return
        except asyncio.TimeoutError:
            logging.warning(f"No trade updates for simulated position {mint} in the last 60 seconds.")
            continue
        except Exception as e:
            logging.error(f"Error monitoring simulated position for {mint}: {str(e)}")
            break

async def monitor_existing_tokens(new_token_queue: asyncio.Queue, trade_event_queue: asyncio.Queue, write_api, sol_usd_price: float):
    """Monitor existing tokens for data collection and potential mooning events using a message queue."""
    logging.info("Starting monitor_existing_tokens task.")
    while True:
        try:
            # Check if the queue is empty (for debugging)
            if trade_event_queue.empty():
                logging.debug("trade_event_queue is empty, waiting for messages...")
                await asyncio.sleep(1)  # Avoid busy-waiting
                continue

            message = await trade_event_queue.get()
            logging.info(f"Processing trade event from queue: {message}")

            data = json.loads(message)
            mint = data.get("mint")
            if mint and data.get("txType") in ["buy", "sell"]:
                current_price = calculate_price(data.get("marketCapSol", 0.0))
                current_volume = data.get("solAmount", 0.0)
                current_time = datetime.utcnow()
                current_timestamp = current_time.isoformat()

                # Update token metrics
                metrics = token_metrics[mint]
                metrics["prices"].append(current_price)
                metrics["volumes"].append(current_volume)
                metrics["last_updated"] = current_time

                # Update existing_tokens with latest data
                existing_tokens[mint] = {
                    "vSolInBondingCurve": data.get("vSolInBondingCurve", 0.0),
                    "timestamp": current_time.isoformat()
                }

                # Log trade event data
                token_data = {
                    "mint": mint,
                    "timestamp": current_time.isoformat(),
                    "solAmount": current_volume,
                    "vSolInBondingCurve": data.get("vSolInBondingCurve", 0.0),
                    "marketCapSol": data.get("marketCapSol", 0.0),
                    "name": data.get("name", ""),
                    "symbol": data.get("symbol", "")
                }
                price_change = (current_price - metrics["prices"][0]) / metrics["prices"][0] if len(metrics["prices"]) >= 2 and metrics["prices"][0] > 0 else 0
                await log_token_data(token_data, sol_usd_price, price_change, tx_type=data.get("txType"))

                # Calculate score for existing token (for logging purposes, not trading in data-gathering mode)
                score, liquidity_usd = calculate_existing_token_score(mint, sol_usd_price, current_price, current_volume)

                # Log potential trade opportunity (for analysis)
                if score >= EXISTING_SCORE_THRESHOLD:
                    logging.info(f"Existing token {mint} would meet criteria (score: {score}) if trading were enabled.")

                    # Simulate a buy if in data-gathering mode
                    if DATA_GATHERING_MODE:
                        if len(simulated_positions) < MAX_CONCURRENT_TRADES:
                            trade_amount_usd = BASE_TRADE_AMOUNT_USD
                            amount_sol = trade_amount_usd / sol_usd_price
                            token_amount = amount_sol / current_price if current_price > 0 else 0
                            await log_potential_trade(mint, current_timestamp, "buy", current_price, amount_sol, token_amount)
                            # Start simulating position monitoring
                            asyncio.create_task(simulate_monitor_position(new_token_queue, trade_event_queue, write_api, mint, current_price, amount_sol, token_amount, sol_usd_price, score, current_timestamp))
                        else:
                            # Check if the new token's score is significantly higher than the lowest-scoring simulated position
                            if simulated_positions:
                                lowest_score = min(position["score"] for position in simulated_positions.values())
                                if score >= lowest_score + SCORE_OVERRIDE_THRESHOLD:
                                    lowest_mint = min(simulated_positions.items(), key=lambda x: x[1]["score"])[0]
                                    lowest_position = simulated_positions[lowest_mint]
                                    # Simulate selling the lowest-scoring position
                                    sol_received = lowest_position["token_amount"] * current_price
                                    profit_sol = sol_received - lowest_position["amount_sol"]
                                    await log_potential_trade(lowest_mint, current_timestamp, "sell", current_price, sol_received, lowest_position["token_amount"], profit_sol)
                                    del simulated_positions[lowest_mint]
                                    del message_queues[lowest_mint]
                                    logging.info(f"Simulated closing position for {lowest_mint} to make room for higher-scoring token {mint}.")

                                    # Simulate buying the new token
                                    trade_amount_usd = BASE_TRADE_AMOUNT_USD
                                    amount_sol = trade_amount_usd / sol_usd_price
                                    token_amount = amount_sol / current_price if current_price > 0 else 0
                                    await log_potential_trade(mint, current_timestamp, "buy", current_price, amount_sol, token_amount)
                                    asyncio.create_task(simulate_monitor_position(new_token_queue, trade_event_queue, write_api, mint, current_price, amount_sol, token_amount, sol_usd_price, score, current_timestamp))
                                else:
                                    logging.info(f"Max concurrent simulated trades ({MAX_CONCURRENT_TRADES}) reached, and new token score ({score}) does not exceed lowest score ({lowest_score}) by {SCORE_OVERRIDE_THRESHOLD} points. Skipping trade for {mint}.")
                            else:
                                logging.info(f"Max concurrent simulated trades ({MAX_CONCURRENT_TRADES}) reached. Skipping trade for {mint}.")

                # Skip trading if in data-gathering mode
                if DATA_GATHERING_MODE:
                    continue

                # Decide whether to trade (if not in data-gathering mode)
                if score >= EXISTING_SCORE_THRESHOLD and len(open_positions) < MAX_CONCURRENT_TRADES:
                    trade_amount_usd = BASE_TRADE_AMOUNT_USD
                    amount_sol = trade_amount_usd / sol_usd_price
                    logging.info(f"Existing token {mint} meets criteria (score: {score}). Executing buy order for ${trade_amount_usd}.")
                    success = await execute_trade("buy", mint, amount_sol)
                    if success:
                        # Log trade to InfluxDB
                        await log_trade_to_influxdb(write_api, mint, "buy", trade_amount_usd, amount_sol, current_price, {"score": score})

                        # Add to open positions
                        open_positions[mint] = {
                            "amount": amount_sol,
                            "initial_price": current_price,
                            "timestamp": current_time.isoformat(),
                            "initial_volume": current_volume
                        }

                        # Start monitoring the position
                        asyncio.create_task(monitor_position(new_token_queue, trade_event_queue, write_api, mint, current_price, amount_sol, sol_usd_price, score))
                    else:
                        logging.warning(f"Buy order failed for existing token {mint}. Skipping trade.")
        except Exception as e:
            logging.error(f"Error processing trade event from queue: {str(e)}")
            continue

async def update_existing_tokens():
    """Update the list of existing tokens to monitor based on historical data."""
    global existing_tokens
    try:
        # Read historical data from token_data.csv
        df = pd.read_csv("token_data.csv", names=["mint", "timestamp", "tx_type", "sol_amount", "liquidity_sol", "market_cap_sol", "price_change_5min", "sentiment_score"])
        
        # Filter tokens with recent activity (last 24 hours)
        now = datetime.utcnow()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df[df['timestamp'] >= now - timedelta(hours=24)]

        # Update existing_tokens (limit to 1000 tokens to avoid overloading)
        existing_tokens.clear()
        for mint, group in df.groupby('mint'):
            latest = group.iloc[-1]
            existing_tokens[mint] = {
                "vSolInBondingCurve": latest['liquidity_sol'],
                "timestamp": latest['timestamp'].isoformat()
            }
        logging.info(f"Updated existing tokens list: {len(existing_tokens)} tokens to monitor.")
    except Exception as e:
        logging.error(f"Failed to update existing tokens list: {str(e)}")

async def pumpportal_websocket(exchanges: Dict[str, ccxt.Exchange], write_api, new_token_queue: asyncio.Queue, trade_event_queue: asyncio.Queue):
    """Connect to PumpPortal WebSocket and process all events, distributing to appropriate queues."""
    global sol_usd_price
    try:
        async with websockets.connect(PUMPPORTAL_WS_URL) as websocket:
            # Subscribe to new token creation events
            subscription_message = json.dumps({"method": "subscribeNewToken"})
            await websocket.send(subscription_message)
            logging.info("Subscribed to new token events on PumpPortal WebSocket.")

            # Subscribe to trade events for all tokens
            trade_subscription = json.dumps({"method": "subscribeTokenTrade", "keys": []})
            await websocket.send(trade_subscription)
            logging.info("Subscribed to trade events for all tokens (general subscription).")

            # List to store mints for specific trade event subscriptions
            subscribed_mints = set()

            # Listen for incoming messages and distribute to queues
            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    logging.info(f"Received WebSocket message: {data}")

                    # Distribute message to appropriate queue based on mint (for position monitoring)
                    mint = data.get("mint")
                    if mint and mint in message_queues:
                        await message_queues[mint].put(message)

                    # Distribute message to appropriate queue based on message type
                    if data.get("txType") == "create":
                        await new_token_queue.put(message)

                        # Subscribe to trade events for this specific mint
                        if mint and mint not in subscribed_mints:
                            specific_trade_subscription = json.dumps({"method": "subscribeTokenTrade", "keys": [mint]})
                            await websocket.send(specific_trade_subscription)
                            subscribed_mints.add(mint)
                            logging.info(f"Subscribed to trade events for mint: {mint}")

                    elif data.get("txType") in ["buy", "sell"]:
                        logging.debug(f"Adding trade event to trade_event_queue: {data['txType']} for mint {mint}")
                        await trade_event_queue.put(message)

                    # Process new token creation events
                    if data.get("txType") == "create":
                        token_data = {
                            "mint": data.get("mint"),
                            "marketCapSol": data.get("marketCapSol"),
                            "initialBuy": data.get("initialBuy"),
                            "solAmount": data.get("solAmount"),
                            "vSolInBondingCurve": data.get("vSolInBondingCurve"),
                            "vTokensInBondingCurve": data.get("vTokensInBondingCurve"),
                            "name": data.get("name"),
                            "symbol": data.get("symbol"),
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        logging.info(f"New token detected: {token_data['mint']}")

                        # Log token data for ML retraining
                        await log_token_data(token_data, sol_usd_price)

                        # Add to existing tokens
                        existing_tokens[token_data["mint"]] = {
                            "vSolInBondingCurve": token_data["vSolInBondingCurve"],
                            "timestamp": token_data["timestamp"]
                        }

                        # Calculate initial price
                        initial_price = calculate_price(token_data["marketCapSol"])

                        # Calculate initial score (without momentum initially)
                        score, liquidity_usd, initial_buy_usd = calculate_score(token_data, sol_usd_price)
                        token_data["score"] = score

                        # Log potential trade opportunity (for analysis)
                        if score >= SCORE_THRESHOLD:
                            logging.info(f"New token {token_data['mint']} would meet criteria (score: {score}) if trading were enabled.")

                            # Simulate a buy if in data-gathering mode
                            if DATA_GATHERING_MODE:
                                if len(simulated_positions) < MAX_CONCURRENT_TRADES:
                                    # Determine trade amount
                                    trade_amount_usd = HIGH_POTENTIAL_TRADE_AMOUNT_USD if (liquidity_usd >= HIGH_POTENTIAL_LIQUIDITY_USD and initial_buy_usd >= HIGH_POTENTIAL_VOLUME_USD) else BASE_TRADE_AMOUNT_USD
                                    amount_sol = trade_amount_usd / sol_usd_price
                                    token_amount = amount_sol / initial_price if initial_price > 0 else 0
                                    await log_potential_trade(token_data["mint"], token_data["timestamp"], "buy", initial_price, amount_sol, token_amount)
                                    # Start simulating position monitoring
                                    asyncio.create_task(simulate_monitor_position(new_token_queue, trade_event_queue, write_api, token_data["mint"], initial_price, amount_sol, token_amount, sol_usd_price, score, token_data["timestamp"]))
                                else:
                                    # Check if the new token's score is significantly higher than the lowest-scoring simulated position
                                    if simulated_positions:
                                        lowest_score = min(position["score"] for position in simulated_positions.values())
                                        if score >= lowest_score + SCORE_OVERRIDE_THRESHOLD:
                                            lowest_mint = min(simulated_positions.items(), key=lambda x: x[1]["score"])[0]
                                            lowest_position = simulated_positions[lowest_mint]
                                            # Simulate selling the lowest-scoring position
                                            current_time = datetime.utcnow()
                                            current_timestamp = current_time.isoformat()
                                            sol_received = lowest_position["token_amount"] * initial_price  # Use the current price at the time of override
                                            profit_sol = sol_received - lowest_position["amount_sol"]
                                            await log_potential_trade(lowest_mint, current_timestamp, "sell", initial_price, sol_received, lowest_position["token_amount"], profit_sol)
                                            del simulated_positions[lowest_mint]
                                            del message_queues[lowest_mint]
                                            logging.info(f"Simulated closing position for {lowest_mint} to make room for higher-scoring token {token_data['mint']}.")

                                            # Simulate buying the new token
                                            trade_amount_usd = HIGH_POTENTIAL_TRADE_AMOUNT_USD if (liquidity_usd >= HIGH_POTENTIAL_LIQUIDITY_USD and initial_buy_usd >= HIGH_POTENTIAL_VOLUME_USD) else BASE_TRADE_AMOUNT_USD
                                            amount_sol = trade_amount_usd / sol_usd_price
                                            token_amount = amount_sol / initial_price if initial_price > 0 else 0
                                            await log_potential_trade(token_data["mint"], token_data["timestamp"], "buy", initial_price, amount_sol, token_amount)
                                            asyncio.create_task(simulate_monitor_position(new_token_queue, trade_event_queue, write_api, token_data["mint"], initial_price, amount_sol, token_amount, sol_usd_price, score, token_data["timestamp"]))
                                        else:
                                            logging.info(f"Max concurrent simulated trades ({MAX_CONCURRENT_TRADES}) reached, and new token score ({score}) does not exceed lowest score ({lowest_score}) by {SCORE_OVERRIDE_THRESHOLD} points. Skipping trade for {token_data['mint']}.")
                                    else:
                                        logging.info(f"Max concurrent simulated trades ({MAX_CONCURRENT_TRADES}) reached. Skipping trade for {token_data['mint']}.")

                        # Skip trading if in data-gathering mode
                        if DATA_GATHERING_MODE:
                            continue

                        # Decide whether to trade (if not in data-gathering mode)
                        if score >= SCORE_THRESHOLD:
                            # Check if we can open a new position
                            if len(open_positions) < MAX_CONCURRENT_TRADES:
                                # Open a new position
                                trade_amount_usd = HIGH_POTENTIAL_TRADE_AMOUNT_USD if (liquidity_usd >= HIGH_POTENTIAL_LIQUIDITY_USD and initial_buy_usd >= HIGH_POTENTIAL_VOLUME_USD) else BASE_TRADE_AMOUNT_USD
                                amount_sol = trade_amount_usd / sol_usd_price
                                logging.info(f"Token {token_data['mint']} meets criteria (score: {score}). Executing buy order for ${trade_amount_usd}.")
                                success = await execute_trade("buy", token_data["mint"], amount_sol)
                                if success:
                                    # Log trade to InfluxDB
                                    await log_trade_to_influxdb(write_api, token_data["mint"], "buy", trade_amount_usd, amount_sol, initial_price, {"score": score})

                                    # Add to open positions
                                    open_positions[token_data["mint"]] = {
                                        "amount": amount_sol,
                                        "initial_price": initial_price,
                                        "timestamp": token_data["timestamp"],
                                        "initial_volume": token_data["solAmount"]
                                    }

                                    # Start monitoring the position
                                    asyncio.create_task(monitor_position(new_token_queue, trade_event_queue, write_api, token_data["mint"], initial_price, amount_sol, sol_usd_price, score))
                                else:
                                    logging.warning(f"Buy order failed for {token_data['mint']}. Skipping trade.")
                            else:
                                # Check if the new token's score is significantly higher than the lowest-scoring open position
                                if open_positions:
                                    lowest_score = min(position["score"] for position in open_positions.values())
                                    if score >= lowest_score + SCORE_OVERRIDE_THRESHOLD:
                                        # Find the lowest-scoring position
                                        lowest_mint = min(open_positions.items(), key=lambda x: x[1]["score"])[0]
                                        lowest_position = open_positions[lowest_mint]
                                        # Sell the lowest-scoring position
                                        logging.info(f"New token {token_data['mint']} (score: {score}) exceeds lowest-scoring position {lowest_mint} (score: {lowest_score}) by {score - lowest_score} points. Closing {lowest_mint} to make room.")
                                        success = await execute_trade("sell", lowest_mint, lowest_position["amount"])
                                        if success:
                                            await log_trade_to_influxdb(write_api, lowest_mint, "sell", lowest_position["amount"] * sol_usd_price, lowest_position["amount"], initial_price, {"reason": "override_for_higher_score"})
                                            del open_positions[lowest_mint]
                                            del message_queues[lowest_mint]  # Clean up the queue
                                            logging.info(f"Closed position for {lowest_mint} to make room for higher-scoring token.")

                                            # Now open the new position
                                            trade_amount_usd = HIGH_POTENTIAL_TRADE_AMOUNT_USD if (liquidity_usd >= HIGH_POTENTIAL_LIQUIDITY_USD and initial_buy_usd >= HIGH_POTENTIAL_VOLUME_USD) else BASE_TRADE_AMOUNT_USD
                                            amount_sol = trade_amount_usd / sol_usd_price
                                            logging.info(f"Token {token_data['mint']} meets criteria (score: {score}). Executing buy order for ${trade_amount_usd}.")
                                            success = await execute_trade("buy", token_data["mint"], amount_sol)
                                            if success:
                                                # Log trade to InfluxDB
                                                await log_trade_to_influxdb(write_api, token_data["mint"], "buy", trade_amount_usd, amount_sol, initial_price, {"score": score})

                                                # Add to open positions
                                                open_positions[token_data["mint"]] = {
                                                    "amount": amount_sol,
                                                    "initial_price": initial_price,
                                                    "timestamp": token_data["timestamp"],
                                                    "initial_volume": token_data["solAmount"]
                                                }

                                                # Start monitoring the position
                                                asyncio.create_task(monitor_position(new_token_queue, trade_event_queue, write_api, token_data["mint"], initial_price, amount_sol, sol_usd_price, score))
                                            else:
                                                logging.warning(f"Buy order failed for {token_data['mint']}. Skipping trade.")
                                        else:
                                            logging.warning(f"Failed to close lowest-scoring position {lowest_mint}. Skipping trade for {token_data['mint']}.")
                                    else:
                                        logging.info(f"Max concurrent trades ({MAX_CONCURRENT_TRADES}) reached, and new token score ({score}) does not exceed lowest score ({lowest_score}) by {SCORE_OVERRIDE_THRESHOLD} points. Skipping trade for {token_data['mint']}.")
                                else:
                                    logging.info(f"Max concurrent trades ({MAX_CONCURRENT_TRADES}) reached. Skipping trade for {token_data['mint']}.")
                        else:
                            logging.info(f"Token {token_data['mint']} does not meet criteria (score: {score}). Skipping trade.")
                except websockets.exceptions.ConnectionClosed:
                    logging.error("WebSocket connection closed. Attempting to reconnect...")
                    break
                except Exception as e:
                    logging.error(f"Error processing WebSocket message: {str(e)}")
                    continue
    except Exception as e:
        logging.error(f"Failed to connect to PumpPortal WebSocket: {str(e)}")
        # Attempt to reconnect after a delay
        await asyncio.sleep(10)
        await pumpportal_websocket(exchanges, write_api, new_token_queue, trade_event_queue)

async def update_sol_usd_price(exchange: ccxt.Exchange):
    """Periodically update the SOL/USD price."""
    global sol_usd_price
    while True:
        try:
            sol_usd_price = await fetch_sol_usd_price(exchange)
        except Exception as e:
            logging.error(f"Failed to update SOL/USD price: {str(e)}")
        await asyncio.sleep(300)  # Update every 5 minutes

async def main():
    """Main function to run the SOL Moonshot Module."""
    # Initialize exchanges
    exchanges = await initialize_exchanges()
    kraken = exchanges.get('kraken')

    # Initialize InfluxDB
    influxdb_client, write_api = await initialize_influxdb()

    # Fetch initial SOL/USD price
    global sol_usd_price
    if kraken:
        sol_usd_price = await fetch_sol_usd_price(kraken)
    else:
        logging.warning("Kraken not initialized. Using fallback SOL/USD price.")

    # Create queues in the main event loop
    new_token_queue = asyncio.Queue()
    trade_event_queue = asyncio.Queue()

    # Update existing tokens list
    await update_existing_tokens()

    # Start tasks in the same event loop
    tasks = [
        asyncio.create_task(pumpportal_websocket(exchanges, write_api, new_token_queue, trade_event_queue)),
        asyncio.create_task(monitor_existing_tokens(new_token_queue, trade_event_queue, write_api, sol_usd_price)),
        asyncio.create_task(fetch_market_data(exchanges, write_api, "BTC", "BTC/USD", "BTC/USDT", btc_data_logger, btc_trade_logger, btc_ema_10, btc_ema_50, btc_atr, position_size_usd=10.0, starting_capital_usd=100.0, asset_unit="BTC", is_btc=True)),
        asyncio.create_task(fetch_market_data(exchanges, write_api, "ETH", "ETH/USD", "ETH/USDT", eth_data_logger, eth_trade_logger, eth_ema_7, eth_ema_21, position_size_usd=10.0, starting_capital_usd=100.0, asset_unit="ETH", is_eth=True)),
        asyncio.create_task(fetch_market_data(exchanges, write_api, "XRP", "XRP/USD", "XRP/USDT", xrp_data_logger, xrp_trade_logger, xrp_ema_5, xrp_ema_20, position_size_usd=12.0, starting_capital_usd=120.0, asset_unit="XRP", is_xrp=True)),
        asyncio.create_task(update_sol_usd_price(kraken)) if kraken else None
    ]
    tasks = [task for task in tasks if task is not None]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logging.info("Shutting down SOL Moonshot Module...")
    finally:
        # Close exchange connections
        for exchange in exchanges.values():
            await exchange.close()
        # Close InfluxDB client
        influxdb_client.close()

if __name__ == "__main__":
    asyncio.run(main())
