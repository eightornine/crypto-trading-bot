import ccxt
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Coinbase exchange with API credentials
coinbase = ccxt.coinbase({
    'apiKey': os.getenv('COINBASE_API_KEY'),
    'secret': os.getenv('COINBASE_API_SECRET'),
    'enableRateLimit': True  # Respect rate limits to avoid bans
})

try:
    print("Attempting to fetch balances from Coinbase Advanced API...")
    balance = coinbase.fetch_balance()
    print("Coinbase Balances:")
    for asset, amount in balance['total'].items():
        if amount > 0:  # Only show assets with non-zero balances
            print(f"{asset}: {amount}")
except Exception as e:
    print(f"Error fetching balances: {str(e)}")
