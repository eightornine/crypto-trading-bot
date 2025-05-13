import ccxt
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Kraken exchange
kraken = ccxt.kraken({
    'apiKey': os.getenv('KRAKEN_API_KEY'),
    'secret': os.getenv('KRAKEN_SECRET'),
    'enableRateLimit': True
})

# Fetch and display balances
try:
    print("Attempting to fetch balances from Kraken...")
    balance = kraken.fetch_balance()
    print("Kraken Balances:")
    for asset, amount in balance['total'].items():
        if amount > 0:  # Only display non-zero balances
            print(f"{asset}: {amount}")
except Exception as e:
    print(f"Error fetching balances: {str(e)}")
