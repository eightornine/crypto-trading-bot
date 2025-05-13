import ccxt.async_support as ccxt
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

async def test_coinbase():
    exchange = ccxt.coinbase({
        'apiKey': os.getenv('COINBASE_API_KEY'),
        'secret': os.getenv('COINBASE_API_SECRET'),
        'enableRateLimit': True
    })
    try:
        await exchange.load_markets()
        for pair in ['BTC/USD', 'ETH/USD', 'XRP/USD']:
            ticker = await exchange.fetch_ticker(pair)
            print(f"{pair}: {ticker}")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(test_coinbase())
