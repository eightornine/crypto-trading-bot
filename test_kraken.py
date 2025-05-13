import ccxt.async_support as ccxt
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

async def test_kraken():
    exchange = ccxt.kraken({
        'apiKey': os.getenv('KRAKEN_API_KEY'),
        'secret': os.getenv('KRAKEN_API_SECRET'),
        'enableRateLimit': True
    })
    try:
        await exchange.load_markets()
        # Test different pair formats
        pairs = ['XBTUSD', 'XBT/USD', 'ETHUSD', 'ETH/USD', 'XRPUSD', 'XRP/USD']
        for pair in pairs:
            try:
                ticker = await exchange.fetch_ticker(pair)
                print(f"{pair}: {ticker}")
            except Exception as e:
                print(f"Error fetching {pair}: {str(e)}")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(test_kraken())
