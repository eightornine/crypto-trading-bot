import pandas as pd

# Load the potential trades
df = pd.read_csv("btc_potential_trades.csv", names=["timestamp", "action", "price", "net_amount_usd", "amount_btc", "profit_usd", "fee_usd"])
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp')

# Initialize variables
initial_balance_usd = 100.0  # Starting with $100
balance_usd = initial_balance_usd
total_profit = 0.0
fee_rate = 0.0043  # 0.43% fee (based on BTC_FEE_PERCENTAGE in sol_moonshot.py)
pending_buy = None

for _, row in df.iterrows():
    action = row['action']
    net_amount_usd = row['net_amount_usd']
    amount_btc = row['amount_btc']
    profit_usd = row['profit_usd']
    fee_usd = row['fee_usd']
    price = row['price']

    if action == "buy":
        buy_cost = amount_btc * price
        buy_fee = buy_cost * fee_rate
        total_buy_cost = buy_cost + buy_fee
        if balance_usd >= total_buy_cost:
            balance_usd -= total_buy_cost
            print(f"Bought {amount_btc:.6f} BTC for {buy_cost:.2f} USD (fee: {buy_fee:.2f} USD). New balance: {balance_usd:.2f} USD")
            pending_buy = {
                "amount_btc": amount_btc,
                "buy_cost": buy_cost,
                "buy_fee": buy_fee,
                "timestamp": row['timestamp']
            }
        else:
            print(f"Skipped buy: Insufficient balance ({balance_usd:.2f} USD)")
            pending_buy = None

    elif action == "sell":
        if pending_buy is None:
            print(f"Skipped sell: No corresponding buy for {amount_btc:.6f} BTC at timestamp {row['timestamp']}")
            continue
        sell_amount = amount_btc * price
        sell_fee = sell_amount * fee_rate
        net_sell_amount = sell_amount - sell_fee
        profit = (sell_amount - pending_buy["buy_cost"]) - (pending_buy["buy_fee"] + sell_fee)
        balance_usd += net_sell_amount
        total_profit += profit
        print(f"Sold {amount_btc:.6f} BTC for {sell_amount:.2f} USD (fee: {sell_fee:.2f} USD). Profit: {profit:.2f} USD. New balance: {balance_usd:.2f} USD")
        pending_buy = None

final_profit = balance_usd - initial_balance_usd
print(f"Final Balance: {balance_usd:.2f} USD")
print(f"Total Profit (Trade Sum): {total_profit:.2f} USD")
print(f"Total Profit (Balance Diff): {final_profit:.2f} USD")
