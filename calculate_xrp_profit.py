import pandas as pd

# Load the potential trades
df = pd.read_csv("xrp_potential_trades.csv", names=["timestamp", "action", "price", "net_amount_usd", "amount_xrp", "profit_usd", "fee_usd"])
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp')

# Initialize variables
initial_balance_xrp = 120.0  # Starting with 120 XRP
balance_xrp = initial_balance_xrp
balance_usd = 0.0  # Track USD profits
total_profit_usd = 0.0
fee_rate = 0.0045  # 0.45% fee (approximated to match 0.11 USD on ~24 USD trades)
pending_buy = None

for _, row in df.iterrows():
    action = row['action']
    net_amount_usd = row['net_amount_usd']
    amount_xrp = row['amount_xrp']
    profit_usd = row['profit_usd']
    fee_usd = row['fee_usd']
    price = row['price']

    if action == "buy":
        if balance_xrp >= amount_xrp:
            buy_cost = amount_xrp * price
            buy_fee = buy_cost * fee_rate
            balance_xrp -= amount_xrp
            print(f"Bought {amount_xrp:.2f} XRP for {buy_cost:.2f} USD (fee: {buy_fee:.2f} USD). New balance: {balance_xrp:.2f} XRP")
            pending_buy = {
                "amount_xrp": amount_xrp,
                "buy_cost": buy_cost,
                "buy_fee": buy_fee,
                "timestamp": row['timestamp']
            }
        else:
            print(f"Skipped buy: Insufficient balance ({balance_xrp:.2f} XRP)")
            pending_buy = None

    elif action == "sell":
        if pending_buy is None:
            print(f"Skipped sell: No corresponding buy for {amount_xrp:.2f} XRP at timestamp {row['timestamp']}")
            continue
        sell_amount = amount_xrp * price
        sell_fee = sell_amount * fee_rate
        net_sell_amount = sell_amount - sell_fee
        profit = (sell_amount - pending_buy["buy_cost"]) - (pending_buy["buy_fee"] + sell_fee)
        balance_xrp += amount_xrp
        balance_usd += profit
        total_profit_usd += profit
        print(f"Sold {amount_xrp:.2f} XRP for {sell_amount:.2f} USD (fee: {sell_fee:.2f} USD). Profit: {profit:.2f} USD. New balance: {balance_xrp:.2f} XRP")
        pending_buy = None

print(f"Final Balance: {balance_xrp:.2f} XRP")
print(f"Total Profit: {total_profit_usd:.2f} USD")
