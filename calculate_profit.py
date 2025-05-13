import pandas as pd
from collections import deque

# Load the potential trades
df = pd.read_csv("potential_trades.csv", names=["mint", "timestamp", "action", "price", "net_sol_amount", "token_amount", "net_profit_sol", "fee_sol"])
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp')

# Initialize variables
initial_balance_sol = 1.14  # Starting with 1.14 SOL
balance_sol = initial_balance_sol
total_profit = 0.0  # This will be updated to reflect net executed trades
fee_rate = 0.005  # 0.5% fee (based on SOL_FEE_PERCENTAGE in sol_moonshot.py)
pending_buys = {}  # Track pending buys by mint as a deque
executed_buys = 0.0  # Track total buy costs for executed trades
executed_sells = 0.0  # Track total sell proceeds for executed trades

for _, row in df.iterrows():
    action = row['action']
    net_sol_amount = row['net_sol_amount']
    token_amount = row['token_amount']
    net_profit_sol = row['net_profit_sol']
    fee_sol = row['fee_sol']
    mint = row['mint']
    price = row['price']

    if action == "buy":
        buy_cost = net_sol_amount  # net_sol_amount is the cost before fee
        buy_fee = buy_cost * fee_rate
        total_buy_cost = buy_cost + buy_fee
        if balance_sol >= total_buy_cost:
            balance_sol -= total_buy_cost
            executed_buys += total_buy_cost
            print(f"Bought {token_amount:.2f} tokens of {mint} for {buy_cost:.6f} SOL (fee: {buy_fee:.6f} SOL). New balance: {balance_sol:.3f} SOL")
            if mint not in pending_buys:
                pending_buys[mint] = deque()
            pending_buys[mint].append({
                "total_buy_cost": total_buy_cost,
                "token_amount": token_amount,
                "timestamp": row['timestamp']
            })
        else:
            print(f"Skipped buy for {mint}: Insufficient balance ({balance_sol:.3f} SOL)")

    elif action == "sell":
        if mint not in pending_buys or not pending_buys[mint]:
            print(f"Skipped sell for {mint}: No corresponding buy at timestamp {row['timestamp']}")
            continue
        # Get the earliest pending buy for this mint
        pending_buy = pending_buys[mint].popleft()
        # Calculate the proportion of the position being sold
        token_ratio = token_amount / pending_buy["token_amount"] if pending_buy["token_amount"] > 0 else 1.0
        sell_amount = net_sol_amount  # net_sol_amount is the amount before fee
        sell_fee = sell_amount * fee_rate
        net_sell_amount = sell_amount - sell_fee
        # Calculate profit based on the proportion of the buy cost
        proportional_buy_cost = pending_buy["total_buy_cost"] * token_ratio
        profit = net_sell_amount - proportional_buy_cost
        balance_sol += net_sell_amount
        executed_sells += net_sell_amount
        print(f"Sold {token_amount:.2f} tokens of {mint} for {sell_amount:.6f} SOL (fee: {sell_fee:.6f} SOL). Profit: {profit:.3f} SOL. New balance: {balance_sol:.3f} SOL")
        # Remove the mint entry if no more pending buys
        if not pending_buys[mint]:
            del pending_buys[mint]

# Calculate total profit as the net effect of executed trades
total_profit = executed_sells - executed_buys
final_profit = balance_sol - initial_balance_sol
print(f"Final Balance: {balance_sol:.3f} SOL")
print(f"Total Profit (Trade Sum): {total_profit:.3f} SOL")
print(f"Total Profit (Balance Diff): {final_profit:.3f} SOL")
print(f"Executed Buys Total: {executed_buys:.3f} SOL")
print(f"Executed Sells Total: {executed_sells:.3f} SOL")
