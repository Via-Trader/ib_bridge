import csv
import os
import schedule
import time
from datetime import datetime
from ib_insync import IB


def calculate_daily_pnl(ib, trades_file='trades.csv', total_pnl_file='total_pnl.csv'):
    """
    Calculate the realized, unrealized, and total profit and loss (P&L) for the current trading day.
    Append results to CSV files.

    Args:
        ib (IB): An instance of the IB class for interacting with Interactive Brokers API.
        trades_file (str): The path to the CSV file where trades will be written.
        total_pnl_file (str): The path to the CSV file where total P&L will be appended.

    Returns:
        tuple: Realized P&L, Unrealized P&L, Total P&L for the day.
    """
    today_str = datetime.now().strftime('%Y%m%d')  # Today's date in 'YYYYMMDD'
    executions = ib.reqExecutions()  # Fetch executions
    print("Executions fetched:")

    if not executions:  # Check if executions list is empty
        print("No executions found for today.")
        return 0.0, 0.0, 0.0

    # Group trades by contract and side (entry/exit)
    trades = []
    trade_tracker = {}  # To track open trades by contract

    print("\nProcessing Executions:")
    for execution in executions:
        try:
            # Extract details
            symbol = execution.contract.symbol
            multiplier = int(execution.contract.multiplier) if execution.contract.multiplier else 1
            price = execution.execution.price
            side = execution.execution.side
            shares = execution.execution.shares
            time = execution.execution.time
            pnl = 0.0

            # Identify entry/exit and pair trades
            if symbol not in trade_tracker:
                trade_tracker[symbol] = []

            # Add the trade details
            trade_tracker[symbol].append({
                'time': time,
                'price': price,
                'side': side,
                'shares': shares,
                'pnl': None  # Placeholder for P&L calculation
            })

            # If we have both entry and exit, calculate P&L
            if len(trade_tracker[symbol]) >= 2:
                entry = trade_tracker[symbol].pop(0)  # Remove the oldest trade
                exit_trade = trade_tracker[symbol][-1]

                # Calculate P&L: (Exit Price - Entry Price) * Shares * Multiplier
                pnl = (exit_trade['price'] - entry['price']) * entry['shares'] * multiplier
                if entry['side'] == 'SLD':  # Reverse P&L for short trades
                    pnl = -pnl

                # Store completed trade
                trades.append({
                    'symbol': symbol,
                    'entry_time': entry['time'],
                    'exit_time': exit_trade['time'],
                    'entry_price': entry['price'],
                    'exit_price': exit_trade['price'],
                    'pnl': pnl
                })

        except Exception as e:
            print(f"An error occurred while processing execution: {e}")

    # Write trades to CSV
    with open(trades_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['Symbol', 'Entry Time', 'Entry Price', 'Exit Time', 'Exit Price', 'P&L'])
        writer.writeheader()
        for trade in trades:
            writer.writerow({
                'Symbol': trade['symbol'],
                'Entry Time': trade['entry_time'],
                'Entry Price': f"{trade['entry_price']:.2f}",
                'Exit Time': trade['exit_time'],
                'Exit Price': f"{trade['exit_price']:.2f}",
                'P&L': f"{trade['pnl']:.2f}"
            })

    # Calculate realized P&L
    realized_pnl = sum(trade['pnl'] for trade in trades)

    # Calculate unrealized P&L
    unrealized_pnl = 0.0
    positions = ib.positions()  # Fetch open positions
    for position in positions:
        contract = position.contract
        market_price = ib.reqMktData(contract).last  # Fetch the latest market price
        if market_price:
            multiplier = int(contract.multiplier) if contract.multiplier else 1
            unrealized_pnl += (market_price - position.avgCost) * position.position * multiplier

    # Calculate total P&L
    total_pnl = realized_pnl + unrealized_pnl

    # Append to total P&L CSV
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    last_row = None
    try:
        with open(total_pnl_file, mode='r') as file:
            rows = list(csv.reader(file))
            if len(rows) > 1:  # Check if there are rows beyond the header
                last_row = rows[-1]
    except FileNotFoundError:
        pass  # File does not exist yet, so no duplicates possible

    # Append only if this is not a duplicate
    if last_row is None or last_row[0] != current_time:
        with open(total_pnl_file, mode='a', newline='') as file:
            writer = csv.writer(file)
            if last_row is None:  # If the file was empty, write the header
                writer.writerow(['Date', 'Realized P&L', 'Unrealized P&L', 'Total P&L'])
            writer.writerow([current_time, f"{realized_pnl:.2f}", f"{unrealized_pnl:.2f}", f"{total_pnl:.2f}"])

    print(f"\nRealized P&L: ${realized_pnl:.2f}")
    print(f"Unrealized P&L: ${unrealized_pnl:.2f}")
    print(f"Total P&L: ${total_pnl:.2f}")
    print(f"Trades written to {trades_file}")
    print(f"Total P&L appended to {total_pnl_file}")
    return realized_pnl, unrealized_pnl, total_pnl


def job():
    """
    Job to be executed every 5 minutes.
    """
    ib = IB()
    try:
        # Connect to IB Gateway or TWS
        ib.connect('127.0.0.1', 7497, clientId=1)

        # Define file paths
        trades_file = os.path.abspath(os.path.join("..", "reports", "trades.csv"))
        total_pnl_file = os.path.abspath(os.path.join("..", "reports", "total_pnl.csv"))

        # Ensure the directory exists
        os.makedirs(os.path.dirname(trades_file), exist_ok=True)

        # Calculate P&L and append to CSV files
        calculate_daily_pnl(ib, trades_file=trades_file, total_pnl_file=total_pnl_file)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Disconnect from IB
        ib.disconnect()


if __name__ == "__main__":
    # Schedule the job every 5 minutes
    schedule.every(5).minutes.do(job)

    print("Scheduler started. Running the job every 5 minutes.")
    while True:
        # schedule.run
        schedule.run_pending()
