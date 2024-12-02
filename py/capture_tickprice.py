from ib_insync import IB, LimitOrder, StopOrder, Future
import datetime
import csv

# Connect to Interactive Brokers
ib = IB()
try:
    ib.connect('127.0.0.1', 7496, clientId=1)  # Replace with 7496 for paper trading
except Exception as e:
    print(f"API connection failed: {e}")
    print("Make sure API port on TWS/IBG is open")
    exit()

# Hardcoded values
symbol = 'MES'            # Contract symbol
expiry = '20241220'       # Expiry for the futures contract
exchange = 'CME'          # Exchange name
currency = 'USD'          # Currency
action = 'BUY'            # Entry Action
quantity = 1              # Quantity for the order
entry_price = 6010        # Entry limit price
stop_loss_price = 6000    # Stop-loss price
log_file = "C:\CoralBayT\csv\mes.csv"  # Log file to store OHLC data

# Define the contract
contract = Future(
    symbol=symbol,
    lastTradeDateOrContractMonth=expiry,
    exchange=exchange,
    currency=currency
)

# Fetch 1-minute OHLC data
def fetch_1min_ohlc(contract):
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='5 D',  # Last 30 days
            barSizeSetting='1 min',
            whatToShow='TRADES',
            useRTH=False  # Include out-of-hours trading
        )
        return bars
    except Exception as e:
        print(f"Error fetching 1-minute OHLC data: {e}")
        return []

# Write 1-minute OHLC data to a CSV file
def log_1min_ohlc_to_csv(symbol, bars, log_file):
    try:
        with open(log_file, 'a', newline='') as file:
            writer = csv.writer(file)
            # Write headers if the file is empty
            if file.tell() == 0:
                writer.writerow(["Date", "Time", "Open", "High", "Low", "Close", "Volume"])
            
            for bar in bars:
                date, time = bar.date.strftime('%Y/%m/%d'), bar.date.strftime('%H:%M')
                writer.writerow([date, time, bar.open, bar.high, bar.low, bar.close, bar.volume])
        print(f"Logged {len(bars)} 1-minute bars for {symbol} to CSV")
    except Exception as e:
        print(f"Error writing to CSV: {e}")

# Fetch and log 1-minute OHLC data
bars = fetch_1min_ohlc(contract)
if bars:
    log_1min_ohlc_to_csv(symbol, bars, log_file)
else:
    print(f"Failed to fetch 1-minute OHLC data for {symbol}")



# Disconnect from IB
ib.disconnect()
