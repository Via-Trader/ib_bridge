import os
import sys
import time
import requests
import configparser
from ib_insync import IB, Future, LimitOrder, StopOrder, Order


# Configuration for ASP script
asp_url = "http://185.17.196.243/cbt/findnewtrades.asp?symbol=SPX&username=noel&password=noel1985"
last_processed_file = "last_processed_id.txt"

# Symbol mapping: Convert SPX to MES (Micro E-mini S&P 500 Futures)
symbol_map = {
    "SPX": "MES"
}

def read_last_processed_id():
    """Read the last processed ID from the file."""
    if os.path.exists(last_processed_file):
        with open(last_processed_file, "r") as file:
            try:
                return int(file.read().strip())
            except ValueError:
                return None
    return None

def write_last_processed_id(last_id):
    """Write the last processed ID to the file."""
    with open(last_processed_file, "w") as file:
        file.write(str(last_id))

def fetch_trade_ideas():
    """Fetch trade ideas from the ASP script."""
    try:
        response = requests.get(asp_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching trade ideas: {e}")
        return []

def fetch_latest_price(ib, contract):
    """Fetch the latest market price for the given contract."""
    ticker = ib.reqMktData(contract)
    ib.sleep(2)  # Adjust the sleep duration based on the API's responsiveness
    if ticker.last > 0:
        return ticker.last
    elif ticker.close > 0:
        return ticker.close
    else:
        raise ValueError("Unable to fetch the latest price for the contract.")

def check_order_limit(ib, contract):
    """Check the number of orders on the contract's buy and sell side."""
    open_orders = ib.reqAllOpenOrders()  # Fetch open orders (Order objects)
    buy_orders = 0
    sell_orders = 0

    # Count orders for the specified contract
    for order in open_orders:
        if isinstance(order, Order):  # Ensure that we are checking Order objects
            # Ensure we are checking orders related to the correct contract
            if order.contract.symbol == contract.symbol and order.contract.lastTradeDateOrContractMonth == contract.lastTradeDateOrContractMonth:
                if order.action == "BUY":
                    buy_orders += 1
                elif order.action == "SELL":
                    sell_orders += 1

    print(f"Open Buy Orders: {buy_orders}, Open Sell Orders: {sell_orders}")
    return buy_orders, sell_orders

def place_orders_if_under_limit(ib, contract, orders):
    """Place orders only if the total open orders on either side are under the limit."""
    buy_orders, sell_orders = check_order_limit(ib, contract)

    if buy_orders < 15 and sell_orders < 15:
        for order in orders:
            ib.placeOrder(contract, order)
        print("Orders placed successfully.")
    else:
        print("Order limit reached. Please cancel or wait for existing orders to be filled.")

def bracket_order(parent_order_id, action, quantity, entry_price, stop_loss_offset, limit_price_offset):
    """Create and return the bracket orders (parent, take-profit, stop-loss)."""
    # Ensure action is either 'BUY' or 'SELL'
    if action not in ['BUY', 'SELL']:
        raise ValueError(f"Invalid action: {action}. Must be 'BUY' or 'SELL'.")

    # Parent order: Limit order
    parent = LimitOrder(action=action, totalQuantity=quantity, lmtPrice=entry_price)
    parent.OrderId = parent_order_id
    parent.Transmit = False  # Prevent immediate transmission

    # Take profit: Limit order (opposite side of parent)
    take_profit_action = 'SELL' if action == 'BUY' else 'BUY'
    take_profit = LimitOrder(action=take_profit_action, totalQuantity=quantity, lmtPrice=entry_price + limit_price_offset)
    take_profit.OrderId = parent_order_id + 1
    take_profit.ParentId = parent_order_id
    take_profit.Transmit = False  # Prevent immediate transmission

    # Stop loss: Stop order (opposite side of parent)
    stop_loss_action = 'SELL' if action == 'BUY' else 'BUY'
    stop_loss = StopOrder(action=stop_loss_action, totalQuantity=quantity, stopPrice=entry_price - stop_loss_offset)
    stop_loss.OrderId = parent_order_id + 2
    stop_loss.ParentId = parent_order_id
    stop_loss.Transmit = True  # Transmit the entire bracket when this order is sent

    return [parent, take_profit, stop_loss]

def process_trade_idea(trade, ib, config):
    """Process an individual trade idea."""
    print(f"Processing Trade ID {trade['ID']}")
    symbol = trade['Symbol']
    action = trade['BuySell']
    
    # Fetch config values from the ORDER section
    entry_price_offset = float(config["ORDER"]["entry_price_offset"])
    stop_loss_offset = float(config["ORDER"]["stop_loss_price"])
    limit_price_offset = float(config["ORDER"]["limit_price"])

    # Convert SPX to MES if needed
    if symbol in symbol_map:
        symbol = symbol_map[symbol]

    # Extract contract details
    contract_details = config['CONTRACT']
    contract = Future(
        symbol=symbol,
        lastTradeDateOrContractMonth=contract_details['expiry'],
        exchange=contract_details['exchange'],
        currency=contract_details['currency']
    )

    # Fetch the latest market price
    qualified_contract = ib.qualifyContracts(contract)
    if not qualified_contract:
        print(f"Contract qualification failed for Trade ID {trade['ID']}.")
        return

    latest_price = fetch_latest_price(ib, qualified_contract[0])

    # Adjust prices based on latest market price and offset
    adjusted_entry_price = latest_price + entry_price_offset  # Calculate entry price
    stop_loss_price = adjusted_entry_price - stop_loss_offset
    target_price = adjusted_entry_price + limit_price_offset

    print(f"Latest Price: {latest_price}")
    print(f"Adjusted Entry Price: {adjusted_entry_price}, Stop Loss: {stop_loss_price}, Target Price: {target_price}")

    # Define the action (BUY/SELL)
    if action == 'S':
        action = 'SELL'
    elif action == 'B':
        action = 'BUY'
    else:
        print(f"Invalid action field '{action}' for Trade ID {trade['ID']}. Skipping trade.")
        return

    # Generate bracket orders
    bracket_orders = bracket_order(ib.client.getReqId(), action, 1, adjusted_entry_price, stop_loss_offset, limit_price_offset)

    # Only place orders if under the order limit
    place_orders_if_under_limit(ib, qualified_contract[0], bracket_orders)

def poll_cashbox_service(config):
    """Poll the ASP script for new trade ideas."""
    last_processed_id = read_last_processed_id()

    # Connect to IB
    ib = IB()
    try:
        ib.connect('127.0.0.1', 7496, clientId=1)
    except Exception as e:
        print(f"API connection failed: {e}")
        return

    while True:
        print("Polling for new trade ideas...")
        trade_ideas = fetch_trade_ideas()

        if trade_ideas:
            trade_ideas = sorted(trade_ideas, key=lambda x: x['ID'])
            for trade in trade_ideas:
                if last_processed_id is None or int(trade['ID']) > last_processed_id:
                    process_trade_idea(trade, ib, config)
                    last_processed_id = int(trade['ID'])
                    write_last_processed_id(last_processed_id)
                else:
                    print(f"Ignoring Trade ID {trade['ID']} (already processed)")

        else:
            print("No new trade ideas found.")

        time.sleep(30)

    ib.disconnect()

if __name__ == "__main__":
    # Check command-line arguments
    if len(sys.argv) != 2:
        print("Usage: python ib_bridge.py contract=<config_file>")
        sys.exit(1)

    # Extract config file from arguments
    arg = sys.argv[1]
    if not arg.startswith("contract="):
        print("Invalid argument. Use: contract=<config_file>")
        sys.exit(1)

    config_file = arg.split("=", 1)[1]

    # Construct the path to the configuration file
    config_file_path = os.path.join(os.path.dirname(__file__), '../cfg', config_file)

    # Check if the config file exists
    if not os.path.exists(config_file_path):
        print(f"Configuration file '{config_file_path}' not found.")
        sys.exit(1)

    # Read the config from the configuration file (assuming .ini format)
    config = configparser.ConfigParser()
    config.read(config_file_path)

    # Run the polling script
    poll_cashbox_service(config)

