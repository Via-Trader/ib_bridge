import os
import sys
import time
import requests
import urllib3
import configparser
from ib_insync import IB, Future, StopLimitOrder, LimitOrder, StopOrder

# Disable SSL warnings (only for testing purposes)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MAX_ORDERS = 15
ORDER_ID_COUNTER = None  # Global counter for unique order IDs

def load_config(file_path):
    """Load the configuration from an INI-style .cfg file."""
    config = configparser.ConfigParser()
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file '{file_path}' not found.")
    config.read(file_path, encoding='utf-8')  # Ensure UTF-8 encoding
    return config

def read_last_processed_id(file_path):
    """Read the last processed ID from the file."""
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            try:
                last_id = int(file.read().strip())
                print(f"Last processed ID read: {last_id}")
                return last_id
            except ValueError:
                print("Invalid last processed ID. Starting from scratch.")
                return 0
    print("No last processed ID file found. Starting from scratch.")
    return 0

def write_last_processed_id(last_id, file_path):
    """Write the last processed ID to the file."""
    with open(file_path, "w") as file:
        file.write(str(last_id))
        print(f"Last processed ID updated to: {last_id}")

def fetch_trade_ideas(url):
    """Fetch trade ideas from the service with SSL verification disabled."""
    try:
        response = requests.get(url, verify=False)  # Disable SSL verification
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching trade ideas: {e}")
        return []

def fetch_latest_price(ib, contract):
    """Fetch the latest market price for the given contract."""
    retry_count = 3
    for attempt in range(retry_count):
        ticker = ib.reqMktData(contract)
        ib.sleep(2)
        if ticker.last > 0:
            return ticker.last
        elif ticker.close > 0:
            return ticker.close
        print(f"Retry {attempt + 1}/{retry_count} fetching market price for {contract.symbol}...")
    raise ValueError(f"Unable to fetch the latest price for {contract.symbol}.")

def initialize_order_id(ib):
    """Initialize a global counter for unique order IDs."""
    global ORDER_ID_COUNTER
    if ORDER_ID_COUNTER is None:
        ORDER_ID_COUNTER = ib.client.getReqId()

def get_next_order_id():
    """Increment and return the next unique order ID."""
    global ORDER_ID_COUNTER
    ORDER_ID_COUNTER += 1
    return ORDER_ID_COUNTER

def bracket_order(action, quantity, stop_price, limit_price, stop_loss_price, take_profit_price):
    """Create and return bracket orders (parent, take-profit, stop-loss) with unique IDs."""
    parent_order_id = get_next_order_id()

    # Parent (entry) stop-limit order
    parent = StopLimitOrder(
        action=action,
        totalQuantity=quantity,
        stopPrice=stop_price,
        lmtPrice=limit_price
    )
    parent.orderId = parent_order_id
    parent.transmit = False

    # Take-profit order
    take_profit = LimitOrder(
        action='SELL' if action == 'BUY' else 'BUY',
        totalQuantity=quantity,
        lmtPrice=take_profit_price
    )
    take_profit.orderId = get_next_order_id()
    take_profit.parentId = parent_order_id
    take_profit.transmit = False

    # Stop-loss order
    stop_loss = StopOrder(
        action='SELL' if action == 'BUY' else 'BUY',
        totalQuantity=quantity,
        stopPrice=stop_loss_price
    )
    stop_loss.orderId = get_next_order_id()
    stop_loss.parentId = parent_order_id
    stop_loss.transmit = True  # This transmits the entire chain

    return [parent, take_profit, stop_loss]

def process_trade_idea(trade, ib, config):
    """Process an individual trade idea."""
    print(f"Processing Trade ID {trade['ID']} from source: {trade.get('source', 'Unknown')}")
    action_map = {'L': 'BUY', 'S': 'SELL'}
    action = trade.get('BuySell', '').strip().upper()
    if action not in action_map:
        print(f"⚠️ Invalid action '{action}' for Trade ID {trade['ID']}. Skipping.")
        return

    action = action_map[action]
    print(f"Mapped action for Trade ID {trade['ID']}: {action}")

    # Read order details from the config
    order_config = config["ORDER"]
    quantity = int(order_config["quantity"])
    stop_offset = float(order_config["stop_offset"])
    limit_offset = float(order_config["limit_offset"])
    stop_loss_offset = float(order_config["stop_loss_offset"])
    take_profit_offset = float(order_config["take_profit_offset"])

    contract = Future(
        symbol=config["CONTRACT"]["symbol"],
        lastTradeDateOrContractMonth=config["CONTRACT"]["expiry"],
        exchange=config["CONTRACT"]["exchange"],
        currency=config["CONTRACT"]["currency"]
    )

    qualified_contract = ib.qualifyContracts(contract)
    if not qualified_contract:
        print(f"⚠️ Contract qualification failed for Trade ID {trade['ID']}.")
        return

    latest_price = fetch_latest_price(ib, qualified_contract[0])

    # Calculate order prices based on action
    if action == 'BUY':
        stop_price = latest_price + abs(stop_offset)
        limit_price = latest_price + abs(limit_offset)
        stop_loss_price = stop_price + stop_loss_offset  # stop_loss_offset should be negative
        take_profit_price = stop_price + abs(take_profit_offset)
    elif action == 'SELL':
        stop_price = latest_price - abs(stop_offset)
        limit_price = latest_price - abs(limit_offset)
        stop_loss_price = stop_price + abs(stop_loss_offset)
        take_profit_price = stop_price - abs(take_profit_offset)
    else:
        print(f"Invalid action '{action}' for Trade ID {trade['ID']}. Skipping.")
        return

    print(f"Latest Price: {latest_price}")
    print(f"Stop Price: {stop_price}, Limit Price: {limit_price}, Stop Loss: {stop_loss_price}, Take Profit: {take_profit_price}")

    # Initialize global order ID counter
    initialize_order_id(ib)

    # Create bracket orders
    bracket_orders = bracket_order(
        action, quantity, stop_price, limit_price, stop_loss_price, take_profit_price
    )

    # Log and place orders
    for order in bracket_orders:
        print(f"Placing order: {order}")
        ib.placeOrder(qualified_contract[0], order)

    ib.sleep(2)  # Allow IB time to process orders

def poll_cashbox_service(config):
    """Poll the service for new trade ideas and handle new trades."""
    ib = IB()
    try:
        ib.connect('127.0.0.1', 7496, clientId=2)
    except Exception as e:
        print(f"API connection failed: {e}")
        return

    service_url = config["SERVICE"]["url"]
    last_processed_file = os.path.abspath("last_processed_id.txt")

    while True:
        # Dynamically re-read last_processed_id at the start of each polling cycle
        last_processed_id = read_last_processed_id(last_processed_file)

        print("Polling for new trade ideas...")
        trade_ideas = fetch_trade_ideas(service_url)
        if trade_ideas:
            # Sort trades by ID to ensure we process in order
            trade_ideas = sorted(trade_ideas, key=lambda t: int(t["ID"]))

            for trade in trade_ideas:
                trade_id = int(trade["ID"])
                trade["source"] = config["ORDER"]["source"]

                # Process trades with IDs greater than the last_processed_id
                if trade_id > last_processed_id:
                    print(f"Processing new Trade ID {trade_id}")
                    process_trade_idea(trade, ib, config)
                    last_processed_id = trade_id
                    write_last_processed_id(last_processed_id, last_processed_file)
                else:
                    print(f"Ignoring Trade ID {trade_id} (already processed or below last_processed_id)")
        else:
            print("No new trade ideas found.")
        time.sleep(30)  # Poll every 30 seconds

    ib.disconnect()


if __name__ == "__main__":
    if len(sys.argv) != 2 or not sys.argv[1].startswith("contract="):
        print("Usage: python ib_bridge2.py contract=<config_file>")
        sys.exit(1)

    config_file = sys.argv[1].split("=", 1)[1]
    config_file_path = os.path.join(os.path.dirname(__file__), '../cfg', config_file)

    if not os.path.exists(config_file_path):
        print(f"Configuration file '{config_file_path}' not found.")
        sys.exit(1)

    config = load_config(config_file_path)
    poll_cashbox_service(config)
