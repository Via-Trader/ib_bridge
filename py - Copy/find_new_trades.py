import requests
import time
import os

# URL of the ASP script
asp_url = "http://185.17.196.243/cbt/findnewtrades.asp?symbol=SPX&username=noel&password=noel1985"

# File to store the last processed ID
last_processed_file = "last_processed_id.txt"

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
        response.raise_for_status()  # Raise an exception for HTTP errors
        trade_ideas = response.json()
        return trade_ideas
    except requests.exceptions.RequestException as e:
        print(f"Error fetching trade ideas: {e}")
        return []

def process_trade_idea(trade):
    """Process an individual trade idea."""
    print(f"Processing Trade ID {trade['ID']}")
    print(f"Symbol: {trade['Symbol']}, BuySell: {trade['BuySell']}")
    print(f"Entry Price: {trade['EntryPrice']}, Stop Loss: {trade['StopLoss']}, Profit Target: {trade['ProfitTarget']}")
    # Add your trade execution or processing logic here

def poll_asp_script():
    """Poll the ASP script for new trade ideas."""
    last_processed_id = read_last_processed_id()

    while True:
        print("Polling for new trade ideas...")
        trade_ideas = fetch_trade_ideas()

        if trade_ideas:
            # Sort trade ideas by ID to ensure correct processing order
            trade_ideas = sorted(trade_ideas, key=lambda x: x['ID'])

            for trade in trade_ideas:
                # Ignore trades with IDs <= last_processed_id
                if last_processed_id is None or int(trade['ID']) > last_processed_id:
                    process_trade_idea(trade)
                    last_processed_id = int(trade['ID'])  # Update the last processed ID
                    write_last_processed_id(last_processed_id)  # Persist to file
                else:
                    print(f"Ignoring Trade ID {trade['ID']} (already processed)")
        else:
            print("No new trade ideas found.")

        time.sleep(30)  # Wait 30 seconds before polling again

if __name__ == "__main__":
    poll_asp_script()
