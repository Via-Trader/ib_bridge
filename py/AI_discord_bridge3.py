# 4 config files, connection info, symbol mapping, modulename mapping, channel info 

import sys
import discord
import pyodbc
import logging
import asyncio
import json

# Logging configuration
logging.basicConfig(
    filename="C:\\viatrader2\\py\\alert_log_debug.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"
)

# Load application configuration from JSON file
with open("C:\\viatrader2\\config\\discord_bot_config.json", "r") as config_file:
    app_config = json.load(config_file)

# Extract Discord bot configuration
discord_bot_config = app_config["discord_bot_config"]
bot_token = discord_bot_config["TOKEN"]
guild_id = discord_bot_config["GUILD_ID"]

# Build the SQL Server connection string
db_config = app_config["database_config"]
connection_string = (
    f"Driver={db_config['Driver']};"
    f"Server={db_config['Server']};"
    f"Database={db_config['Database']};"
    f"UID={db_config['UID']};"
    f"PWD={db_config['PWD']};"
)

# Load channel configuration from JSON file
with open("C:\\viatrader2\\config\\channel_config.json", "r") as config_file:
    channels_config = json.load(config_file)

# Load symbol mapping and settings from JSON file
with open("C:\\viatrader2\\config\\symbol_mapping.json", "r") as symbol_file:
    symbol_data = json.load(symbol_file)

# Load module mapping from JSON file
with open("C:\\viatrader2\\config\\module_mapping.json", "r") as module_file:
    module_mapping = json.load(module_file)["module_mapping"]

# Extract settings and mappings
show_futures = symbol_data["settings"]["show_futures"]
symbol_mapping = symbol_data["mappings"]

# Intents setup
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True

bot = discord.Client(intents=intents)

# Polling interval (seconds)
POLL_INTERVAL = 60

def get_futures_symbol_and_adjusted_price(stock, price):
    """Get the futures symbol and adjusted price for the given stock."""
    if stock in symbol_mapping:
        futures_symbol = symbol_mapping[stock]["futures_symbol"]
        price_adjustment = symbol_mapping[stock]["price_adjustment"]
        adjusted_price = price + price_adjustment
        return futures_symbol, adjusted_price
    return stock, price

def map_module_name(module_name, time_period):
    """Map the module name to its display name and perform post-processing."""
    # Step 1: Apply initial mapping
    if module_name in module_mapping:
        module_name = module_mapping[module_name]["display_name"]

    # Step 2: Post-process to replace "week" or "month" with calculated time
    if "week" in module_name or "month" in module_name:
        if "week" in module_name:
            factor = 7  # Assume 1 week = 7 days
            unit = "minutes"
        elif "month" in module_name:
            factor = 30  # Assume 1 month = 30 days
            unit = "minutes"

        # Calculate new time period
        calculated_time = time_period * factor
        module_name = module_name.replace("week", f"{calculated_time} {unit}")
        module_name = module_name.replace("month", f"{calculated_time} {unit}")

    return module_name

async def fetch_and_post_alerts(channel_name, sql_query, channel_id):
    """Fetch alerts and post them to the specified Discord channel."""
    conn = None
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()

        if rows:
            # Format data based on the flag
            if show_futures:
                table = "\n".join([
                    f"{row[0]}|{get_futures_symbol_and_adjusted_price(row[1], row[3])[0]}|{map_module_name(row[2], row[5])}|{get_futures_symbol_and_adjusted_price(row[1], row[3])[1]:.2f}|{row[4]}|{row[5]}"
                    for row in rows if row[1] in symbol_mapping
                ])
            else:
                table = "\n".join([
                    f"{row[0]}|{row[1]}|{map_module_name(row[2], row[5])}|{row[3]:.2f}|{row[4]}|{row[5]}"
                    for row in rows
                ])
            
            # Insert into logging table
            insert_query = """
            INSERT INTO discord..alert_log (ModuleName, Stock, AlertTime)
            VALUES (?, ?, ?);
            """
            for row in rows:
                cursor.execute(insert_query, (row[2], row[1], row[4]))
            conn.commit()

            # Send to Discord
            channel = bot.get_channel(channel_id)
            if channel:
                await channel.send(f"{channel_name} Alerts:\n```\n{table}\n```")
        else:
            logging.info(f"No new alerts for {channel_name}.")
    except Exception as e:
        logging.error(f"Error processing alerts for {channel_name}: {e}")
    finally:
        if conn:
            conn.close()

@bot.event
async def on_ready():
    logging.info(f"Bot logged in as {bot.user}")
    
    # Parse command-line arguments
    passed_channels = set(sys.argv[1:])  # Get all arguments after the script name
    logging.debug(f"Passed channels: {passed_channels}")

    # Filter channels to process
    channels_to_process = {}

    for name, config in channels_config.items():
        # If no specific channels are passed or the channel is in the passed arguments
        if not passed_channels or name in passed_channels:
            channels_to_process[name] = config

    if not channels_to_process:
        logging.error("No valid channels passed in arguments.")
        await bot.close()
        return

    logging.info(f"Processing channels: {list(channels_to_process.keys())}")

    while True:
        for channel_name, config in channels_to_process.items():
            await fetch_and_post_alerts(channel_name, config["sql_query"], config["channel_id"])
        await asyncio.sleep(POLL_INTERVAL)

# Run the bot
bot.run(bot_token)
