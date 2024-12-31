import sys
import discord
import pyodbc
import logging
import asyncio
import json
import os
import re

# ----------------------------------------
# 1. Configuration Management
# ----------------------------------------
class ConfigManager:
    """
    Loads JSON configuration files from a specified directory.
    """
    def __init__(self, config_dir):
        self.config_dir = config_dir
        
        self.logging_config = self.load_config("logging_config.json")
        self.bot_config = self.load_config("bot_config.json")
        self.database_config = self.load_config("database_config.json")
        self.channels_config = self.load_config("channels_config.json")
        self.module_mapping = self.load_config("module_mapping.json").get("module_mapping", {})
        self.symbol_settings = self.load_config("symbol_mappings.json")

    def load_config(self, file_name):
        """
        Loads a single JSON configuration file from the config directory.
        """
        file_path = os.path.join(self.config_dir, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Configuration file not found: {file_path}")
        with open(file_path, "r") as file:
            return json.load(file)


# ----------------------------------------
# 2. Logging Setup
# ----------------------------------------
class LogManager:
    """
    Manages Python's logging based on a configuration dictionary.
    """
    @staticmethod
    def setup_logging(logging_config):
        """
        Sets up logging according to the specified configuration, ensuring only file logging.
        """
        logging.basicConfig(
            filename=logging_config["filename"],
            level=getattr(logging, logging_config["level"].upper()),
            format=logging_config["format"],
            filemode=logging_config["filemode"]
        )

        # Ensure only file logging is enabled (remove StreamHandlers)
        for handler in logging.root.handlers[:]:
            if isinstance(handler, logging.StreamHandler):
                logging.root.removeHandler(handler)


# ----------------------------------------
# 3. Database Management
# ----------------------------------------
class DatabaseManager:
    """
    Handles the database connection and queries using pyodbc.
    """
    def __init__(self, database_config):
        self.connection_string = (
            f"Driver={database_config['Driver']};"
            f"Server={database_config['Server']};"
            f"Database={database_config['Database']};"
            f"UID={database_config['UID']};"
            f"PWD={database_config['PWD']};"
        )

    def get_connection(self):
        """
        Returns a new pyodbc connection using the configured connection string.
        """
        return pyodbc.connect(self.connection_string)


# ----------------------------------------
# 4. Alert Processing / Symbol Adjustment
# ----------------------------------------
class AlertProcessor:
    """
    Contains utility methods for processing alert rows:
      - Duration replacement (e.g., day/week/month -> calculated minutes)
      - Symbol lookup and price adjustment based on configuration
    """
    def __init__(self, module_mapping, symbol_settings):
        self.module_mapping = module_mapping
        self.symbol_settings = symbol_settings

    def replace_duration(self, match, time_period):
        """
        Replace day/week/month in a string with the calculated minutes.
        """
        number = int(match.group(1))
        unit = match.group(2)
        logging.debug(f"Matched duration: {number} {unit}")
        if unit == "day":
            minutes = number * time_period
            return f"{minutes} minutes"
        elif unit == "week":
            days = number * 7
            minutes = days * time_period
            return f"{minutes} minutes"
        elif unit == "month":
            days = number * 31
            minutes = days * time_period
            return f"{minutes} minutes"
        return match.group(0)

    def lookup_symbol_and_adjust_price(self, row):
        """
        Perform symbol lookup, price adjustment, and module name mapping.
        """
        # Row structure: (ID, Symbol, ModuleName, Price, Timestamp, TimePeriod)
        original_symbol = row[1]
        original_price = row[3]
        original_module_name = row[2]
        time_period = int(row[5])

        # Map the ModuleName using module_mapping
        mapped_module_name = self.module_mapping.get(original_module_name, {}).get("display_name", original_module_name)

        # Replace durations in ModuleName
        mapped_module_name = re.sub(
            r"(\d+)\s*(day|week|month)",
            lambda m: self.replace_duration(m, time_period),
            mapped_module_name
        )

        show_futures = self.symbol_settings["settings"].get("show_futures", False)
        mappings = self.symbol_settings["mappings"]

        # Adjust symbol & price if show_futures is enabled and symbol is found in mappings
        if show_futures and original_symbol in mappings:
            mapping = mappings[original_symbol]
            adjusted_symbol = mapping["futures_symbol"]
            price_adjustment = mapping.get("price_adjustment", 0)
            adjusted_price = original_price + price_adjustment

            # Round the price to the nearest quarter
            adjusted_price = round(adjusted_price * 4) / 4

            return (
                row[0],
                adjusted_symbol,
                mapped_module_name,
                f"{adjusted_price:.2f}",
                row[4],
                row[5]
            )
        else:
            return (
                row[0],
                original_symbol,
                mapped_module_name,
                f"{original_price:.2f}",
                row[4],
                row[5]
            )


# ----------------------------------------
# 5. Discord Bot
# ----------------------------------------
class AlertBot(discord.Client):
    """
    Discord Client that periodically fetches alerts from the database and posts them to channels.
    """
    def __init__(self, config_manager, db_manager, alert_processor, poll_interval=10):
        intents = discord.Intents.default()
        intents.messages = True
        intents.message_content = True
        
        super().__init__(intents=intents)

        self.config_manager = config_manager
        self.db_manager = db_manager
        self.alert_processor = alert_processor
        self.poll_interval = poll_interval

        self.token = self.config_manager.bot_config["TOKEN"]
        self.guild_id = self.config_manager.bot_config["GUILD_ID"]
        self.channels_config = self.config_manager.channels_config["channels"]

        # For command-line channels
        self.passed_channels = set(sys.argv[1:])

    async def on_ready(self):
        logging.info(f"Bot logged in as {self.user}")
        
        # Determine which channels to process
        logging.debug(f"Passed channels: {self.passed_channels}")
        logging.debug(f"Available channels in config: {list(self.channels_config.keys())}")

        channels_to_process = {}
        for name, config in self.channels_config.items():
            if not self.passed_channels or name in self.passed_channels:
                channels_to_process[name] = config

        if not channels_to_process:
            logging.error("No valid channels passed in arguments.")
            await self.close()
            return

        logging.info(f"Processing channels: {list(channels_to_process.keys())}")
        
        # Start the periodic task
        while True:
            for channel_name, ch_config in channels_to_process.items():
                await self.fetch_and_post_alerts(channel_name, ch_config["sql_query"], ch_config["channel_id"])
            await asyncio.sleep(self.poll_interval)

    async def fetch_and_post_alerts(self, channel_name, sql_query, channel_id):
        """
        Fetch alerts from the database, process them, and post them to Discord.
        """
        conn = None
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql_query)
            rows = cursor.fetchall()

            if rows:
                # Process each row
                adjusted_rows = [self.alert_processor.lookup_symbol_and_adjust_price(row) for row in rows]

                # Format data for Discord output
                table = "\n".join([
                    f"{r[0]}|{r[1]}|{r[2]}|{r[3]}|{r[4]}|{r[5]}"
                    for r in adjusted_rows
                ])

                # Insert into logging table
                insert_query = """
                    INSERT INTO discord..alert_log (ModuleName, Stock, AlertTime)
                    VALUES (?, ?, ?);
                """
                for original_row in rows:  # Use the original ModuleName here
                    try:
                        cursor.execute(insert_query, (original_row[2], original_row[1], original_row[4]))
                    except pyodbc.IntegrityError:
                        logging.warning(
                            f"Duplicate detected, skipping: {original_row[2]}, "
                            f"{original_row[1]}, {original_row[4]}"
                        )
                        continue

                conn.commit()

                # Send to Discord
                channel = self.get_channel(channel_id)
                if channel:
                    await channel.send(f"{channel_name} Alerts:\n```\n{table}\n```")
            else:
                logging.info(f"No new alerts for {channel_name}.")

        except Exception as e:
            logging.error(f"Error processing alerts for {channel_name}: {e}")
        finally:
            if conn:
                conn.close()


# ----------------------------------------
# 6. Bot Runner
# ----------------------------------------
def run_bot():
    """
    Initialize configurations, set up logging, and run the bot.
    """
    # 6.1. Set the config directory
    CONFIG_DIR = "C:\\viatrader2\\config"

    # 6.2. Create and load configurations
    config_manager = ConfigManager(CONFIG_DIR)

    # 6.3. Set up logging
    LogManager.setup_logging(config_manager.logging_config)

    # 6.4. Initialize database manager
    db_manager = DatabaseManager(config_manager.database_config)

    # 6.5. Initialize alert processor
    alert_processor = AlertProcessor(config_manager.module_mapping, config_manager.symbol_settings)

    # 6.6. Create the bot instance
    bot = AlertBot(
        config_manager=config_manager,
        db_manager=db_manager,
        alert_processor=alert_processor,
        poll_interval=10  # Adjust your poll interval if needed
    )

    # 6.7. Run the bot within a managed event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(bot.start(bot.token))
    except KeyboardInterrupt:
        loop.run_until_complete(bot.close())
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
            try:
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


# ----------------------------------------
# 7. Script Entry Point
# ----------------------------------------
if __name__ == "__main__":
    run_bot()
