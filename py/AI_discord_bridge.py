import discord
import pyodbc
import logging
import asyncio
from tabulate import tabulate

# Logging configuration
logging.basicConfig(
    filename="C:\\viatrader2\\py\\alert_log_debug.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"
)

# Bot configuration
TOKEN = "YOUR_BOT_TOKEN"  # Replace with your bot token
GUILD_ID = 1320394408188711033  # Replace with your server ID

# SQL Server connection string
connection_string = (
    "Driver={SQL Server Native Client 10.0};"
    "Server=vt4;"
    "Database=machinelearning;"
    "UID=esp_priceaction_101;"
    "PWD=Coventry_1985_City$222897;"
)

# Channels configuration: channel_name -> (SQL query, Discord channel ID)
channels_config = {
    "spx-alerts": (
        """
        SELECT TOP 5 id, stock, modulename, ROUND(price, 2) AS price, alerttime, timeperiod
        FROM esp..a AS a
        WHERE stock = 'SPX500' AND NOT EXISTS (
            SELECT 1 FROM discord..alert_log AS log
            WHERE log.ModuleName = a.ModuleName AND log.Stock = a.Stock AND log.AlertTime = a.AlertTime
        )
        ORDER BY alerttime DESC;
        """,
        1320395865764008007
    ),
    "spx-alerts-gold": (
        """
        SELECT TOP 5 id, stock, modulename, ROUND(price, 2) AS price, alerttime, timeperiod
        FROM esp..a AS a
        WHERE stock = 'SPX500-GOLD' AND NOT EXISTS (
            SELECT 1 FROM discord..alert_log AS log
            WHERE log.ModuleName = a.ModuleName AND log.Stock = a.Stock AND log.AlertTime = a.AlertTime
        )
        ORDER BY alerttime DESC;
        """,
        1320395865764008008
    ),
    "rty-alerts": (
        """
        SELECT TOP 5 id, stock, modulename, ROUND(price, 2) AS price, alerttime, timeperiod
        FROM esp..a AS a
        WHERE stock = 'US2000' AND NOT EXISTS (
            SELECT 1 FROM discord..alert_log AS log
            WHERE log.ModuleName = a.ModuleName AND log.Stock = a.Stock AND log.AlertTime = a.AlertTime
        )
        ORDER BY alerttime DESC;
        """,
        1320395865764008009
    )
}

# Intents setup
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True

bot = discord.Client(intents=intents)

# Polling interval (seconds)
POLL_INTERVAL = 60

async def fetch_and_post_alerts(channel_name, sql_query, channel_id):
    """Fetch alerts and post them to the specified Discord channel."""
    conn = None
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()

        if rows:
            # Format data
            table = "\n".join([
                f"{row[0]}, {row[1]}, {row[2]}, {row[3]:.2f}, {row[4]}, {row[5]}"
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
    while True:
        for channel_name, (sql_query, channel_id) in channels_config.items():
            await fetch_and_post_alerts(channel_name, sql_query, channel_id)
        await asyncio.sleep(POLL_INTERVAL)

# Run the bot
bot.run(TOKEN)
