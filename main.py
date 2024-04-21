import datetime
import os
import pandas as pd
from zoneinfo import ZoneInfo  # Import the ZoneInfo class
from arcticdb import Arctic
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import Coinbase
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ArcticDB connection and configuration
AC_ENDPOINT = 's3://s3ip:port:bucket'
AC_ACCESS_KEY = 'R5RhikEM91s34QdZWi2y'
AC_SECRET_KEY = 'kqkPcDn4GZNDmjh4I2UxXNSulL5s6FfTUzrn34i5'
ac = Arctic(f'{AC_ENDPOINT}?access={AC_ACCESS_KEY}&secret={AC_SECRET_KEY}')

# ac.create_library('crypto_tick_data')
# ac.create_library('crypto_tick_data_archive')

# Define library names
LIBRARY_NAME = 'crypto_tick_data'
ARCHIVE_LIBRARY_NAME = 'crypto_tick_data_archive'

# Check and create libraries if they do not exist
for lib_name in [LIBRARY_NAME, ARCHIVE_LIBRARY_NAME]:
    if lib_name not in ac.list_libraries():
        ac.create_library(lib_name)

library = ac[LIBRARY_NAME]
archive_library = ac[ARCHIVE_LIBRARY_NAME]

def archive_old_data():
    """Archives data older than one week."""
    try:
        one_week_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(weeks=1)
        for symbol in library.list_symbols():
            data = library.read(symbol)
            old_data = data[data['timestamp'] < one_week_ago.isoformat()]
            if not old_data.empty:
                archive_library.append(symbol, old_data)  # Append to archive
                logging.info(f"Archived data for {symbol} older than one week.")
    except Exception as e:
        logging.error("Failed to archive old data: %s", e)

async def arctic_callback(trade, receipt_timestamp):
    try:
        # Convert ISO timestamp to UNIX timestamp float
        unix_timestamp = trade.timestamp
        
        data = {
            'symbol': [trade.symbol],
            'timestamp': [unix_timestamp],  # Directly use UNIX timestamp
            'side': [trade.side],
            'amount': [float(trade.amount)],
            'price': [float(trade.price)]
        }
        
        df = pd.DataFrame(data)
        library.append(trade.symbol, df)
        logging.info(f"Appended data for {trade.symbol} at UNIX time {unix_timestamp}")
    except Exception as e:
        logging.error("Error processing trade data: %s", e)


def main():
    f = FeedHandler()
    f.add_feed(Coinbase(channels=[TRADES], symbols=['BTC-USD'], callbacks={TRADES: arctic_callback}))
    f.run()

    # Periodically archive data (can be scheduled separately as a cron job)
    archive_old_data()

if __name__ == '__main__':
    main()
