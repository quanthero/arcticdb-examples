import logging
from datetime import datetime, timedelta
import pytz
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import Coinbase
import pandas as pd
from arctic import Arctic

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ArcticDB connection and configuration
AC_ENDPOINT = 's3://***'
AC_ACCESS_KEY = 'your_access_key'
AC_SECRET_KEY = 'your_secret_key'
ac = Arctic(f'{AC_ENDPOINT}?access={AC_ACCESS_KEY}&secret={AC_SECRET_KEY}')

LIBRARY_NAME = 'tick_data'
ARCHIVE_LIBRARY_NAME = 'tick_data_archive'

# Check and create libraries if they do not exist
for lib_name in [LIBRARY_NAME, ARCHIVE_LIBRARY_NAME]:
    if lib_name not in ac.list_libraries():
        ac.create_library(lib_name)

library = ac[LIBRARY_NAME]
archive_library = ac[ARCHIVE_LIBRARY_NAME]

def archive_old_data():
    """Archives data older than one week."""
    try:
        one_week_ago = datetime.now(pytz.utc) - timedelta(weeks=1)
        for symbol in library.list_symbols():
            data = library.read(symbol)
            old_data = data[data['timestamp'] < one_week_ago.isoformat()]
            if not old_data.empty:
                archive_library.append(symbol, old_data)  # Append to archive
                # Now remove old data from main library
                # Assume `delete` is a hypothetical method to remove data. This depends on Arctic's capabilities.
                library.delete(symbol, old_data.index)
                logging.info(f"Archived data for {symbol} older than one week.")
    except Exception as e:
        logging.error("Failed to archive old data: %s", e)

async def arctic_callback(trade, receipt_timestamp):
    try:
        trade.timestamp = trade.timestamp.astimezone(pytz.timezone('Asia/Shanghai'))
        iso_timestamp = trade.timestamp.isoformat()
        data = {
            'symbol': [trade.symbol],
            'timestamp': [iso_timestamp],
            'side': [trade.side],
            'amount': [float(trade.amount)],
            'price': [float(trade.price)]
        }
        df = pd.DataFrame(data)
        library.append(trade.symbol, df)
        logging.info(f"Appended data for {trade.symbol} at {iso_timestamp}")
    except Exception as e:
        logging.error("Error processing trade data: %s", e)

def main():
    f = FeedHandler()
    f.add_feed(Coinbase(channels=[TRADES], symbols=['BTC-USD'], callbacks={TRADES: arctic_callback}))
    f.run()

    # Periodically archive data
    archive_old_data()

if __name__ == '__main__':
    main()
