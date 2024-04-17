from arcticdb import Arctic
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import Coinbase
import pandas as pd
from datetime import datetime
import pytz
# Connect to ArcticDB
ac = Arctic('s3://***?access=R5RhikEM91s34QdZWi2y&secret=kqkPcDn4GZNDmjh4I2UxXNSulL5s6FfTUzrn34i5')

# Create a new library (if it doesn't exist)
#ac.create_library('tick_data')

# Access the 'tick_data' library
library = ac['tick_data']

async def arctic_callback(trade, receipt_timestamp):  # Note the 'async' keyword
    # Convert the timestamp to the 'Asia/Shanghai' timezone
    trade.timestamp = trade.timestamp.astimezone(pytz.timezone('Asia/Shanghai'))

    # Convert the timestamp to ISO 8601 format
    iso_timestamp = trade.timestamp.isoformat()
    data = {
        'symbol': [trade.symbol],  # Use 'symbol' instead of 'pair'
        'timestamp': [iso_timestamp],
        'side': [trade.side],
        'amount': [float(trade.amount)],  # Convert amount to float
        'price': [float(trade.price)]  # Convert price to float
    }
    df = pd.DataFrame(data)  # Convert the dictionary to a DataFrame
    # Write the data to the 'tick_data' library
    library.append(trade.symbol, df)

def main():
    f = FeedHandler()
    f.add_feed(Coinbase(channels=[TRADES], symbols=['BTC-USD'], callbacks={TRADES: arctic_callback}))
    f.run()

if __name__ == '__main__':
    main()
