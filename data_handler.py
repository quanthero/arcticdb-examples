import arcticdb as adb
from datetime import datetime, timedelta
from cryptofeed import FeedHandler
from cryptofeed.defines import TICKER
from cryptofeed.exchanges import COINBASE

# 连接到ArcticDB
ac = adb.Arctic('s3://your_bucket_name/arcticdb')

# 创建两个库
recent_lib = ac.initialize_library('recent_tickers')
history_lib = ac.initialize_library('history_tickers', lib_type=adb.CHUNK_STORE, chunk_size='W')

# 设置压缩选项
history_lib.set_compression(adb.CompressionsType.LZMA)

# 定义一周前的日期
one_week_ago = datetime.now() - timedelta(days=7)

# 处理Ticker数据的回调函数
async def ticker_handler(feed, pair, data, timestamp):
    symbol = pair.split('-')[0]
    ticker = {
        'feed': feed,
        'pair': pair,
        'bid': data.bid,
        'ask': data.ask,
        'timestamp': timestamp
    }
    
    # 写入最近一周库
    recent_lib.write(symbol, ticker, metadata={'asof': timestamp})
    
    # 移动历史数据到压缩库
    move_to_history(symbol)
        
def move_to_history(symbol):
    recent_data = recent_lib.read(symbol).data
    history_data = recent_data[recent_data.index < one_week_ago]
    if not history_data.empty:
        history_lib.write(symbol, history_data)
    recent_lib.write(symbol, recent_data[recent_data.index >= one_week_ago])

# 订阅Coinbase的BTC-USD Ticker数据
fh = FeedHandler()
fh.subscribe(COINBASE, TICKER, ['BTC-USD'])
fh.add_feed(TICKER, ticker_handler)

# 启动数据获取
fh.run()
