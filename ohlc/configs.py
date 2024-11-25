LIST_OF_SOURCES = ['ramzinex', 'nobitex', 'wallex', 'bitpin']
DB_NAME = "market_making"
COLLECTION_NAME = "last_trades"
OHLC_COLLECTION_NAME = "ohlc"
INTERVALS = {
    '1D': '1D',
    '4H': '4h',
    '1H': '1h',
    '15Min': '15min',
    '5Min': '5min',
    '1Min': '1min'
}
INTERVAL_SECONDS = {
    '1D': 86400,
    '4H': 14400,
    '1H': 3600,
    '15Min': 900,
    '5Min': 300,
    '1Min': 60
}
