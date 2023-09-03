import json, datetime
from hashlib import md5

def las_trade_parser(data):
    raw_trades = data['result']['latestTrades']
    trades = []
    for raw_trade in raw_trades:
        trades.append(
            {
                'time': datetime.datetime.strptime(raw_trade['timestamp'], '%Y-%m-%dT%H:%M:%SZ').isoformat(),
                'price': round(float(raw_trade['price']), 3),
                'amount': float(raw_trade['quantity']),
                'type': 'buy' if raw_trade['isBuyOrder'] == 'true' else 'sell',
                'market_name': raw_trade['symbol'].replace('TMN', 'IRT'),
                'source': 'wallex',
                'unifier': md5((raw_trade['timestamp'] + raw_trade['quantity']).encode()).hexdigest()[:10]
            }
        )
    return trades

def get_symbols_parser(data, filter: str = []):
    symbols = data["result"]['symbols']
    markets = {}
    for key, val in symbols.items():
        if 'EXCHANGE' in val:
            if not filter or val['EXCHANGE']['quoteAsset'] in filter:
                markets[key] = val['EXCHANGE']

    return markets