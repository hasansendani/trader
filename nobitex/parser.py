import datetime
from hashlib import md5


def get_symbols_parser(data, filter: list[str] = []):
    data.pop('status')
    keys = list(data['markets'].keys())
    pairs = []
    for key in keys:
        if not filter or key[-3:] in filter or key[-4:] in filter:
            pairs.append(key)
    return pairs


def get_last_trade_parser(data, market_name):
    matches = [
        {
            "time": datetime.datetime.fromtimestamp(match["time"] // 1000).isoformat(),
            "price": float(match["price"]),
            "amount": float(match["volume"]),
            "type": match['type'],
            "market_name": market_name,
            "source": "nobitex",
            "unifier": md5((str(match["time"]) + match['volume'] + match['price']).encode()).hexdigest()[:10]

        } for match in data["trades"]
    ]
    return matches
