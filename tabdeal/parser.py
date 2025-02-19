import datetime
from hashlib import md5
from time import perf_counter


def get_symbols_parser(data, filter: list[str] = []):
    pairs = {}
    for pair in data:
        pairs[pair["symbol"]] = pair["id"]
    return pairs


def get_last_trade_parser(data, market_name):
    matches = [
        {
            "time": match["created"],
            "price": float(match["price"]),
            "amount": float(match["amount"]),
            "type": "buy" if match['side_name'] == "Buy" else "sell",
            "market_name": market_name.replace("_", ""),
            "source": "tabdeal",
            "unifier": md5(((match["created"][16: -10]) +
                            str(match['amount']) +
                            str(match['price'])).encode()).hexdigest()[:10]

        } for match in data["trades"]
    ]
    return matches
