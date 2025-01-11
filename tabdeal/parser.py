import datetime
from hashlib import md5
from time import perf_counter


def get_symbols_parser(data, filter: list[str] = []):
    start = perf_counter()
    pairs = {}
    for pair in data:
        pairs[pair["symbol"]] = pair["id"]
    print(f'get_symbols_parser: {perf_counter()-start:.3f}')
    return pairs


def get_last_trade_parser(data, market_name):
    matches = [
        {
            "time": datetime.datetime.fromisoformat(match["created"])
            .isoformat(),
            "price": float(match["price"]),
            "amount": float(match["amount"]),
            "type": "buy" if match['side_name'] == "Buy" else "sell",
            "market_name": market_name.strip().replace("_", ""),
            "source": "tabdeal",
            "unifier": md5((str(match["created"][16: -10]) +
                            str(match['amount']) +
                            str(match['price'])).encode()).hexdigest()[:10]

        } for match in data["trades"]
    ]
    return matches
