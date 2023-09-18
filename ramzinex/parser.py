from hashlib import md5 
import datetime
def get_symbols_parser(data, filter: list[str] = []):
    pairs = {}
    for pair in data:
        if not filter or pair['quote_currency_symbol']['en'] in filter:
            pairs[pair['tv_symbol']['ramzinex']] = pair['pair_id']
    return pairs
    

def get_recent_parser(data, market_name):
    matches = []

    if data:
        for match in data:
            matches.append({
                "time": datetime.datetime.strptime(match[2], '%Y-%m-%d %H:%M:%S').isoformat(),
                "price": match[0],
                "amount": match[1],
                "type": match[3],
                "market_name": market_name,
                "source": "ramzinex",
                "unifier": match[5][:10]
            })
        return matches
    else:
        return []