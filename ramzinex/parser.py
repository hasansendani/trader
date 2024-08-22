from datetime import datetime, timedelta


def get_symbols_parser(data, filter: list[str] = []):
    pairs = {}
    for pair in data:
        if not filter or pair['quote_currency_symbol']['en'] in filter:
            pairs[pair['tv_symbol']['ramzinex']] = pair['pair_id']
    return pairs
    

def get_recent_parser(data, market_name: str):
    matches = []

    if data:
        for match in data:
            time_str = (datetime.strptime(match[2], '%Y-%m-%d %H:%M:%S') +
                        timedelta(hours=3, minutes=30)).isoformat()
            matches.append({
                "time": time_str,
                "price": match[0],
                "amount": match[1],
                "type": match[3],
                "market_name": market_name.upper(),
                "source": "ramzinex",
                "unifier": match[5][:10],
                "changed_timezone": True
            })
        return matches
    else:
        return []
