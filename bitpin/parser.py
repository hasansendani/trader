import datetime
from hashlib import md5
def get_symbols_parser(data, filter = []) -> list:
    results = []
    for market in data['results']:
        if 'otc_market' in market and market['otc_market'] == False:
            results.append({
                'id': market['id'],
                'base': market['currency1']['code'],
                'qoute': market['currency2']['code']
                            })
    if not filter: 
        return results
    else: 
        filtered_result = []
        for res in results:
            if res['qoute'].lower() in filter:
                filtered_result.append(res)
        return filtered_result

def get_last_trades_parser(data, market_name):
    matches = []
    for match in data:
        matches.append({
            'time': datetime.datetime.fromtimestamp(int(match['time'])).isoformat(),
            'price': match['price'],
            'amount': float(match['match_amount']),
            'type': match['type'],
            'market_name': market_name,
            'source': 'bitpin',
            'unifier': md5(match['match_id'].encode()).hexdigest()[:10]
        })
    return matches
