import datetime
from hashlib import md5
def get_symbols_parser(data) -> []:
    results = []
    for market in data['results']:
        if 'otc_market' in market and market['otc_market'] == False:
            results.append({
                'id': market['id'],
                'base': market['currency1']['code'],
                'qoute': market['currency2']['code']
                            })
    return results

def get_last_trades_parser(data):
    matches = []
    for match in data:
        matches.append({
            'time': datetime.datetime.fromtimestamp(int(match['time'])).isoformat(),
            'price': match['price'],
            'amount': match['match_amount'],
            'type': match['type'],
            'source': 'bitpin',
            'unifier': md5(match['match_id'].encode()).hexdigest()[:10]
        })
    return matches
