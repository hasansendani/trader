import json
def las_trade_parser(data):
    print(json.dumps(data, indent=3))

def get_symbols_parser(data, filter: str = []):
    symbols = data["result"]['symbols']
    markets = {}
    for key, val in symbols.items():
        if 'EXCHANGE' in val:
            if not filter or val['EXCHANGE']['quoteAsset'] in filter:
                markets[key] = val['EXCHANGE']

    return markets