from aiohttp import ClientSession

BASE_URL = "https://api-web.tabdeal.org/r/"


async def get_json(url):
    """
    Get JSON from url
    """
    async with ClientSession(trust_env=True) as session:
        async with session.get(url) as response:
            data = await response.json(encoding='utf8')
            if response.status == 200:
                return data
            else:
                return None


async def get_symbols():
    url = BASE_URL + "plots/market_information_cached/"
    return await get_json(url)


async def get_last_trades(market_id):
    print(market_id)
    url = BASE_URL + "api/trade/?market_id={}".format(market_id)
    
    data = await get_json(url)
    print(data)
    return data

