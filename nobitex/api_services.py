from aiohttp import ClientSession

BASE_URL = "https://api.nobitex.ir/"

async def get_json(url):
    """
    Get JSON from url
    """
    async with ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json(encoding='utf8')
            if response.status == 200 and data['status'] == 'ok': 
                return data
            else:
                return None
async def get_symbols():
    url = BASE_URL + "v2/orderbook/all"
    return await get_json(url)

async def get_last_trades(market_name):
    url = BASE_URL + "v2/trades/{0}".format(market_name)
    return await get_json(url)


