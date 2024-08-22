from aiohttp import ClientSession

BASE_URL = "https://api.bitpin.ir/v1/"


async def get_last_trades(market_id):
    url = BASE_URL + f'mth/matches/{market_id}/'
    async with ClientSession() as session: 
        async with session.get(url) as resp:
            data = await resp.json(encoding='utf-8')
            return data


async def get_symbols():
    url = BASE_URL + 'mkt/markets/'
    async with ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json(encoding='utf-8')
            return data