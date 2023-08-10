import aiohttp
from ..dbservice import write

async def get_price():
    async with aiohttp.ClientSession() as session:
        async with session.get('https://api.wallex.ir/v1/all-markets') as resp:
            data = await resp.json(encoding='utf-8')
            return data
        

async def get_last_trade(symbol, loop):
    async with aiohttp.ClientSession() as session:
        get_url = 'https://api.wallex.ir/v1/trades?symbol=' + symbol
        async with session.get(get_url) as resp:
            data = await resp.json(encoding='utf-8')
            data = data["result"]["latestTrades"][:20]
            loop.create_task(write(data))
            # print(data)
            return data

async def get_symbols():
    async with aiohttp.ClientSession() as session:
        async with session.get('https://api.wallex.ir/v1/all-markets') as resp:
            data = await resp.json(encoding='utf-8')
            return data
