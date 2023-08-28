import aiohttp

MAIN_URL="https://api.wallex.ir/v1/"
        

async def get_last_trade(symbol):
    async with aiohttp.ClientSession() as session:
        url = MAIN_URL + 'trades?symbol=' + symbol
        async with session.get(url) as resp:
            data = await resp.json(encoding='utf-8')
            if resp.status == 200:
                return data
            raise ConnectionError(f'response code is {resp.status}')

async def get_symbols():
    url = MAIN_URL + 'all-markets'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json(encoding='utf-8')
                return data
            raise ConnectionError(f'response code is {resp.status}')
