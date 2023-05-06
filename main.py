import aiohttp
import asyncio
from dbservice import write
from apscheduler.schedulers.asyncio import AsyncIOScheduler


async def get_symbols():
    async with aiohttp.ClientSession() as session:
        async with session.get('https://api.wallex.ir/v1/all-markets') as resp:
            data = await resp.json(encoding='utf-8')
            return data


async def get_last_trade(symbol):
    async with aiohttp.ClientSession() as session:
        get_url = 'https://api.wallex.ir/v1/trades?symbol=' + symbol
        async with session.get(get_url) as resp:
            data = await resp.json(encoding='utf-8')
            data = data["result"]["latestTrades"][:20]
            loop.create_task(write(data))
            # print(data)
            return data


async def main_service():
    symbols = await get_symbols()
    symbols = [i for i in symbols["result"]["symbols"].keys() if i.endswith("TMN")]
    for symbol in symbols:
        loop.create_task(get_last_trade(symbol))
    return


if __name__ == "__main__":
    scheduler = AsyncIOScheduler()
    scheduler.add_job(main_service, 'interval', minutes=1)
    scheduler.start()
    loop = asyncio.get_event_loop()
    loop.run_forever()
