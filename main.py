import aiohttp
import asyncio
from dbservice import write
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from wallex.api_service import get_symbols, get_last_trade

loop = asyncio.get_event_loop()


async def main_service():
    symbols = await get_symbols()
    symbols = [i for i in symbols["result"]["symbols"].keys() if i.endswith("TMN")]
    for symbol in symbols:
        loop.create_task(get_last_trade(symbol, loop))
    return


if __name__ == "__main__":
    scheduler = AsyncIOScheduler()
    scheduler.add_job(main_service, 'interval', minutes=1)
    scheduler.start()
    
    loop.run_forever()
