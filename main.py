import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bitpin.api_services import  get_last_trades as last_bitpin
from bitpin.parser import get_last_trades_parser as last_trade_parser_bitpin
from bitpin.markets import markets
from dbservice import write
from wallex.api_service import get_last_trade as last_wallex, get_symbols 
from wallex.parser import las_trade_parser as last_trade_parser_wallex , get_symbols_parser


async def get_bitpin_data():
    tasks = []
    for key, val in markets.items():
        tasks.append(call_and_save_bitpin( key, val))
    await asyncio.gather(*tasks)

async def call_and_save_bitpin( key, val):
    data = await last_bitpin(val)
    matches = last_trade_parser_bitpin(data)
    for match in matches:
        match['market_name'] = key
        await write(match)
    
    
async def main():
    data = await get_symbols()
    get_symbols_parser(data, ['TMN'])
    data = await last_wallex('BTCTMN')
    last_trade_parser_wallex(data)
    # scheduler = AsyncIOScheduler()
    # scheduler.add_job(get_bitpin_data, 'interval', minutes=2)
    # scheduler.start()
    # while True:
    #     await asyncio.sleep(1000)


if __name__ == "__main__":
    
    asyncio.run(main())