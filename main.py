import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bitpin.api_services import get_last_trades as last_bitpin
from bitpin.parser import get_last_trades_parser as last_trade_parser_bitpin
from bitpin.markets import markets
from dbservice import write
from wallex.api_service import get_last_trade as last_wallex, get_symbols
from wallex.parser import last_trade_parser as last_trade_parser_wallex, \
    get_symbols_parser
from ramzinex.api_services import get_last_trades as last_trades_ramzinex, \
    get_symbols as get_symbols_ramzinex
from ramzinex.parser import get_recent_parser as get_recent_parser_ramzinex, \
    get_symbols_parser as get_symbols_parser_ramzinex

from ohlc import create_ohlc, update_ohlc_intervals


async def call_and_save_nobitex(market_name):
    from nobitex.api_services import get_last_trades
    from nobitex.parser import get_last_trade_parser
    data = await get_last_trades(market_name)
    matches = get_last_trade_parser(data, market_name)
    for match in matches:
        try:
            await write(match)
        except ValueError:
            break


async def call_and_save_bitpin(key, val):
    data = await last_bitpin(val)
    matches = last_trade_parser_bitpin(data, key)
    for match in matches:
        try:
            await write(match)
        except ValueError:
            break


async def call_and_save_wallex(market):
    matches = last_trade_parser_wallex(await last_wallex(market))
    for match in matches:
        try:
            await write(match)
        except ValueError:
            break


async def call_and_save_ramzinex(market_name, pair_id):
    data = await last_trades_ramzinex(pair_id)
    matches = get_recent_parser_ramzinex(data, market_name)
    for match in matches:
        try:
            await write(match)
        except ValueError:
            break


async def get_wallex_data():
    markets = get_symbols_parser(await get_symbols(), ['TMN'])

    tasks = []
    for market in markets.keys():
        tasks.append(call_and_save_wallex(market))
    await asyncio.gather(*tasks)


async def get_bitpin_data():
    tasks = []
    for key, val in markets.items():
        tasks.append(call_and_save_bitpin(key, val))
    await asyncio.gather(*tasks)


async def get_ramzinx_data():
    symbols = get_symbols_parser_ramzinex(await get_symbols_ramzinex())
    tasks = []
    for market_name, pair_id in symbols.items():
        tasks.append(call_and_save_ramzinex(market_name, pair_id))
    await asyncio.gather(*tasks)


async def get_nobitex_data():
    from nobitex.api_services import get_symbols
    from nobitex.parser import get_symbols_parser
    symbols = get_symbols_parser(await get_symbols())

    tasks = []
    for market_name in symbols:
        tasks.append(call_and_save_nobitex(market_name))
    await asyncio.gather(*tasks)


async def get_tabdeal_data():
    from tabdeal.api_services import get_symbols
    from tabdeal.parser import get_symbols_parser
    symbols = get_symbols_parser(await get_symbols())
    tasks = []
    for market_name, market_id in symbols.items():
        tasks.append(call_and_save_tabdeal(market_id, market_name))
    await asyncio.gather(*tasks)


async def call_and_save_tabdeal(market_id, market_name):
    from tabdeal.api_services import get_last_trades
    from tabdeal.parser import get_last_trade_parser
    last_trades = get_last_trade_parser(await get_last_trades(market_id),
                                        market_name)
    for trade in last_trades:
        try:
            await write(trade)
        except ValueError:
            break


async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(get_bitpin_data, 'interval', minutes=2)
    scheduler.add_job(get_wallex_data, 'interval', minutes=1)
    scheduler.add_job(get_ramzinx_data, 'interval', minutes=1)
    scheduler.add_job(get_nobitex_data, 'interval', minutes=1)
    scheduler.add_job(get_tabdeal_data, 'interval', minutes=5)
    scheduler.start()
    event = asyncio.Event()
    await event.wait()


async def run_ohlc_realtime():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(update_ohlc_intervals, 'interval', minutes=2,seconds=51,
                      args=['bitpin'])

    scheduler.add_job(update_ohlc_intervals, 'interval', minutes=2,
                      args=['nobitex'])

    scheduler.add_job(update_ohlc_intervals, 'interval', minutes=2, seconds=10,
                      args=['ramzinex'])

    scheduler.add_job(update_ohlc_intervals, 'interval', minutes=2, seconds=21,
                      args=['wallex'])

    scheduler.start()
    event = asyncio.Event()
    await event.wait()


async def run_ohlc_historical():
    await create_ohlc.create_historical_data()

if __name__ == "__main__":
    import sys

    param = sys.argv[1] if len(sys.argv) > 1 else None
    if param == 'crawler':
        asyncio.run(main())
    elif param == 'ohlc_historical':
        asyncio.run(run_ohlc_historical())
    elif param == 'ohlc_realtime':
        asyncio.run(run_ohlc_realtime())
    else:
        print('No such parameter')
