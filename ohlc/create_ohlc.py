from datetime import datetime
from dbservice import get_client
from .configs import DB_NAME, COLLECTION_NAME, LIST_OF_SOURCES
import asyncio


async def fetch_data_for_day_from_source(date, source) -> tuple[list, str]:
    client = get_client()

    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    start_of_day = datetime(date.year, date.month, date.day)
    start_of_day_regex = '{}T*'.format(start_of_day.strftime('%Y-%m-%d'))
    print(start_of_day_regex)
    cursor = collection.find({
        'time': {
            '$regex': start_of_day_regex,
        },
        'source': source
    })
    trades = []
    async for doc in cursor:
        trades.append(doc)
    client.close()
    print(len(trades), source)
    return (trades, source)


async def get_data_for_a_day(date):
    corutines = []
    for source in LIST_OF_SOURCES:
        corutines.append(fetch_data_for_day_from_source(date, source))
    trades = await asyncio.gather(*corutines)
