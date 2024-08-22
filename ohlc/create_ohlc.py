from datetime import datetime, timedelta
from dbservice import get_client
from .configs import DB_NAME, COLLECTION_NAME, LIST_OF_SOURCES, \
    OHLC_COLLECTION_NAME, INTERVALS
import asyncio
import pandas as pd


async def fetch_data_for_day_from_source(date, source) -> tuple[list, str]:
    client = get_client()

    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    start_of_day = datetime(date.year, date.month, date.day)
    end_of_day = start_of_day + timedelta(days=1)
    cursor = collection.find({
        'time': {
            '$gte': start_of_day.strftime('%Y-%m-%dT%H:%M:%S'),
            '$lt': end_of_day.strftime('%Y-%m-%dT%H:%M:%S')
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
    trades_data = await asyncio.gather(*corutines)
    return trades_data


def calculate_ohlc(trades_df: pd.DataFrame):
    ohlc_dict = {}
    for label, interval in INTERVALS.items():
        resampled = trades_df['price'].resample(interval, on='time').ohlc().agg({
            'price': ['first', 'max', 'min', 'last', 'mean', 'median'],
            'amount': 'sum',
            'price': 'count'
        })

        resampled.columns = [
            '_'.join(col).strip() for col in resampled.columns.values
            ]
        resampled = resampled.rename(columns={
            'price_first': 'open',
            'price_max': 'high',
            'price_min': 'low',
            'price_last': 'close',
            'price_mean': 'mean',
            'price_median': 'median',
            'amount_sum': 'volume',
            'price_count': 'count'
        })

        resampled.dropna(subset=['open', 'high', 'low', 'close'])
        ohlc_dict[label] = resampled
    return ohlc_dict


async def create_ohlc_for_a_day(date):
    trades_data = await get_data_for_a_day(date)
    for trades, source in trades_data:
        all_trades = []
        for trade in trades:
            all_trades.append({
                'time': datetime.strptime(trade['time'], '%Y-%m-%dT%H:%M:%S'),
                'price': trade['price'],
                'amount': trade['amount'],
                'source': trade['source']
            })
            trades_df = pd.DataFrame(all_trades)
            ohlc_data = calculate_ohlc(trades_df)
            await save_ohlc_data(ohlc_data, date, source)


async def save_ohlc_data(ohlc_data, date, source):
    pass