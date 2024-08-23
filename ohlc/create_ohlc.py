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

    trades_df['time'] = pd.to_datetime(trades_df['time'])
    trades_df.set_index('time', inplace=True)

    grouped = trades_df.groupby("market_name")
    # print(trades_df.dtypes)  # Check the data types
    # print(trades_df.index)
    ohlc_dict = {}
    for market, group in grouped:
        ohlc_dict[market] = {}
        for label, interval in INTERVALS.items():

            resampled = group['price'].resample(interval).ohlc()
            resampled['volume'] = group['amount'].resample(interval).sum()
            resampled['mean'] = group['price'].resample(interval).mean()
            resampled['median'] = group['price'].resample(interval).median()
            resampled['count'] = group['price'].resample(interval).count()
            if not {'open', 'high', 'low', 'close'}.issubset(resampled.columns):
                continue

            resampled = resampled.dropna(subset=['open', 'high', 'low', 'close'])
            ohlc_dict[market][label] = resampled
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
                'source': trade['source'],
                'market_name': trade['market_name']
            })
        trades_df = pd.DataFrame(all_trades)
        ohlc_data = calculate_ohlc(trades_df)
        print(ohlc_data)
        await save_ohlc_data(ohlc_data, date, source)


async def save_ohlc_data(ohlc_data, date, source):
    client = get_client()
    db = client[DB_NAME]
    ohlc_collection = db[OHLC_COLLECTION_NAME]

    for market_name, intervals_data in ohlc_data.items():
        for interval, df in intervals_data.items():
            # Convert the DataFrame to a dictionary of records
            records = df.reset_index().to_dict('records')

            # Prepare the data for insertion
            for record in records:
                record['market_name'] = market_name
                record['interval'] = interval
                record['source'] = source
                record['date'] = date.strftime('%Y-%m-%d')

            if records:
                await ohlc_collection.insert_many(records)

    client.close()
