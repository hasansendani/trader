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
    coroutines = []
    for source in LIST_OF_SOURCES:
        coroutines.append(fetch_data_for_day_from_source(date, source))
    trades_data = await asyncio.gather(*coroutines)
    return trades_data


def calculate_ohlc(trades_df: pd.DataFrame):

    trades_df['time'] = pd.to_datetime(trades_df['time'])
    trades_df.set_index('time', inplace=True)

    grouped = trades_df.groupby(["market_name", "source"])
    ohlc_dict = {}
    for (market, source), group in grouped:
        market_source_key = f"{market}_{source}"
   
        if market_source_key not in ohlc_dict:
            ohlc_dict[market_source_key] = {}

        for label, interval in INTERVALS.items():
            resampled = group['price'].resample(interval).ohlc()
            resampled['volume'] = group['amount'].resample(interval).sum()
            resampled['mean'] = group['price'].resample(interval).mean()
            resampled['median'] = group['price'].resample(interval).median()
            resampled['count'] = group['price'].resample(interval).count()
            resampled['source'] = source

            if not {'open', 'high', 'low', 'close'}.issubset(resampled.columns):
                continue

            resampled = resampled.dropna(subset=['open', 'high', 'low', 'close'])
            if label not in ohlc_dict[market_source_key]:
                ohlc_dict[market_source_key][label] = resampled
            else:
                ohlc_dict[market_source_key][label] = \
                    pd.concat([ohlc_dict[market_source_key][label], resampled])
    return ohlc_dict


async def create_ohlc_for_a_day(date):
    trades_data = await get_data_for_a_day(date)
    all_trades = []
    for trades, source in trades_data:
        for trade in trades:
            all_trades.append({
                'time': datetime.strptime(trade['time'], '%Y-%m-%dT%H:%M:%S'),
                'price': trade['price'],
                'amount': float(trade['amount']),  # Ensure amount is numeric
                'source': trade['source'],
                'market_name': trade['market_name']
            })
    trades_df = pd.DataFrame(all_trades)
    ohlc_data = calculate_ohlc(trades_df)
    await save_ohlc_data(ohlc_data, date)


async def save_ohlc_data(ohlc_data, date):
    client = get_client()
    db = client[DB_NAME]
    ohlc_collection = db[OHLC_COLLECTION_NAME]

    for market_name, intervals_data in ohlc_data.items():
        for interval, df in intervals_data.items():
            # Convert the DataFrame to a dictionary of records
            records = df.reset_index().to_dict('records')

            # Prepare the data for insertion
            for record in records:
                market_name_str = ""
                if market_name.endswith("1"):
                    market_name_str = market_name.split("_")[0] + \
                        market_name.split("_")[1]
                else:
                    market_name_str = market_name.split("_")[0]

                record['market_name'] = market_name_str
                record['interval'] = interval
                record['date'] = date.strftime('%Y-%m-%d')
                record['total'] = record['mean'] * record['volume']

            if records:
                await ohlc_collection.insert_many(records)

    client.close()


async def create_historical_data():
    first_date = datetime(2023, 9, 18)
    for i in range(200):
        date = first_date + timedelta(days=i)
        await create_ohlc_for_a_day(date)