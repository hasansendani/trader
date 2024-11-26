from datetime import datetime, timedelta
from dbservice import get_client
from .configs import DB_NAME, COLLECTION_NAME, LIST_OF_SOURCES, \
    OHLC_COLLECTION_NAME, INTERVALS, INTERVAL_SECONDS
import asyncio
import pandas as pd
import logging


async def fetch_data_for_day_from_source(start_time, end_time, source) -> tuple[list, str]:
    if not end_time:
        end_time = start_time + timedelta(days=1)

    client = get_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Convert datetime to string format for querying
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S')

    cursor = collection.find({
        'time': {
            '$gte': start_time_str,
            '$lt': end_time_str
        },
        'source': source
    })
    trades = []
    async for doc in cursor:
        doc['time'] = datetime.strptime(doc['time'], '%Y-%m-%dT%H:%M:%S')
        trades.append(doc)
    client.close()
    print(len(trades), source)
    return (trades, source)


async def get_data_for_a_day(date):
    coroutines = []
    for source in LIST_OF_SOURCES:
        coroutines.append(fetch_data_for_day_from_source(date, None, source))
    trades_data = await asyncio.gather(*coroutines)
    return trades_data


def calculate_ohlc(trades_df: pd.DataFrame, intervals=None):
    if intervals is None:
        intervals = INTERVALS

    trades_df['time'] = pd.to_datetime(trades_df['time'])
    trades_df.set_index('time', inplace=True)

    grouped = trades_df.groupby(["market_name", "source"])
    ohlc_dict = {}
    for (market, source), group in grouped:
        market_source_key = f'{market}_{source}'

        if market_source_key not in ohlc_dict:
            ohlc_dict[market_source_key] = {}

        for label, interval in intervals.items():
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
                'time': trade['time'],  # Already converted to datetime
                'price': float(trade['price']),
                'amount': float(trade['amount']),
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
            # Convert the DataFrame to a list of records
            records = df.reset_index().to_dict('records')

            # Prepare the data for insertion
            for record in records:
                market_name_str = ""
                if market_name.startswith("1") and \
                        record['source'] == "nobitex":
                    market_name_str = market_name.split("_")[0] + \
                        market_name.split("_")[1]
                else:
                    market_name_str = market_name.split("_")[0]

                record['market_name'] = market_name_str
                record['interval'] = interval
                record['date'] = date.strftime('%Y-%m-%d')
                record['total'] = record['mean'] * record['volume']
                # Convert 'time' index to string
                record['time'] = record['time'].strftime('%Y-%m-%dT%H:%M:%S')

            if records:
                await ohlc_collection.insert_many(records)

    client.close()


async def create_historical_data():
    first_date = datetime(2023, 9, 28)
    for i in range(2):
        date = first_date + timedelta(days=i)
        await create_ohlc_for_a_day(date)


async def update_ohlc_intervals():
    current_time = datetime.now()
    tasks = []

    for label in INTERVALS.keys():
        should_update, since_time = await should_update_interval(label,
                                                                 current_time)
        if should_update:
            tasks.append(update_ohlc_for_interval(label, since_time))

    if tasks:
        await asyncio.gather(*tasks)
    else:
        logging.warning("No OHLC intervals to update at this time.")


async def should_update_interval(label, current_time):
    interval_seconds = INTERVAL_SECONDS[label]
    last_time = await get_last_saved_ohlc_time(label)

    if last_time is None:
        # No previous OHLC data; process all trades
        return True, None

    elapsed_time = (current_time - last_time).total_seconds()
    if elapsed_time >= interval_seconds:
        return True, last_time
    else:
        return False, last_time


async def get_last_saved_ohlc_time(interval_label):
    client = get_client()
    db = client[DB_NAME]
    ohlc_collection = db[OHLC_COLLECTION_NAME]

    # Find the latest OHLC record for the given interval
    cursor = ohlc_collection.find(
        {'interval': interval_label}
    ).sort('time', -1).limit(1)
    latest_records = await cursor.to_list(length=1)
    client.close()
    if latest_records:
        latest_record = latest_records[0]
        # 'time' is stored as string, convert to datetime
        last_time = datetime.strptime(latest_record['time'], '%Y-%m-%dT%H:%M:%S')
        return last_time
    else:
        return None


async def update_ohlc_for_interval(label, since_time):
    logging.info(f"Updating OHLC data for interval: {label}")
    interval = INTERVALS[label]

    # Fetch new trades since the last OHLC time for this interval
    new_trades = await fetch_new_trades(since_time)
    if not new_trades:
        logging.info(f"No new trades to process for interval: {label}")
        return

    trades_df = pd.DataFrame(new_trades)
    if trades_df.empty:
        logging.info(f"Trades DataFrame is empty for interval: {label}")
        return

    # Only calculate OHLC for the specific interval (label)
    ohlc_data = calculate_ohlc(trades_df, intervals={label: interval})
    await save_ohlc_data(ohlc_data, datetime.now())


async def fetch_new_trades(since_time):
    client = get_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    query = {}
    if since_time:
        since_time_str = since_time.strftime('%Y-%m-%dT%H:%M:%S')
        query['time'] = {'$gt': since_time_str}

    cursor = collection.find(query)
    trades = []
    async for doc in cursor:
        trades.append({
            'time': datetime.strptime(doc['time'], '%Y-%m-%dT%H:%M:%S'), 
            'price': float(doc['price']),
            'amount': float(doc['amount']),
            'source': doc['source'],
            'market_name': doc['market_name'],
        })
    client.close()
    return trades