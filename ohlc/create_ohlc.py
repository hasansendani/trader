from datetime import datetime, timedelta
from dbservice import get_client
from .configs import DB_NAME, COLLECTION_NAME, LIST_OF_SOURCES, \
    OHLC_COLLECTION_NAME, INTERVALS, INTERVAL_SECONDS
import asyncio
import pandas as pd
import logging
from pymongo import UpdateOne

logging.basicConfig(level=logging.INFO)


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
    logging.info(f"calculating ohlc for {len(trades_df)} trades and in intervals: {intervals}")
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
            # Perform a single resample operation with multiple aggregations
            resampled = group.resample(interval).agg({
                'price': ['first', 'max', 'min', 'last',
                          'mean', 'median', 'std', 'count'],
                'amount': 'sum'
            })

            # Flatten the MultiIndex columns
            resampled.columns = ['_'.join(col).strip()
                                 for col in resampled.columns.values]

            if resampled.empty:
                continue

            # Rename columns to match your original naming convention
            resampled.rename(columns={
                'price_first': 'open',
                'price_max': 'high',
                'price_min': 'low',
                'price_last': 'close',
                'price_mean': 'mean',
                'price_median': 'median',
                'price_std': 'std',
                'price_count': 'count',
                'amount_sum': 'volume'
            }, inplace=True)

            resampled['source'] = source
            resampled.dropna(subset=['open', 'high', 'low', 'close'],
                             inplace=True)

            if resampled.empty:
                continue

            if label not in ohlc_dict[market_source_key]:
                ohlc_dict[market_source_key][label] = resampled
            else:
                ohlc_dict[market_source_key][label] = pd.concat([
                    ohlc_dict[market_source_key][label],
                    resampled
                ])

    return ohlc_dict


async def create_ohlc_for_a_day(date):
    trades_data = await get_data_for_a_day(date)
    all_trades = []
    for trades, source in trades_data:
        for trade in trades:
            all_trades.append({
                'time': trade['time'],
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
    bulk_operations = []

    for market_name, intervals_data in ohlc_data.items():
        for interval, df in intervals_data.items():
            # Convert the DataFrame to a list of records
            records = df.reset_index().to_dict('records')

            # Prepare the data for upsert operations
            for record in records:
                if market_name.startswith("1") and record['source'] == "nobitex":
                    market_name_str = market_name.split("_")[0] + market_name.split("_")[1]
                else:
                    market_name_str = market_name.split("_")[0]

                record['market_name'] = market_name_str
                record['interval'] = interval
                record['date'] = date.strftime('%Y-%m-%d')
                record['total'] = record['mean'] * record['volume']
                record['time'] = record['time'].strftime('%Y-%m-%dT%H:%M:%S')

                filter_query = {
                    'market_name': record['market_name'],
                    'source': record['source'],
                    'interval': record['interval'],
                    'time': record['time']
                }
                update_operation = {
                    '$set': record
                }
                bulk_operations.append(
                    UpdateOne(filter_query, update_operation, upsert=True)
                )

    if bulk_operations:
        await ohlc_collection.bulk_write(bulk_operations, ordered=False)

    client.close()


async def create_historical_data():
    first_date = datetime(2023, 9, 28)
    for i in range(2):
        date = first_date + timedelta(days=i)
        await create_ohlc_for_a_day(date)


async def update_ohlc_intervals(source):
    logging.info(f'call for {source}')
    
    current_time = datetime.now()
    tasks = []

    for label in INTERVALS.keys():
        should_update, since_time = await should_update_interval(label, current_time, source)
        if should_update:
            tasks.append(update_ohlc_for_interval(label, since_time, source))

    if tasks:
        await asyncio.gather(*tasks)
    else:
        logging.warning("No OHLC intervals to update at this time.")


# ─── MODIFIED FUNCTION: should_update_interval ──────────────────────────────
async def should_update_interval(label, current_time, source):
    if label == '1D':
        # For daily candle: we want to update the previous day once it is complete.
        prev_day = current_time - timedelta(days=1)
        start_of_prev_day = prev_day.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_prev_day = start_of_prev_day + timedelta(days=1)
        last_time = await get_last_saved_ohlc_time(label, source)
        # If there is no daily candle or the last candle does not fall within yesterday's boundaries, update.
        if last_time is None or not (start_of_prev_day <= last_time < end_of_prev_day):
            return True, start_of_prev_day
        else:
            return False, last_time
    else:
        interval_seconds = INTERVAL_SECONDS[label]
        last_time = await get_last_saved_ohlc_time(label, source)
        if last_time is None:
            return True, None
        elapsed_time = (current_time - last_time).total_seconds()
        if elapsed_time >= interval_seconds:
            return True, last_time
        else:
            return False, last_time
# ─────────────────────────────────────────────────────────────────────────────


async def get_last_saved_ohlc_time(interval_label, source):
    logging.info(f"get last ohlc for {interval_label}, from {source}")
    client = get_client()
    db = client[DB_NAME]
    ohlc_collection = db[OHLC_COLLECTION_NAME]

    doc = await ohlc_collection.find_one(
            {
                'interval': interval_label,
                'source': source
            },
            sort=[('time', -1)]
        )
    client.close()
    if doc:
        # 'time' is stored as a string, convert to datetime
        last_time = datetime.strptime(doc['time'], '%Y-%m-%dT%H:%M:%S')
        return last_time
    else:
        return None


def optimize_dataframe(df):
    for col in df.select_dtypes(include=['float']):
        df[col] = pd.to_numeric(df[col], downcast='float')
    for col in df.select_dtypes(include=['int']):
        df[col] = pd.to_numeric(df[col], downcast='integer')
    return df


# ─── MODIFIED FUNCTION: update_ohlc_for_interval ─────────────────────────────
async def update_ohlc_for_interval(label, since_time, source):
    logging.info(f"Updating OHLC data for interval: {label} and source: {source}")
    interval = INTERVALS[label]

    # Fetch new trades in batches
    ohlc_data = {}

    async for batch in fetch_new_trades_in_batches(since_time, source, 2000, interval):
        trades_df = pd.DataFrame(batch)
        optimize_dataframe(trades_df)
        batch_ohlc_data = calculate_ohlc(trades_df, intervals={label: interval})
        # Merge the result into ohlc_data
        merge_ohlc_data(ohlc_data, batch_ohlc_data)

    if ohlc_data:
        await save_ohlc_data(ohlc_data, datetime.now())
    else:
        logging.info(f"No new OHLC data to save for interval: {label} and source: {source}")
# ─────────────────────────────────────────────────────────────────────────────


def merge_ohlc_data(ohlc_data, trade_ohlc_data):
    for market_key in trade_ohlc_data:
        if market_key not in ohlc_data:
            ohlc_data[market_key] = trade_ohlc_data[market_key]
        else:
            for interval in trade_ohlc_data[market_key]:
                if interval not in ohlc_data[market_key]:
                    ohlc_data[market_key][interval] = trade_ohlc_data[market_key][interval]
                else:
                    # Concatenate the DataFrames
                    ohlc_data[market_key][interval] = pd.concat([
                        ohlc_data[market_key][interval],
                        trade_ohlc_data[market_key][interval]
                    ])


# ─── MODIFIED FUNCTION: fetch_new_trades_in_batches ──────────────────────────
async def fetch_new_trades_in_batches(since_time, source, batch_size=20000, interval=None):
    logging.info(f"get new trades since {since_time}, from {source}")
    
    client = get_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    query = {'source': source}
    if since_time:
        if interval == '1D':
            # For daily candle, restrict query to trades from the complete previous day.
            start_of_day = since_time.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = start_of_day + timedelta(days=1)
            start_time_str = start_of_day.strftime('%Y-%m-%dT%H:%M:%S')
            end_time_str = end_of_day.strftime('%Y-%m-%dT%H:%M:%S')
            query['time'] = {'$gte': start_time_str, '$lt': end_time_str}
        else:
            since_time_str = since_time.strftime('%Y-%m-%dT%H:%M:%S')
            query['time'] = {'$gt': since_time_str}

    cursor = collection.find(query, projection={
                                '_id': 0,
                                'time': 1,
                                'price': 1,
                                'amount': 1,
                                'source': 1,
                                'market_name': 1
                            })
    batch = []

    async for doc in cursor:
        batch.append({
            'time': datetime.strptime(doc['time'], '%Y-%m-%dT%H:%M:%S'),
            'price': float(doc['price']),
            'amount': float(doc['amount']),
            'source': doc['source'],
            'market_name': doc['market_name'],
        })
        if len(batch) == batch_size:
            yield batch
            batch = []

    if batch:
        yield batch

    client.close()
# ─────────────────────────────────────────────────────────────────────────────