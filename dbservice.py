import motor.motor_asyncio
import time
import datetime

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://root:example@0.0.0.0:27017')
db = client.test
collection = db.get_collection('test')


async def unifier(documents):
    docs = []
    for document in documents:
        doc = {}
        doc['pair'], doc['price'], doc['amount'], doc['total'] =\
        document['symbol'], float(document['price']), float(document['quantity']), float(document['sum'])
        doc['side'] = 'buy' if document['isBuyOrder'] == True else 'sell'
        doc['time'] = int(time.mktime(datetime.datetime.strptime(document['timestamp'], "%Y-%m-%dT%H:%M:%SZ").timetuple()))
        doc['unifier'] = hex(hash((doc['time'] + hash(doc['pair']) + hash(doc['amount']) + hash(doc['price']))))
        docs.append(doc)

    return docs


async def write(documents):
    docs = await unifier(documents)
    for document in docs:
        try:
            await collection.insert_one(document)
        except Exception as e:
            print(e)


