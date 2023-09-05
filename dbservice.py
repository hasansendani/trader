from motor.motor_asyncio import AsyncIOMotorClient
from redis import Redis
from decouple import config

client: AsyncIOMotorClient = AsyncIOMotorClient(config("MONGO_HOST"))
db = client.market_making
collection = db.last_trades
redis_client = Redis(config('REDIS_HOST'), port=6379, db=0)
ttl = int(config('REDIS_TTL'))

async def write(document):
    unifier = document['unifier']
    if not redis_client.exists(unifier):
        try:
            await collection.insert_one(document)
            redis_client.setex(unifier, ttl, 1)
        except Exception as e:
            print(e)
    else: 
        raise ValueError()
