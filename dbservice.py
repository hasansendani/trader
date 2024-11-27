from motor.motor_asyncio import AsyncIOMotorClient
from redis import Redis
from decouple import config

redis_client = Redis(str(config('REDIS_HOST')), port=6379, db=0)
ttl = int(config('REDIS_TTL'))


async def write(document):
    client = get_client()
    db = client.market_making
    collection = db.last_trades

    unifier = document['unifier']
    if not redis_client.exists(unifier):
        try:
            await collection.insert_one(document)
            redis_client.setex(unifier, ttl, 1)
        except Exception as e:
            print(e)
        finally:
            client.close()
    else:
        raise ValueError()


def get_client() -> AsyncIOMotorClient:
    client = AsyncIOMotorClient("mongodb://root:strong_password_haha@168.119.187.186:27018")
    return client
