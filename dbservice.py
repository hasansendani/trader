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
    try:
        await collection.insert_one(document)
        redis_client.setex(unifier, ttl, 1)
    except Exception as e:
        print(e)
    finally:
        client.close()


async def write_bulk(documents):
    """Bulk insert documents with fallback to individual writes on failure"""
    if not documents:
        return

    client = get_client()
    db = client.market_making
    collection = db.last_trades

    try:
        # Try bulk insert first
        await collection.insert_many(documents)
        # Set Redis keys for all documents
        for doc in documents:
            redis_client.setex(doc['unifier'], ttl, 1)
    except Exception as e:
        print(f"Bulk insert failed, falling back to individual writes: {e}")
        # Fallback to individual writes
        for document in documents:
            try:
                await collection.insert_one(document)
                redis_client.setex(document['unifier'], ttl, 1)
            except Exception as e:
                print(f"Individual write failed for {document.get('unifier', 'unknown')}: {e}")
    finally:
        client.close()


def get_client() -> AsyncIOMotorClient:
    client = AsyncIOMotorClient("mongodb://root:strong_password_haha@168.119.187.186:27018")
    return client
