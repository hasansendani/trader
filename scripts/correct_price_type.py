from pymongo import MongoClient, UpdateMany
from datetime import datetime, timedelta


MONGO_URI = "mongodb://localhost:27018"
DATABASE_NAME = "market_making"
COLLECTION_NAME = "last_trades"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

query = {"source": "bitpin"}
projection = {"_id": 1, "price": 1}

documents = collection.find(query, projection)
bulk_update = []

for i, doc in enumerate(documents):
    original_price = doc.get("price")
    if isinstance(original_price, str) and not doc.get("price_update_type_flag"):
        bulk_update.append(
            UpdateMany({"_id": doc["_id"]}, {"$set": {
                "price": float(original_price),
                "price_update_type_flag": True
                }})
        )

    if i % 1000 == 0:
        collection.bulk_write(bulk_update, ordered=False)
        bulk_update.clear()
        print(f"{i} documents processed...")

print("Time fields updated successfully.")

client.close()
