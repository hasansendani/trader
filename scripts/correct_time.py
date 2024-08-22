from pymongo import MongoClient
from datetime import datetime, timedelta


MONGO_URI = "mongodb://localhost:27018"
DATABASE_NAME = "market_making"
COLLECTION_NAME = "last_trades"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

query = {"source": {"$in": ["wallex", "ramzinex"]}}

documents = collection.find(query)

for i, doc in enumerate(documents):
    original_time_str = doc.get("time")

    if original_time_str:
        original_time = datetime.strptime(original_time_str, "%Y-%m-%dT%H:%M:%S")

        updated_time = original_time + timedelta(hours=3, minutes=30)

        updated_time_str = updated_time.strftime("%Y-%m-%dT%H:%M:%S")

        collection.update_one({"_id": doc["_id"]}, {"$set": {
            "time": updated_time_str,
            "changed_timezone": True
            }})

    if i % 1000 == 0:
        print(f"{i} documents updated")
print("Time fields updated successfully.")

client.close()
