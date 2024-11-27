from pymongo import MongoClient
from ohlc.configs import INTERVALS
# Connect to the MongoDB server
client = MongoClient("") 
db = client["market_making"]
collection = db["ohlc"]

for interval in INTERVALS.keys():
    pipeline = [
        {
            "$match": {
                "interval": interval,
                "time": {"$type": "date"}
            }
        },
        {
            "$project": {
                "market_name": 1,
                "source": 1,
                "interval": 1,
                "formattedTime": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S",
                        "date": "$time",
                        "timezone": "UTC"
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "market_name": "$market_name",
                    "source": "$source",
                    "interval": "$interval",
                    "time": "$formattedTime"
                },
                "ids": {"$push": "$_id"},
                "count": {"$sum": 1}
            }
        },
        {
            "$match": {
                "count": {"$gt": 1}
            }
        }
    ]

    print("Finding duplicates...")
    duplicates = list(collection.aggregate(pipeline))

    # Step 2: Delete duplicates
    ids_to_delete = []
    for group in duplicates:
        ids = group["ids"]
        ids_to_delete.extend(ids[1:])  # Keep the first document, delete the rest

    if ids_to_delete:
        print(f"Deleting {len(ids_to_delete)} duplicate documents...")
        delete_result = collection.delete_many({"_id": {"$in": ids_to_delete}})
        print(f"{delete_result.deleted_count} documents deleted.")
    else:
        print("No duplicates found.")

    # Step 3: Perform the update with error handling
    print("Updating documents...")
    cursor = collection.find({"interval": "1H", "time": {"$type": "date"}})

    updated_count = 0
    skipped_count = 0

    for doc in cursor:
        formatted_time = doc["time"].strftime("%Y-%m-%dT%H:%M:%S")  # Format the time
        try:
            # Attempt to update the document
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"time": formatted_time}}
            )
            updated_count += 1
        except errors.DuplicateKeyError:
            # Handle duplicates by deleting the conflicting document
            print(f"Duplicate detected for document {_id}, deleting it.")
            collection.delete_one({"_id": doc["_id"]})
            skipped_count += 1

    print(f"Update completed: {updated_count} documents updated, {skipped_count} skipped.")
# Close the connection
client.close()