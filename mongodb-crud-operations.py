from pymongo import MongoClient
from datetime import datetime
from typing import Optional

class MongoDBHandler:
    def __init__(self, uri: str, db_name: str, collection_name: str):
        self.uri = uri
        self.client = MongoClient("mongodb://localhost:27017/")
        self.collection = self.client[db_name][collection_name]

    def insert_records(self, total: int = 1_000_000, batch_size: int = 10_000) -> None:
        print(f"Starting to insert {total} records in batches of {batch_size}...")
        records = []
        for i in range(1, total + 1):
            record = {
                "record_id": i,
                "name": f"User_{i}",
                "email": f"user_{i}@example.com",
                "created_at": datetime.utcnow()
            }
            records.append(record)
            if len(records) == batch_size:
                self.collection.insert_many(records)
                records = []
                print(f"Inserted {i} records...")
        if records:
            self.collection.insert_many(records)
        print(f"Finished inserting {total} records!")

    def delete_latest_10_records(self) -> None:
        print("Deleting the latest 10 records...")
        latest_records = self.collection.find().sort("created_at", -1).limit(10)
        latest_ids = [record["_id"] for record in latest_records]
        result = self.collection.delete_many({"_id": {"$in": latest_ids}})
        print(f"Deleted {result.deleted_count} records.")

    def fetch_record(self, record_id: int) -> Optional[dict]:
        print(f"Fetching record with record_id: {record_id}")
        record = self.collection.find_one({"record_id": record_id})
        if record:
            print(f"Record found: {record}")
        else:
            print("Record not found.")
        return record

    def update_record(self, record_id: int, new_name: str) -> None:
        print(f"Updating record with record_id: {record_id}")
        result = self.collection.update_one(
            {"record_id": record_id},
            {"$set": {"name": new_name}}
        )
        if result.modified_count > 0:
            print(f"Record {record_id} updated successfully.")
        else:
            print("Record not found or not updated.")

    def count_records(self) -> int:
        count = self.collection.count_documents({})
        print(f"Total records in the collection: {count}")
        return count

    def close(self) -> None:
        self.client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    try:
        handler = MongoDBHandler("mongodb://localhost:27017/", "test_db", "test_collection")
        handler.insert_records()
        handler.delete_latest_10_records()
        handler.fetch_record(500)
        handler.update_record(500, "Updated_User_500")
        handler.count_records()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        handler.close()
