import pymongo

db_client = pymongo.MongoClient("mongodb://localhost:27017/")
# print all databases
print(db_client.list_database_names())

db = db_client["metrics"]
# check contents of the database
for collection in db.list_collection_names():
    print(f"Collection: {collection}")
    for doc in db[collection].find():
        print(doc)
    print("\n")


