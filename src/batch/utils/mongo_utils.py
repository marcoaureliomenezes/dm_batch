from pymongo import MongoClient


def populate_mongo_db(host, port, db, collection, num_rows, method_data_generator):
    cluster = MongoClient(f"mongodb://{host}:{port}")
    db = cluster[db]
    collection = db[collection]
    for i in range(num_rows):
        nubank_operations = [method_data_generator() for i in range(5)]
        collection.insert_many(nubank_operations)
    print(f"BATCH DATA ON MONGODB FINISHED!!!")