import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECT_URL = os.getenv("MONGODB_CONNECT_URL")

client = MongoClient(MONGODB_CONNECT_URL)

db = client.thestackreport

def upload_attributes_to_mongo(key, attributes, collection="datasets"):
    prev_doc = db[collection].find_one({"key": key})
    if prev_doc == None:
        db[collection].insert_one(attributes)
    else:
        doc_id = prev_doc.get("_id")
        new_attrs = {
            "$set": attributes
        }
        db[collection].update_one(
            {"_id": doc_id},
            new_attrs
        )

def server_info():
    return client.server_info()
