import os
from typing import Any, Dict, Optional
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def get_mongo_client() -> MongoClient:
    mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017/htap_raw_lake")
    return MongoClient(mongo_url, serverSelectionTimeoutMS=2000)

def get_raw_collection(collection_name: str = "raw_documents"):
    client = get_mongo_client()
    db_name = os.getenv("MONGO_DB", "htap_raw_lake")
    return client[db_name][collection_name]
