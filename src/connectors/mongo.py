import atexit
import os
import threading
from typing import Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

load_dotenv()

_client_lock = threading.Lock()
_mongo_client: Optional[MongoClient] = None
_mongo_client_url: Optional[str] = None


def _build_default_mongo_url() -> str:
    """Build the authenticated default MongoDB URI from environment variables."""
    user = os.getenv("MONGO_INITDB_ROOT_USERNAME") or os.getenv("MONGO_USER") or "comex_mongo_user"
    password = os.getenv("MONGO_INITDB_ROOT_PASSWORD") or os.getenv("MONGO_PASSWORD") or "comex_mongo_pass"
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27018")
    db_name = os.getenv("MONGO_DB", "comex_raw_lake")
    return f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db_name}?authSource=admin"


def get_mongo_client() -> MongoClient:
    """Return a thread-safe singleton/pooled MongoClient instance."""
    global _mongo_client, _mongo_client_url

    mongo_url = os.getenv("MONGO_URL") or _build_default_mongo_url()
    timeout_ms = int(os.getenv("MONGO_TIMEOUT_MS", "2000"))

    with _client_lock:
        if _mongo_client is not None and _mongo_client_url == mongo_url:
            return _mongo_client

        if _mongo_client is not None:
            try:
                _mongo_client.close()
            except Exception:
                pass
            _mongo_client = None
            _mongo_client_url = None

        _mongo_client = MongoClient(
            mongo_url,
            maxPoolSize=50,
            minPoolSize=1,
            serverSelectionTimeoutMS=timeout_ms,
            connectTimeoutMS=timeout_ms,
        )
        _mongo_client_url = mongo_url
        return _mongo_client


def close_mongo_client() -> None:
    """Close and reset the pooled singleton MongoClient."""
    global _mongo_client, _mongo_client_url

    with _client_lock:
        if _mongo_client is not None:
            try:
                _mongo_client.close()
            finally:
                _mongo_client = None
                _mongo_client_url = None


atexit.register(close_mongo_client)


def get_raw_collection(collection_name: str = "raw_documents") -> Collection:
    """Return a MongoDB collection from the configured raw landing database."""
    client = get_mongo_client()
    db_name = os.getenv("MONGO_DB", "comex_raw_lake")
    return client[db_name][collection_name]
