from pymongo import MongoClient
import os
from typing import Dict, Any, List


MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "mergington")


def get_client() -> MongoClient:
    return MongoClient(MONGO_URL)


def get_db():
    client = get_client()
    return client[DB_NAME]


def get_activities_collection():
    db = get_db()
    return db.activities


def list_activities() -> List[Dict[str, Any]]:
    coll = get_activities_collection()
    return list(coll.find({}, {"_id": 0}))


def get_activity(activity_name: str) -> Dict[str, Any]:
    coll = get_activities_collection()
    return coll.find_one({"name": activity_name}, {"_id": 0})


def create_activity(doc: Dict[str, Any]):
    coll = get_activities_collection()
    coll.insert_one(doc)


def upsert_activity_by_name(name: str, doc: Dict[str, Any]):
    coll = get_activities_collection()
    coll.update_one({"name": name}, {"$set": doc}, upsert=True)


def add_participant(activity_name: str, email: str) -> None:
    coll = get_activities_collection()
    coll.update_one({"name": activity_name}, {"$addToSet": {"participants": email}})


def remove_participant(activity_name: str, email: str) -> None:
    coll = get_activities_collection()
    coll.update_one({"name": activity_name}, {"$pull": {"participants": email}})


def count_activities() -> int:
    coll = get_activities_collection()
    return coll.count_documents({})


def seed_activities(initial_activities: Dict[str, Dict]):
    """Seed DB with initial activities if collection is empty."""
    coll = get_activities_collection()
    if coll.count_documents({}) == 0:
        docs = []
        for name, data in initial_activities.items():
            doc = {"name": name}
            doc.update(data)
            docs.append(doc)
        if docs:
            coll.insert_many(docs)
