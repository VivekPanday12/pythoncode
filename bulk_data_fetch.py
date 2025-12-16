"""
*Script is created to fetch input data in a bulk
*@original_reference : Data Deletion Script (CSV Based)
*@purpose : Fetch the records in chunks for the provided org_id and doc_type from the respective data collection
*@author : Pratiksha Gaikwad
*@created_on : 17 November 2025
"""

import json
import uuid
import config
import boto3
from datetime import datetime, timezone
from pymongo import MongoClient


class Fetch_Input_Data:
    def __init__(self):
        self.__db_instance()

    def __db_instance(self):
        """Initialize MongoDB database instance"""
        try:
            self.mongo = MongoClient(config.mongo_uri)
            self.__db = self.mongo[config.db]
            return [True, self.__db]
        except Exception as mongo_err:
            return [False, mongo_err, "E2001"]

    def Fetch_Data(self):
        """Fetch documents based on ORG_ID, DOC_TYPE, with pagination (skip & limit)"""
        org_id = config.org_id
        doc_type = config.doc_type
        collection_name = config.collection_name
        fetch_limit = getattr(config, "fetch_limit", 0)      # Records per batch
        fetch_offset = getattr(config, "fetch_offset", 0)    # Skip count (for pagination)

        if org_id and doc_type and collection_name:
            try:
                if collection_name not in self.__db.list_collection_names():
                    return [False, f"ERROR : Collection '{collection_name}' does not exist!"]

                query = {"ORG_ID": org_id, "DOC_TYPE": doc_type}
                projection = {"_id": 0}

                # Applying pagination: skip() + limit()
                cursor = self.__db[collection_name].find(query, projection)

                if isinstance(fetch_limit, int) and fetch_limit > 0:
                    cursor = cursor.skip(fetch_offset).limit(fetch_limit)

                docs = list(cursor)
                total_count = len(docs)

                if total_count > 0:
                    print(f"Records fetched in this batch: {total_count}")
                else:
                    print("No more records available to fetch")

                return [True, total_count, docs]

            except Exception as e:
                return [False, f"ERROR : An issue occurred during data fetch - {str(e)}"]

        else:
            return [False, "ERROR : Ensure org_id, doc_type & collection_name are passed correctly!"]

def send_data_to_sqs(queue_url, message_body: dict):
    """Send Python dictionary to SQS correctly"""
    sqs = boto3.client('sqs', region_name='ap-south-1')
    try:
        # Convert dict to JSON string
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body, ensure_ascii=False, default=json_serializer),
            MessageGroupId='default-group',
            MessageDeduplicationId=str(uuid.uuid4())
        )
        return [True, f"Message sent! MessageId: {response.get('MessageId', 'N/A')}"]
    except Exception as e:
        return [False, f"Error sending message: {str(e)}"]

def json_serializer(obj):
    """Custom JSON serializer to handle datetime & ObjectId"""
    from bson import ObjectId
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


if __name__ == "__main__":
    fetch_data = Fetch_Input_Data()
    FetchStatus = fetch_data.Fetch_Data()

    # Check if fetching was successful and if there's data
    if FetchStatus[0] and FetchStatus[1] > 0:
        docs_to_send = FetchStatus[2]  # List of fetched documents

        sqs_queue_url = config.sqs_queue_url  # Define in config

        print(f"\nSending {len(docs_to_send)} records to SQS...")
        for idx, doc in enumerate(docs_to_send, start=1):
            resp = send_data_to_sqs(sqs_queue_url, doc)
    else:
        print("No data available to send to SQS.")
