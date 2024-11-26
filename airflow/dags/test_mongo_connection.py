import pymongo
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def test_mongo_connection():
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://airflow:airflow@mongo:27017/")
    
    try:
        # Test the connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB")
        
        # Get database and create a test collection
        db = client['airflow']
        collection = db['test_collection']
        
        # Insert a test document
        test_doc = {"test": "data", "timestamp": datetime.now()}
        result = collection.insert_one(test_doc)
        print(f"Inserted test document with ID: {result.inserted_id}")
        
        # Query the document back
        found_doc = collection.find_one({"_id": result.inserted_id})
        print(f"Retrieved document: {found_doc}")
        
        # Clean up
        collection.delete_one({"_id": result.inserted_id})
        print("Test document cleaned up")
        
    except Exception as e:
        print(f"Error testing MongoDB connection: {e}")
    finally:
        client.close()
        print("MongoDB connection closed")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_mongo_connection',
    default_args=default_args,
    description='Test MongoDB Connection',
    schedule_interval=timedelta(days=1),
)

test_task = PythonOperator(
    task_id='test_mongo_connection',
    python_callable=test_mongo_connection,
    dag=dag,
) 