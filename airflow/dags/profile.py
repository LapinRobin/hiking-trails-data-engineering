import airflow
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from pymongo import MongoClient

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

profile = DAG(
    dag_id='profile',
    default_args=default_args_dict,
    catchup=False,
)

def calculate_elevation_profile(**context):
    """Calculate total ascending and descending elevations for all trails in MongoDB documents."""
    # MongoDB setup
    client = MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']
    collection = db['walking_trails']
    
    try:
        # Fetch all documents from MongoDB
        documents = list(collection.find({}))
        print(f"Found {len(documents)} documents to process")
        
        for doc in documents:
            modified = False
            total_ascending = 0.0
            total_descending = 0.0
            
            # Process each segment in segments array
            if 'segments' in doc:
                for segment in doc['segments']:
                    # Calculate elevation changes between consecutive points
                    for j in range(len(segment) - 1):
                        point1 = segment[j]
                        point2 = segment[j + 1]
                        
                        # Skip if either point doesn't have elevation data
                        if 'elevation' not in point1 or 'elevation' not in point2:
                            continue
                        
                        # Calculate elevation difference
                        elevation_diff = float(point2['elevation']) - float(point1['elevation'])
                        
                        # Accumulate ascending and descending values
                        if elevation_diff > 0:
                            total_ascending += elevation_diff
                        else:
                            total_descending += abs(elevation_diff)
                        
                        modified = True
            
            if modified:
                # Update document with only total ascending and descending values
                result = collection.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "elevation_profile.total_ascending": float(total_ascending),
                            "elevation_profile.total_descending": float(total_descending),
                            "updated_at": datetime.datetime.utcnow()
                        }
                    }
                )
                print(f"Updated document {doc['_id']}: "
                      f"Total ascending: {total_ascending:.1f}m, "
                      f"Total descending: {total_descending:.1f}m. "
                      f"MongoDB reports {result.modified_count} documents modified")
                
    finally:
        client.close()

# Create elevation profile calculation task
calculate_elevation_profile_task = PythonOperator(
    task_id='calculate_elevation_profile',
    python_callable=calculate_elevation_profile,
    dag=profile,
)

end = DummyOperator(
    task_id='end',
    dag=profile,
    trigger_rule='none_failed'
)

# Set task dependencies
calculate_elevation_profile_task >> end
