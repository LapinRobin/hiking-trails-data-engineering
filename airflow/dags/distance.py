import airflow
import datetime
import math
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

distance = DAG(
    dag_id='distance',
    default_args=default_args_dict,
    catchup=False,
)

def calculate_distance(coord1, coord2):
    """
    Calculate distance between two coordinates using the Equirectangular approximation.
    This is suitable for small distances and offers good performance.
    Distance unit is in kilometers.
    
    Reference: https://www.movable-type.co.uk/scripts/latlong.html
    """
    import math
    
    # Earth's mean radius in kilometers
    R = 6371.0
    
    # Convert to radians
    lat1 = math.radians(coord1['lat'])
    lon1 = math.radians(coord1['lon'])
    lat2 = math.radians(coord2['lat'])
    lon2 = math.radians(coord2['lon'])
    
    # Differences in coordinates
    delta_lon = lon2 - lon1
    delta_lat = lat2 - lat1
    
    # Equirectangular approximation
    # x = Δλ * cos(φm)    where φm is average latitude
    # y = Δφ
    # d = R * √(x² + y²)
    x = delta_lon * math.cos((lat1 + lat2) / 2)
    y = delta_lat
    
    distance = R * math.sqrt(x*x + y*y)
    return distance

def process_distances(**context):
    """Process distances for all segments in MongoDB documents."""
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
            segment_distances = []
            total_distance = 0
            
            # Process each segment in segments array
            if 'segments' in doc:
                for segment in doc['segments']:
                    segment_distance = 0
                    
                    # Calculate distance between consecutive points in the segment
                    for j in range(len(segment) - 1):
                        coord1 = segment[j]
                        coord2 = segment[j + 1]
                        distance = calculate_distance(coord1, coord2)
                        segment_distance += distance
                    
                    segment_distances.append(segment_distance)
                    total_distance += segment_distance
                    modified = True
            
            if modified:
                # Update document with segment distances and total distance
                result = collection.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "total_distance": total_distance,
                            "segment_distances": segment_distances
                        }
                    }
                )
                print(f"Updated document {doc['_id']}: {result.modified_count} modified")
                
    finally:
        client.close()

# Task: Process distances
process_distances = PythonOperator(
    task_id='process_distances',
    dag=distance,
    python_callable=process_distances
)

end = DummyOperator(
    task_id='end',
    dag=distance,
    trigger_rule='none_failed'
)

# Set task dependencies
process_distances >> end
