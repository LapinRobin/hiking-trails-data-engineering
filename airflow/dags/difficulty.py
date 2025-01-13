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

difficulty = DAG(
    dag_id='difficulty',
    default_args=default_args_dict,
    catchup=False,
)

def calculate_trail_difficulty(length_km, elevation_gain_m):
    """
    Calculate trail difficulty based on length and elevation gain.
    
    Args:
        length_km (float): Trail length in kilometers
        elevation_gain_m (float): Total elevation gain in meters
    
    Returns:
        str: Difficulty level ('Easy', 'Moderate', or 'Difficult')
    """
    # Length score
    if length_km <= 5:
        length_score = 1
    elif 5 < length_km <= 15:
        length_score = 2
    else:
        length_score = 3

    # Elevation score
    if elevation_gain_m <= 250:
        elevation_score = 1
    elif 250 < elevation_gain_m <= 750:
        elevation_score = 2
    else:
        elevation_score = 3

    # Difficulty score
    difficulty_score = length_score + elevation_score

    # Determine difficulty level
    if difficulty_score <= 2:
        return "Easy"
    elif 3 <= difficulty_score <= 4:
        return "Moderate"
    else:
        return "Difficult"

def process_trail_difficulties(**context):
    """Process and update difficulty levels for all trails in MongoDB."""
    # MongoDB setup
    client = MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']
    collection = db['walking_trails']
    
    try:
        # Fetch all documents from MongoDB
        documents = list(collection.find({}))
        print(f"Found {len(documents)} documents to process")
        
        for doc in documents:
            # Skip if required fields are missing
            if 'total_distance' not in doc or 'elevation_profile' not in doc:
                print(f"Skipping document {doc['_id']}: Missing required fields")
                continue
            
            # Get total distance (in km) and elevation gain
            length_km = float(doc['total_distance'])
            elevation_gain_m = float(doc['elevation_profile']['total_ascending'])
            
            # Calculate difficulty
            difficulty_level = calculate_trail_difficulty(length_km, elevation_gain_m)
            
            # Update document with difficulty level
            result = collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "difficulty": {
                            "level": difficulty_level,
                            "metrics": {
                                "length_km": length_km,
                                "elevation_gain_m": elevation_gain_m
                            }
                        },
                        "updated_at": datetime.datetime.utcnow()
                    }
                }
            )
            print(f"Updated document {doc['_id']}: "
                  f"Length: {length_km:.1f}km, "
                  f"Elevation gain: {elevation_gain_m:.1f}m, "
                  f"Difficulty: {difficulty_level}. "
                  f"MongoDB reports {result.modified_count} documents modified")
                
    finally:
        client.close()

# Create difficulty calculation task
process_difficulties_task = PythonOperator(
    task_id='process_difficulties',
    python_callable=process_trail_difficulties,
    dag=difficulty,
)

end = DummyOperator(
    task_id='end',
    dag=difficulty,
    trigger_rule='none_failed'
)

# Set task dependencies
process_difficulties_task >> end 