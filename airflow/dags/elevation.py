import airflow
import datetime
import urllib.request as request
import requests
import json
import subprocess
import openmeteo_requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from openmeteo_sdk.Variable import Variable
from pymongo import MongoClient


om = openmeteo_requests.Client()
params = {
    "latitude": 52.54,
    "longitude": 13.41,
    "hourly": ["temperature_2m", "precipitation", "wind_speed_10m"],
    "current": ["temperature_2m", "relative_humidity_2m"]
}


default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

elevation = DAG(
    dag_id='elevation',
    default_args=default_args_dict,
    catchup=False,
)

def fetch_start_and_goal_elevation_from_mongo():
    """Fetch start and goal coordinates from MongoDB, query the Elevation API, and update MongoDB with elevations."""
    # MongoDB setup
    client = MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']
    collection = db["computed_paths"]

    # OpenMeteo Elevation API URL
    elevation_url = "https://api.open-meteo.com/v1/elevation"

    # Fetch all documents from MongoDB
    documents = collection.find()

    for doc in documents:
        coordinates_to_update = []

        # Add start and goal coordinates to the list if they exist
        if "start" in doc:
            coordinates_to_update.append(("start", tuple(doc["start"])))
        if "goal" in doc:
            coordinates_to_update.append(("goal", tuple(doc["goal"])))

        if coordinates_to_update:
            # Split latitude and longitude for the API request
            latitudes = ",".join([str(coord[1][1]) for coord in coordinates_to_update])  # Latitude is the second value
            longitudes = ",".join([str(coord[1][0]) for coord in coordinates_to_update])  # Longitude is the first value

            # Query the Elevation API
            response = requests.get(f"{elevation_url}?latitude={latitudes}&longitude={longitudes}")

            if response.status_code == 200:
                elevation_data = response.json()["elevation"]

                # Update MongoDB with the elevation data for start and goal
                for idx, (key, coord) in enumerate(coordinates_to_update):
                    # Include the elevation in the coordinate tuple
                    updated_coord = coord + (elevation_data[idx],)  # Add elevation to the coordinate tuple
                    
                    # Check if the elevation field already exists in the document
                    elevation_field = f"{key}_elevation"
                    
                    # Query to find if the elevation already exists
                    if f"{key}_elevation" not in doc:
                        # Update MongoDB to set the coordinate with elevation only if not already set
                        collection.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {key: updated_coord, elevation_field: elevation_data[idx]}}  # Set the coordinate and elevation
                        )
                    else:
                        print(f"{elevation_field} already exists. Skipping update.")
            else:
                print(f"Error {response.status_code}: {response.text}")

    print("Start and goal elevations updated successfully!")


# Task 1: Test connectivity
test_connectivity = BashOperator(
    task_id="test_connectivity",
    dag=elevation,
    bash_command="curl -v -I https://api.open-elevation.com/api/v1/lookup"
)

# Task 2: Fetch elevation data
fetch_start_and_goal_elevation_from_mongo = PythonOperator(
    task_id='fetch_start_and_goal_elevation_from_mongo',
    dag=elevation,
    python_callable=fetch_start_and_goal_elevation_from_mongo
)

end = DummyOperator(
task_id='end',
dag=elevation,
trigger_rule='none_failed'
)

# Set task dependencies
test_connectivity >> fetch_start_and_goal_elevation_from_mongo >> end
