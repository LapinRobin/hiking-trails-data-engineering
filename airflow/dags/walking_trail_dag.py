import airflow
import datetime
import urllib.request as request
import requests
import json
import subprocess
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator



default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

walking_trail_dag = DAG(
    dag_id='walking_trail_dag',
    default_args=default_args_dict,
    catchup=False,
)


def _check_overpass_availability():
    """Check if the Overpass API is available."""
    import urllib.parse
    import urllib.error
    try:
        # First try with a simple query
        query = "way[highway=path][tourism=information];out body;"
        encoded_query = urllib.parse.quote(query)
        url = f"https://overpass-api.de/api/interpreter?data={encoded_query}"
        response = request.urlopen(url)
        if response.getcode() == 200:
            print("Overpass is available")
            return True
        else:
            print(f"Overpass returned status code: {response.getcode()}")
            return False
    except urllib.error.URLError as e:
        print(f"Failed to connect to Overpass API: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error while checking Overpass API: {e}")
        return False
    
check_overpass_availability = PythonOperator(
    task_id='check_overpass_availability',
    dag=walking_trail_dag,
    python_callable=_check_overpass_availability,
)

def _fetch_osm_raw():
    """Fetch raw OSM data using the Overpass API."""
    import os
    
    # Use a more explicit path
    base_path = "/opt/airflow/dags/data"
    os.makedirs(f"{base_path}/osm_xml/", exist_ok=True)
    os.makedirs(f"{base_path}/geojson/", exist_ok=True)
    
    lat_start = 45.740545
    lat_end = 45.766177
    lon_start = 4.829162
    lon_end = 4.893063
    step = 0.02

    overpass_url = "https://overpass-api.de/api/interpreter"
    query = """
    [out:xml][timeout:25];
    (
      way
        ["highway"~"^(path|track|footway|steps|bridleway)$"]
        ({lat_min},{lon_min},{lat_max},{lon_max});
    );
    out body;
    >;
    out skel qt;
    """.format(
        lat_min=lat_start,
        lon_min=lon_start,
        lat_max=lat_end,
        lon_max=lon_end
    )
    
    response = requests.post(overpass_url, data={"data": query})
    if response.status_code == 200:
        # Save raw OSM XML data
        osm_filename = f"{base_path}/osm_xml/osm_data_{lat_start}_{lon_start}_{lat_end}_{lon_end}.xml"
        with open(osm_filename, "w", encoding='utf-8') as f:
            f.write(response.text)
        print(f"Raw OSM data saved to {osm_filename}")
        return osm_filename
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def _transform_to_geojson():
    """Transform the raw OSM XML data to GeoJSON format."""
    from osm2geojson import convert_osm_to_geojson
    import glob
    import os
    base_path = "/opt/airflow/dags/data"
    # Create the directory if it doesn't exist
    os.makedirs(f"{base_path}/geojson/", exist_ok=True)
    
    # Find the most recent XML file
    xml_files = glob.glob(f"{base_path}/osm_xml/*.xml")
    if not xml_files:
        raise FileNotFoundError("No OSM XML files found to convert")
    
    latest_xml = max(xml_files, key=os.path.getctime)
    
    # Read the XML file
    with open(latest_xml, 'r', encoding='utf-8') as f:
        osm_xml = f.read()
    
    # Convert to GeoJSON
    geojson = convert_osm_to_geojson(
        osm_xml,
        enhanced_properties=True,
        verbose=True
    )
    
    # Create output filename based on input filename
    base_name = os.path.splitext(os.path.basename(latest_xml))[0]
    geojson_filename = f"{base_path}/geojson/{base_name}.geojson"
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(geojson_filename), exist_ok=True)
    
    # Save the GeoJSON file
    with open(geojson_filename, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2)
    
    print(f"GeoJSON data saved to {geojson_filename}")
    return geojson_filename

def _insert_and_flatten_geojson_to_mongodb(**context):
    import glob
    import json
    import os
    import pymongo
    
    # Get the latest GeoJSON file
    base_path = "/opt/airflow/dags/data"
    geojson_files = glob.glob(f"{base_path}/geojson/*.geojson")
    if not geojson_files:
        raise FileNotFoundError("No GeoJSON files found to insert")
    
    latest_geojson = max(geojson_files, key=os.path.getctime)
    
    # Read the GeoJSON file
    with open(latest_geojson, 'r') as f:
        geojson_data = json.load(f)
    
    # Connect to MongoDB directly
    client = pymongo.MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']
    # collection = db['geojson']
    
    # # Insert the GeoJSON data
    # collection.insert_one(geojson_data)
    # print(f"Inserted GeoJSON data from {latest_geojson} into MongoDB")
    # client.close()

    geojson_collection = db['geojson']
    flattened_collection = db['flattened_geojson']

    # Clear collections before inserting
    geojson_collection.delete_many({})
    flattened_collection.delete_many({})

    # Insert raw GeoJSON data
    geojson_collection.insert_one(geojson_data)

    # Flatten features using aggregation pipeline
    pipeline = [
        {"$unwind": "$features"},
        {
            "$project": {
                "_id": "$features.id",
                "coordinates": "$features.geometry.coordinates",
                "properties": "$features.properties",
            }
        }
    ]
    flattened_docs = list(geojson_collection.aggregate(pipeline))
    if flattened_docs:
        flattened_collection.insert_many(flattened_docs)

    print("Inserted and flattened GeoJSON data in MongoDB.")
    client.close()

def _find_paths_between_points():
    import pymongo
    import math
    import heapq
    from pymongo import MongoClient

    def haversine(coord1, coord2):
        """Calculate the great-circle distance between two points."""
        R = 6371  # Earth radius in km
        lat1, lon1 = math.radians(coord1[1]), math.radians(coord1[0])
        lat2, lon2 = math.radians(coord2[1]), math.radians(coord2[0])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def build_graph(db):
        """Build a graph representation from MongoDB data."""
        graph = {}
        docs = list(db.flattened_geojson.find({}))  # Fetch all at once to reduce round-trips
        for doc in docs:
            coordinates = doc['coordinates']
            for i in range(len(coordinates) - 1):
                start = tuple(coordinates[i])
                end = tuple(coordinates[i + 1])
                distance = haversine(start, end)
                if start not in graph:
                    graph[start] = []
                if end not in graph:
                    graph[end] = []
                graph[start].append((end, distance))
                graph[end].append((start, distance))
        return graph

    def a_star_search(graph, start, goal):
        """Perform A* search to find the shortest path."""
        open_set = []
        heapq.heappush(open_set, (0, start))
        came_from = {}
        g_score = {node: float('inf') for node in graph}
        g_score[start] = 0
        f_score = {node: float('inf') for node in graph}
        f_score[start] = haversine(start, goal)

        while open_set:
            _, current = heapq.heappop(open_set)
            if current == goal:
                path = []
                while current in came_from:
                    path.append(current)
                    current = came_from[current]
                path.append(start)
                return path[::-1]

            for neighbor, cost in graph.get(current, []):
                tentative_g_score = g_score[current] + cost
                if tentative_g_score < g_score[neighbor]:
                    came_from[neighbor] = current
                    g_score[neighbor] = tentative_g_score
                    f_score[neighbor] = g_score[neighbor] + haversine(neighbor, goal)
                    heapq.heappush(open_set, (f_score[neighbor], neighbor))

        return None

    client = MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']

    graph = build_graph(db)

    # Define start and goal
    start = (4.8609411, 45.7434554)
    goal = (4.8608326, 45.7605576)

    # Find path
    path = a_star_search(graph, start, goal)
    if path:
        print("Shortest path found:")
        for coord in path:
            print(coord)

        # Insert the path into a new MongoDB collection
        path_collection = db['computed_paths']
        path_document = {
            "start": start,
            "goal": goal,
            "path": path,
            "path_length": len(path),  # Number of nodes in the path
        }
        path_collection.insert_one(path_document)
        print(f"Path inserted into MongoDB collection 'computed_paths': {path_document}")
    else:
        print("No path found.")


    client.close()

insert_and_flatten_geojson_to_mongodb = PythonOperator(
    task_id='insert_and_flatten_geojson_to_mongodb',
    python_callable=_insert_and_flatten_geojson_to_mongodb,
    dag=walking_trail_dag,
)

# Create the tasks
fetch_osm_raw = PythonOperator(
    task_id='fetch_osm_raw',
    python_callable=_fetch_osm_raw,
    dag=walking_trail_dag,
)

transform_to_geojson = PythonOperator(
    task_id='transform_to_geojson',
    python_callable=_transform_to_geojson,
    dag=walking_trail_dag,
)

find_paths_between_points = PythonOperator(
    task_id='find_paths_between_points',
    python_callable=_find_paths_between_points,
    dag=walking_trail_dag,
)

end = DummyOperator(
    task_id='end',
    dag=walking_trail_dag,
    trigger_rule='none_failed'
)

# Update the DAG structure
check_overpass_availability >> fetch_osm_raw >> transform_to_geojson >> insert_and_flatten_geojson_to_mongodb >> find_paths_between_points >> end