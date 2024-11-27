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

def _insert_geojson_to_mongodb(**context):
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
    collection = db['geojson']
    
    # Insert the GeoJSON data
    collection.insert_one(geojson_data)
    print(f"Inserted GeoJSON data from {latest_geojson} into MongoDB")
    client.close()

insert_geojson_to_mongodb = PythonOperator(
    task_id='insert_geojson_to_mongodb',
    python_callable=_insert_geojson_to_mongodb,
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

end = DummyOperator(
    task_id='end',
    dag=walking_trail_dag,
    trigger_rule='none_failed'
)

# Update the DAG structure
check_overpass_availability >> fetch_osm_raw >> transform_to_geojson >> insert_geojson_to_mongodb >> end