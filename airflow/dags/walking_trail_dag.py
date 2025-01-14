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
import rasterio
from pathlib import Path
import zipfile
import shutil
from pymongo import MongoClient



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
    offline_path = "/opt/airflow/dags/offline_data" 
    os.makedirs(f"{base_path}/osm_xml/", exist_ok=True)
    os.makedirs(f"{base_path}/geojson/", exist_ok=True)
    
    """ lat_start = 45.740545
    lat_end = 45.966177
    lon_start = 4.829162
    lon_end = 4.893063 """


    lat_start = 45.740545
    lat_end = 45.766177
    lon_start = 4.829162
    lon_end = 4.893063

    step = 0.02

    overpass_url = "https://overpass-api.de/api/interpreter"
    query = """
    [out:xml][timeout:25];
    (
      relation["type"~"route|superroute"]["route"~"foot|walking|hiking"]
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

    try:
        # Attempt to fetch from Overpass API
        response = requests.post(overpass_url, data={"data": query}, timeout=10)
        if response.status_code == 200:
            osm_filename = f"{base_path}/osm_xml/osm_data_{lat_start}_{lon_start}_{lat_end}_{lon_end}.xml"
            with open(osm_filename, "w", encoding='utf-8') as f:
                f.write(response.text)
            print(f"Raw OSM data saved to {osm_filename}")
            return osm_filename
        else:
            print(f"Overpass API returned an error: {response.status_code}")
    except (requests.ConnectionError, requests.Timeout):
        print("Internet not available, reverting to offline dataset.")
    
    # Fallback to offline dataset
    if os.path.exists(offline_path):
        print(f"Using offline dataset from {offline_path}")
        return f"{offline_path}/osm_data_45.740545_4.829162_45.766177_4.893063.xml"
    else:
        print(f"Offline dataset not found at {offline_path}")
        return None

def _transform_to_geojson():
    """Transform the raw OSM XML data to GeoJSON format."""
    from osm2geojson import convert_osm_to_geojson
    import glob
    import os
    base_path = "/opt/airflow/dags/data"
    # Create the directory if it doesn't exist
    os.makedirs(f"{base_path}/geojson/", exist_ok=True)

    offline_path = "/opt/airflow/dags/offline_data" 
    
    # Look for OSM XML files in both default and offline locations
    xml_files = glob.glob(f"{base_path}/osm_xml/*.xml") + glob.glob(f"{offline_path}/*.xml")
    if not xml_files:
        raise FileNotFoundError("No OSM XML files found in default or offline locations to convert")
    
    latest_xml = max(xml_files, key=os.path.getctime)
    
    # Read the XML file
    with open(latest_xml, 'r', encoding='utf-8') as f:
        osm_xml = f.read()
    
    # Convert to GeoJSON
    geojson = convert_osm_to_geojson(osm_xml)
    
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
    """Insert GeoJSON features as separate documents with flattened coordinates."""
    import glob
    import json
    import os
    import pymongo
    from datetime import datetime
    
    # Get the latest GeoJSON file
    base_path = "/opt/airflow/dags/data"
    offline_path = "/opt/airflow/dags/offline_data"

    # Search for GeoJSON files in both default and offline locations
    geojson_files = glob.glob(f"{base_path}/geojson/*.geojson") + glob.glob(f"{offline_path}/*.geojson")
    if not geojson_files:
        raise FileNotFoundError("No GeoJSON files found in default or offline locations to insert")

    latest_geojson = max(geojson_files, key=os.path.getctime)
    
    # Read the GeoJSON file
    with open(latest_geojson, 'r') as f:
        geojson_data = json.load(f)
    
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']
    collection = db['walking_trails']
    
    try:
        # Clear existing data
        collection.delete_many({})
        
        # Process each feature
        processed_features = []
        for feature in geojson_data['features']:
            # Extract basic information
            trail = {
                'relation_id': feature['id'].split('/')[-1],  # Extract numeric ID from "relation/123456"
                'properties': feature['properties'],
                'type': feature['geometry']['type'],
                'updated_at': datetime.utcnow()
            }
            
            # Process coordinates based on geometry type
            if feature['geometry']['type'] == 'MultiLineString':
                # Flatten MultiLineString coordinates
                trail['segments'] = [
                    [
                        {'lon': coord[0], 'lat': coord[1]} 
                        for coord in segment
                    ]
                    for segment in feature['geometry']['coordinates']
                ]
            elif feature['geometry']['type'] == 'LineString':
                # Flatten LineString coordinates
                trail['segments'] = [
                    [
                        {'lon': coord[0], 'lat': coord[1]}
                        for coord in feature['geometry']['coordinates']
                    ]
                ]
            
            processed_features.append(trail)
        
        # Insert all processed features
        if processed_features:
            collection.insert_many(processed_features)
            print(f"Inserted {len(processed_features)} walking trails into MongoDB")
        else:
            print("No features found to insert")
            
    finally:
        client.close()
    
    return len(processed_features)

# Create the MongoDB insertion task
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

def download_srtm_files():
    """Download SRTM zip files from CGIAR website."""
    import os
    import pymongo
    # Create necessary directory
    base_path = "/opt/airflow/dags/data"
    offline_path = "/opt/airflow/dags/offline_data"
    zip_dir = os.path.join(base_path, "zip")
    os.makedirs(zip_dir, exist_ok=True)
    
    # List of SRTM files to download
    srtm_files = ["36_03", "36_04", "37_03", "37_04", "38_03", "38_04"]
    base_url = "https://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF"
    
    downloaded_files = []
    for srtm_id in srtm_files:
        zip_filename = f"srtm_{srtm_id}.zip"
        zip_path = os.path.join(zip_dir, zip_filename)
        
        # Download if not already exists
        if not os.path.exists(zip_path):
            url = f"{base_url}/{zip_filename}"
            print(f"Downloading {url}")
            try:
                response = requests.get(url)
                response.raise_for_status()
                with open(zip_path, 'wb') as f:
                    f.write(response.content)
                downloaded_files.append(zip_path)
            except requests.exceptions.RequestException as e:
                print(f"Error downloading {url}: {e}")
                continue
        else:
            downloaded_files.append(zip_path)
    
    return downloaded_files

def unzip_srtm_files(**context):
    """Extract downloaded SRTM zip files."""
    import os
    # Get the list of downloaded files from the previous task
    zip_files = context['task_instance'].xcom_pull(task_ids='download_srtm_files')
    if not zip_files:
        raise ValueError("No zip files were downloaded in the previous task")
    
    base_path = "/opt/airflow/dags/data"
    extracted_paths = []
    
    for zip_path in zip_files:
        try:
            # Create temporary extraction directory
            srtm_id = os.path.basename(zip_path).replace('.zip', '').replace('srtm_', '')
            temp_extract_dir = os.path.join(base_path, "zip", f"temp_{srtm_id}")
            os.makedirs(temp_extract_dir, exist_ok=True)
            
            # Extract the zip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_dir)
            
            extracted_paths.append(temp_extract_dir)
            
        except zipfile.BadZipFile as e:
            print(f"Error extracting {zip_path}: {e}")
            continue
    
    return extracted_paths

def move_tif_files(**context):
    """Move extracted TIF files to the final destination."""
    import os
    # Get the list of extracted directories from the previous task
    extracted_dirs = context['task_instance'].xcom_pull(task_ids='unzip_srtm_files')
    if not extracted_dirs:
        raise ValueError("No directories were extracted in the previous task")
    
    base_path = "/opt/airflow/dags/data"
    tif_dir = os.path.join(base_path, "tif")
    os.makedirs(tif_dir, exist_ok=True)
    
    moved_files = []
    for extract_dir in extracted_dirs:
        try:
            # Find and move TIF files
            for root, _, files in os.walk(extract_dir):
                for file in files:
                    if file.endswith('.tif'):
                        source_path = os.path.join(root, file)
                        srtm_id = os.path.basename(extract_dir).replace('temp_', '')
                        dest_path = os.path.join(tif_dir, f"srtm_{srtm_id}.tif")
                        os.rename(source_path, dest_path)
                        moved_files.append(dest_path)
                        print(f"Moved {file} to {dest_path}")
            
            # Clean up temporary directory
            shutil.rmtree(extract_dir)
            
        except Exception as e:
            print(f"Error processing directory {extract_dir}: {e}")
            continue
    
    return moved_files

def find_appropriate_tiff(lat, lon, tiff_directory):
    """Find the appropriate TIFF file for given coordinates."""
    for tiff_file in Path(tiff_directory).glob("srtm_*.tif"):
        try:
            with rasterio.open(tiff_file) as dataset:
                bounds = dataset.bounds
                if (bounds.left <= lon <= bounds.right and 
                    bounds.bottom <= lat <= bounds.top):
                    return str(tiff_file)
        except rasterio.errors.RasterioError:
            continue
    return None

def get_elevation(lat, lon, tiff_directory):
    """Get elevation for a given latitude and longitude from SRTM GeoTIFF files."""
    appropriate_tiff = find_appropriate_tiff(lat, lon, tiff_directory)
    
    if not appropriate_tiff:
        print(f"No suitable TIFF file found for coordinates ({lat}, {lon})")
        return None
    
    print(f"Using TIFF file: {appropriate_tiff}")
        
    try:
        with rasterio.open(appropriate_tiff) as dataset:
            # Get the row, col index for the given coordinate
            row, col = dataset.index(lon, lat)
            
            # Read the elevation value at that pixel
            elevation = dataset.read(1)[row, col]
            
            return float(elevation)
            
    except rasterio.errors.RasterioError as e:
        print(f"Error reading GeoTIFF file: {e}")
        return None

def process_elevations(**context):
    """Process elevations for all coordinates in MongoDB documents."""
    import datetime
    from pymongo import MongoClient
    
    # MongoDB setup
    client = MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']
    collection = db['walking_trails']
    
    # Path to TIFF files
    tiff_directory = "/opt/airflow/dags/data/tif"
    
    try:
        # Fetch all documents from MongoDB
        documents = list(collection.find({}))
        print(f"Found {len(documents)} documents to process")
        
        for doc in documents:
            modified = False
            updates = []  # Track all updates for this document
            
            # Process each segment in segments array
            if 'segments' in doc:
                for i, segment in enumerate(doc['segments']):
                    for j, coord in enumerate(segment):
                        # Skip if elevation is already present
                        if 'elevation' in coord:
                            continue
                            
                        lat = coord['lat']
                        lon = coord['lon']
                        
                        # Get elevation for the coordinate
                        elevation = get_elevation(lat, lon, tiff_directory)
                        
                        if elevation is not None:
                            # Add update to our list
                            updates.append((i, j, float(elevation)))
                            modified = True
                            print(f"Found elevation {elevation}m for coord ({lat}, {lon})")
            
            if modified and updates:
                # Prepare bulk update
                update_dict = {
                    f"segments.{i}.{j}.elevation": elevation
                    for i, j, elevation in updates
                }
                
                # Add timestamp
                update_dict["updated_at"] = datetime.datetime.utcnow()
                
                # Perform update
                result = collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": update_dict}
                )
                
                print(f"Document {doc['_id']}: Updated {len(updates)} coordinates. "
                      f"MongoDB reports {result.modified_count} documents modified")
                
    finally:
        client.close()

# Create elevation-related tasks
download_srtm = PythonOperator(
    task_id='download_srtm_files',
    python_callable=download_srtm_files,
    dag=walking_trail_dag,
)

unzip_srtm = PythonOperator(
    task_id='unzip_srtm_files',
    python_callable=unzip_srtm_files,
    dag=walking_trail_dag,
)

move_tif = PythonOperator(
    task_id='move_tif_files',
    python_callable=move_tif_files,
    dag=walking_trail_dag,
)

process_elevations_task = PythonOperator(
    task_id='process_elevations',
    python_callable=process_elevations,
    dag=walking_trail_dag,
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
                            "segment_distances": segment_distances,
                            "updated_at": datetime.datetime.utcnow()
                        }
                    }
                )
                print(f"Updated document {doc['_id']}: {result.modified_count} modified")
                
    finally:
        client.close()

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
    
# Create distance calculation task
process_distances_task = PythonOperator(
    task_id='process_distances',
    python_callable=process_distances,
    dag=walking_trail_dag,
)

# Create elevation profile calculation task
calculate_elevation_profile_task = PythonOperator(
    task_id='calculate_elevation_profile',
    python_callable=calculate_elevation_profile,
    dag=walking_trail_dag,
)

# Create difficulty calculation task
process_difficulties_task = PythonOperator(
    task_id='process_trail_difficulties',
    python_callable=process_trail_difficulties,
    dag=walking_trail_dag,
)

# OSM data pipeline
check_overpass_availability >> fetch_osm_raw >> transform_to_geojson >> insert_geojson_to_mongodb >> process_distances_task >> process_difficulties_task >> end

# SRTM data pipeline and distance calculation
[download_srtm >> unzip_srtm >> move_tif, insert_geojson_to_mongodb] >> process_elevations_task >> calculate_elevation_profile_task >> process_difficulties_task >> end
    