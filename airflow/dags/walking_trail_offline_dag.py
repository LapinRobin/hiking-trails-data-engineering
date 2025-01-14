import airflow
import datetime
import json
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import rasterio
from pathlib import Path
import shutil
from pymongo import MongoClient
from osm2geojson import convert_osm_to_geojson

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

walking_trail_offline_dag = DAG(
    dag_id='walking_trail_offline_dag',
    default_args=default_args_dict,
    catchup=False,
)

def _transform_to_geojson():
    """Transform the local OSM XML data to GeoJSON format."""
    base_path = "/opt/airflow/dags/data"
    xml_path = "/opt/airflow/dags/offline/xml/osm_data.xml"
    
    # Create the directory if it doesn't exist
    os.makedirs(f"{base_path}/geojson/", exist_ok=True)
    
    # Read the XML file
    with open(xml_path, 'r', encoding='utf-8') as f:
        osm_xml = f.read()
    
    # Convert to GeoJSON
    geojson = convert_osm_to_geojson(osm_xml)
    
    # Create output filename
    geojson_filename = f"{base_path}/geojson/osm_data.geojson"
    
    # Save the GeoJSON file
    with open(geojson_filename, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2)
    
    print(f"GeoJSON data saved to {geojson_filename}")
    return geojson_filename

def _insert_geojson_to_mongodb(**context):
    """Insert GeoJSON features as separate documents with flattened coordinates."""
    import json
    from datetime import datetime
    
    # Get the GeoJSON file
    geojson_filename = f"/opt/airflow/dags/data/geojson/osm_data.geojson"
    
    # Read the GeoJSON file
    with open(geojson_filename, 'r') as f:
        geojson_data = json.load(f)
    
    # Connect to MongoDB
    client = MongoClient("mongodb://airflow:airflow@mongo:27017/")
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
                'relation_id': feature['id'].split('/')[-1],
                'properties': feature['properties'],
                'type': feature['geometry']['type'],
                'updated_at': datetime.utcnow()
            }
            
            # Process coordinates based on geometry type
            if feature['geometry']['type'] == 'MultiLineString':
                trail['segments'] = [
                    [{'lon': coord[0], 'lat': coord[1]} for coord in segment]
                    for segment in feature['geometry']['coordinates']
                ]
            elif feature['geometry']['type'] == 'LineString':
                trail['segments'] = [
                    [{'lon': coord[0], 'lat': coord[1]}
                     for coord in feature['geometry']['coordinates']]
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
            row, col = dataset.index(lon, lat)
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
    tiff_directory = "/opt/airflow/dags/offline/tif"
    
    try:
        documents = list(collection.find({}))
        print(f"Found {len(documents)} documents to process")
        
        for doc in documents:
            modified = False
            updates = []
            
            if 'segments' in doc:
                for i, segment in enumerate(doc['segments']):
                    for j, coord in enumerate(segment):
                        if 'elevation' in coord:
                            continue
                            
                        lat = coord['lat']
                        lon = coord['lon']
                        elevation = get_elevation(lat, lon, tiff_directory)
                        
                        if elevation is not None:
                            updates.append((i, j, float(elevation)))
                            modified = True
                            print(f"Found elevation {elevation}m for coord ({lat}, {lon})")
            
            if modified and updates:
                update_dict = {
                    f"segments.{i}.{j}.elevation": elevation
                    for i, j, elevation in updates
                }
                update_dict["updated_at"] = datetime.datetime.utcnow()
                
                result = collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": update_dict}
                )
                
                print(f"Document {doc['_id']}: Updated {len(updates)} coordinates. "
                      f"MongoDB reports {result.modified_count} documents modified")
                
    finally:
        client.close()

def calculate_elevation_profile(**context):
    """Calculate total ascending and descending elevations for all trails."""
    client = MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']
    collection = db['walking_trails']
    
    try:
        documents = list(collection.find({}))
        print(f"Found {len(documents)} documents to process")
        
        for doc in documents:
            modified = False
            total_ascending = 0.0
            total_descending = 0.0
            
            if 'segments' in doc:
                for segment in doc['segments']:
                    for j in range(len(segment) - 1):
                        point1 = segment[j]
                        point2 = segment[j + 1]
                        
                        if 'elevation' not in point1 or 'elevation' not in point2:
                            continue
                        
                        elevation_diff = float(point2['elevation']) - float(point1['elevation'])
                        
                        if elevation_diff > 0:
                            total_ascending += elevation_diff
                        else:
                            total_descending += abs(elevation_diff)
                        
                        modified = True
            
            if modified:
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
    """Calculate distance between two coordinates using the Equirectangular approximation."""
    import math
    
    R = 6371.0
    
    lat1 = math.radians(coord1['lat'])
    lon1 = math.radians(coord1['lon'])
    lat2 = math.radians(coord2['lat'])
    lon2 = math.radians(coord2['lon'])
    
    delta_lon = lon2 - lon1
    delta_lat = lat2 - lat1
    
    x = delta_lon * math.cos((lat1 + lat2) / 2)
    y = delta_lat
    
    distance = R * math.sqrt(x*x + y*y)
    return distance

def process_distances(**context):
    """Process distances for all segments in MongoDB documents."""
    client = MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']
    collection = db['walking_trails']
    
    try:
        documents = list(collection.find({}))
        print(f"Found {len(documents)} documents to process")
        
        for doc in documents:
            modified = False
            segment_distances = []
            total_distance = 0
            
            if 'segments' in doc:
                for segment in doc['segments']:
                    segment_distance = 0
                    
                    for j in range(len(segment) - 1):
                        coord1 = segment[j]
                        coord2 = segment[j + 1]
                        distance = calculate_distance(coord1, coord2)
                        segment_distance += distance
                    
                    segment_distances.append(segment_distance)
                    total_distance += segment_distance
                    modified = True
            
            if modified:
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

def calculate_trail_difficulty(length_km, elevation_gain_m):
    """Calculate trail difficulty based on length and elevation gain."""
    if length_km <= 5:
        length_score = 1
    elif 5 < length_km <= 15:
        length_score = 2
    else:
        length_score = 3

    if elevation_gain_m <= 250:
        elevation_score = 1
    elif 250 < elevation_gain_m <= 750:
        elevation_score = 2
    else:
        elevation_score = 3

    difficulty_score = length_score + elevation_score

    if difficulty_score <= 2:
        return "Easy"
    elif 3 <= difficulty_score <= 4:
        return "Moderate"
    else:
        return "Difficult"

def process_trail_difficulties(**context):
    """Process and update difficulty levels for all trails in MongoDB."""
    client = MongoClient("mongodb://airflow:airflow@mongo:27017/")
    db = client['airflow']
    collection = db['walking_trails']
    
    try:
        documents = list(collection.find({}))
        print(f"Found {len(documents)} documents to process")
        
        for doc in documents:
            if 'total_distance' not in doc or 'elevation_profile' not in doc:
                print(f"Skipping document {doc['_id']}: Missing required fields")
                continue
            
            length_km = float(doc['total_distance'])
            elevation_gain_m = float(doc['elevation_profile']['total_ascending'])
            
            difficulty_level = calculate_trail_difficulty(length_km, elevation_gain_m)
            
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

# Create tasks
transform_to_geojson = PythonOperator(
    task_id='transform_to_geojson',
    python_callable=_transform_to_geojson,
    dag=walking_trail_offline_dag,
)

insert_geojson_to_mongodb = PythonOperator(
    task_id='insert_geojson_to_mongodb',
    python_callable=_insert_geojson_to_mongodb,
    dag=walking_trail_offline_dag,
)

process_elevations_task = PythonOperator(
    task_id='process_elevations',
    python_callable=process_elevations,
    dag=walking_trail_offline_dag,
)

calculate_elevation_profile_task = PythonOperator(
    task_id='calculate_elevation_profile',
    python_callable=calculate_elevation_profile,
    dag=walking_trail_offline_dag,
)

process_distances_task = PythonOperator(
    task_id='process_distances',
    python_callable=process_distances,
    dag=walking_trail_offline_dag,
)

process_difficulties_task = PythonOperator(
    task_id='process_trail_difficulties',
    python_callable=process_trail_difficulties,
    dag=walking_trail_offline_dag,
)

end = DummyOperator(
    task_id='end',
    dag=walking_trail_offline_dag,
    trigger_rule='none_failed'
)

# Define task dependencies
transform_to_geojson >> insert_geojson_to_mongodb >> process_distances_task >> process_difficulties_task >> end
insert_geojson_to_mongodb >> process_elevations_task >> calculate_elevation_profile_task >> process_difficulties_task >> end 