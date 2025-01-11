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

from pymongo import MongoClient
import os
import zipfile
import shutil
import rasterio
from pathlib import Path

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

def download_srtm_files():
    """Download SRTM zip files from CGIAR website."""
    # Create necessary directory
    base_path = "/opt/airflow/dags/data"
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
    """
    Find the appropriate TIFF file for given coordinates
    
    Args:
        lat (float): Latitude
        lon (float): Longitude
        tiff_directory (str): Directory containing SRTM TIFF files
    
    Returns:
        str: Path to the appropriate TIFF file, or None if not found
    """
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
    """
    Get elevation for a given latitude and longitude from SRTM GeoTIFF files
    
    Args:
        lat (float): Latitude
        lon (float): Longitude
        tiff_directory (str): Directory containing SRTM TIFF files
    
    Returns:
        float: Elevation value at the given coordinate
    """
    appropriate_tiff = find_appropriate_tiff(lat, lon, tiff_directory)
    
    if not appropriate_tiff:
        print(f"No suitable TIFF file found for coordinates ({lat}, {lon})")
        return None
        
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
                
                # Verify the update
                updated_doc = collection.find_one({"_id": doc["_id"]})
                if updated_doc:
                    print(f"Verification - Document now has updated_at: {updated_doc.get('updated_at')}")
                    # Check first updated coordinate
                    if updates:
                        i, j, elevation = updates[0]
                        actual_elevation = (updated_doc.get('segments', [])[i][j]
                                         .get('elevation', None) if updated_doc.get('segments') else None)
                        print(f"Verification - First coordinate elevation: {actual_elevation}")
    
    finally:
        client.close()

# Task 1: Download SRTM files
download_zip = PythonOperator(
    task_id='download_srtm_files',
    dag=elevation,
    python_callable=download_srtm_files
)

# Task 2: Unzip files
unzip_srtm_files = PythonOperator(
    task_id='unzip_srtm_files',
    dag=elevation,
    python_callable=unzip_srtm_files
)

# Task 3: Move TIF files
move_tif_files = PythonOperator(
    task_id='move_tif_files',
    dag=elevation,
    python_callable=move_tif_files
)

# Task 4: Process elevations
process_elevations = PythonOperator(
    task_id='process_elevations',
    dag=elevation,
    python_callable=process_elevations
)

end = DummyOperator(
    task_id='end',
    dag=elevation,
    trigger_rule='none_failed'
)

# Set task dependencies
download_zip >> unzip_srtm_files >> move_tif_files >> process_elevations >> end