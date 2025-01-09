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
import os
import zipfile
import shutil

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

# Task 1: Download SRTM files
download_task = PythonOperator(
    task_id='download_srtm_files',
    dag=elevation,
    python_callable=download_srtm_files
)

# Task 2: Unzip files
unzip_task = PythonOperator(
    task_id='unzip_srtm_files',
    dag=elevation,
    python_callable=unzip_srtm_files
)

# Task 3: Move TIF files
move_files_task = PythonOperator(
    task_id='move_tif_files',
    dag=elevation,
    python_callable=move_tif_files
)

end = DummyOperator(
    task_id='end',
    dag=elevation,
    trigger_rule='none_failed'
)

# Set task dependencies
download_task >> unzip_task >> move_files_task >> end
