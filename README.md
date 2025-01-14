# DataEng 2024 Template Repository

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students: **Bachir GUEDDOUDA, Zhong Qing SIA, Yikang SU**

### Abstract

This project focuses on analyzing and enriching hiking trail data in the Lyon area. It implements a data pipeline using Apache Airflow to collect, process, and analyze walking trail information from OpenStreetMap (OSM). The pipeline enriches the trail data with elevation profiles, distance calculations, and difficulty assessments to provide comprehensive information for hikers.

## Datasets Description 

The project utilizes the following data sources:

1. **OpenStreetMap (OSM) Data**
   - Walking trails and paths data from the Lyon area
   - Includes information about trail segments, surface types, and accessibility
   - Data is extracted using OSM's Overpass API

2. **Elevation Data**
   - Elevation information obtained from Open-Meteo's Elevation API
   - Used to calculate elevation profiles for trails
   - Limited to 10,000 API calls per day with max 100 coordinates per call

## Queries and Features

The project implements several data processing features:

1. **Distance Calculation**
   - Computes the distance of each trail segment
   - Calculates total trail distance in kilometers

2. **Elevation Profile**
   - Determines total ascending and descending elevations
   - Processes elevation data for trail segments
   - Optimized to handle API call limitations

3. **Trail Difficulty Assessment**
   - Classifies trails as Easy, Moderate, or Difficult
   - Based on:
     - Trail length (km)
     - Total elevation gain (m)
   - Uses a scoring system combining distance and elevation metrics

## Requirements

To run this project, you need:

1. **Docker and Docker Compose**
   - For running Apache Airflow and MongoDB services

2. **Python Dependencies**
   - Apache Airflow
   - MongoDB client
   - Geospatial processing libraries (rasterio)

3. **API Access**
   - OpenStreetMap Overpass API (public access)


## How to run the project

`cd airflow`
`docker compose up -d`

When the container is running, you can access the Airflow UI at `http://localhost:8080`.

The main DAG is `walking_trail_dag.py`, you can run it by clicking on the DAG and then clicking on the `Trigger DAG` button.

Normally the DAG will finish all the tasks and succeed. The task `process_elevations` will take a while to finish, as it has to process all the coordinates and retrieve the elevation data from the corresponding TIFF files.

To see the results connect to MongoDB using Studio 3T client, connect with the configuration: 

- Server: `localhost`
- Username: `airflow`
- Password: `airflow`
- Authentication DB: `admin`

The collection is `walking_trails`.

## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf

