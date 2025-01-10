# Use the Apache Airflow 2.7.1 image as the base image
FROM apache/airflow:2.7.1

# Switch to root to install system dependencies
USER root

# Install any system dependencies if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gdal-bin \
        libgdal-dev \
        python3-gdal \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

    # Set GDAL environment variables
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV GDAL_VERSION=3.2.2  


# Switch back to the airflow user
USER airflow

# Install pymongo directly using pip
RUN pip install --no-cache-dir pymongo==4.6.1 
RUN pip install --no-cache-dir rasterio