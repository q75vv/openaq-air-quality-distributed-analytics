#!/bin/bash

#Create directory structure
hdfs dfs -mkdir -p /air_quality/locations
hdfs dfs -mkdir -p /air_quality/sensors
hdfs dfs -mkdir -p /air_quality/measurements

#Upload JSON files
hdfs dfs -put -f data_clean/locations.json /air_quality/locations/
hdfs dfs -put -f data_clean/sensors.json /air_quality/sensors/
hdfs dfs -put -f data_clean/measurements.json /air_quality/measurements/

#Ensure files moved.
hdfs dfs -ls /air_quality
hdfs dfs -ls /air_quality/measurements

echo "JSON files imported into Hadoop!"
