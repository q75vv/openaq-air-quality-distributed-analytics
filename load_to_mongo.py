from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os


#Connection details for MongoDB
MONGO_URI = os.getenv("MONGO_URI") #mongodb://localhost:27017"
MONGO_DB = os.getenv("MONGO_DB") #"air_quality"

#HDFS paths for JSON files 
HDFS_LOCATIONS = os.getenv("HDFS_LOCATIONS") #"hdfs:///air_quality/locations/locations.json"
HDFS_SENSORS = os.getenv("HDFS_SENSORS") #"hdfs:///air_quality/sensors/sensors.json"
HDFS_MEASUREMENTS = os.getenv("HDFS_MEASUREMENTS") #"hdfs:///air_quality/measurements/measurements.json"

def create_spark_session():
    """
    Creates and configures a SparkSession with the MongoDB Spark connector
    """
    spark = (
        SparkSession.builder
        .appName("OpenAQ_HDFS_to_MongoDB")
        #Add MongoDB Spark connector to classpath
        #   Bundles the MongoDB connector so no extra flags are needed when running the script.
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
        .getOrCreate()
    )
    return spark

def load_locations(spark):
    """
    Loads locations.json from HDFS into a Spark DataFrame, then writes
        it to a MongoDB locations collection
    """
    print(f"Reading locations from {HDFS_LOCATIONS}")
    df_locations = spark.read.json(HDFS_LOCATIONS)

    print("Writing locations to MongoDB!")

    (
        df_locations.write
        .format("mongodb")
        .mode("overwrite") #Replace existing documents in the collection
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", "locations")
        .save()
    )

    print(f"Locations write complete. Count: {df_locations.count()}")

def load_sensors(spark):
    """
    Load sensors.json from HDFS into a Spark DataFrame, then writes
        it to a MongoDB sensors collection
    """
    print(f"Reading sensors from {HDFS_SENSORS}")
    df_sensors = spark.read.json(HDFS_SENSORS)

    print("Writing sensors to MongoDB...")
    (
        df_sensors.write
        .format("mongodb")
        .mode("overwrite")  #Replace existing documents in the collection
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", "sensors")
        .save()
    )
    print(f"Sensors write complete. Count: {df_sensors.count()}")

def load_measurements(spark):
    """
    Load measurements.json from HDFS into a Spark DataFrame, adds a
        date_only fields, and writes it to MongoDB

    date_only is extracted from data.utc as YYYY-MM-DD to make
        date querys and aggregations more efficient
    """
    print(f"Reading measurements from {HDFS_MEASUREMENTS}")
    df_measurements = spark.read.json(HDFS_MEASUREMENTS)

    #Add date only field for easier queries instead of using timestamp
    from pyspark.sql.functions import col, substring
    df_measurements = df_measurements.withColumn(
        "date_only",
        substring(col("date.utc"), 1, 10) #Extract YYYY-MM-DD from YYYY-MM-DDTHH:MM:SSZ
    )

    print("Writing measurements to MongoDB...")

    (
        df_measurements.write
        .format("mongodb")
        .mode("overwrite")  #Replace existing documents in the collection
        .option("uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", "measurements")
        .save()
    )
    print(f"Measurements write complete. Count: {df_measurements.count()}")

def main():
    """
    Starts SparkSession with Mongo connector, load and write locations, sensors, and measurements. 
    Stops SparkSession
    """
    spark = create_spark_session()

    load_locations(spark)
    load_sensors(spark)
    load_measurements(spark)

    spark.stop()

if __name__ == "__main__":
    main()

