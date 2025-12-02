import subprocess
import sys
import time

def run_step(desc, cmd):
    """
    Run an external shell command as one step in the project pipeline. 
    """
    print(f"\n=== {desc} ===")
    print(f"Running: {cmd}")
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        #If any steps fail, stop the pipeline
        print(f"ERROR: Step failed — {desc}")
        print(e)
        sys.exit(1)
    print(f"✔ Completed: {desc}\n")
    time.sleep(1)  # small pause between steps

def main():
    """
    Runs the entire pipeline sequentially. 

    1. Download OpenAQ raw csv files
    2. Clean and normalize CSV files into JSON
    3. Import normalized JSON into Hadoop HDFS
    4. Use Spark to load HDFS data into MongoDB
    5. Execute MongoDB pipeline
    6. Generate visuals
    """
    #Start hadoop
    #run_step("Starting Hadoop", "bash start_hadoop.sh")

    #Start MongoDB
    #run_step("Starting MongoDB", "bash start_mongo.sh")

    #Download raw data
    run_step("Downloading OpenAQ Data", "python3 data_download.py")

    #Clean CSV into JSON
    run_step("Cleaning and normalizing data", "python3 data_clean.py")

    #Import cleaned JSON files into HDFS
    run_step("Importing data into HDFS", "bash import_hadoop_data.sh")

    #Load HDFS JSON into MongoDB using Spark
    run_step("Loading HDFS data into MongoDB (Spark)", "python3 load_to_mongo.py")

    #Run MongoDB analytics queries
    run_step("Running analytics", "python3 analytics.py")

    #visualizations.py is used via analytics.py, it does not have it's own main function

    #Stop hadoop
    #run_step("Stopping Hadoop", "bash stop_hadoop.sh")

    #Stop MongoDB
    #run_step("Stopping MongoDB", "bash stop_mongo.sh")

    print("Pipeline complete. ")

if __name__ == "__main__":
    main()