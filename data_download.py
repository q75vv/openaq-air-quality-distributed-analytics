import os
import subprocess
import gzip
import shutil

#Map Location Names to LocationIds
LOCATIONS = {"Saint John": 749, "Fredericton": 8132, "Moncton": 746}

#Extract locationIds
LOCATION_IDS = list(LOCATIONS.values())

#Years of data to download
YEARS = [2015, 2016, 2017, 2018, 2019, 2020]

#Root dir where raw S3 files will be downloaded and extracted. 
BASE_DIR = "data_raw"

def download_data(location_id, year):
    """
    Download all measurement files from the OpenAQ S3 archive for one locationId and year

    Each dataset is stored at s3://openaq-data-archive/records/csv.gz/locationid=<id>/year=<year>/

    Function creates data_raw/location_<id>/year_<year> and downloads files into it

    Downloaded File Structure: 12 month folders -> in each, .gz files representing each day. Inside each .gz file contains a .csv contatining the real data. 
    """

    #Create local dirs
    target_dir = f"{BASE_DIR}/location_{location_id}/year_{year}"
    os.makedirs(target_dir, exist_ok=True)

    #S3 parh for use with AWS CLI
    s3_path = f"s3://openaq-data-archive/records/csv.gz/locationid={location_id}/year={year}/"
    print(f"Downloading {s3_path} ...")

    #Use AWS CLI to download entire dir without creds. The OpenAQ bucket allows for anon access
    subprocess.run([
        "aws", "s3", "cp", "--no-sign-request", "--recursive",
        s3_path, target_dir
    ])

def extract_gz_files(root_dir):
    """
    Recursively walks through a dir and extracts all .gz files found. 

    For each file: 
        Extracts file.gz into file
        Removes the .gz file
    """

    #Walk entire dir tree (location/year folder)
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            #Only process .gz files
            if file.endswith(".gz"):
                gz_path = os.path.join(root, file)
                csv_path = gz_path.replace(".gz", "")

                #Extract .gz into .csv using stream copy
                with gzip.open(gz_path, 'rb') as f_in:
                    with open(csv_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                #Remove original .gz
                os.remove(gz_path)
                print(f"Extracted {csv_path}")

def main():
    """
    For every locationId year pair, download compressed CSV files from OpenAW S3 & extract all .gz files into raw CSV

    Creates folder structure like: 
    data_raw/
        location_id/
            year_2015/
                data1.csv
                data2.csv
    """
    for loc in LOCATION_IDS:
        for year in YEARS:
            download_data(loc, year)
            extract_gz_files(f"{BASE_DIR}/location_{loc}/year_{year}")

if __name__ == "__main__":
    main()
                    
