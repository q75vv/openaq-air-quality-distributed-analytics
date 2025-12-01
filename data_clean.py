import os
import glob
import json
import hashlib
import pandas as pd

#Root dir where raw CSV data is stored
RAW_DIR = "data_raw"

#Output dir where cleaned & normalized JSON files will be written
OUT_DIR = "data_clean"  #Where JSON will be written

#Make sure output dir exists
os.makedirs(OUT_DIR, exist_ok=True)


locations = {}  #locationId -> location doc | One doc per locationId
sensors = {}    #sensorId -> sensor doc | One doc per sensorId
measurements = []   #list of measurement docs | one doc per measurement, can be very large

def make_measurement_id(location_id, sensor_id, parameter, utc_str, value):
    """
    Generate a unique measurementId from key data fields

    Hashes a combination of location_id, sensor_id, parameter name, UTC datetime, measurement value
    """
    #Concatenate key fields with seperator to form unique string
    key = f"{location_id}|{sensor_id}|{parameter}|{utc_str}|{value}"

    #Use SHA-1 and take first 12 hex chars to keep ID short but unique
    return "m_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]

def clean_dataframe(df):
    """
    Clean and normalize a raw CSV dataframe into consistent and safe format

    Standardizes column names, drops missing critical identifiers/time/value, removes rows with invalid (NaN) values, 
        parses datetime, deduplicate measurements (based on location_id, sensors_id, parameter, utc)
    """
    #Standardize column names just in case
    df = df.rename(columns={
        "location_id": "location_id",
        "sensors_id": "sensors_id",
        "location": "location", 
        "datetime": "datetime",
        "lat": "lat",
        "lon": "lon",
        "parameter": "parameter",
        "units": "units",
        "value": "value"
    })

    #Drop rows mising critical fields
    df = df.dropna(subset=["location_id", "sensors_id", "datetime", "parameter", "value"])

    #Remove obvious invalid values, ensures no values are NaN
    df = df[df["value"].notna()]

    #Parse datetime with timezone and convert to UTC
    # Pandas will treat e.g. "2020-01-01T01:00:00-07:00" as offset-aware.
    dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")

    #Keep only rows with valid datetimes
    valid_mask = dt.notna()
    df = df[valid_mask].copy()
    dt = dt[valid_mask]

    #Store normalized UTC timestamp as just a date string
    df["utc"] = dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


    #Deduplicate within this file
    df = df.drop_duplicates(subset=["location_id", "sensors_id", "parameter", "utc"])

    return df

def process_csv(csv_path: str):
    """
    Process a single raw CSV file into normalized entities

    For each csv in the path, 
        Load into DataFrame
        Clean data
        Fill missing lat/lon per location (take median of group)
        Populate locations and sensors dicts, measurements list
    """
    
    global locations, sensors, measurements

    print(f"Processing {csv_path}")
    df = pd.read_csv(csv_path)

    #Skip empty files
    if df.empty:
        return

    #Clean and normalize fields, timestamps, and duplicates
    df = clean_dataframe(df)

    #Try to infer missing lat/lon using other rows in the same file
    if "lat"in df.columns and "lon" in df.columns:
        #For each location_id, fill NaN lat/lon with the median for that group
        #Avoids losing rows due to missing coords with others exist
        df["lat"] = df.groupby("location_id")["lat"].transform(lambda s: s.fillna(s.median()))
        df["lon"] = df.groupby("location_id")["lon"].transform(lambda s: s.fillna(s.median()))

    #Iterate row by row to build doc
    for idx, row in df.iterrows():
        loc_id = int(row["location_id"])
        sensor_id = int(row["sensors_id"])
        location_name = row["location"]

        #Coords may still be missing after fill, handle it
        lat = float(row["lat"]) if not pd.isna(row["lat"]) else None
        lon = float(row["lon"]) if not pd.isna(row["lon"]) else None
        

        parameter = row["parameter"]
        unit = row["units"]
        value = float(row["value"])
        utc_str = row["utc"]

        #Locations collection
        if loc_id not in locations:
            if lat is None or lon is None:
                #If coords are still missing, store None to indicate unknown
                coords = None
            else:
                coords = {"latitude": lat, "longitude": lon}
            

            locations[loc_id] = {
                "locationId": loc_id,
                "location": location_name,
                "coordinates": coords
            }

        #Sensors collection
        if sensor_id not in sensors:
            sensors[sensor_id] = {
                "sensorId": sensor_id,
                "locationId": loc_id,
                "parameter": parameter,
                "unit": unit
            }


        #Measurement collection

        #Generate measurementId
        measurement_id = make_measurement_id(loc_id, sensor_id, parameter, utc_str, value)

        #Store a measurement doc in normalized form in the list
        measurements.append({
            "measurementId": measurement_id,
            "locationId": loc_id,
            "sensorId": sensor_id,
            "parameter": parameter,
            "value": value,
            "date": {
                "utc": utc_str
            }
            # coordinates & unit intentionally omitted to avoid redundancy
        })


def main():
    """
    Discover all raw CSV files under RAW_DIR with the expected folder structure:
        data_raw/location_*/year_*/month=*/location-*.csv

    For each CSV, parse and normalize data into three collections
        locations
        sensors
        measurements

    Convert collections into JSON and write them to OUT_DIR
    """
    #Find all CSV files within RAW_DIR
    pattern = os.path.join(RAW_DIR, "location_*", "year_*", "month=*", "location-*.csv")
    csv_files = glob.glob(pattern, recursive=True)

    print(f"Found {len(csv_files)} CSV files")

    #Process each CSV into global collections
    for csv_path in csv_files:
        process_csv(csv_path)

    #Convert dicts to lists for output
    locations_list = list(locations.values())
    sensors_list = list(sensors.values())

    #Sort for readability
    locations_list.sort(key=lambda d: d["locationId"])
    sensors_list.sort(key=lambda d: d["sensorId"])

    #Save final normalized JSON files

    #Write locations.json
    with open(os.path.join(OUT_DIR, "locations.json"), "w") as f:
        #Write one JSON doc per line
        for doc in locations_list:
            f.write(json.dumps(doc) + "\n")

    with open(os.path.join(OUT_DIR, "sensors.json"), "w") as f:
        for doc in sensors_list:
            f.write(json.dumps(doc) + "\n")

    with open(os.path.join(OUT_DIR, "measurements.json"), "w") as f:
        for doc in measurements:
            f.write(json.dumps(doc) + "\n")

    print(f"✔ Wrote {len(locations_list)} locations, {len(sensors_list)} sensors, {len(measurements)} measurements.")
    
if __name__ == "__main__":
    main()


