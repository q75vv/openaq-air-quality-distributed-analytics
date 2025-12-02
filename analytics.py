from pymongo import MongoClient
from pprint import pprint
from dotenv import load_dotenv
import os
import json
import visualizations
from data_download import LOCATIONS

load_dotenv()

#Directory where computed analytics will be stored as JSON files
RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

#MongoDB config
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print("\nConnected to MongoDB:", MONGO_URI)

def save_results(name, data):
    """
    Save a list of analytics results to the results/ directory
    """
    path = os.path.join(RESULTS_DIR, f"{name}.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} records to {path}")

def get_unit_for_parameter(parameter, location_id=None):
    """
    Find the unit for a given parameter.

    If location_id is provided, we try to restrict to that location;
    otherwise we just grab the first matching sensor.
    """
    query = {"parameter": parameter}
    if location_id is not None:
        query["locationId"] = location_id

    doc = db.sensors.find_one(query, {"unit": 1})
    if doc and "unit" in doc:
        return doc["unit"]
    return None


def avg_pollutant_daily(parameter="pm25", location_id=749):
    """
    Computes the daily average/min/max/count of a pollutant for a specific location

    Grouping key -> substring of data.utc(YYYY-MM-DD)
    """
    print(f"\n=== Daily Average for {parameter} at location {location_id} ===")

    pipeline = [
        {"$match": {
            "parameter": parameter,
            "locationId": location_id
        }},
        {"$group": {
            "_id": {"date": {"$substr": ["$date.utc", 0, 10]}},
            "avgValue": {"$avg": "$value"},
            "minValue": {"$min": "$value"},
            "maxValue": {"$max": "$value"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.date": 1}}
    ]

    results = list(db.measurements.aggregate(pipeline))

    unit = get_unit_for_parameter(parameter, location_id=location_id)

    for doc in results:
        doc["parameter"] = parameter
        doc["unit"] = unit

    #Display sample output to console
    #for doc in results[:20]:    #Show first 20 rows
        #pprint(doc)
    return results

def pollution_hotspots(parameter="pm25", min_readings=24):
    """
    Compute average pollutant level per location across the dataset and list
        locations with the highest average pollution

    Locations with fewer than min_readings are excluded
    """
    print(f"\n=== Pollution hotspots for {parameter} (min_readings={min_readings}) ===")

    pipeline = [
        {"$match": {"parameter": parameter}},
        {"$group": {
            "_id": "$locationId",
            "avgValue": {"$avg": "$value"},
            "maxValue": {"$max": "$value"},
            "readings": {"$sum": 1}
        }},
        # filter out locations with very few data points
        {"$match": {"readings": {"$gte": min_readings}}},
        {"$sort": {"avgValue": -1}}
    ]

    results = list(db.measurements.aggregate(pipeline))

    unit = get_unit_for_parameter(parameter)

    for doc in results:
        doc["parameter"] = parameter
        doc["unit"] = unit

    #for doc in results[:20]:
        #pprint(doc)
    return results


def days_exceeding_threshold(location_id=749, parameter="pm25", safe_limit=25):
    """
    Count the days at a location where the daily average pollutant level exceeds
        a safety threshold
    """
    print(f"\n=== Days exceeding {safe_limit} for {parameter} at location {location_id} ===")

    pipeline = [
        {"$match": {
            "locationId": location_id,
            "parameter": parameter
        }},
        {"$group": {
            "_id": {
                "date": {"$substr": ["$date.utc", 0, 10]}
            },
            "dailyAvg": {"$avg": "$value"}
        }},
        {"$match": {
            "dailyAvg": {"$gt": safe_limit}
        }},
        {"$sort": {"_id.date": 1}}
    ]

    results = list(db.measurements.aggregate(pipeline))

    unit = get_unit_for_parameter(parameter, location_id=location_id)

    for doc in results:
        doc["parameter"] = parameter
        doc["unit"] = unit

    #for doc in results:
        #pprint(doc)

    print(f"Total days exceeding threshold: {len(results)}")
    return results


def sensor_uptime_for_location(location_id):
    """
    Show a total readings and the time range they cover
    """
    print(f"\n=== Sensor uptime for location {location_id} ===")

    pipeline = [
        {"$match": {"locationId": location_id}},
        {"$group": {
            "_id": "$sensorId",
            "totalReadings": {"$sum": 1},
            "firstReading": {"$min": "$date.utc"},
            "lastReading": {"$max": "$date.utc"}
        }},
        {"$sort": {"totalReadings": -1}}
    ]

    results = list(db.measurements.aggregate(pipeline))

    #for doc in results:
        #pprint(doc)
    return results


def compare_locations_daily(loc1, loc2, parameter="pm25"):
    print(f"\n=== Daily {parameter} comparison: {loc1} vs {loc2} ===")

    pipeline = [
        {"$match": {
            "parameter": parameter,
            "locationId": {"$in": [loc1, loc2]}
        }},
        {"$group": {
            "_id": {
                "locationId": "$locationId",
                "date": {"$substr": ["$date.utc", 0, 10]}
            },
            "avgValue": {"$avg": "$value"},
            "minValue": {"$min": "$value"},
            "maxValue": {"$max": "$value"},
            "count": {"$sum": 1}
        }},
        {"$sort": {
            "_id.date": 1,
            "_id.locationId": 1
        }}
    ]

    results = list(db.measurements.aggregate(pipeline))

    unit = get_unit_for_parameter(parameter)

    for doc in results:
        doc["parameter"] = parameter
        doc["unit"] = unit

    #for doc in results[:400]:
        #pprint(doc)
    return results


def avg_pollutant_daily_global(parameter="pm25"):
    """
    Compare daily pollutant averages between all

    Groups by locationId, date pairs
    """
    print(f"\n=== Global daily average for {parameter} across all locations ===")

    pipeline = [
        {"$match": {"parameter": parameter}},
        {"$group": {
            "_id": {"date": {"$substr": ["$date.utc", 0, 10]}},
            "avgValue": {"$avg": "$value"},
            "minValue": {"$min": "$value"},
            "maxValue": {"$max": "$value"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.date": 1}}
    ]

    results = list(db.measurements.aggregate(pipeline))

    unit = get_unit_for_parameter(parameter)

    for doc in results:
        doc["parameter"] = parameter
        doc["unit"] = unit

    #for doc in results[:20]:
        #pprint(doc)
    return results

def main():
    """
    Execute all MongoDB analytics pipelines and generate JSON results in /results and
        visualizations via visualizations.py
    """

    daily_749_pm25 = avg_pollutant_daily(parameter="pm25", location_id=LOCATIONS["Saint John"])
    save_results(name="daily_avg_749_pm25", data=daily_749_pm25)
    visualizations.plot_avg_pollutant_daily(docs=daily_749_pm25, parameter="pm25", location_id=LOCATIONS["Saint John"], year=2016, show=False)

    hotspots_pm25 = pollution_hotspots(parameter="pm25", min_readings=24)
    visualizations.plot_pollution_hotspots(docs=hotspots_pm25, parameter="pm25", top_n=3, show=False)
    save_results(name="hotspots_pm25", data=hotspots_pm25)

    days_exceed_749_pm25 = days_exceeding_threshold(location_id=LOCATIONS["Saint John"], parameter="pm25", safe_limit=20)
    save_results(name="days_exceed_749_pm25", data=days_exceed_749_pm25)
    visualizations.plot_days_exceeding_threshold(docs=days_exceed_749_pm25, parameter="pm25", location_id=LOCATIONS["Saint John"], safe_limit=20, year=2016, show=False)
    visualizations.plot_days_exceeding_threshold(docs=days_exceed_749_pm25, parameter="pm25", location_id=LOCATIONS["Saint John"], safe_limit=20, show=False)

    uptime_749 = sensor_uptime_for_location(location_id=LOCATIONS["Saint John"])
    save_results(name="uptime_749", data=uptime_749)
    visualizations.plot_sensor_uptime_for_location(docs=uptime_749, location_id=LOCATIONS["Saint John"], show=False)

    compare_749_8132 = compare_locations_daily(loc1=LOCATIONS["Saint John"], loc2=LOCATIONS["Fredericton"], parameter="pm25")
    save_results(name="compare_749_8132", data=compare_749_8132)
    visualizations.plot_compare_locations_daily(docs=compare_749_8132, loc1=LOCATIONS["Saint John"], loc2=LOCATIONS["Fredericton"], parameter="pm25", show=False)
    visualizations.plot_compare_locations_daily(docs=compare_749_8132, loc1=LOCATIONS["Saint John"], loc2=LOCATIONS["Fredericton"], parameter="pm25", year=2020, show=False)

    compare_749_10907 = compare_locations_daily(loc1=LOCATIONS["Saint John"], loc2=LOCATIONS["India - Gwalior"], parameter="pm25")
    save_results(name="compare_749_10907", data=compare_749_10907)
    visualizations.plot_compare_locations_daily(docs=compare_749_10907, loc1=LOCATIONS["Saint John"], loc2=LOCATIONS["India - Gwalior"], parameter="pm25",show=False)
    visualizations.plot_compare_locations_daily(docs=compare_749_10907, loc1=LOCATIONS["Saint John"], loc2=LOCATIONS["India - Gwalior"], parameter="pm25", year=2020, show=False)

    daily_avg_pm25_global = avg_pollutant_daily_global(parameter="pm25")
    save_results(name="daily_avg_pm25_global", data=daily_avg_pm25_global)
    visualizations.plot_avg_pollutant_daily_global(docs=daily_avg_pm25_global, parameter="pm25", show=False)
    visualizations.plot_avg_pollutant_daily_global(docs=daily_avg_pm25_global, parameter="pm25", year=2019, show=False)

    print("\nAnalytics complete.\n")


if __name__ == "__main__":
    main()