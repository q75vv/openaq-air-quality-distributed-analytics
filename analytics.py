from pymongo import MongoClient
from pprint import pprint
from dotenv import load_dotenv
import os
import json
import visualizations

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

#Config
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print("\nConnected to MongoDB:", MONGO_URI)

#Save aggregate results to JSON file in results dir
def save_results(name, data):
    path = os.path.join(RESULTS_DIR, f"{name}.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} records to {path}")


#Daily Average Pollutant for a specified location id
def avg_pollutant_daily(parameter="pm25", location_id=749):
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
    for doc in results[:20]:    #Show first 20 rows
        pprint(doc)
    return results

#For a given pollutant, computes the average value per location over full dataset. 
def pollution_hotspots(parameter="pm25", min_readings=24):
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
    for doc in results[:20]:
        pprint(doc)
    return results

#Counts how many days at a given location have a daily average above a "safe" limit
def days_exceeding_threshold(location_id=749, parameter="pm25", safe_limit=25):
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
    for doc in results:
        pprint(doc)

    print(f"Total days exceeding threshold: {len(results)}")
    return results

#Shows number of readings per sensor at a given location, and the time range they cover
def sensor_uptime_for_location(location_id=749):
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
    for doc in results:
        pprint(doc)
    return results

#Compares daily avereage values of a pollutant between two locations
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
    for doc in results[:400]:
        pprint(doc)
    return results

#Global daily avg accross all locations
def avg_pollutant_daily_global(parameter="pm25"):
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
    for doc in results[:20]:
        pprint(doc)
    return results

def main():

    daily_749_pm25 = avg_pollutant_daily("pm25", 749)
    save_results("daily_avg_749_pm25", daily_749_pm25)
    visualizations.plot_avg_pollutant_daily(daily_749_pm25, "pm25", 749, False)
    visualizations.plot_avg_pollutant_daily2(daily_749_pm25, "pm25", 749, year=2016, show=False)
    #visualizations.plot_daily_avg(daily_749_pm25, "Daily Pm25 - Location 749", "PM25", "µg/m³", "Daily Pm25 - Location 749")
    #daily_746_pm25 = avg_pollutant_daily("pm25", 746)
    #save_results("daily_746_pm25", daily_746_pm25)
    #visualizations.plot_daily_avg(daily_746_pm25, "Daily Pm25 - Location 746", "PM25", "µg/m³", "Daily Pm25 - Location 746")


    hotspots_pm25 = pollution_hotspots("pm25", 24)
    visualizations.plot_pollution_hotspots(hotspots_pm25, "pm25", 3, False)
    save_results("hotspots_pm25", hotspots_pm25)

    days_exceed_749_pm25 = days_exceeding_threshold(749, "pm25", 5)
    save_results("days_exceed_749_pm25", days_exceed_749_pm25)
    visualizations.plot_days_exceeding_threshold(days_exceed_749_pm25, "pm25", 749, 5, False)

    uptime_749 = sensor_uptime_for_location(749)
    save_results("uptime_749", uptime_749)
    visualizations.plot_sensor_uptime_for_location(uptime_749, 749, False)

    compare_749_8132 = compare_locations_daily(749, 8132, "pm25")
    save_results("compare_749_8132", compare_749_8132)
    visualizations.plot_compare_locations_daily(compare_749_8132, 749, 8132, "pm25", False)

    daily_avg_pm25_global = avg_pollutant_daily_global("pm25")
    save_results("daily_avg_pm25_global", daily_avg_pm25_global)
    visualizations.plot_avg_pollutant_daily_global(daily_avg_pm25_global, "pm25", False)

    print("\nAnalytics complete.\n")


if __name__ == "__main__":
    main()