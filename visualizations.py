import os
from datetime import datetime
import matplotlib.pyplot as plt

FIG_DIR = "figures"
os.makedirs(FIG_DIR, exist_ok=True)

def _parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()

#Plot daily average pollutant for a single location
def plot_avg_pollutant_daily(docs, parameter, location_id, show=False):
    if not docs:
        print("No data to plot for plot_avg_pollutant_daily")
        return None

    dates = [_parse_date(d["_id"]["date"]) for d in docs]
    avg_values = [d["avgValue"] for d in docs]
    min_values = [d["minValue"] for d in docs]
    max_values = [d["maxValue"] for d in docs]

    fig, ax = plt.subplots()
    ax.plot(dates, avg_values, marker="o", linewidth=1, label="Daily avg")
    ax.fill_between(dates, min_values, max_values, alpha=0.2, label="Min-Max")

    ax.set_xlabel("Date")
    ax.set_ylabel(f"{parameter} value")
    ax.set_title(f"Daily {parameter} averages at location {location_id}")
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()

    fname = f"avg_daily_{parameter}_loc{location_id}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path

def plot_avg_pollutant_daily2(docs, parameter, location_id, year=None, start_year=None, end_year=None, show=False):
    if not docs:
        print("No data to plot for plot_avg_pollutant_daily")
        return None
    
    def _in_range(dt):
        y = dt.year
        if year is not None:
            return y == year
        if start_year is not None and end_year is not None:
            return start_year <= y <= end_year
        return True
    
    parsed = [(_parse_date(d["_id"]["date"]), d) for d in docs]
    filtered = [(dt, d) for dt, d in parsed if _in_range(dt)]

    if not filtered:
        print("No data found after applying year filter")
        return None
        

    dates = [dt for dt, d in filtered]
    avg_values = [d["avgValue"] for dt, d in filtered]
    min_values = [d["minValue"] for dt, d in filtered]
    max_values = [d["maxValue"] for dt, d in filtered]

    fig, ax = plt.subplots()
    ax.plot(dates, avg_values, marker="o", linewidth=1, label="Daily avg")
    ax.fill_between(dates, min_values, max_values, alpha=0.2, label="Min-Max")

    ax.set_xlabel("Date")
    ax.set_ylabel(f"{parameter} value")
    subtitle = ""
    if year: subtitle = f" ({year})"
    if start_year and end_year: subtitle = f"({start_year}-{end_year})"
    ax.set_title(f"Daily {parameter} averages at location {location_id}")
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()

    fname_extra = (
        f"_y{year}" if year
        else f"_{start_year}-{end_year}" if start_year and end_year
        else ""
    )
    fname = f"avg_daily_{parameter}_loc{location_id or 'unknown'}{fname_extra}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path

#Pollutant hotspots (top N locations)
def plot_pollution_hotspots(docs, parameter, top_n=3, show=False):
    if not docs:
        print("No data to plot for pollution hotspots")
        return None
    
    #Take top N
    top = docs[:top_n]
    locations = [str(d["_id"]) for d in top]
    avg_values = [d["avgValue"] for d in top]

    fig, ax = plt.subplots()
    ax.barh(locations, avg_values)
    ax.invert_yaxis() #Highest at the top

    ax.set_xlabel(f"Average {parameter}")
    ax.set_ylabel(f"LocationId")
    ax.set_title(f"Top {len(top)} {parameter} hotspots (by avg value)")
    fig.tight_layout()

    fname = f"hotspots_{parameter}_top{len(top)}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path

#Days exceeding theshold
def plot_days_exceeding_threshold(docs, parameter, location_id, safe_limit, show=False):
    
    if not docs:
        print("No days exceeding threshold; nothingt to plot.")
        return None
    
    dates = [_parse_date(d["_id"]["date"]) for d in docs]
    daily_avgs = [d["dailyAvg"] for d in docs]

    fig, ax = plt.subplots()
    ax.bar(dates, daily_avgs)

    ax.axhline(safe_limit, linestyle="--", linewidth=1)
    ax.set_xlabel("Date")
    ax.set_ylabel(f"Daily avg {parameter}")
    loc_label = f" at location {location_id}" if location_id is not None else ""
    ax.set_title(f"Days with daily {parameter} above {safe_limit}{loc_label}")
    fig.autofmt_xdate()
    fig.tight_layout()

    fname = f"days_exceed_{parameter}_limit{safe_limit}_loc{location_id}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path

def plot_days_exceeding_threshold2(docs, parameter, location_id, safe_limit, year=None, start_year=None, end_year=None, show=False):
    if not docs:
        print("No days exceeding threshold; nothing to plot. ")
        return None
    
    #Parse dates and apply same year-range logic
    parsed = [(_parse_date(d["_id"]["date"]), d) for d in docs]

    def _in_range(dt):
        y = dt.year
        if year is not None:
            return y == year
        if start_year is not None and end_year is not None:
            return start_year <= y <= end_year
        return True
    
    filtered = [(dt, d) for dt, d in parsed if _in_range(dt)]

    if not filtered:
        print("No data found after applying year filter in plot_days_exceeding_threshold")
        return None
    
    dates = [dt for dt, d in filtered]
    daily_avgs = [d["dailyAvg"] for dt, d in filtered]

    fig, ax = plt.subplots()
    ax.bar(dates, daily_avgs)

    ax.axhline(safe_limit, linestyle="--", linewidth=1, label="Safe limit")
    ax.set_xlabel("Date")
    ax.set_ylabel(f"Daily avg {parameter}")

    loc_label = f" at location {location_id}"

    subtitle = ""
    if year: subtitle = f" ({year})"
    if start_year and end_year: subtitle = f" ({start_year}-{end_year})"

    ax.set_title(f"Days with daily {parameter} above {safe_limit}{loc_label}{subtitle}")
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()

    fname_extra = (
        f"_y{year}" if year
        else f"_{start_year}-{end_year}" if start_year and end_year
        else ""
    )

    fname = f"days_exceed_{parameter}_limit{safe_limit}_loc{location_id}{fname_extra}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path

def plot_sensor_uptime_for_location(docs, location_id, show=False):
    if not docs:
        print("No data to plot for sensor uptime")
        return None
    
    sensors = [str(d["_id"]) for d in docs]
    readings = [d["totalReadings"] for d in docs]

    fig, ax = plt.subplots()
    ax.barh(sensors, readings)
    ax.invert_yaxis()

    ax.set_xlabel("Total readings")
    ax.set_ylabel("SensorId")
    loc_label = f" at location {location_id}"
    ax.set_title(f"Sensor uptime (readings per sensor) {loc_label}")
    fig.tight_layout()

    fname = f"sensor_uptime_loc{location_id}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path

def plot_compare_locations_daily(docs, loc1, loc2, parameter, show=False):
    if not docs:
        print("No data to plot for compare_locations_daily")
        return None
    
    series = {}
    for d in docs:
        loc = d["_id"]["locationId"]
        date = _parse_date(d["_id"]["date"])
        series.setdefault(loc, {"dates": [], "avg": []})
        series[loc]["dates"].append(date)
        series[loc]["avg"].append(d["avgValue"])

    fig, ax = plt.subplots()
    for loc, data in series.items():
        label = f"Location {loc}"
        ax.plot(data["dates"], data["avg"], marker="o", linewidth=1, label=label)

    ax.set_xlabel("Date")
    ax.set_ylabel(f"Daily avg {parameter}")
    ax.set_title(f"Daily {parameter}: location {loc1} vs {loc2}")
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()

    fname = f"compare_{parameter}_{loc1}_vs_{loc2}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path

def plot_compare_locations_daily2(docs, loc1, loc2, parameter, year=None, start_year=None, end_year=None, show=False):
    if not docs:
        print("No data to plot for compare_locations_daily")
        return None
    
    #Parse dates and apply the same-year range filter
    parsed = [(_parse_date(d["_id"]["date"]), d) for d in docs]

    def _in_range(dt):
        y = dt.year
        if year is not None:
            return y == year
        if start_year is not None and end_year is not None:
            return start_year <= y <= end_year
        return True
    
    filtered = [(dt, d) for dt, d in parsed if _in_range(dt)]

    if not filtered:
        print("No data found after applying year filter in compare_locations_daily")
        return None
    
    #Build series for each location using filtered docs
    series = {}
    for dt, d in filtered:
        loc = d["_id"]["locationId"]
        series.setdefault(loc, {"dates": [], "avg": []})
        series[loc]["dates"].append(dt)
        series[loc]["avg"].append(d["avgValue"])

    fig, ax = plt.subplots()
    for loc, data in series.items():
        label = f"Location {loc}"
        ax.plot(data["dates"], data["avg"], marker="o", linewidth=1, label=label)
        
    ax.set_xlabel("Date")
    ax.set_ylabel(f"Daily avg {parameter}")

    subtitle = ""
    if year is not None: subtitle = f" ({year})"
    elif start_year is not None and end_year is not None: subtitle = f" ({start_year}-{end_year})"

    ax.set_title(f"Daily {parameter}: location {loc1} vs {loc2}{subtitle}")
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()

    fname_extra = (
        f"_{year}" if year is not None
        else f"_{start_year}-{end_year}" if start_year is not None and end_year is not None
        else ""
    )

    fname = f"compare_{parameter}_{loc1}_vs_{loc2}{fname_extra}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path
            


def plot_avg_pollutant_daily_global(docs, parameter, show=False):
    if not docs:
        print("No data to plot for avg_pollutant_daily_global")
        return None
    
    dates = [_parse_date(d["_id"]["date"]) for d in docs]
    avg_values = [d["avgValue"] for d in docs]

    fig, ax = plt.subplots()
    ax.plot(dates, avg_values, marker="o", linewidth=1)

    ax.set_xlabel("Date")
    ax.set_ylabel(f"Global avg {parameter}")
    ax.set_title(f"Global daily average {parameter} across all locations")
    fig.autofmt_xdate()
    fig.tight_layout()

    fname = f"global_avg_daily_{parameter}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path

def plot_avg_pollutant_daily_global2(docs, parameter, year=None, start_year=None, end_year=None, show=False):
    if not docs:
        print("No data to plot for avg_pollutant_daily_global")
        return None
    
    parsed = [(_parse_date(d["_id"]["date"]), d) for d in docs]

    def _in_range(dt):
        y = dt.year
        if year is not None:
            return y == year
        if start_year is not None and end_year is not None:
            return start_year <= y <= end_year
        return True
    
    filtered = [(dt, d) for dt, d in parsed if _in_range(dt)]

    if not filter:
        print("No data found after applying year filter in avg_pollutant_daily_global")
        return None
    
    dates = [dt for dt, d in filtered]
    avg_values = [d["avgValue"] for dt, d in filtered]

    fig, ax = plt.subplots()
    ax.plot(dates, avg_values, marker="o", linewidth=1)

    ax.set_xlabel("Date")
    ax.set_ylabel(f"Global avg {parameter}")

    subtitle = ""
    if year is not None:
        subtitle = f" ({year})"
    elif start_year is not None and end_year is not None:
        subtitle = f" ({start_year}-{end_year})"

    ax.set_title(f"Global daily average {parameter} across all locations{subtitle}")
    fig.autofmt_xdate()
    fig.tight_layout()

    fname_extra = (
        f"_y{year}" if year is not None
        else f"_y{start_year}-{end_year}" if start_year is not None and end_year is not None
        else ""
    )

    fname = f"global_avg_daily_{parameter}{fname_extra}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)

    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path