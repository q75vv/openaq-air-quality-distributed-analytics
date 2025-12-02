import os
from datetime import datetime
import matplotlib.pyplot as plt
from data_download import LOCATIONS

FIG_DIR = "figures"
os.makedirs(FIG_DIR, exist_ok=True)

def _parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def _in_range(dt, year=None, start_year=None, end_year=None):
    """Check if a date is within a specified year or range of years."""
    y = dt.year
    if year is not None:
        return y == year
    if start_year is not None and end_year is not None:
        return start_year <= y <= end_year
    return True

def _location_label(location_id):
    """
    Return a human-friendly label like '749 (Saint John Uptown)'
    using the LOCATIONS dict (name -> id). If no name is found,
    just return the ID as a string.
    """
    for name, lid in LOCATIONS.items():
        if str(lid) == str(location_id):
            return f"{location_id} ({name})"
    return str(location_id)

def _extract_param_unit(doc, parameter_fallback):
    """
    Try to pull parameter and unit from the document.
    Falls back to the parameter passed into the function if needed.
    """
    # parameter can be at top level or under _id
    param = doc.get("parameter")
    if param is None and isinstance(doc.get("_id"), dict):
        param = doc["_id"].get("parameter")
    if param is None:
        param = parameter_fallback

    # unit can be at top level or under _id
    unit = doc.get("unit")
    if unit is None and isinstance(doc.get("_id"), dict):
        unit = doc["_id"].get("unit")

    return param, unit

def _format_param_unit(parameter, unit):
    """Return 'parameter (unit)' if unit is present, else just parameter."""
    return f"{parameter} ({unit})" if unit else str(parameter)

# Plot daily average pollutant for a single location
def plot_avg_pollutant_daily(docs, parameter, location_id, year=None, start_year=None, end_year=None, show=False):
    if not docs:
        print("No data to plot for plot_avg_pollutant_daily")
        return None
    
    parsed = [(_parse_date(d["_id"]["date"]), d) for d in docs]
    # NOTE: preserving your existing _in_range usage (no behavior change)
    filtered = [(dt, d) for dt, d in parsed if _in_range(dt, year=year, start_year=start_year, end_year=end_year)]

    if not filtered:
        print("No data found after applying year filter")
        return None

    # Use first filtered doc to get parameter + unit
    sample_doc = filtered[0][1]
    param_from_doc, unit = _extract_param_unit(sample_doc, parameter)
    param_label = _format_param_unit(param_from_doc, unit)

    dates = [dt for dt, d in filtered]
    avg_values = [d["avgValue"] for dt, d in filtered]
    min_values = [d["minValue"] for dt, d in filtered]
    max_values = [d["maxValue"] for dt, d in filtered]

    fig, ax = plt.subplots()
    ax.plot(dates, avg_values, marker=".", linewidth=1, label="Daily avg")
    ax.fill_between(dates, min_values, max_values, alpha=0.2, label="Min-Max")

    ax.set_xlabel("Date")
    ax.set_ylabel(f"{param_label} value")

    subtitle = ""
    if year is not None:
        subtitle = f" ({year})"
    if start_year is not None and end_year is not None:
        subtitle = f"({start_year}-{end_year})"

    loc_label = _location_label(location_id)
    ax.set_title(f"Daily {param_label} averages at location {loc_label}{subtitle}")
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()

    fname_extra = (
        f"_y{year}" if year is not None
        else f"_{start_year}-{end_year}" if start_year is not None and end_year is not None
        else ""
    )
    # keep filenames as-is (no unit)
    fname = f"avg_daily_{parameter}_loc{location_id or 'unknown'}{fname_extra}.png"
    path = os.path.join(FIG_DIR, fname)
    fig.savefig(path, dpi=150)
    if show:
        plt.show()
    else:
        plt.close(fig)
    print(f"Saved plot: {path}")
    return path

# Pollutant hotspots (top N locations)
def plot_pollution_hotspots(docs, parameter, top_n=3, show=False):
    if not docs:
        print("No data to plot for pollution hotspots")
        return None
    
    # Take top N
    top = docs[:top_n]
    # Use first doc to get parameter + unit
    sample_doc = top[0]
    param_from_doc, unit = _extract_param_unit(sample_doc, parameter)
    param_label = _format_param_unit(param_from_doc, unit)

    # Label each bar with "id (Name)" where possible
    locations = [_location_label(d["_id"]) for d in top]
    avg_values = [d["avgValue"] for d in top]

    fig, ax = plt.subplots()
    ax.barh(locations, avg_values)
    ax.invert_yaxis()  # Highest at the top

    ax.set_xlabel(f"Average {param_label}")
    ax.set_ylabel("Location")
    ax.set_title(f"Top {len(top)} {param_label} hotspots (by avg value)")
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

# Days exceeding threshold
def plot_days_exceeding_threshold(docs, parameter, location_id, safe_limit, year=None, start_year=None, end_year=None, show=False):
    if not docs:
        print("No days exceeding threshold; nothing to plot. ")
        return None
    
    # Parse dates and apply same year-range logic
    parsed = [(_parse_date(d["_id"]["date"]), d) for d in docs]
    # preserving behavior
    filtered = [(dt, d) for dt, d in parsed if _in_range(dt, year=year, start_year=start_year, end_year=end_year)]

    if not filtered:
        print("No data found after applying year filter in plot_days_exceeding_threshold")
        return None

    # Use first filtered doc to get parameter + unit
    sample_doc = filtered[0][1]
    param_from_doc, unit = _extract_param_unit(sample_doc, parameter)
    param_label = _format_param_unit(param_from_doc, unit)

    dates = [dt for dt, d in filtered]
    daily_avgs = [d["dailyAvg"] for dt, d in filtered]

    fig, ax = plt.subplots()
    ax.bar(dates, daily_avgs)

    if unit:
        safe_label = f"Safe limit ({safe_limit} {unit})"
    else:
        safe_label = "Safe limit"

    ax.axhline(safe_limit, linestyle="--", linewidth=1, label=safe_label)
    ax.set_xlabel("Date")
    ax.set_ylabel(f"Daily avg {param_label}")

    loc_label = _location_label(location_id)

    subtitle = ""
    if year:
        subtitle = f" ({year})"
    if start_year and end_year:
        subtitle = f" ({start_year}-{end_year})"

    limit_text = f"{safe_limit} {unit}" if unit else f"{safe_limit}"
    ax.set_title(f"Days with daily {param_label} above {limit_text} at location {loc_label}{subtitle}")
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()

    fname_extra = (
        f"_y{year}" if year is not None
        else f"_{start_year}-{end_year}" if start_year is not None and end_year is not None
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
    loc_label = _location_label(location_id)
    ax.set_title(f"Sensor uptime (readings per sensor) at location {loc_label}")
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


def plot_compare_locations_daily(docs, loc1, loc2, parameter, year=None, start_year=None, end_year=None, show=False):
    if not docs:
        print("No data to plot for compare_locations_daily")
        return None
    
    # Parse dates and apply the same-year range filter
    parsed = [(_parse_date(d["_id"]["date"]), d) for d in docs]
    # preserving behavior
    filtered = [(dt, d) for dt, d in parsed if _in_range(dt, year=year, start_year=start_year, end_year=end_year)]

    if not filtered:
        print("No data found after applying year filter in compare_locations_daily")
        return None

    # Use first filtered doc to get parameter + unit
    sample_doc = filtered[0][1]
    param_from_doc, unit = _extract_param_unit(sample_doc, parameter)
    param_label = _format_param_unit(param_from_doc, unit)
    
    # Build series for each location using filtered docs
    series = {}
    for dt, d in filtered:
        loc = d["_id"]["locationId"]
        series.setdefault(loc, {"dates": [], "avg": []})
        series[loc]["dates"].append(dt)
        series[loc]["avg"].append(d["avgValue"])

    fig, ax = plt.subplots()
    for loc, data in series.items():
        label = f"Location {_location_label(loc)}"
        ax.plot(data["dates"], data["avg"], marker=".", linewidth=1, label=label)
        
    ax.set_xlabel("Date")
    ax.set_ylabel(f"Daily avg {param_label}")

    subtitle = ""
    if year is not None:
        subtitle = f" ({year})"
    elif start_year is not None and end_year is not None:
        subtitle = f" ({start_year}-{end_year})"

    loc1_label = _location_label(loc1)
    loc2_label = _location_label(loc2)

    ax.set_title(f"Daily {param_label}: location {loc1_label} vs {loc2_label}{subtitle}")
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
            

def plot_avg_pollutant_daily_global(docs, parameter, year=None, start_year=None, end_year=None, show=False):
    if not docs:
        print("No data to plot for avg_pollutant_daily_global")
        return None
    
    parsed = [(_parse_date(d["_id"]["date"]), d) for d in docs]
    # preserving behavior
    filtered = [(dt, d) for dt, d in parsed if _in_range(dt, year=year, start_year=start_year, end_year=end_year)]

    if not filtered:
        print("No data found after applying year filter in avg_pollutant_daily_global")
        return None

    # Use first filtered doc to get parameter + unit
    sample_doc = filtered[0][1]
    param_from_doc, unit = _extract_param_unit(sample_doc, parameter)
    param_label = _format_param_unit(param_from_doc, unit)
    
    dates = [dt for dt, d in filtered]
    avg_values = [d["avgValue"] for dt, d in filtered]

    fig, ax = plt.subplots()
    ax.plot(dates, avg_values, marker=".", linewidth=1)

    ax.set_xlabel("Date")
    ax.set_ylabel(f"Global avg {param_label}")

    subtitle = ""
    if year is not None:
        subtitle = f" ({year})"
    elif start_year is not None and end_year is not None:
        subtitle = f" ({start_year}-{end_year})"

    ax.set_title(f"Global daily average {param_label} across all locations{subtitle}")
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
