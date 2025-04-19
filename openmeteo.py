import time
import pandas as pd

import openmeteo_requests
import requests_cache
from retry_requests import retry


OPEN_METEO_START_DATE = pd.Timestamp("2022-03-01")
# Does not support variables sunset and sunrise currently.
DAILY_VARS = (
    "surface_pressure_mean", "weather_code", "sunshine_duration",
    "daylight_duration", "precipitation_sum", "precipitation_hours",
    "wind_direction_10m_dominant", "cloud_cover_min", "cloud_cover_mean",
    "temperature_2m_mean", "relative_humidity_2m_min", "wind_speed_10m_mean",
    "shortwave_radiation_sum"
)
MAX_API_CALLS_MINUTE = 600
MAX_API_CALLS_HOUR = 5000
OPEN_METEO_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"


def api_cost_calc(table_of_pv_systems, date_format = "%Y-%m-%d"):
    if isinstance(table_of_pv_systems, str):
        print(f"Assuming that {table_of_pv_systems} is the name of a file...")
        with open("data/" + table_of_pv_systems, "r") as file:
            split_idx = next((idx for idx, line in enumerate(file) if line.strip("\n,") == ""), 0)
        if split_idx:
            table_of_pv_systems = pd.read_csv("data/" + table_of_pv_systems, parse_dates = ["Earliest Output Date", "Latest Output Date"], date_format = date_format, nrows = split_idx-1)
        else:
            table_of_pv_systems = pd.read_csv("data/" + table_of_pv_systems, parse_dates = ["Earliest Output Date", "Latest Output Date"], date_format = date_format)

    result = table_of_pv_systems["Latest Output Date"] - table_of_pv_systems["Earliest Output Date"]
    result = result.apply(lambda x: x.days * len(DAILY_VARS) / 140)
    print(f"Total API cost: {result.sum()}")
    print(f"Min: {result.min()}, Mean: {result.mean()}, Max: {result.max()}")
    return result


def get_weather_for_locations(query, daily_vars = DAILY_VARS, date_format = "%Y-%m-%d"):
    if isinstance(query, str):
        print(f"Assuming that {query} is the name of a file...")
        with open("data/" + query, "r") as file:
            split_idx = next((idx for idx, line in enumerate(file) if line.strip("\n,") == ""), 0)
        if split_idx:
            query = pd.read_csv("data/" + query, parse_dates = ["Earliest Output Date", "Latest Output Date"], date_format = date_format, nrows = split_idx-1)
        else:
            query = pd.read_csv("data/" + query, parse_dates = ["Earliest Output Date", "Latest Output Date"], date_format = date_format)

    query.loc[query["Earliest Output Date"] < OPEN_METEO_START_DATE, "Earliest Output Date"] = OPEN_METEO_START_DATE

    api_cost = api_cost_calc(query)
    if api_cost.sum() > MAX_API_CALLS_HOUR:
        print("Total API cost exceeds the hourly limit of 5000.")
        return
    if api_cost.max() > MAX_API_CALLS_MINUTE:
        print("API cost of at least one location exceeds the minutely limit of 600.")
        return

    total = 0
    start_of_chunk = 0
    chunks = []

    for i, n in enumerate(api_cost):
        total += n
        if total > MAX_API_CALLS_MINUTE:
            chunks.append(query.iloc[start_of_chunk:i])
            start_of_chunk = i
            total = n
    chunks.append(query.iloc[start_of_chunk:])

    # Set up the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    open_meteo = openmeteo_requests.Client(session = retry_session)

    location_dfs = []
    if "System ID" in query:
        location_ids = (i for i in query["System ID"])
    else:
        location_ids = (i for i in range(len(query)))
    for i, chunk in enumerate(chunks):
        if i > 0:
            print("Waiting 60s before next API call...")
            time.sleep(60)

        params = {
            "latitude": chunk["Latitude"].tolist(),
            "longitude": chunk["Longitude"].tolist(),
            "start_date": chunk["Earliest Output Date"].dt.strftime("%Y-%m-%d").tolist(),
            "end_date": chunk["Latest Output Date"].dt.strftime("%Y-%m-%d").tolist(),
            "daily": list(daily_vars)
        }
        responses = open_meteo.weather_api(OPEN_METEO_URL, params = params)

        for response in responses:
            daily = response.Daily()
            daily_data = {
                "id": next(location_ids),
                "date": pd.date_range(
                    start = pd.to_datetime(daily.Time(), unit = "s"),
                    end = pd.to_datetime(daily.TimeEnd(), unit = "s"),
                    freq = pd.Timedelta(seconds = daily.Interval()),
                    inclusive = "left"
                )
            }
            for var_no, var_name in enumerate(daily_vars):
                daily_data[var_name] = daily.Variables(var_no).ValuesAsNumpy()

            location_dfs.append(pd.DataFrame(data = daily_data))

    open_meteo_df = pd.concat(location_dfs, ignore_index = True)
    open_meteo_df.to_csv("data/Weather_v1_pt1.csv", index = False)
    return open_meteo_df