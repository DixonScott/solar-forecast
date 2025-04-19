import time
import pandas as pd

import openmeteo_requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests


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


def get_weather_for_locations(query, daily_vars = DAILY_VARS, date_format = "%Y-%m-%d", output_file_name = "weather.csv"):
    if isinstance(query, str):
        print(f"Assuming that {query} is the name of a file...")
        with open("data/" + query, "r") as file:
            split_idx = next((idx for idx, line in enumerate(file) if line.strip("\n,") == ""), 0)
        if split_idx:
            query = pd.read_csv("data/" + query, parse_dates = ["Earliest Output Date", "Latest Output Date"], date_format = date_format, nrows = split_idx-1)
        else:
            query = pd.read_csv("data/" + query, parse_dates = ["Earliest Output Date", "Latest Output Date"], date_format = date_format)

    query.loc[query["Earliest Output Date"] < OPEN_METEO_START_DATE, "Earliest Output Date"] = OPEN_METEO_START_DATE

    retry_strategy = Retry(
        total = 3,
        backoff_factor = 1,
        status_forcelist = [502, 503, 504],
        allowed_methods = ["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    open_meteo = openmeteo_requests.Client(session=session)

    location_dfs = []
    failed_locations = []
    if "System ID" in query:
        location_ids = (i for i in query["System ID"])
    else:
        location_ids = (i for i in range(len(query)))
    first_idx = query.index[0]
    for idx, location in query.iterrows():
        location_id = next(location_ids)
        if idx != first_idx:
            print("Waiting 10s before next API call...")
            time.sleep(10)

        params = {
            "latitude": location["Latitude"],
            "longitude": location["Longitude"],
            "start_date": location["Earliest Output Date"].strftime("%Y-%m-%d"),
            "end_date": location["Latest Output Date"].strftime("%Y-%m-%d"),
            "daily": list(daily_vars)
        }
        print(f"{time.strftime("%H:%M:%S", time.localtime())} - Making request for id {location_id}...")
        try:
            responses = open_meteo.weather_api(OPEN_METEO_URL, params = params)
        except Exception as e:
            print(f"{time.strftime("%H:%M:%S", time.localtime())} - Request for id {location_id} failed.")
            print(f"An error occurred: {e}")
            continue
        print(f"{time.strftime("%H:%M:%S", time.localtime())} - Request {location_id} successful.")

        response = responses[0]
        daily = response.Daily()
        daily_data = {
            "id": location_id,
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

    if location_dfs:
        open_meteo_df = pd.concat(location_dfs, ignore_index = True)
        open_meteo_df.to_csv("data/" + output_file_name, index = False)
        return open_meteo_df
    return