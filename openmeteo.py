from datetime import timedelta
import time

import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry


def api_call(latitude, longitude, start_date, end_date, timezone):

    # Set up the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "daily": ["weather_code",
                  "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
                  "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours",
                  "sunrise", "sunset", "daylight_duration", "sunshine_duration",
                  "shortwave_radiation_sum", "cloud_cover_mean", "cloud_cover_max", "cloud_cover_min",
                  "relative_humidity_2m_mean", "relative_humidity_2m_max", "relative_humidity_2m_min",
                  "dew_point_2m_mean", "dew_point_2m_max", "dew_point_2m_min",
                  "surface_pressure_mean", "surface_pressure_max", "surface_pressure_min",
                  "wind_direction_10m_dominant", "wind_gusts_10m_mean", "wind_speed_10m_mean",
                  "wind_gusts_10m_max", "wind_speed_10m_max", "wind_gusts_10m_min", "wind_speed_10m_min"],
        "timezone": timezone
    }
    responses = openmeteo.weather_api(url, params = params)
    response = responses[0]

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(2).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(3).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(4).ValuesAsNumpy()
    daily_rain_sum = daily.Variables(5).ValuesAsNumpy()
    daily_snowfall_sum = daily.Variables(6).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(7).ValuesAsNumpy()
    daily_sunrise = daily.Variables(8).ValuesInt64AsNumpy()
    daily_sunset = daily.Variables(9).ValuesInt64AsNumpy()
    daily_daylight_duration = daily.Variables(10).ValuesAsNumpy()
    daily_sunshine_duration = daily.Variables(11).ValuesAsNumpy()
    daily_shortwave_radiation_sum = daily.Variables(12).ValuesAsNumpy()
    daily_cloud_cover_mean = daily.Variables(13).ValuesAsNumpy()
    daily_cloud_cover_max = daily.Variables(14).ValuesAsNumpy()
    daily_cloud_cover_min = daily.Variables(15).ValuesAsNumpy()
    daily_relative_humidity_2m_mean = daily.Variables(16).ValuesAsNumpy()
    daily_relative_humidity_2m_max = daily.Variables(17).ValuesAsNumpy()
    daily_relative_humidity_2m_min = daily.Variables(18).ValuesAsNumpy()
    daily_dew_point_2m_mean = daily.Variables(19).ValuesAsNumpy()
    daily_dew_point_2m_max = daily.Variables(20).ValuesAsNumpy()
    daily_dew_point_2m_min = daily.Variables(21).ValuesAsNumpy()
    daily_surface_pressure_mean = daily.Variables(22).ValuesAsNumpy()
    daily_surface_pressure_max = daily.Variables(23).ValuesAsNumpy()
    daily_surface_pressure_min = daily.Variables(24).ValuesAsNumpy()
    daily_wind_direction_10m_dominant = daily.Variables(25).ValuesAsNumpy()
    daily_wind_gusts_10m_mean = daily.Variables(26).ValuesAsNumpy()
    daily_wind_speed_10m_mean = daily.Variables(27).ValuesAsNumpy()
    daily_wind_gusts_10m_max = daily.Variables(28).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(29).ValuesAsNumpy()
    daily_wind_gusts_10m_min = daily.Variables(30).ValuesAsNumpy()
    daily_wind_speed_10m_min = daily.Variables(31).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    ), "weather_code": daily_weather_code,
        "temperature_2m_mean": daily_temperature_2m_mean,
        "temperature_2m_max": daily_temperature_2m_max,
        "temperature_2m_min": daily_temperature_2m_min,
        "precipitation_sum": daily_precipitation_sum,
        "rain_sum": daily_rain_sum,
        "snowfall_sum": daily_snowfall_sum,
        "precipitation_hours": daily_precipitation_hours,
        "sunrise": daily_sunrise,
        "sunset": daily_sunset,
        "daylight_duration": daily_daylight_duration,
        "sunshine_duration": daily_sunshine_duration,
        "shortwave_radiation_sum": daily_shortwave_radiation_sum,
        "cloud_cover_mean": daily_cloud_cover_mean,
        "cloud_cover_max": daily_cloud_cover_max,
        "cloud_cover_min": daily_cloud_cover_min,
        "relative_humidity_2m_mean": daily_relative_humidity_2m_mean,
        "relative_humidity_2m_max": daily_relative_humidity_2m_max,
        "relative_humidity_2m_min": daily_relative_humidity_2m_min,
        "dew_point_2m_mean": daily_dew_point_2m_mean,
        "dew_point_2m_max": daily_dew_point_2m_max,
        "dew_point_2m_min": daily_dew_point_2m_min,
        "surface_pressure_mean": daily_surface_pressure_mean,
        "surface_pressure_max": daily_surface_pressure_max,
        "surface_pressure_min": daily_surface_pressure_min,
        "wind_direction_10m_dominant": daily_wind_direction_10m_dominant,
        "wind_gusts_10m_mean": daily_wind_gusts_10m_mean,
        "wind_speed_10m_mean": daily_wind_speed_10m_mean,
        "wind_gusts_10m_max": daily_wind_gusts_10m_max,
        "wind_speed_10m_max": daily_wind_speed_10m_max,
        "wind_gusts_10m_min": daily_wind_gusts_10m_min,
        "wind_speed_10m_min": daily_wind_speed_10m_min}

    daily_dataframe = pd.DataFrame(data = daily_data)
    daily_dataframe["date"] = daily_dataframe["date"].dt.strftime('%Y-%m-%d')

    return daily_dataframe


def get_output(latitude, longitude, start_date, end_date, timezone):

    openmeteo_df = pd.DataFrame()
    current_date = start_date
    while current_date <= end_date:

        date_to = current_date + timedelta(days = 364)
        if date_to > end_date:
            date_to = end_date

        new_df = api_call(latitude, longitude, current_date, date_to, timezone)
        openmeteo_df = pd.concat([openmeteo_df, new_df], ignore_index = True)

        current_date += timedelta(days = 365)

        if date_to == end_date:
            print("All API calls complete.")
        else:
            print("API call complete, now waiting 60 seconds.")
            time.sleep(60)

    file_name = f"Weather at {latitude}°N {longitude}°E from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}.csv"
    openmeteo_df.to_csv("data/" + file_name, index=False)
    print(f"Saved {file_name} in data.")

    return file_name