import os
import time

import joblib
import openmeteo_requests
import pandas as pd
import psutil
import requests
from flask import Flask, request, jsonify, render_template
import urllib.request


process = psutil.Process(os.getpid())
def print_memory_usage():
    print(f"Memory usage: {process.memory_info().rss / 1048576:.2f} MiB")


elevation_url = "https://api.open-elevation.com/api/v1/lookup?locations="
open_meteo_url = "https://api.open-meteo.com/v1/forecast"
weather_code_mapping = {
    0: "Clear sky ☀️",
    1: "Mainly clear 🌤️", 2: "Partly cloudy ⛅", 3: "Overcast ☁️",
    45: "Fog 🌫️", 48: "Freezing fog 🌫️",
    51: "Light drizzle 🌦️", 53: "Moderate drizzle 🌦️", 55: "Dense drizzle 🌧️",
    56: "Light freezing drizzle 🧊🌧️", 57: "Dense freezing drizzle 🧊🌧️",
    61: "Slight rain 🌧️", 63: "Moderate rain 🌧️", 65: "Heavy rain 🌧️",
    66: "Light freezing rain ❄️🌧️", 67: "Heavy freezing rain ❄️🌧️",
    71: "Slight snowfall 🌨️", 73: "Moderate snowfall 🌨️", 75: "Heavy snowfall 🌨️",
    77: "Snow grains 🌨️",
    80: "Slight rain showers 🌦️", 81: "Moderate rain showers 🌦️", 82: "Violent rain showers 🌧️⚡",
    85: "Slight snow showers 🌨️", 86: "Heavy snow showers 🌨️",
    95: "Thunderstorm ⛈️", 96: "Thunderstorm with slight hail ⛈️🧊", 99: "Thunderstorm with heavy hail ⛈️🧊"
}

model_path = "rf_100_v1.pkl"
if not os.path.exists(model_path):
    model_url = "https://github.com/DixonScott/Solar_Power/releases/download/v1.0/rf_100_v1.pkl"
    print(f"Model not found, downloading from {model_url}")
    print_memory_usage()
    start = time.time()
    urllib.request.urlretrieve(model_url, model_path)
    end = time.time()
    print(f"Download time: {(end - start):.1f}s")
    print_memory_usage()
rf = joblib.load(model_path)
print("Model loaded.")
print_memory_usage()

app = Flask(__name__)


@app.route('/')
def index():
    print_memory_usage()
    return render_template('index.html')


@app.route('/predict', methods=['POST'])
def predict():
    print_memory_usage()

    data = request.get_json()
    lat = data.get("latitude")
    lon = data.get("longitude")
    power_rating = data.get("power_rating", None)
    print(f"Received coordinates: {lat}, {lon}")

    # fetch the elevation from open-elevation.com
    response = requests.get(elevation_url + f"{lat},{lon}")
    print(f"Got response {response} from open-elevation.")
    elevation = response.json()["results"][0]["elevation"]

    # fetch the weather forecast from open-meteo.com
    open_meteo = openmeteo_requests.Client()

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ["surface_pressure_mean", "weather_code", "sunshine_duration", "daylight_duration",
                  "precipitation_sum", "precipitation_hours", "wind_direction_10m_dominant", "cloud_cover_mean",
                  "cloud_cover_min", "temperature_2m_mean", "relative_humidity_2m_min", "wind_speed_10m_mean",
                  "shortwave_radiation_sum"],
        "timezone": "Europe/London"
    }

    try:
        response = open_meteo.weather_api(open_meteo_url, params=params)[0]
        print("Successfully fetched forecast from open-meteo.")
    except Exception as e:
        print("Request Failed: ", e)

    lat = response.Latitude()
    lon = response.Longitude()
    daily = response.Daily()
    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True).tz_convert("Europe/London"),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True).tz_convert("Europe/London"),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        ),
        "Elevation (m)": elevation
    }
    for var_no, var_name in enumerate(params["daily"]):
        daily_data[var_name] = daily.Variables(var_no).ValuesAsNumpy()
    forecast = pd.DataFrame(data=daily_data)

    # Clean the data in the same way as the training data
    forecast = forecast.dropna().copy()
    forecast['cloud_cover_mean'] = forecast['cloud_cover_mean'].clip(upper=100)
    forecast = forecast[forecast['cloud_cover_min'] != -1]

    weather_codes = forecast["weather_code"].copy()  # Save the original "weather_code" before it's changed
    conditions = weather_codes.map(weather_code_mapping)  # Convert weather codes to user-friendly descriptions
    forecast = weather_code_to_category(forecast)  #  Convert weather codes to categories for use by the model

    predictions = rf.predict(forecast[rf.feature_names_in_])
    print("Predictions made.")

    predictions_list = []
    for i in range(len(predictions)):
        pred_dict = {
            "date": forecast["date"].iloc[i].strftime("%a %d/%m"),
            "condition": conditions[i],
            "value": float(predictions[i]),
        }
        if power_rating is not None:
            pred_dict["output"] = float(predictions[i]) * power_rating
        predictions_list.append(pred_dict)

    print_memory_usage()

    return jsonify({
        "predictions": predictions_list,
        "latitude": round(lat, 4),
        "longitude": round(lon, 4)
    })


def weather_code_to_category(data):
    weather_map = {
        0: 'clear',
        1: 'partly_cloudy', 2: 'partly_cloudy',
        3: 'overcast',
        45: 'fog', 48: 'fog',
        51: 'drizzle', 53: 'drizzle', 55: 'drizzle', 56: 'drizzle', 57: 'drizzle',
        61: 'rain', 63: 'rain', 65: 'rain', 66: 'rain', 67: 'rain',
        71: 'snow', 73: 'snow', 75: 'snow', 77: 'snow', 85: 'snow', 86: 'snow',
        80: 'rain_showers', 81: 'rain_showers', 82: 'rain_showers',
        95: 'thunderstorm', 96: 'thunderstorm', 99: 'thunderstorm'
    }
    data['weather_category'] = data['weather_code'].map(weather_map)

    expected_weather_dummies = [
        'weather_category_clear',
        'weather_category_partly_cloudy',
        'weather_category_overcast',
        'weather_category_fog',
        'weather_category_drizzle',
        'weather_category_rain',
        'weather_category_snow',
        'weather_category_rain_showers',
        'weather_category_thunderstorm'
    ]

    data = pd.get_dummies(data, columns=['weather_category'], drop_first=False)
    # Add back missing columns
    for col in expected_weather_dummies:
        if col not in data.columns:
            data[col] = False
    data = data.drop(columns=['weather_code', 'weather_category_clear'])

    return data


if __name__ == '__main__':
    app.run(debug=True)
