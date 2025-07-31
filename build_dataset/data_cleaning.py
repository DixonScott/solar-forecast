import pandas as pd

# These system ids are based on my own exploration of my dataset (see /notebooks/data_cleaning.ipynb)
SIDS_TO_REMOVE = (3099, 8224, 4113, 32351, 46979, 3641, 6090)


def clean_dataset(data):
    if isinstance(data, str):
        data = pd.read_csv("data/" + data, parse_dates=["Date"])

    # remove NAs
    data = data.dropna().copy()
    # clip some values of cloud cover mean which are >100 to 100
    data['cloud_cover_mean'] = data['cloud_cover_mean'].clip(upper=100)
    # remove some rows where cloud cover min is -1
    data = data[data['cloud_cover_min'] != -1]
    # remove rows where efficiency is 0
    # check if the column exists first, in case this is a dataset for prediction
    if "Efficiency (kWh/kW)" in data.columns:
        data = data[data["Efficiency (kWh/kW)"] != 0]
    return data


def remove_systems(data, sids = SIDS_TO_REMOVE):
    data = data[~data["System ID"].isin(sids)]
    return data


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
