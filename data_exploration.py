import pandas as pd


def clean_dataset(data):
    """
    Cleans the dataset from data_handling.combine_weather_and_pvoutput().
    There is a not very much cleaning necessary:
    1. Drop NAs, these are very rare.
    2. Clip values of cloud_cover_mean to 100.
    3. Drop rows where cloud_cover_min is -1.

    Parameters:
        data (str or pd.DataFrame): the dataset to be cleaned. If a string, it should be the filename of a csv in the "data" folder.

    Returns:
        pd.DataFrame: the cleaned dataset.
    """
    if isinstance(data, str):
        data = pd.read_csv("data/" + data, parse_dates=["Date"])
    data = data.dropna()
    data['cloud_cover_mean'] = data['cloud_cover_mean'].clip(upper=100)
    data = data[data['cloud_cover_min'] != -1]
    return data