import pandas as pd

from . import utils


def combine_weather_and_pvoutput(weather_df, pvoutput_df, filepath):
    if weather_df is None:
        print("No weather data.")
        return
    if isinstance(weather_df, str):
        weather_df = pd.read_csv(weather_df, parse_dates=["date"])
    if isinstance(pvoutput_df, str):
        with open(pvoutput_df, "r") as file:
            split_idx = next((idx for idx, line in enumerate(file) if line.strip("\n,") == ""), 0)
        pvoutput_df = pd.read_csv(
            pvoutput_df,
            parse_dates=["Date"],
            header=split_idx
        )

    dataset = pd.merge(pvoutput_df, weather_df, left_on=["System ID", "Date"], right_on=["id", "date"], how="inner")
    dataset.drop(columns=['Energy Generated (Wh)',
                          'Energy Exported (Wh)', 'Energy Used (Wh)', 'Peak Power (W)',
                          'Peak Time', 'Condition', 'Min Temp (°C)', 'Max Temp (°C)',
                          'Peak Energy Import (Wh)', 'Off Peak Energy Import (Wh)',
                          'Shoulder Energy Import (Wh)', 'High Shoulder Energy Import (Wh)',
                          'Insolation (Wh)', 'id', 'date'], inplace=True)

    final_path = utils.safe_to_csv(dataset, filepath, index=False)
    print(f"Saved CSV to {final_path}")
    return dataset
