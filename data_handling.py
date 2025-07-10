import pandas as pd


def standardize_input(input_df_or_path, date_format):
    if isinstance(input_df_or_path, pd.DataFrame):
        df = input_df_or_path
    elif isinstance(input_df_or_path, str):
        print(f"Assuming that {input_df_or_path} is the name of a file...")
        with open("data/" + input_df_or_path, "r") as file:
            split_idx = next((idx - 1 for idx, line in enumerate(file) if line.strip("\n,") == ""), None)
        df = pd.read_csv(
            "data/" + input_df_or_path,
            parse_dates=["Earliest Output Date", "Latest Output Date"],
            date_format=date_format,
            nrows=split_idx
        )
    elif isinstance(input_df_or_path, tuple):
        print(f"Input is a tuple, taking the first element...")
        df = input_df_or_path[0]
    else:
        raise ValueError("Input must be a pandas DataFrame or a valid path to a CSV file.")
    return df


def combine_weather_and_pvoutput(weather_df, pvoutput_df, filename):
    if weather_df is None:
        print("No weather data.")
        return
    if isinstance(weather_df, str):
        weather_df = pd.read_csv("data/" + weather_df, parse_dates=["date"])
    if isinstance(pvoutput_df, str):
        with open("data/" + pvoutput_df, "r") as file:
            split_idx = next((idx for idx, line in enumerate(file) if line.strip("\n,") == ""), 0)
        pvoutput_df = pd.read_csv(
            "data/" + pvoutput_df,
            parse_dates=["Date"],
            header=split_idx
        )

    dataset = pd.merge(pvoutput_df, weather_df, left_on=["System ID", "Date"], right_on=["id", "date"], how="inner")
    dataset.drop(columns=['Energy Generated (Wh)',
       'Energy Exported (Wh)', 'Energy Used (Wh)', 'Peak Power (W)',
       'Peak Time', 'Condition', 'Min Temp (°C)', 'Max Temp (°C)',
       'Peak Energy Import (Wh)', 'Off Peak Energy Import (Wh)',
       'Shoulder Energy Import (Wh)', 'High Shoulder Energy Import (Wh)',
       'Insolation (Wh)', 'id', 'date'], inplace = True)
    dataset.to_csv("data/" + filename, index=False)
    return dataset
