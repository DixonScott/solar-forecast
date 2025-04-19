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
    else:
        raise ValueError("Input must be a pandas DataFrame or a valid path to a CSV file.")
    return df