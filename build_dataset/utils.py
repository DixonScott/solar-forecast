import os

import pandas as pd


def standardize_input(input_df_or_path, date_format):
    if isinstance(input_df_or_path, pd.DataFrame):
        df = input_df_or_path
    elif isinstance(input_df_or_path, str):
        print(f"Assuming that {input_df_or_path} is the name of a file...")
        with open(input_df_or_path, "r") as file:
            split_idx = next((idx - 1 for idx, line in enumerate(file) if line.strip("\n,") == ""), None)
        df = pd.read_csv(
            input_df_or_path,
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


def safe_to_csv(df: pd.DataFrame, filepath: str, overwrite=False, **kwargs):
    """
    Saves a DataFrame to CSV safely.

    Parameters:
        df (pd.DataFrame): The DataFrame to save.
        filepath (str): Path to save the CSV file, including the filename. Creates it if it does not exist.
        overwrite (bool): Whether to overwrite an existing file. Default is False.
        **kwargs: Keyword arguments passed to pandas.DataFrame.to_csv().

    Returns:
        str: The final path the file was saved to.
    """
    directory = os.path.dirname(filepath)

    # Create directory if it does not exist
    if directory and not os.path.isdir(directory):
        os.makedirs(directory)

    # Create a unique filename if necessary
    if os.path.isfile(filepath) and not overwrite and kwargs.get("mode", "w") != "a":
        base, ext = os.path.splitext(filepath)
        counter = 1
        new_filepath = f"{base}({counter}){ext}"
        while os.path.isfile(new_filepath):
            counter += 1
            new_filepath = f"{base}({counter}){ext}"
        filepath = new_filepath

    # Save to CSV
    df.to_csv(filepath, **kwargs)

    return filepath
