from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import os
from dotenv import load_dotenv

import data_handling


load_dotenv()
CREDENTIALS = {
    "sid": os.getenv("System_ID"),
    "key": os.getenv("PV_API_key")
}
PVOUTPUT_BASE_URL = "https://pvoutput.org/service/r2/"
# The Historical Forecast API on open-meteo.com starts at
# 2022-03-01 for the UK Met Office.
OPEN_METEO_START_DATE = pd.Timestamp("2022-03-01")


def get_elevation(lat_series, lon_series):
    coord_string = "|".join(lat_series.astype(str) + "," + lon_series.astype(str))

    elevation_url = f"https://api.open-elevation.com/api/v1/lookup?locations={coord_string}"
    response = requests.get(elevation_url)
    data = response.json()

    elevations = [item["elevation"] for item in data["results"]]
    return pd.Series(elevations)


def save_outputs_to_csv(system_ids, mode = "info_only", filename = None):
    if mode not in ("info_only", "full"):
        print(f"Invalid mode: {mode}.")
        return

    system_list = [get_system_info_from_id(sid) for sid in system_ids]
    system_df = pd.DataFrame(system_list)
    prepare_query_for_open_meteo(system_df)

    if filename is None:
        filename = f"PV_output_for_{len(system_ids)}_systems_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv"
    system_df.to_csv("data/" + filename, index=False)

    if mode == "info_only":
        print("Info only mode: saved system info without output data.")
        return system_df

    pvoutput_df = append_output_data_to_file(filename, system_df)
    return system_df, pvoutput_df


def get_system_info_from_id(sid):
    columns = [
        "System ID",
        "System Name", "System Size", "Postcode/Zipcode",
        "Panels", "Panel Power (W)", "Panel Brand",
        "Inverters", "Inverter Power (W)", "Inverter Brand",
        "Orientation", "Array Tilt (°)", "Shade", "Install Date",
        "Co-ordinate Precision", "Latitude", "Longitude",
        "Status Interval",
        "Earliest Output Date", "Latest Output Date"
    ]
    params = {**CREDENTIALS, "sid1": sid}
    response = requests.get(PVOUTPUT_BASE_URL + "getsystem.jsp", params = params)
    #The text response is a list of values divided into sections by ";", of which only the
    #first section is needed, and then into individual values by ",".
    system_info = response.text.split(";")[0].split(",")[:16]
    response = requests.get(PVOUTPUT_BASE_URL + "getstatistic.jsp", params = params)
    start_date, end_date = [pd.to_datetime(date, format = "%Y%m%d") for date in response.text.split(",")[7:9]]

    #Check the number of decimal places of latitude to differentiate between exact co-ordinates
    #given by the system owner, or rough co-ordinates generated automatically.
    if len(system_info[13].split(".")[1]) <= 2:
        coord_precision = "Approximate"
    else:
        coord_precision = "Exact"

    system_info = [sid] + system_info[:13] + [coord_precision] + system_info[-3:] + [start_date, end_date]

    return dict(zip(columns, system_info))


def get_output_from_id(sid, start_date = 0, end_date = 0):
    if not start_date or not end_date:
        response = requests.get(
            PVOUTPUT_BASE_URL + "getstatistic.jsp",
            params ={**CREDENTIALS, "sid1": sid}
        )
        if not start_date:
            if not end_date:
                start_date, end_date = response.text.split(",")[7:9]
                start_date = pd.to_datetime(start_date, format = "%Y%m%d")
                end_date = pd.to_datetime(end_date, format = "%Y%m%d")
            else:
                start_date = pd.to_datetime(response.text.split(",")[7], format = "%Y%m%d")
        else:
            end_date = pd.to_datetime(response.text.split(",")[8], format = "%Y%m%d")

    pvoutput_df_columns = [
        "System ID",
        "Date",
        "Energy Generated (Wh)",
        "Efficiency (kWh/kW)",
        "Energy Exported (Wh)",
        "Energy Used (Wh)",
        "Peak Power (W)",
        "Peak Time",
        "Condition",
        "Min Temp (°C)",
        "Max Temp (°C)",
        "Peak Energy Import (Wh)",
        "Off Peak Energy Import (Wh)",
        "Shoulder Energy Import (Wh)",
        "High Shoulder Energy Import (Wh)",
        "Insolation (Wh)"
    ]
    pvoutput_dfs = []

    # Set end_date to yesterday if it's today to prevent a partial day.
    if end_date.date() == datetime.now().date():
        print(f"End date for System ID {sid} is today. Subtracting one day to avoid partial output data.")
        end_date = end_date - timedelta(days = 1)
    current_date = start_date
    while current_date <= end_date:

        date_from = current_date.strftime("%Y%m%d")
        date_to = current_date + timedelta(days = 149)
        if date_to > end_date:
            date_to = end_date
        date_to = date_to.strftime("%Y%m%d")

        params = {
            **CREDENTIALS,
            "sid1": sid,
            "limit": "150", # The maximum for donors: https://pvoutput.org/help/api_specification.html#id37
            "df": date_from,
            "dt": date_to,
            "insolation": "1"
        }

        response = requests.get(PVOUTPUT_BASE_URL + "getoutput.jsp", params = params)
        if "Bad request" != response.text[:11]:
            response_df = pd.DataFrame([[sid] + row.split(",") for row in response.text.split(";")],
                                       columns = pvoutput_df_columns
                                       )
            response_df["Date"] = pd.to_datetime(response_df["Date"])
            pvoutput_dfs = [response_df] + pvoutput_dfs
        current_date += timedelta(days = 150)

    if pvoutput_dfs:
        return pd.concat(pvoutput_dfs, ignore_index = True)
    return None


def prepare_query_for_open_meteo(table_of_pv_systems, timezone_str = "UTC", date_format = "%d/%m/%Y"):
    table_of_pv_systems = data_handling.standardize_input(table_of_pv_systems, date_format = date_format)

    query = table_of_pv_systems[["Latitude", "Longitude", "Latest Output Date"]]
    query.insert(2, "Elevation", "")
    query.insert(3, "Timezone", timezone_str)
    query.insert(4, "Earliest Output Date", OPEN_METEO_START_DATE)
    # Remove rows where pvoutput data ends before open-meteo data starts.
    query = query[query["Earliest Output Date"] <= query["Latest Output Date"]]

    query.to_csv(f"data/Open_meteo_query_{query.iloc[0,0]}N{query.iloc[0,1]}E_and_{len(query)-1}_others.csv", index = False, header = True)
    return query


def append_output_data_to_file(filename, system_df = None, date_format = "%d/%m/%Y"):
    if system_df is None:
        system_df = pd.read_csv("data/" + filename, parse_dates = ["Earliest Output Date", "Latest Output Date"], date_format = date_format)

    system_ids = system_df["System ID"].tolist()

    master_list = []
    for sid in system_ids:
        start_date = system_df.loc[system_df["System ID"] == sid, "Earliest Output Date"].iloc[0]
        if start_date < OPEN_METEO_START_DATE:
            start_date = OPEN_METEO_START_DATE
        end_date = system_df.loc[system_df["System ID"] == sid, "Latest Output Date"].iloc[0]
        pvoutput_df = get_output_from_id(sid, start_date, end_date)
        if pvoutput_df is not None:
            master_list += [pvoutput_df]

    if master_list:
        master_df = pd.concat(master_list, ignore_index=True)

        with open("data/" + filename, "a") as f:
            f.write("\n")
        master_df.to_csv("data/" + filename, mode="a", index=False)
        return master_df
    print(f"No output data found. No changes made to {filename}.")


def check_api_limit():
    response = requests.get(
        PVOUTPUT_BASE_URL + "getstatistic.jsp",
        params = CREDENTIALS,
        headers = {"X-Rate-Limit": "1"}
    )

    for key, value in response.headers.items():
        if key.startswith("X-Rate-Limit"):
            if key == "X-Rate-Limit-Reset":
                value = datetime.fromtimestamp(int(value), tz=timezone.utc).strftime('%d-%m-%Y %H:%M:%S')
            print(f"{key[13:]}: {value}")


def api_cost_calc(table_of_pv_systems, date_format = "%d/%m/%Y"):
    table_of_pv_systems = data_handling.standardize_input(table_of_pv_systems, date_format = date_format)

    result = table_of_pv_systems["Latest Output Date"].apply(lambda x: (x - OPEN_METEO_START_DATE).days)
    result = (result + 1)//150+1 # +1 because date range is inclusive, //150+1 because an API call returns data for up to 150 days
    result = result.sum()

    print(f"That will cost {result} API calls.")
    return result
