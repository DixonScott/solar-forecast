from datetime import datetime, timedelta

import requests
import pandas as pd

import os
from dotenv import load_dotenv
load_dotenv()
credentials = {
    "sid": os.getenv("System_ID"),
    "key": os.getenv("PV_API_key")
}
pvoutput_base_url = "https://pvoutput.org/service/r2/"


def save_outputs_to_csv(system_ids = (), mode = "info_only"):
    if mode not in ("info_only", "full"):
        print(f"Invalid mode: {mode}.")
        return

    if not system_ids:
        print("No system IDs provided.")
        return

    system_list = [get_system_info_from_id(sid) for sid in system_ids]
    system_df = pd.DataFrame(system_list)

    file_path = f"data/PV_output_for_{len(system_ids)}_systems_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv"
    system_df.to_csv(file_path, index=False)

    if mode == "info_only":
        print("Info only mode: saved system info without output data.")
        return

    master_list = [get_output_from_id(sid) for sid in system_ids]
    master_df = pd.concat(master_list, ignore_index = True)

    with open(file_path, "a") as f:
        f.write("\n")
    master_df.to_csv(file_path, mode = "a", index = False)


def get_system_info_from_id(sid):
    columns = [
        "System ID",
        "System Name", "System Size", "Postcode/Zipcode",
        "Panels", "Panel Power (W)", "Panel Brand",
        "Inverters", "Inverter Power (W)", "Inverter Brand",
        "Orientation", "Array Tilt (°)", "Shade", "Install Date",
        "Co-ordinate Precision", "Latitude", "Longitude",
        "Status Interval"
    ]
    response = requests.get(
        pvoutput_base_url + "getsystem.jsp",
        params={**credentials, "sid1": sid}
    )
    #The text response is a list of values divided into sections by ";", of which only the
    #first section is needed, and then into individual values by ",".
    system_info = response.text.split(";")[0].split(",")[:16]

    #Check the number of decimal places of latitude to differentiate between exact co-ordinates
    #given by the system owner, or rough co-ordinates generated automatically.
    if len(system_info[13].split(".")[1]) <= 2:
        coord_precision = "Approximate"
    else:
        coord_precision = "Exact"

    system_info = [sid] + system_info[:13] + [coord_precision] + system_info[-3:]

    return dict(zip(columns, system_info))


def get_output_from_id(sid, start_date = 0, end_date = 0):
    if not start_date or not end_date:
        response = requests.get(
            pvoutput_base_url + "getstatistic.jsp",
            params ={**credentials, "sid1": sid}
        )
        if not start_date:
            if not end_date:
                start_date, end_date = response.text.split(",")[7:9]
                start_date = datetime.strptime(start_date, "%Y%m%d")
                end_date = datetime.strptime(end_date, "%Y%m%d")
            else:
                start_date = datetime.strptime(response.text.split(",")[7], "%Y%m%d")
        else:
            end_date = datetime.strptime(response.text.split(",")[8], "%Y%m%d")


    # data frame to store all pvoutput data
    pvoutput_df = pd.DataFrame(columns=["System ID",
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
                               )

    current_date = start_date
    while current_date <= end_date:

        date_from = current_date.strftime("%Y%m%d")
        date_to = current_date + timedelta(days = 149)
        if date_to > end_date:
            date_to = end_date
        date_to = date_to.strftime("%Y%m%d")

        params = {
            **credentials,
            "sid1": sid,
            "limit": "150", # The maximum for donors: https://pvoutput.org/help/api_specification.html#id37
            "df": date_from,
            "dt": date_to,
            "insolation": "1"
        }

        response = requests.get(pvoutput_base_url + "getoutput.jsp", params = params)
        if "Bad request" != response.text[:11]:
            response_df = pd.DataFrame([[sid] + row.split(",") for row in response.text.split(";")],
                                       columns = pvoutput_df.columns
                                       )
            response_df["Date"] = (response_df["Date"].str[:4] + '-'
                                   + response_df["Date"].str[4:6] + '-'
                                   + response_df["Date"].str[6:])
            pvoutput_df = pd.concat([response_df, pvoutput_df], ignore_index = True)
        current_date += timedelta(days = 150)

    return pvoutput_df