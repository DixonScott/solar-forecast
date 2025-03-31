from datetime import datetime, timedelta
import requests
import pandas as pd

import os
from dotenv import load_dotenv
load_dotenv()
PV_API_key = os.getenv("PV_API_key")
System_ID = os.getenv("System_ID")

pvoutput_base_url = "https://pvoutput.org/service/r2/"


def get_output(sid, start_date = 0, end_date = 0):

    base_params = {
        "sid": System_ID,
        "key": PV_API_key,
        "sid1": sid
    }

    response = requests.get(pvoutput_base_url + "getsystem.jsp", params = base_params)
    system_name = response.text.split(",")[0]

    if not start_date or not end_date:
        response = requests.get(pvoutput_base_url + "getstatistic.jsp", params = base_params)
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
    pvoutput_df = pd.DataFrame(columns=["date",
                                        "energy generated (Wh)",
                                        "efficiency (kWh/kW)",
                                        "energy exported (Wh)",
                                        "energy used (Wh)",
                                        "peak power (W)",
                                        "peak time",
                                        "condition",
                                        "min temp (°C)",
                                        "max temp (°C)",
                                        "peak energy import (Wh)",
                                        "off peak energy import (Wh)",
                                        "shoulder energy import (Wh)",
                                        "high shoulder energy import (Wh)",
                                        "insolation (Wh)"
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
            **base_params,
            "limit": "150", # The maximum for donors: https://pvoutput.org/help/api_specification.html#id37
            "df": date_from,
            "dt": date_to,
            "insolation": "1"
        }

        response = requests.get(pvoutput_base_url + "getoutput.jsp", params = params)
        response_df = pd.DataFrame([row.split(",") for row in response.text.split(";")],
                                   columns = pvoutput_df.columns
                                   )

        pvoutput_df = pd.concat([response_df, pvoutput_df], ignore_index = True)

        current_date += timedelta(days = 150)

    pvoutput_df.to_csv(f"{system_name} {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}.csv", index=False)