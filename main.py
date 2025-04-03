from datetime import datetime
import pandas as pd

import openmeteo
import pvoutput

# set to True where you want to get data from the corresponding API
PVOUTPUT_ENABLED = False
OPENMETEO_ENABLED = False
# or provide file names, the files must be in the data folder
PVOUTPUT_FILE = "Ledbury Community Hospital 2020-02-02 to 2020-02-05.csv"
OPENMETEO_FILE = "Weather at 52.036°N -2.425°E from 2020-02-02 to 2020-02-05.csv"

system_id = "66991" # the id of the solar power system on pvoutput.org
start_date = datetime(2020,2,2)
end_date = datetime(2020,2,5)

# pvoutput only provides the postcode district, so more precise co-ordinates requires a little effort
latitude = 52.036
longitude = -2.425
timezone = "GMT"


def main():

    if PVOUTPUT_ENABLED:
        pv_file_name = pvoutput.get_output(system_id, start_date, end_date)
    else:
        pv_file_name = PVOUTPUT_FILE
    pvoutput_df = pd.read_csv("data/" + pv_file_name)

    if OPENMETEO_ENABLED:
        om_file_name = openmeteo.get_output(latitude, longitude, start_date, end_date, timezone)
    else:
        om_file_name = OPENMETEO_FILE
    openmeteo_df = pd.read_csv("data/" + om_file_name)

    dataset = openmeteo_df.merge(pvoutput_df, on = "date")
    dataset.to_csv("data/" + pv_file_name[:-4] + " with weather.csv", index = False)


if __name__ == '__main__':
    main()