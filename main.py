from datetime import datetime

import openmeteo
import pvoutput


PVOUTPUT_ENABLED = True
OPENMETEO_ENABLED = True

system_id = "66991" # the id of the solar power system on pvoutput.org
start_date = datetime(2020,2,2)
end_date = datetime(2020,2,5)

# pvoutput only provides the postcode district, so more precise co-ordinates requires a little effort
latitude = 52.036
longitude = -2.425
timezone = "GMT"


def main():

    if PVOUTPUT_ENABLED:
        pvoutput.get_output(system_id, start_date, end_date)

    if OPENMETEO_ENABLED:
        openmeteo.get_output(latitude, longitude, start_date, end_date, timezone)


if __name__ == '__main__':
    main()