from datetime import datetime

import openmeteo
import pvoutput


PVOUTPUT_ENABLED = True
OPENMETEO_ENABLED = False

system_ids = [
    4502, 5007, 6854
]
start_date = datetime(2025,2,2)
end_date = datetime(2025,2,5)

# pvoutput only provides the postcode district, so more precise co-ordinates requires a little effort
latitude = 52.036
longitude = -2.425
timezone = "GMT"


def main():
    if PVOUTPUT_ENABLED:
        pvoutput.save_outputs_to_csv(system_ids)

    if OPENMETEO_ENABLED:
        openmeteo.get_output(latitude, longitude, start_date, end_date, timezone)


if __name__ == '__main__':
    main()