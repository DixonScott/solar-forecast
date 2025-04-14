from datetime import datetime

import openmeteo
import pvoutput


PVOUTPUT_ENABLED = True
OPEN_METEO_ENABLED = False

system_ids = [
    8688, 4592, 6986, 4473, 6090, 12290, 6410, 11259, 13551, 14696, 16971,
    35955, 11144, 2270, 34738, 68000, 3984, 63970, 4407, 13333, 78610, 6040,
    16162, 16040, 74437, 5526, 72329, 24464, 18205, 30434
]
start_date = datetime(2025,2,2)
end_date = datetime(2025,2,5)

# pvoutput only provides the postcode district, so more precise co-ordinates requires a little effort
latitude = 52.036
longitude = -2.425
timezone = "GMT"


def main():
    if PVOUTPUT_ENABLED:
        pvoutput.save_outputs_to_csv(system_ids, filename="Northern_Ireland_p2.csv")

    if OPEN_METEO_ENABLED:
        openmeteo.get_output(latitude, longitude, start_date, end_date, timezone)


if __name__ == '__main__':
    main()