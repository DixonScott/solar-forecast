import openmeteo
import pvoutput


PVOUTPUT_ENABLED = False
OPEN_METEO_ENABLED = True

SYSTEM_IDS = [
    66991
]
QUERY = "test.csv"


def main():
    if PVOUTPUT_ENABLED:
        pvoutput.save_outputs_to_csv(SYSTEM_IDS, filename = "test.csv")

    if OPEN_METEO_ENABLED:
        openmeteo.get_weather_for_locations(QUERY, date_format = "%d/%m/%Y")


if __name__ == '__main__':
    main()