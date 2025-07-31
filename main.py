from build_dataset import pvoutput, openmeteo, combine_data


SYSTEM_IDS = [  # Example system IDs to demonstrate making a dataset
    66991, 11542, 5242
]


def main():
    # This will fetch data from pvoutput.org, then open-meteo.com, combine them and save it as "dataset.csv" to folder "data"
    system_df, pvoutput_df = pvoutput.save_outputs_to_csv(SYSTEM_IDS, mode="full", filename="pvoutput.csv")
    weather_df = openmeteo.get_weather_for_locations(system_df, output_file_name="weather.csv")
    combine_data.combine_weather_and_pvoutput(weather_df, pvoutput_df, "dataset.csv")


if __name__ == '__main__':
    main()
