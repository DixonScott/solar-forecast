import data_exploration
import openmeteo
import pvoutput
import data_handling


SYSTEM_IDS = [
    66991, 11542, 5242
]


def main():
    system_df, pvoutput_df = pvoutput.save_outputs_to_csv(SYSTEM_IDS, mode="full", filename="pvoutput.csv")
    weather_df = openmeteo.get_weather_for_locations(system_df, output_file_name="weather.csv")
    dataset = data_handling.combine_weather_and_pvoutput(weather_df, pvoutput_df, "dataset.csv")
    dataset = data_exploration.clean_dataset(dataset)

if __name__ == '__main__':
    main()