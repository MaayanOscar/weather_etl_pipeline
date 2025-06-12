from upload_to_aws import jsons_to_s3
from utils.load_config import load_config
from fetch_weather import create_json_weather
from process_with_pyspark import process_weather_data
from weather_dashboard import show_dashboard


def main():
    config = load_config()

    files_paths = create_json_weather(config)
    files_paths_s3 = jsons_to_s3(files_paths, config)
    final_dfs, summary_items = process_weather_data(files_paths_s3, config)
    show_dashboard(final_dfs, summary_items[0], config)


if __name__ == '__main__':
    main()
