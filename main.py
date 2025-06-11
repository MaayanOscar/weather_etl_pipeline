from upload_to_aws import jsons_to_s3
from fetch_weather import create_json_weather
from process_with_pyspark import process_weather_data
from utils.load_config import load_config


def main():
    config = load_config()
    print(config)
    files_paths = create_json_weather(config)
    files_paths_s3 = jsons_to_s3(files_paths, config)
    print(files_paths_s3)
    process_weather_data(files_paths_s3, config)


if __name__ == '__main__':
    main()
