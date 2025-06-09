from upload_to_aws import files_to_s3
from fetch_weather import create_json_weather
from process_with_pyspark import process_weather_data
from utils.load_config import load_config


def main():
    config = load_config()
    print(config)
    files_paths = create_json_weather()
    files_paths_s3 = files_to_s3(files_paths)
    process_weather_data(files_paths_s3)


if __name__ == '__main__':
    main()
