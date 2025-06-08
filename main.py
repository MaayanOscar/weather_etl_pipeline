from fetch_weather import create_json_weather
from upload_to_aws import files_to_s3


def main():
    create_json_weather()
    files_to_s3()


if __name__ == '__main__':
    main()
