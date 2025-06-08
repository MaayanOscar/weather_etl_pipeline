import os
import boto3
from datetime import datetime


def files_to_s3():
    s3 = boto3.client('s3')

    files_dir = os.path.join(os.path.dirname(__file__), 'weather_data')
    files = [f for f in os.listdir(files_dir) if os.path.isfile(os.path.join(files_dir, f))]

    todays_date = datetime.today().strftime('%Y-%m-%d')

    for file in files:
        key = f"raw/{todays_date}/{file}"
        s3.upload_file(os.path.join(files_dir, file), "weather-data-aws", key)


if __name__ == '__main__':
    files_to_s3()
