import os
import boto3
from datetime import datetime


def files_to_s3(files_paths):
    files_paths_s3 = []
    s3 = boto3.client('s3')

    # files_dir = os.path.join(os.path.dirname(__file__), 'weather_data')
    # files = [f for f in os.listdir(files_dir) if os.path.isfile(os.path.join(files_dir, f))]

    todays_date = datetime.today().strftime('%Y-%m-%d')
    bucket_name = "weather-data-aws"

    for file in files_paths:
        key = f"raw/{todays_date}/{file}"
        s3.upload_file(file, bucket_name, key)
        files_paths_s3.append(fr"s3a://{bucket_name}/{key}")
    return files_paths_s3


if __name__ == '__main__':
    files_to_s3()
