import os
import boto3
from datetime import datetime


def jsons_to_s3(files_paths, bucket_name, raw_prefix):
    files_paths_s3 = []
    s3 = boto3.client('s3')

    # files_dir = os.path.join(os.path.dirname(__file__), 'weather_data')
    # files = [f for f in os.listdir(files_dir) if os.path.isfile(os.path.join(files_dir, f))]

    todays_date = datetime.today().strftime('%Y-%m-%d')

    for file in files_paths:
        key = f"{raw_prefix}/{todays_date}/{os.path.basename(file)}"
        s3.upload_file(file, bucket_name, key)
        files_paths_s3.append(fr"s3a://{bucket_name}/{key}")
    return files_paths_s3


def dfs_to_s3(upload_dfs, bucket_name, processed_prefix):
    todays_date = datetime.today().strftime('%Y-%m-%d')

    for file_name, df in upload_dfs.items():
        # "s3a://your-bucket-name/processed/"
        output_path = f"s3a://{bucket_name}/{processed_prefix}/{todays_date}/"
        # df.coalesce(1).write.option("header", True).mode("overwrite").csv(f"{output_path}{file_name}/")
        # df.write.csv(f"{output_path}{file_name}/", header=True)
        df.write.mode("overwrite").csv(f"{output_path}{file_name}/", header=True)
        print(f'upload {file_name} CSV')
        # df.write.mode("overwrite").csv(ou)


if __name__ == '__main__':
    jsons_to_s3()
