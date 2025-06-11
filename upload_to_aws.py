import os
import boto3
from datetime import datetime
from dotenv import load_dotenv


def jsons_to_s3(files_paths, config):
    files_paths_s3 = []
    raw_prefix = config['s3']['raw_prefix']
    bucket_name = config['s3']['bucket_name']
    s3 = boto3.client('s3')

    todays_date = datetime.today().strftime('%Y-%m-%d')

    for file in files_paths:
        key = f"{raw_prefix}/{todays_date}/{os.path.basename(file)}"
        s3.upload_file(file, bucket_name, key)
        files_paths_s3.append(fr"s3a://{bucket_name}/{key}")
    return files_paths_s3


def dfs_to_s3(upload_dfs, bucket_name, processed_prefix):
    todays_date = datetime.today().strftime('%Y-%m-%d')

    for file_name, df in upload_dfs.items():
        output_path = f"s3a://{bucket_name}/{processed_prefix}/{todays_date}/{file_name}"
        df.write.mode("overwrite").parquet(output_path)
        print(f'Uploaded {file_name} to {output_path}')


def txt_to_s3(summary_text, summery_df, bucket_name, summaries_prefix, output_folder_name):
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    todays_date = datetime.today().strftime('%Y-%m-%d')
    summary_file_path = f"{summaries_prefix}/{todays_date}"
    output_dir = os.path.join(os.path.dirname(__file__), output_folder_name)

    # Save summary locally
    local_summary_path = f"{output_dir}/weather_summary.txt"
    with open(local_summary_path, "w", encoding="utf-8") as f:
        f.write(summary_text)

    # Upload summary to S3
    s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3.upload_file(local_summary_path, bucket_name, fr'{summary_file_path}/weather_summary.txt')

    output_path = f"s3a://{bucket_name}/{summary_file_path}/weather_summary_row"
    summery_df.write.mode("overwrite").parquet(output_path)
    print(f'Uploaded weather_summary to {output_path}')


if __name__ == '__main__':
    jsons_to_s3()
