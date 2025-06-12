import os
import boto3
from datetime import datetime
from dotenv import load_dotenv


def jsons_to_s3(files_paths, config):
    '''
    Uploads local JSON files to S3 bucket.

    :param files_paths: List of local file paths to the JSON files.
    :param config: Configuration dictionary loaded from YAML file.
    :return: A list of S3 paths where the files were uploaded.
    '''

    files_paths_s3 = []
    raw_prefix = config['s3']['raw_prefix']
    bucket_name = config['s3']['bucket_name']

    # Create S3 client using default credentials
    s3 = boto3.client('s3')

    # today's date to use in the S3 path
    todays_date = datetime.today().strftime('%d-%m-%Y')

    for file in files_paths:
        # Generate a key for saving the file
        key = f"{raw_prefix}/{todays_date}/{os.path.basename(file)}"

        # Upload the local file to S3
        s3.upload_file(file, bucket_name, key)

        # Save the S3 path in a list.
        files_paths_s3.append(f"s3a://{bucket_name}/{key}")

    return files_paths_s3


def dfs_to_s3(upload_df, bucket_name, processed_prefix):
    '''
    Uploads multiple PySpark DataFrames to S3 bucket in Parquet format, grouped by date.

    :param upload_df: Dictionary where the key is the output filename and the value is a Spark DataFrame.
    :param bucket_name: Name of the S3 bucket to upload the data to.
    :param processed_prefix: Path prefix in the bucket.
    '''
    todays_date = datetime.today().strftime('%d-%m-%Y')

    for file_name, df in upload_df.items():
        # Create full S3 output path
        output_path = f"s3a://{bucket_name}/{processed_prefix}/{todays_date}/{file_name}"

        # Write DataFrame as Parquet, overwrite if there is previous file
        df.write.mode("overwrite").parquet(output_path)

        print(f'Uploaded {file_name} to {output_path}')


def txt_to_s3(summary_text, summery_df, bucket_name, summaries_prefix, output_folder_name):
    '''
    Saves a weather summary as a local text file and uploads both the summary text and DataFrame to S3.

    :param summary_text: String containing a textual summary of the weather data.
    :param summery_df: PySpark DataFrame containing the structured summary data.
    :param bucket_name: Name of the S3 bucket for upload.
    :param summaries_prefix: Folder prefix in the S3 bucket for storing summaries.
    :param output_folder_name: Local folder name to temporarily store the summary file.
    '''

    # Load credentials from .env file
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    todays_date = datetime.today().strftime('%d-%m-%Y')
    summary_file_path = f"{summaries_prefix}/{todays_date}"
    output_dir = os.path.join(os.path.dirname(__file__), output_folder_name)

    # Ensure the local folder exists
    os.makedirs(output_dir, exist_ok=True)

    # Save the summary as a .txt file locally
    local_summary_path = os.path.join(output_dir, "weather_summary.txt")
    with open(local_summary_path, "w", encoding="utf-8") as f:
        f.write(summary_text)

    # Upload the text summary to S3
    s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3.upload_file(local_summary_path, bucket_name, f"{summary_file_path}/weather_summary.txt")

    # Upload the summary DataFrame as Parquet file to s3
    output_path = f"s3a://{bucket_name}/{summary_file_path}/weather_summary_table"
    summery_df.write.mode("overwrite").parquet(output_path)

    print(f'Uploaded weather_summary to {output_path}')


def dashboard_to_s3(local_path, bucket_name, s3_key):
    '''
    Uploads a local HTML dashboard file.

    :param local_path: Path to the local HTML to upload.
    :param bucket_name: Name of the S3 bucket to upload the file to.
    :param s3_key: Path within the S3 bucket where the HTML will be stored.
    '''

    # Create S3 client using credentials from .env file
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    # Upload the HTML to the specified location in S3
    s3.upload_file(local_path, bucket_name, s3_key)
    print(f"Dashboard uploaded to s3://{bucket_name}/{s3_key}")
