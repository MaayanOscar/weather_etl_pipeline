import os
from pyspark.sql import SparkSession


def get_data(files_paths_s3):
    spark = SparkSession.builder \
        .appName("WeatherETL") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    for file in files_paths_s3:
        df = spark.read.json(file)
        print(df)
        print(df.columns)


# def process_data():


def process_weather_data(files_paths_s3):
    os.environ["AWS_PROFILE"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot"
    get_data(files_paths_s3)


if __name__ == '__main__':
    process_weather_data()
