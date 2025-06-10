import os
import sys
from upload_to_aws import dfs_to_s3
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, from_unixtime, round


def define_spark():
    os.environ["SPARK_HOME"] = r"C:\spark\spark-3.5.1-bin-hadoop3"
    os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

    sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python"))
    sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python", "lib"))


def process_data(files_paths_s3):
    final_dfs = {}
    # spark = SparkSession.builder \
    #     .appName("S3Uploader") \
    #     .master("local[*]") \
    #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    #     .config("spark.hadoop.io.native.lib.available", "false").getOrCreate()
    # from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("S3Uploader") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.local.dir", r"C:\tmp\hadoop-שירז").getOrCreate()

    # spark = SparkSession.builder \
    #     .appName("ReadFromS3") \
    #     .master("local[*]") \
    #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    #     .getOrCreate()
    # spark = SparkSession.builder \
    #     .appName("ReadFromS3") \
    #     .master("local[*]") \
    #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    #     .config("spark.hadoop.io.native.lib.available", "false") \
    #     .getOrCreate()

    for file in files_paths_s3:
        file_name = os.path.splitext(os.path.basename(file))[0]
        print(file_name)
        df = spark.read.json(file, multiLine=True)
        # df.show()
        # df.printSchema()

        # temp | feels_like | temp_min | temp_max
        df = df.withColumn("round_temp", round(col("main.temp")))
        df = df.withColumn("round_feels_like", round(col("main.feels_like")))
        df = df.withColumn("round_temp_min", round(col("main.temp_min")))
        df = df.withColumn("round_temp_max", round(col("main.temp_max")))
        df = df.withColumn("temp_range", concat_ws(" - ", col("round_temp_min"), col("round_temp_max")))

        filtered_df = df.select(
            col("name").alias("city"),
            # col("sys.country").alias("country"),
            from_unixtime("dt").alias("datetime"),
            # col("coord.lat"),
            # col("coord.lon"),
            col("round_temp").alias("temp"),
            col("round_feels_like").alias("feels_like"),
            col("temp_range"),
            # col("round_temp_min").alias("temp_min"),
            # col("round_temp_max").alias("temp_max"),
            col("main.humidity"),
            # col("main.pressure"),
            # col("wind.speed").alias("wind_speed"),
            # col("wind.deg").alias("wind_deg"),
            col("clouds.all").alias("clouds")
            # col("temp_range")
            # col("weather")[0]["main"].alias("weather_main"),
            # col("weather")[0]["description"].alias("weather_description")
        )
        filtered_df.show()
        final_dfs[file_name] = filtered_df
    return final_dfs


def process_weather_data(files_paths_s3, bucket_name, processed_prefix):
    define_spark()
    final_dfs = process_data(files_paths_s3)
    dfs_to_s3(final_dfs, bucket_name, processed_prefix)


if __name__ == '__main__':
    define_spark()
    process_weather_data(['s3a://weather-data-aws/raw/2025-06-10/tel_aviv_weather.json', 's3a://weather-data-aws/raw/2025-06-10/jerusalem_weather.json', 's3a://weather-data-aws/raw/2025-06-10/haifa_weather.json', 's3a://weather-data-aws/raw/2025-06-10/ashdod_weather.json'])