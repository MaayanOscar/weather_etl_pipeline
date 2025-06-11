import os
import sys
import builtins
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Row
from upload_to_aws import dfs_to_s3, txt_to_s3
from pyspark.sql.functions import col, from_unixtime, concat_ws, avg, round as spark_round


def define_spark():
    os.environ["SPARK_HOME"] = r"C:\spark\spark-3.5.1-bin-hadoop3"
    os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

    os.environ['TMPDIR'] = 'C:\\temp'
    if not os.path.exists('C:\\temp'):
        os.makedirs('C:\\temp')

    sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python"))
    sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python", "lib"))

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    spark = SparkSession.builder \
        .appName("MyETLPipeline") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .getOrCreate()
    return spark


def process_data(files_paths_s3, spark):
    final_dfs = {}

    for file in files_paths_s3:
        file_name = os.path.splitext(os.path.basename(file))[0]
        df = spark.read.json(file, multiLine=True)

        df = df.withColumn("round_temp", spark_round(col("main.temp"), 1)) \
               .withColumn("round_feels_like", spark_round(col("main.feels_like"), 1)) \
               .withColumn("round_temp_min", spark_round(col("main.temp_min"), 1)) \
               .withColumn("round_temp_max", spark_round(col("main.temp_max"), 1)) \
               .withColumn("temp_range", concat_ws(" - ", col("round_temp_min"), col("round_temp_max")))

        filtered_df = df.select(
            col("name").alias("city"),
            from_unixtime("dt").alias("datetime"),
            col("round_temp").alias("temp"),
            col("round_feels_like").alias("feels_like"),
            col("temp_range"),
            col("main.humidity").alias("humidity"),
            col("clouds.all").alias("clouds")
        )
        final_dfs[file_name] = filtered_df

    # Combine all cities
    combined_df = None
    for df in final_dfs.values():
        combined_df = df if combined_df is None else combined_df.unionByName(df)

    # Calculate statistics
    hottest = combined_df.orderBy(col("temp").desc()).first()
    coldest = combined_df.orderBy(col("temp").asc()).first()
    most_humid = combined_df.orderBy(col("humidity").desc()).first()
    cloudiest = combined_df.orderBy(col("clouds").desc()).first()

    # Collect average stats as Python values
    stats_row = combined_df.agg(
        avg("temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    ).collect()[0]

    # Extract values from Row object using Python's round
    avg_temp = builtins.round(stats_row["avg_temp"], 1)
    avg_humidity = builtins.round(stats_row["avg_humidity"], 1)

    # Create summary text
    summary_text = f"""\    
    Weather Summary – Today

    🔥 Hottest city: {hottest['city']} (temp: {hottest['temp']}°C)
    ❄️ Coldest city: {coldest['city']} (temp: {coldest['temp']}°C)
    💧 Most humid city: {most_humid['city']} (humidity: {most_humid['humidity']}%)
    🌥 Cloudiest city: {cloudiest['city']} (clouds: {cloudiest['clouds']}%)

    Average temperature: {avg_temp}°C | Average humidity: {avg_humidity}%
    """

    print(summary_text)

    # Create summary dataframe
    summary_row = Row(
        hottest_city=hottest["city"],
        coldest_city=coldest["city"],
        most_humid_city=most_humid["city"],
        cloudiest_city=cloudiest["city"],
        avg_temp=avg_temp,
        avg_humidity=avg_humidity
    )
    summary_df = spark.createDataFrame([summary_row])

    return final_dfs, [summary_text, summary_df]


def process_weather_data(files_paths_s3, config):
    spark = define_spark()
    final_dfs, summary_items = process_data(files_paths_s3, spark)
    dfs_to_s3(final_dfs, config['s3']['bucket_name'], config['s3']['processed_prefix'])
    txt_to_s3(summary_items[0], summary_items[1], config['s3']['bucket_name'], config['s3']['summaries_prefix'], config['local_save']['folder_name'])


if __name__ == '__main__':
    define_spark()
    process_weather_data(['s3a://weather-data-aws/raw/2025-06-10/tel_aviv_weather.json', 's3a://weather-data-aws/raw/2025-06-10/jerusalem_weather.json', 's3a://weather-data-aws/raw/2025-06-10/haifa_weather.json', 's3a://weather-data-aws/raw/2025-06-10/ashdod_weather.json'])