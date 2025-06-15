import os
import sys
import builtins
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Row
from upload_to_aws import dfs_to_s3, txt_to_s3
from pyspark.sql.functions import col, lit, concat, to_date, from_unixtime, concat_ws, avg, round as spark_round


def define_spark():
    '''
    Sets up environment variables and returns SparkSession for AWS S3 access.

    :return: SparkSession object with AWS.
    '''

    # Load AWS credentials from environment
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    # Create and return Spark session with S3 configuration
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
    '''
    Reads the weather data from S3 JSON files, processes it, and returns a cleaned DataFrame and summary.

    :param files_paths_s3: List of paths to the JSON files stored in S3.
    :param spark: SparkSession object used to read and transform the data.
    :return:
        - Dictionary with the processed DataFrame named "Daily forecast"
        - List containing a summary text and summary DataFrame
    '''

    all_dfs = {}

    # Load each JSON file from S3 to DataFrame and store it in a dictionary
    for file in files_paths_s3:
        file_name = os.path.splitext(os.path.basename(file))[0]
        df = spark.read.json(file, multiLine=True)
        all_dfs[file_name] = df

    combined_df = None

    # extract relevant columns and add "city" column for each city's DataFrame
    for city_name, df in all_dfs.items():
        df_with_city = df.withColumn("city", lit(city_name.capitalize()))
        filtered_df = df_with_city.select(
            col("name").alias("city"),
            col("dt"),
            col("main.temp"),
            col("main.feels_like"),
            col("main.temp_min"),
            col("main.temp_max"),
            col("main.humidity"),
            col("clouds.all").alias("clouds"))

        # Move the 'city' column to be firs
        cols = filtered_df.columns
        reordered_cols = ['city'] + [column for column in cols if column != 'city']
        filtered_df = filtered_df.select(reordered_cols)

        # Merge the DataFreames into one combined DataFrame
        combined_df = filtered_df if combined_df is None else combined_df.unionByName(filtered_df,
                                                                                      allowMissingColumns=True)

    # Add rounded versions of numeric columns and create temp range
    combined_df = combined_df.withColumn("round_temp", spark_round(col("temp"), 1)) \
        .withColumn("round_feels_like", spark_round(col("feels_like"), 1)) \
        .withColumn("round_temp_min", spark_round(col("temp_min"), 1)) \
        .withColumn("round_temp_max", spark_round(col("temp_max"), 1)) \
        .withColumn("round_humidity", spark_round(col("humidity"), 0)) \
        .withColumn("round_clouds", spark_round(col("clouds"), 0)) \
        .withColumn("temp_range", concat_ws(" - ", col("round_temp_min"), col("round_temp_max")))

    # Select final columns and change names
    final_df_numbers = combined_df.select(
        col("city"),
        to_date(from_unixtime(col("dt"))).alias("date"),
        col("round_temp").alias("temp"),
        col("round_feels_like").alias("feels_like"),
        col("temp_range"),
        col("round_humidity").alias("humidity"),
        col("round_clouds").alias("clouds"))

    # Calculate statistics for summary
    hottest = final_df_numbers.orderBy(col("temp").desc()).first()
    coldest = final_df_numbers.orderBy(col("temp").asc()).first()
    most_humid = final_df_numbers.orderBy(col("humidity").desc()).first()
    cloudiest = final_df_numbers.orderBy(col("clouds").desc()).first()

    stats = final_df_numbers.agg(
        avg("temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    ).collect()[0]

    avg_temp = builtins.round(stats["avg_temp"], 1) if stats["avg_temp"] is not None else None
    avg_humidity = builtins.round(stats["avg_humidity"], 1) if stats["avg_humidity"] is not None else None

    # Final formatted DataFrame for upload
    final_df = final_df_numbers.select(
        col("city"),
        col("date"),
        concat(col("temp").cast("int"), lit("°C")).alias("temp"),
        concat(col("feels_like").cast("int"), lit("°C")).alias("feels_like"),
        col("temp_range"),
        concat(col("humidity").cast("int"), lit("%")).alias("humidity"),
        concat(col("clouds").cast("int"), lit("%")).alias("clouds"))

    # Create weather summary string
    summary_text = f"""Weather Summary – Today

🔥 Hottest city: {hottest['city']} (temp: {hottest['temp']}°C)
❄️ Coldest city: {coldest['city']} (temp: {coldest['temp']}°C)
💧 Most humid city: {most_humid['city']} (humidity: {most_humid['humidity']}%)
⛅ Cloudiest city: {cloudiest['city']} (clouds: {cloudiest['clouds']}%)

Average temperature: {avg_temp}°C | Average humidity: {avg_humidity}%
"""

    # Create Spark DataFrame version of the summary
    summary_df = spark.createDataFrame([Row(
        hottest_city=hottest["city"],
        coldest_city=coldest["city"],
        most_humid_city=most_humid["city"],
        cloudiest_city=cloudiest["city"],
        avg_temp=avg_temp,
        avg_humidity=avg_humidity)])

    return {"Daily forecast": final_df}, [summary_text, summary_df]


def process_weather_data(files_paths_s3, config):
    '''
    Runs the full Spark pipeline: defines Spark session, processes weather files, uploads results to S3.

    :param files_paths_s3: List of S3 paths to JSON weather data files.
    :param config: Configuration dictionary loaded from YAML.
    :return:
        - final_df: Dictionary with processed DataFrame.
        - summary_items: List containing summary text and summary DataFrame.
    '''

    spark = define_spark()
    final_df, summary_items = process_data(files_paths_s3, spark)

    # Upload results to S3 (processed and summary)
    dfs_to_s3(
        final_df,
        config['s3']['bucket_name'],
        config['s3']['processed_prefix'])

    txt_to_s3(
        summary_items[0],
        summary_items[1],
        config['s3']['bucket_name'],
        config['s3']['summaries_prefix'],
        config['local_save']['output_folder_name'])

    return final_df, summary_items
