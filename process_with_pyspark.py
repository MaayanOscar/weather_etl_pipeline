import os
import sys
import builtins
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Row
from upload_to_aws import dfs_to_s3, txt_to_s3
from pyspark.sql.functions import col, lit, concat, to_date, from_unixtime, concat_ws, avg, round as spark_round


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
    all_dfs = {}

    for file in files_paths_s3:
        file_name = os.path.splitext(os.path.basename(file))[0]
        df = spark.read.json(file, multiLine=True)
        all_dfs[file_name] = df

    combined_df = None
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

        cols = filtered_df.columns
        reordered_cols = ['city'] + [column for column in cols if column != 'city']
        filtered_df = filtered_df.select(reordered_cols)
        combined_df = filtered_df if combined_df is None else combined_df.unionByName(filtered_df, allowMissingColumns=True)

    combined_df = combined_df.withColumn("round_temp", spark_round(col("temp"), 1)) \
        .withColumn("round_feels_like", spark_round(col("feels_like"), 1)) \
        .withColumn("round_temp_min", spark_round(col("temp_min"), 1)) \
        .withColumn("round_temp_max", spark_round(col("temp_max"), 1)) \
        .withColumn("round_humidity", spark_round(col("humidity"), 0)) \
        .withColumn("round_clouds", spark_round(col("clouds"), 0)) \
        .withColumn("temp_range", concat_ws(" - ", col("round_temp_min"), col("round_temp_max")))

    final_df_numbers = combined_df.select(
        col("city"),
        to_date(from_unixtime(col("dt"))).alias("date"),
        col("round_temp").alias("temp"),
        col("round_feels_like").alias("feels_like"),
        col("temp_range"),
        col("round_humidity").alias("humidity"),
        col("round_clouds").alias("clouds"))

    hottest = final_df_numbers.orderBy(col("temp").desc()).first()
    coldest = final_df_numbers.orderBy(col("temp").asc()).first()
    most_humid = final_df_numbers.orderBy(col("humidity").desc()).first()
    cloudiest = final_df_numbers.orderBy(col("clouds").desc()).first()

    stats = final_df_numbers.agg(avg("temp").alias("avg_temp"), avg("humidity").alias("avg_humidity")).collect()[0]

    avg_temp = builtins.round(stats["avg_temp"], 1) if stats["avg_temp"] is not None else None
    avg_humidity = builtins.round(stats["avg_humidity"], 1) if stats["avg_humidity"] is not None else None

    final_df = final_df_numbers.select(
        col("city"),
        col("date"),
        concat(col("temp").cast("int"), lit("°C")).alias("temp"),
        concat(col("feels_like").cast("int"), lit("°C")).alias("feels_like"),
        col("temp_range"),
        concat(col("humidity").cast("int"), lit("%")).alias("humidity"),
        concat(col("clouds").cast("int"), lit("%")).alias("clouds"))

    summary_text = f"""Weather Summary – Today

🔥 Hottest city: {hottest['city']} (temp: {hottest['temp']}°C)
❄️ Coldest city: {coldest['city']} (temp: {coldest['temp']}°C)
💧 Most humid city: {most_humid['city']} (humidity: {most_humid['humidity']}%)
⛅ Cloudiest city: {cloudiest['city']} (clouds: {cloudiest['clouds']}%)

Average temperature: {avg_temp}°C | Average humidity: {avg_humidity}%
"""

    summary_df = spark.createDataFrame([
        Row(
            hottest_city=hottest["city"],
            coldest_city=coldest["city"],
            most_humid_city=most_humid["city"],
            cloudiest_city=cloudiest["city"],
            avg_temp=avg_temp,
            avg_humidity=avg_humidity)])

    return {"Daily forecast": final_df}, [summary_text, summary_df]


def process_weather_data(files_paths_s3, config):
    spark = define_spark()
    final_dfs, summary_items = process_data(files_paths_s3, spark)
    dfs_to_s3(final_dfs, config['s3']['bucket_name'], config['s3']['processed_prefix'])
    txt_to_s3(summary_items[0], summary_items[1], config['s3']['bucket_name'], config['s3']['summaries_prefix'], config['local_save']['folder_name'])
    return final_dfs, summary_items
