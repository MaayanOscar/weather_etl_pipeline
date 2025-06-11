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


# def process_data(files_paths_s3):
#     final_dfs = {}
#     access_key = os.getenv("AWS_ACCESS_KEY_ID")
#     secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
#     # spark = SparkSession.builder \
#     #     .appName("S3Uploader") \
#     #     .master("local[*]") \
#     #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     #     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
#     #     .config("spark.hadoop.io.native.lib.available", "false").getOrCreate()
#     # from pyspark.sql import SparkSession
#
#     # spark = SparkSession.builder \
#     #     .appName("S3Uploader") \
#     #     .master("local[*]") \
#     #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     #     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
#     #     .config("spark.hadoop.io.native.lib.available", "false") \
#     #     .config("spark.local.dir", r"C:\tmp\hadoop-שירז").getOrCreate()
#
#     # spark = SparkSession.builder \
#     #     .appName("MyApp") \
#     #     .config("spark.hadoop.hadoop.tmp.dir", "C:/tmp/hadoop-שירז") \
#     #     .config("spark.hadoop.io.native.lib.available", "false") \
#     #     .config("spark.hadoop.hadoop.native.lib", "false") \
#     #     .getOrCreate()
#
#     # spark = SparkSession.builder \
#     #     .appName("ReadFromS3") \
#     #     .master("local[*]") \
#     #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     #     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
#     #     .getOrCreate()
#     # spark = SparkSession.builder \
#     #     .appName("ReadFromS3") \
#     #     .master("local[*]") \
#     #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     #     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
#     #     .config("spark.hadoop.io.native.lib.available", "false") \
#     #     .getOrCreate()
#
#     # spark = SparkSession.builder \
#     #     .appName("Weather ETL") \
#     #     .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_PUBLIC_KEY")) \
#     #     .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AES_SECRET_KEY")) \
#     #     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
#     #     .getOrCreate()
#
#     # spark = SparkSession.builder \
#     #     .appName("Weather ETL") \
#     #     .config("spark.hadoop.io.native.lib.available", "false") \
#     #     .config("spark.local.dir", r"C:\\temp") \
#     #     .getOrCreate()
#
#     # spark = SparkSession.builder \
#     #         .appName("MyETLPipeline") \
#     #         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     #         .config("spark.hadoop.fs.s3a.access.key", access_key) \
#     #         .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
#     #         .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
#     #         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     #         .config("spark.hadoop.fs.s3a.fast.upload", "true") \
#     #         .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
#     #         .config("spark.sql.parquet.compression.codec", "snappy") \
#     #         .getOrCreate()
#
#     spark = SparkSession.builder \
#         .appName("MyETLPipeline") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.access.key", access_key) \
#         .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
#         .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.fast.upload", "true") \
#         .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
#         .config("spark.sql.parquet.compression.codec", "snappy") \
#         .config("spark.hadoop.io.native.lib.available", "false") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A") \
#         .config("spark.hadoop.hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping") \
#         .getOrCreate()
#
#     print(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
#
#     for file in files_paths_s3:
#         file_name = os.path.splitext(os.path.basename(file))[0]
#         print(file_name)
#         df = spark.read.json(file, multiLine=True)
#         # df.show()
#         # df.printSchema()
#
#         # temp | feels_like | temp_min | temp_max
#         df = df.withColumn("round_temp", round(col("main.temp")))
#         df = df.withColumn("round_feels_like", round(col("main.feels_like")))
#         df = df.withColumn("round_temp_min", round(col("main.temp_min")))
#         df = df.withColumn("round_temp_max", round(col("main.temp_max")))
#         df = df.withColumn("temp_range", concat_ws(" - ", col("round_temp_min"), col("round_temp_max")))
#
#         filtered_df = df.select(
#             col("name").alias("city"),
#             # col("sys.country").alias("country"),
#             from_unixtime("dt").alias("datetime"),
#             # col("coord.lat"),
#             # col("coord.lon"),
#             col("round_temp").alias("temp"),
#             col("round_feels_like").alias("feels_like"),
#             col("temp_range"),
#             # col("round_temp_min").alias("temp_min"),
#             # col("round_temp_max").alias("temp_max"),
#             col("main.humidity"),
#             # col("main.pressure"),
#             # col("wind.speed").alias("wind_speed"),
#             # col("wind.deg").alias("wind_deg"),
#             col("clouds.all").alias("clouds")
#             # col("temp_range")
#             # col("weather")[0]["main"].alias("weather_main"),
#             # col("weather")[0]["description"].alias("weather_description")
#         )
#         filtered_df.show()
#         final_dfs[file_name] = filtered_df
#     return final_dfs


# def process_data(files_paths_s3):
#     final_dfs = {}
#     access_key = os.getenv("AWS_ACCESS_KEY_ID")
#     secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
#
#     spark = SparkSession.builder \
#         .appName("MyETLPipeline") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.access.key", access_key) \
#         .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
#         .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.fast.upload", "true") \
#         .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
#         .config("spark.sql.parquet.compression.codec", "snappy") \
#         .config("spark.hadoop.io.native.lib.available", "false") \
#         .getOrCreate()
#
#     for file in files_paths_s3:
#         file_name = os.path.splitext(os.path.basename(file))[0]
#         df = spark.read.json(file, multiLine=True)
#
#         df = df.withColumn("round_temp", round(col("main.temp"))) \
#                .withColumn("round_feels_like", round(col("main.feels_like"))) \
#                .withColumn("round_temp_min", round(col("main.temp_min"))) \
#                .withColumn("round_temp_max", round(col("main.temp_max"))) \
#                .withColumn("temp_range", concat_ws(" - ", col("round_temp_min"), col("round_temp_max")))
#
#         filtered_df = df.select(
#             col("name").alias("city"),
#             from_unixtime("dt").alias("datetime"),
#             col("round_temp").alias("temp"),
#             col("round_feels_like").alias("feels_like"),
#             col("temp_range"),
#             col("main.humidity"),
#             col("clouds.all").alias("clouds")
#         )
#         final_dfs[file_name] = filtered_df
#
#     # Combine all cities
#     combined_df = None
#     for df in final_dfs.values():
#         if combined_df is None:
#             combined_df = df
#         else:
#             combined_df = combined_df.unionByName(df)
#
#     # Calculate statistics
#     hottest = combined_df.orderBy(col("temp").desc()).first()
#     coldest = combined_df.orderBy(col("temp").asc()).first()
#     most_humid = combined_df.orderBy(col("humidity").desc()).first()
#     cloudiest = combined_df.orderBy(col("clouds").desc()).first()
#
#     # Collect average stats as Python values
#     stats_row = combined_df.agg(
#         avg("temp").alias("avg_temp"),
#         avg("humidity").alias("avg_humidity")
#     ).collect()[0]
#
#     # Extract values from Row object
#     avg_temp = round(stats_row["avg_temp"], 1)
#     avg_humidity = round(stats_row["avg_humidity"], 1)
#
#     # Create summary text
#     summary_text = f"""\
#     Weather Summary – Today
#
#     🔥 Hottest city: {hottest['city']} (temp: {hottest['temp']}°C)
#     ❄️ Coldest city: {coldest['city']} (temp: {coldest['temp']}°C)
#     💧 Most humid city: {most_humid['city']} (humidity: {most_humid['humidity']}%)
#     🌥 Cloudiest city: {cloudiest['city']} (clouds: {cloudiest['clouds']}%)
#
#     Average temperature: {avg_temp}°C | Average humidity: {avg_humidity}%
#     """
#
#     print(summary_text)
#
#     # Add summary to final_dfs as a one-row DataFrame
#     summary_row = Row(
#         hottest_city=hottest["city"],
#         coldest_city=coldest["city"],
#         most_humid_city=most_humid["city"],
#         cloudiest_city=cloudiest["city"],
#         avg_temp=round(stats_row['avg_temp'], 1),
#         avg_humidity=round(stats_row['avg_humidity'], 1)
#     )
#     summary_df = spark.createDataFrame([summary_row])
#     # final_dfs["weather_summary"] = summary_df
#
#     return final_dfs, [summary_text, summary_df]

def process_data(files_paths_s3):
    import os
    from pyspark.sql import SparkSession, Row
    from pyspark.sql.functions import col, from_unixtime, concat_ws, avg, round as spark_round
    import builtins  # for using Python's built-in round()

    final_dfs = {}
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
    define_spark()
    final_dfs, summary_items = process_data(files_paths_s3)
    dfs_to_s3(final_dfs, config['s3']['bucket_name'], config['s3']['processed_prefix'])
    txt_to_s3(summary_items[0], summary_items[1], config['s3']['bucket_name'], config['s3']['summaries_prefix'], config['local_save']['folder_name'])


if __name__ == '__main__':
    define_spark()
    process_weather_data(['s3a://weather-data-aws/raw/2025-06-10/tel_aviv_weather.json', 's3a://weather-data-aws/raw/2025-06-10/jerusalem_weather.json', 's3a://weather-data-aws/raw/2025-06-10/haifa_weather.json', 's3a://weather-data-aws/raw/2025-06-10/ashdod_weather.json'])