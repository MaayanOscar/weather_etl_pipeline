from upload_to_aws import jsons_to_s3
from utils.load_config import load_config
from fetch_weather import create_json_weather
from process_with_pyspark import process_weather_data
from weather_dashboard import show_dashboard


def main():
    """
    Executes the complete ETL pipeline for weather data collection, processing, and visualization.

    Workflow:
    1. Load configuration settings from a YAML file.
    2. Fetch current weather data from the API for multiple cities and save locally as JSON files.
    3. Upload the JSON files to an AWS S3 bucket.
    4. Process the weather data using PySpark, performing necessary transformations and calculations.
    5. Upload the processed data and a summary report to S3.
    6. Generate and display an interactive dashboard with Streamlit and create an HTML version, which is also uploaded to S3.

    Returns:
        None
    """

    config = load_config()

    files_paths = create_json_weather(config)
    files_paths_s3 = jsons_to_s3(files_paths, config)
    final_dfs, summary_items = process_weather_data(files_paths_s3, config)
    show_dashboard(final_dfs, summary_items[0], config)


if __name__ == '__main__':
    main()
