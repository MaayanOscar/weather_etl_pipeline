import os
import json
import requests
from dotenv import load_dotenv


def save_weather_to_file(city, response, output_folder_name):
    '''
    Saves the API weather response to a local JSON file.

    :param city: The name of the city.
    :param response: The JSON response from the weather API.
    :param output_folder_name: The name of the folder where the file will be saved.
    :return: The full file path where the data was saved.
    '''

    # Create output directory if it doesn't exist
    output_dir = os.path.join(os.path.dirname(__file__), output_folder_name)
    os.makedirs(output_dir, exist_ok=True)

    # Create a file name based on city name
    filename = f"{city.lower().replace(' ', '_')}_weather.json"
    file_path = os.path.join(output_dir, filename)

    # Save the weather API response to the file as JSON.
    with open(file_path, 'w') as f:
        json.dump(response, f, indent=2)

    return file_path


def create_json_weather(config):
    '''
    Gets the current weather for a list of cities and saves the results as JSON files.

    :param config: Configuration dictionary loaded from YAML file.
    :return: List with the file paths of the saved JSON files.
    '''

    files_paths = []

    # Load variables from the .env file
    load_dotenv()
    api_key = os.getenv("WEATHER_API_KEY")

    # Read list of cities and output folder name from the config dictionary
    cities = config['general']['cities']
    output_folder_name = config['local_save']['data_folder_name']

    # Loop through each city, get its weather, and save it to JSON file
    for city in cities:
        url = f"{config['api']['base_url']}/data/2.5/weather?q={city}&appid={api_key}&units=metric"

        # Make a GET request and parse the response as JSON
        response = requests.get(url).json()

        files_paths.append(save_weather_to_file(city, response, output_folder_name))

    return files_paths
