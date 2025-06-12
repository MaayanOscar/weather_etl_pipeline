import os
import json
import requests
from dotenv import load_dotenv


def save_weather_to_file(city, response, output_folder_name):
    output_dir = os.path.join(os.path.dirname(__file__), output_folder_name)
    os.makedirs(output_dir, exist_ok=True)

    filename = f"{city.lower().replace(' ', '_')}_weather.json"
    file_path = os.path.join(output_dir, filename)

    with open(file_path, 'w') as f:
        json.dump(response, f, indent=2)
    return file_path


def create_json_weather(config):
    files_paths = []
    load_dotenv()
    api_key = os.getenv("WEATHER_API_KEY")
    cities = config['general']['cities']
    output_folder_name = config['local_save']['folder_name']

    for city in cities:
        url = f"{config['api']['base_url']}/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        response = requests.get(url).json()
        files_paths.append(save_weather_to_file(city, response, output_folder_name))
    return files_paths
