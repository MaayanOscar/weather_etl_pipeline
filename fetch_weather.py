import os
import json
import requests
from dotenv import load_dotenv


def save_weather_to_file(city, response):
    output_dir = os.path.join(os.path.dirname(__file__), 'weather_data')
    os.makedirs(output_dir, exist_ok=True)

    filename = f"{city.lower().replace(' ', '_')}_weather.json"
    file_path = os.path.join(output_dir, filename)

    with open(file_path, 'w') as f:
        json.dump(response, f, indent=2)


def create_json_weather():
    load_dotenv()
    api_key = os.getenv("WEATHER_API_KEY")
    cities = ['Tel Aviv', 'Jerusalem', 'Haifa', 'Ashdod']

    for city in cities:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        response = requests.get(url).json()
        save_weather_to_file(city, response)


if __name__ == '__main__':
    create_json_weather()
