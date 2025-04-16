import requests
import logging
import time
from datetime import datetime

# Logger setup
logger = logging.getLogger('fetching_data_logger')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API Configuration
api_key = "YOUR_API_KEY"
base_url = "https://api.openweathermap.org/data/3.0/onecall"


def get_weather(lat, lon, units='metric', lang='en'):
    """
    Fetches weather data for a given latitude and longitude using OpenWeatherMap API.
    """
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': units,
        'lang': lang
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        logging.info(f'Weather data fetched successfully for coordinates ({lat}, {lon})')
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err} - Status code: {response.status_code}')
    except requests.exceptions.ConnectionError:
        logging.error('Connection error occurred')
    except requests.exceptions.Timeout:
        logging.error('Request timed out')
    except requests.exceptions.RequestException as e:
        logging.error(f'An unexpected error occurred: {e}')
    
    return None


def fetch_all_cities_weather(cities, units='metric', lang='en'):
    """
    Fetches weather data for multiple cities and returns the data in a list.
    """
    all_weather_data = []

    for city in cities:
        logging.info(f'Fetching weather for {city["name"]}')
        data = get_weather(city['lat'], city['lon'], units=units, lang=lang)

        if data:
            data['city'] = city['name']
            all_weather_data.append(data)

        # Sleep to avoid hitting rate limits
        time.sleep(1)

    return all_weather_data


def main():
    """
    Main function to fetch weather data for all cities and log the result.
    """
    weather_data = fetch_all_cities_weather(cities, units='metric', lang='de')
    logging.info(f'Total cities fetched: {len(weather_data)}')


if __name__ == "__main__":
    main()
