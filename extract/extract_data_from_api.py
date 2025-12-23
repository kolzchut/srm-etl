from typing import Union
import requests


def fetch_json_from_api(api_url: str) -> Union[dict, list, None]:
    """
    Sends a GET request to the given API URL and returns the JSON data.

    :param api_url: API endpoint URL.
    :return: JSON response as dict or list, or None if failed.
    """
    try:
        response = requests.get(api_url, headers={'User-Agent': 'generic-api-client'})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    except ValueError as e:
        print(f"JSON decode error: {e}")
        print(f"Response text: {response.text}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

