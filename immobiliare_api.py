import pandas as pd
import requests
import glob

AUTOCOMPLETE_ENDPOINT = 'https://www.immobiliare.it/search/autocomplete'
LISTINGS_ENDPOINT = 'https://www.immobiliare.it/api-next/search-list/real-estates/'

## Get city ID

def create_payload(query: str) -> dict:
    """
    Create payload for city info API call.
    
    Args:
        query (str): The city name.
    
    Returns:
        dict: The payload for the API request.
    """
    payload = {
        "macrozones": 1,
        "microzones": 1,
        "min_level": 9,
        "query": query,
        "withRegions": True,
        "withCountries": True,
        "international": True
    }
    return payload


def get_response(payload: dict) -> dict:
    """
    Make a GET request to the city info API and return the JSON response.
    
    Args:
        payload (dict): The payload for the API request.
    
    Raises:
        SystemExit: If the request fails for any reason, including network errors, timeouts, or the servers being down.
    
    Returns:
        dict: The JSON response from the API.
    """
    try:
        response = requests.get(AUTOCOMPLETE_ENDPOINT, params=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print(f"API request failed due to {err}")
        raise SystemExit(err)

    return response.json()


def extract_city_info(data: dict) -> dict:
    """
    Extract the city information from the API response data.
    
    Args:
        data (dict): The JSON response from the API.
    
    Raises:
        KeyError: If no city information could be found in the data.
    
    Returns:
        dict: The city information.
    """
    city_info = next((item for item in data if item.get("admin_centre")), None)

    if city_info is None:
        print(f"No city information found in the data.")
        raise KeyError("City not found")

    return city_info


def call_autocomplete_API(query: str) -> dict:
    """
    Call city info API and extract city information from the API response.
    
    Args:
        query (str): The city name.
    
    Returns:
        dict: The city information.
    """
    payload = create_payload(query)
    data = get_response(payload)
    city_info = extract_city_info(data)

    return city_info


