import pandas as pd
import traveltimepy as ttpy
import os
from dotenv import load_dotenv
load_dotenv()

def call_traveltime_api(_locations):
    _departures = [location for location in _locations if location["id"] != 'arrival']
    _arrival = [location for location in _locations if location["id"] == 'arrival']
    arrival_search = {
        "id": "backward search example",
        "departure_location_ids": [departure["id"] for departure in _departures],
        "arrival_location_id": _arrival[0]["id"],
        "transportation": {"type": "public_transport"},
        "arrival_time": '2022-04-13T07:00:00.000Z',
        "travel_time": 7200,
        "properties": ["travel_time"]
    }
    return ttpy.time_filter(locations=_locations, arrival_searches=arrival_search)
