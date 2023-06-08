# The purpose of this module is to gather macrozone and neighbourhood IDs
# for province capitals in Italy. The IDs are needed to call the API.

import pandas as pd
import requests
import tqdm
import sys
import logging

logging.basicConfig(level=logging.INFO)

CITY_LIST = "./table_builder/italy_citylist.txt"
AUTOCOMPLETE_ENDPOINT = "https://www.immobiliare.it/search/autocomplete"


def call_autocomplete_API(query: str, s: requests.Session) -> dict:
    payload = {
        "macrozones": 1,
        "microzones": 1,
        "min_level": 9,
        "query": query,
        "withRegions": True,
        "withCountries": True,
        "international": True,
    }

    try:
        response = s.get(AUTOCOMPLETE_ENDPOINT, params=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print(f"An error occurred: {err}")
        sys.exit(1)

    return response.json()


def parse_macrozone_data(city_info: dict) -> pd.DataFrame:
    city_name = city_info["label"]
    city_id = city_info["id"]
    province_name = city_info["parents"][0]["label"]
    province_id = city_info["parents"][0]["id"]
    region_name = city_info["parents"][1]["label"]
    region_id = city_info["parents"][1]["id"]

    def create_row(macrozone=None, child=None):
        row = {
            "region_name": region_name,
            "region_id": region_id,
            "province_name": province_name,
            "province_id": province_id,
            "city_name": city_name,
            "city_id": city_id,
            "macrozone_name": "" if macrozone is None else macrozone["label"],
            "macrozone_keyurl": "" if macrozone is None else macrozone["keyurl"],
            "macrozone_id": 0 if macrozone is None else macrozone["id"],
            "neighbourhood_name": "" if child is None else child["label"],
            "neighbourhood_id": 0 if child is None else child["id"],
        }
        return row

    if not city_info.get("macrozones"):
        return pd.DataFrame([create_row()])

    rows = []
    for macrozone in city_info["macrozones"]:
        for child in macrozone["children"]:
            rows.append(create_row(macrozone, child))

    return pd.DataFrame(rows)


def log_missing(missing_cities):
    if missing_cities:
        logging.info(
            f"Found {len(missing_cities)} missing cities: {', '.join(missing_cities)}"
        )
    else:
        logging.info("All cities found!")


def build_index_table():
    with open(CITY_LIST, "r") as f:
        cities = f.readlines()
        cities = [x.strip() for x in cities]

    s = requests.Session()

    df = pd.DataFrame(
        columns=[
            "region_name",
            "region_id",
            "province_name",
            "province_id",
            "city_name",
            "city_id",
            "macrozone_name",
            "macrozone_id",
            "neighbourhood_name",
            "neighbourhood_id",
        ]
    )

    missing_cities = []

    for city in tqdm.tqdm(cities, desc="Getting city info", smoothing=0.005):
        response = call_autocomplete_API(city, s)
        data = next(
            (item for item in response if item.get("admin_centre") == True), None
        )
        if data:
            df = df._append(parse_macrozone_data(data), ignore_index=True)
        else:
            missing_cities.append(city)

    df = df.astype({"city_id": int, "macrozone_id": int, "neighbourhood_id": int})
    df.to_csv("./table_builder/index_table.csv", index=False)

    logging.info(
        f"Saved data for {len(cities) - len(missing_cities)}/{len(cities)} cities"
    )

    log_missing(missing_cities)


if __name__ == "__main__":
    pass
