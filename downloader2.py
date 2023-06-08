import json
import pandas as pd
import requests
import glob
import pathlib
import time
import tempfile
import re
import pprint
import tqdm
import concurrent.futures
import logging 

AUTOCOMPLETE_ENDPOINT = "https://www.immobiliare.it/search/autocomplete"
LISTINGS_ENDPOINT = "https://www.immobiliare.it/api-next/search-list/real-estates/"
ID_CONTRATTO = 1  # 1 = SALES, 2 = RENTALS


def generate_payloads(index: dict) -> dict:
    """
    Generate API payloads based on the information contained in a CSV file.

    Args:
        row (pd.Series): A pandas series containing the data.

    Returns:
        dict: An API payload.
    """
    payload = {
        "fkRegione": index["region_id"],
        "idProvincia": index["province_id"],
        "idComune": int(index["city_id"]),
        "idNazione": "IT",
        "idContratto": ID_CONTRATTO,
        "idCategoria": 1,
        "criterio": "rilevanza",
        "noAste": 1,
        "__lang": "it",
        "pag": index.get("page_num", 0),
        "paramsCount": 1,
        "path": "%2F",
    }

    #check macrozone id is not nan
    if index["macrozone_id"] != 0:
        payload.update(
            {
                "idMZona[0]": int(index["macrozone_id"]),
                "idQuartiere[0]": int(index["neighbourhood_id"]),
            }
        )

    return payload


##def build_indexes(macrozone_df: pd.DataFrame) -> list:
##    indexes = []
##    with requests.Session() as session:
##        for _, row in tqdm.tqdm(
##            macrozone_df.iterrows(),
##            total=len(macrozone_df),
##            desc="Building indexes",
##            smoothing=0.05,
##        ):
##            response = session.get(LISTINGS_ENDPOINT, params=generate_payloads(row))
##
##            errors = []
##            
##            try: 
##                data = response.json()
##            except json.decoder.JSONDecodeError:
##                errors.append((row["city_name"], row["macrozone_name"]))
##                
##                
##            max_pages = data.get("maxPages", 0)
##            for page in range(1, max_pages + 1):
##                indexes.append(
##                    {
##                        "region_id": row.get("region_id", None),
##                        "province_id": row.get("province_id", None),
##                        "city_id": row.get("city_id", None),
##                        "macrozone_id": row.get("macrozone_id", None),
##                        "neighbourhood_id": row.get("neighbourhood_id", None),
##                        "page_num": page,
##                    }
##                )
##        logging.info(f"Could not parse the following cities: {errors.join(', ')}")  
##    return indexes


def get_data(row: pd.Series, session: requests.Session) -> list:
    response = session.get(LISTINGS_ENDPOINT, params=generate_payloads(row))
    try: 
        data = response.json()
        #print(f"URL: \t{response.url}")
        #pprint.pprint(data)
        max_pages = data.get("maxPages", 0)
        indexes = []
        for page in range(1, max_pages + 1):
            indexes.append(
                {
                    "region_id": row.get("region_id", None),
                    "province_id": row.get("province_id", None),
                    "city_id": row.get("city_id", None),
                    "macrozone_id": row.get("macrozone_id", None),
                    "neighbourhood_id": row.get("neighbourhood_id", None),
                    "page_num": page,
                }
            )
        return indexes
    except json.decoder.JSONDecodeError:
        return (row["city_name"], row["macrozone_name"])

def build_indexes(macrozone_df: pd.DataFrame) -> list:
    indexes = []
    errors = []
    with requests.Session() as session:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(get_data, row, session): row for _, row in macrozone_df.iterrows()}
            for future in tqdm.tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Building indexes", smoothing=0.05):
                result = future.result()
                if isinstance(result, list):
                    indexes.extend(result)
                else:
                    errors.append(result)
    ## log a list of cities that could not be parsed
    if errors:
        logging.info(f"Could not parse {len(errors)} macrozones:")
        for error in errors:
            logging.info(f"{error[0]} {error[1]}")

    return indexes


def download_listings_page(
    index: dict, session: requests.Session, save_path: pathlib.Path
) -> None:
    response = session.get(LISTINGS_ENDPOINT, params=generate_payloads(index))
    with open(
        save_path
        / f"{index['region_id']}_{index['province_id']}_{index['city_id']}_{index['macrozone_id']}_{index['neighbourhood_id']}_{index['page_num']}.json",
        "w",
    ) as f:
        json.dump(response.json(), f)


def download_listings(indexes: list) -> None:
    save_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/json/")
    save_path.mkdir(parents=True, exist_ok=True)

    with requests.Session() as session:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(download_listings_page, index, session, save_path)
                for index in indexes
            ]
            for future in tqdm.tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Downloading listings",
                smoothing=0.05,
            ):
                try:
                    future.result()
                except Exception as e:
                    print(f"Exception occurred in worker thread: {e}")


def parse_result(result: dict) -> dict:
    real_estate = result.get("realEstate", {})
    properties = real_estate.get("properties", [{}])[0]
    location = properties.get("location", {})
    price = real_estate.get("price", {})
    floor_info = properties.get("floor", {})
    typology = properties.get("typology", {})

    id = real_estate.get("id")
    city = location.get("city")
    macrozone = location.get("macrozone")
    neighbourhood = location.get("microzone")
    price_value = price.get("value")
    surface_string = properties.get("surface")
    rooms_string = properties.get("rooms")
    floor = floor_info.get("abbreviation")
    type = typology.get("name")

    surface = int(re.search(r"-?\d+(\.\d+)?", str(surface_string)).group().replace(".", "")) if surface_string else None
    rooms = int(re.search(r"-?\d+(\.\d+)?", str(rooms_string)).group()) if rooms_string else None

    price_per_sqm = price_value / surface if price_value and surface else None

    return {
        "id": id,
        "city": city,
        "macrozone": macrozone,
        "neighbourhood": neighbourhood,
        "price": price_value,
        "price_per_sqm": price_per_sqm,
        "surface": surface,
        "rooms": rooms,
        "floor": floor,
        "type": type,
    }
    

def parse_listings_page(file_path: pathlib.Path) -> pd.DataFrame:
    with open(file_path, "r") as f:
        try:
            data = json.load(f)
        except json.decoder.JSONDecodeError:
            print(f"Could not parse {file_path}")
            return pd.DataFrame()
            
        results = data.get("results", [])
        parsed_results = [parse_result(result) for result in results]
        df = pd.DataFrame(parsed_results)
        return df
    
    
def compile_city_tables() -> None:
    #build a list of unique region-province combinations
    #indexes = {(index["region_id"], index["province_id"]) for index in indexes}
    # group the files by region-province
    json_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/json/")
    json_files = [path for path in json_path.glob("*.json")]
    indexes = {path.stem[:6] for path in json_files}
    save_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/csv/")
    save_path.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame()
    for index in tqdm.tqdm(indexes, desc="Compiling city tables"):
        # select only the json files that have the same city
        json_files = [
            path
            for path in json_path.glob("*.json")
            if path.stem[:6] == index
        ]

        dfs = [parse_listings_page(json_file) for json_file in json_files]
        df = pd.concat(dfs)
    # drop rows with no price or no surface
    df = df.dropna(subset=["price", "surface"])
    df.to_csv(save_path / f"{index}.csv", index=False)


def compile_macrozone_summary_table():
    csv_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/csv/")
    save_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/out/")
    save_path.mkdir(parents=True, exist_ok=True)
    macrozone_summary = pd.DataFrame(
            columns=[
                "city_name",
                "macrozone_name",
                "price_mean",
                "price_median",
                "price_std",
                "price_min",
                "price_max",
                "price_q50",
                "price_q90",
                "price_per_sqm_mean",
                "price_per_sqm_median",
                "price_per_sqm_std",
                "price_per_sqm_min",
                "price_per_sqm_max",
                "price_per_sqm_q50",
                "price_per_sqm_q90",
                "surface_mean",
                "surface_median",
                "surface_std",
                "surface_min",
                "surface_max",
                "surface_q50",
                "surface_q90",
            ]
        )
    
    def q50(x):
        return x.quantile(0.5)

    def q90(x):
        return x.quantile(0.9)
            
    for csv_file in tqdm.tqdm(csv_path.glob("*.csv"), desc="Compiling macrozone summary table", smoothing=0.05, total=len(list(csv_path.glob("*.csv")))):
        city_data = pd.read_csv(csv_file)
        city_data = city_data.dropna(subset=["price", "surface"])
        city_name = city_data["city"].iloc[0]
        
        #eliminate outliers
        city_data = city_data[city_data["price_per_sqm"] < city_data["price_per_sqm"].quantile(0.99)]
                
        macrozone_data = city_data.groupby("macrozone").agg(
            {
                "price": ["mean", "median", "std", "min", "max", q50, q90],
                "price_per_sqm": ["mean", "median", "std", "min", "max", q50, q90],
                "surface": ["mean", "median", "std", "min", "max", q50, q90],
            }
        )
        
        for macrozone, data in macrozone_data.iterrows():
            macrozone_summary = macrozone_summary._append(
                {
                    "city_name": city_name,
                    "macrozone_name": macrozone,
                    "price_mean": data["price"]["mean"],
                    "price_median": data["price"]["median"],
                    "price_std": data["price"]["std"],
                    "price_min": data["price"]["min"],
                    "price_max": data["price"]["max"],
                    "price_q50": data["price"]["q50"],
                    "price_q90": data["price"]["q90"],
                    "price_per_sqm_mean": data["price_per_sqm"]["mean"],
                    "price_per_sqm_median": data["price_per_sqm"]["median"],
                    "price_per_sqm_std": data["price_per_sqm"]["std"],
                    "price_per_sqm_min": data["price_per_sqm"]["min"],
                    "price_per_sqm_max": data["price_per_sqm"]["max"],
                    "price_per_sqm_q50": data["price_per_sqm"]["q50"],
                    "price_per_sqm_q90": data["price_per_sqm"]["q90"],
                    "surface_mean": data["surface"]["mean"],
                    "surface_median": data["surface"]["median"],
                    "surface_std": data["surface"]["std"],
                    "surface_min": data["surface"]["min"],
                    "surface_max": data["surface"]["max"],
                    "surface_q50": data["surface"]["q50"],
                    "surface_q90": data["surface"]["q90"],
                },
                ignore_index=True,
            )

    macrozone_summary = macrozone_summary.round(2)        
    macrozone_summary.to_csv(f"./listings/{time.strftime('%y%m%d')}/out/summary_table.csv", index=False)
    
    return macrozone_summary    
        
        
#def compile_daily_tables(indexes: list) -> None:
#    json_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/json/")
#    save_path = pathlib.Path(f"./listings/{time.strftime('%y%m%d')}/csv/")
#    save_path.mkdir(parents=True, exist_ok=True)
#    # select only the json files that have the same city
#    json_files = [
#        path
#        for path in json_path.glob("*.json")
#        if path.stem.split("_")[2] == indexes[0]["city_id"]
#    ]
#    for json_file in tqdm.tqdm(json_files, desc="Compiling daily tables"):
#        with open(json_file, "r") as f:
#            data = json.load(f)
#            results = data.get("results", [])
#            parsed_results = [parse_result(result) for result in results]
#            df = pd.DataFrame(parsed_results)
#            df.to_csv(save_path / f"{json_file.stem}.csv", index=False)


if __name__ == "__main__":
    macrodata = pd.read_csv("./table_builder/index_table.csv")
    #indexes = build_indexes(macrodata)
    #download_listings(indexes)
    #compile_city_tables()
    compile_macrozone_summary_table()


# def get_row_listings(row: pd.Series, s: requests.Session) -> dict:
#        data = s.get(LISTINGS_ENDPOINT, params=generate_payloads(row, 1))
#        data = data.json()
#        max_pages = data.get("maxPages", 0)
#        property_data = []
#        for page in range(1, max_pages + 1):
#            #use the session to keep the cookies
#            data = s.get(LISTINGS_ENDPOINT, params=generate_payloads(row, page))
#            #print(data.url)
#            data = data.json()
#            results = data.get("results", [])
#            for result in results:
#                id = result.get("realEstate").get("id", None)
#                city = result.get("realEstate").get("properties", [])[0].get("location", None).get("city", None)
#                macrozone = result.get("realEstate").get("properties", [])[0].get("location", None).get("macrozone", None)
#                neighbourhood = result.get("realEstate").get("properties", [])[0].get("location", None).get("microzone", None)
#                price = result.get("realEstate").get("price", None).get("value", None)
#                surface = result.get("realEstate").get("properties", [])[0].get("surface", None)
#                rooms = result.get("realEstate").get("properties", [])[0].get("rooms", None)
#                floor = result.get("realEstate").get("properties", [])[0].get("floor", {}).get("abbreviation", None)
#                type = result.get("realEstate").get("properties", [])[0].get("typology", None).get("name", None)
#
#                # Clean data
#                surface = int(re.search(r"-?\d+(\.\d+)?", str(surface)).group().replace('.', '')) if surface else None
#
#                rooms = int(re.search(r"-?\d+(\.\d+)?", str(rooms)).group()) if rooms else None
#                # Not fond of this solution. Sometimes floor is maked as a letter like T or S (which is ground floor and basement respectively)
#                # I'm just going to assume that if it's a letter it's either T or S and set it to 0, but this is not a good solution.
#                #try:
#                #    floor = int(re.search(r"-?\d+(\.\d+)?", str(floor)).group())
#                #except AttributeError:
#                #    floor = 0
#                price_per_sqm = price / surface if price and surface else None
#                property_data.append({
#                    "id": id,
#                    "city": city,
#                    "macrozone": macrozone,
#                    "neighbourhood": neighbourhood,
#                    "price": price,
#                    "price_per_sqm": price_per_sqm,
#                    "surface": surface,
#                    "rooms": rooms,
#                    "floor": floor,
#                    "type": type
#                })
#            #turn property_data into a dataframe and save it
#        #df = pd.DataFrame(property_data)
#        #df.to_csv(f"{row['city_name']}_{row['macrozone_name']}_{row['neighbourhood_name']}_page_{page}.csv", index=False)
#        return property_data
#
#


# build a list of indexes to send the requests, it should be [city_id, macrozone_id, neighbourhood_id, page_num]


# data = []
##create a session to keep the cookies
# with requests.Session() as s:
#    for i, row in macrozone_df.iterrows():
#        print(f"Getting listings for {row['city_name']} {row['macrozone_name']} {row['neighbourhood_name']}\t{i+1}/{len(macrozone_df)}, {len(data)+1} listings")
#        listings = get_row_listings(row, s)
#        data.extend(listings)
# return data
